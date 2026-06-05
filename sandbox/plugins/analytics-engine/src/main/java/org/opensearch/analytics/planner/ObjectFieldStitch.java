/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.opensearch.analytics.schema.ObjectType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Lowers PPL {@code | fields parent.object} queries onto a leaf-only physical plan plus a
 * coordinator-side row stitch.
 *
 * <h2>Why this exists</h2>
 *
 * <p>{@code OpenSearchSchemaBuilder} surfaces every object-parent (e.g. {@code city} when
 * the mapping has a {@code city} object with sub-fields) as a synthetic {@link ObjectType}
 * column alongside the flat dotted leaves ({@code city.name}, {@code city.location.lat}, …).
 * The synthetic column is what lets PPL's name resolver validate {@code | fields city} —
 * but the storage backend can't read it. We have to remove it from the plan before
 * DataFusion / Substrait emission while preserving the user's intent that the output column
 * be a nested {@code Map<String,Object>}.
 *
 * <h2>What we do</h2>
 *
 * <p>Three ordered steps, all on the way IN to {@link
 * org.opensearch.analytics.planner.PlannerImpl}:
 * <ol>
 *   <li><b>Strip.</b> Rebuild each {@link TableScan} with a row type that excludes
 *       {@link ObjectType} columns. Capture each parent's descriptor so the topmost-Project
 *       rewriter can later expand it into leaves.</li>
 *   <li><b>Remap.</b> Walk upstream and remap every {@link RexInputRef} index through the
 *       post-strip name → index map. Drop intermediate-Project items that pass through a
 *       stripped column. {@link LogicalSort}'s {@link
 *       org.apache.calcite.rel.RelCollation} is remapped explicitly because RexShuttle does
 *       not sweep collation field indices. {@link LogicalFilter}'s condition is rewritten
 *       before the new Filter is constructed (Calcite's RexChecker would otherwise reject
 *       the unrewritten condition).</li>
 *   <li><b>Expand.</b> At the topmost {@link LogicalProject}, replace each ObjectType
 *       reference with projections of the underlying leaves, and emit a {@link
 *       Stitch.Output.ObjectMap} that reassembles them into a nested Map at row time.</li>
 * </ol>
 *
 * <p>The walker is needed because Calcite's HEP {@code transformTo} enforces row-type
 * equivalence between the matched node and its replacement — we can't strip ObjectType
 * columns from a {@code LogicalTableScan} INSIDE a planner rule, only before. Once we
 * strip the scan pre-planning, every operator above with a positional {@link RexInputRef}
 * has to be remapped.
 *
 * <h2>Operators with leaf-level expansion</h2>
 * <p>Two operator shapes have a defined semantic over an object parent and are expanded
 * into leaf-level equivalents by {@link IndexRemap#shuttle}:
 * <ul>
 *   <li>{@code | dedup parent} → {@code ROW_NUMBER OVER (PARTITION BY parent)}: partition
 *       keys are expanded to the parent's full leaf list (tuple equality).</li>
 *   <li>{@code | where isnotnull(parent)} / {@code isnull(parent)}: expand to OR/AND of
 *       the same predicate over each leaf ({@code _exists_:parent} semantic).</li>
 * </ul>
 *
 * <h2>What we don't support</h2>
 * <p>Sorts, scalar comparisons, aggregates, and arithmetic over an {@link ObjectType} fail
 * fast — there's no defined semantic for ordering or computing on an opaque map placeholder.
 * Multi-input operators ({@link Join}, {@link SetOp}) with ObjectType columns crossing the
 * boundary likewise reject up-front; the walker is single-input only.
 *
 * @opensearch.internal
 */
public final class ObjectFieldStitch {

    private ObjectFieldStitch() {}

    /** Engine-safe plan plus the row reshape (or empty {@link Stitch} if no reshape needed). */
    public record Rewrite(RelNode plan, Stitch stitch) {}

    /**
     * Returns a {@link Rewrite} when the plan contains any {@link ObjectType} columns,
     * {@link Optional#empty()} otherwise — caller plans the input as-is.
     */
    public static Optional<Rewrite> maybeRewrite(RelNode root) {
        if (!hasObjectTypeColumns(root)) return Optional.empty();
        Rewriter w = new Rewriter(root.getCluster().getRexBuilder(), findTopProject(root));
        RelNode rewritten = w.visit(root);
        // If no top-Project rewrite ran (e.g., the scan has ObjectType columns but the
        // query never projects a parent), produce a passthrough Stitch matching the engine
        // plan's row type. Without this fallback an empty outputs list yields zero-column
        // result rows for queries like `| stats count()` over tables with object schemas.
        List<Stitch.Output> outputs = w.outputs.isEmpty() ? passthroughStitch(rewritten) : w.outputs;
        return Optional.of(new Rewrite(rewritten, new Stitch(outputs)));
    }

    /** Build a passthrough Stitch matching {@code plan}'s row type 1:1. */
    private static List<Stitch.Output> passthroughStitch(RelNode plan) {
        List<Stitch.Output> outs = new ArrayList<>(plan.getRowType().getFieldCount());
        for (int i = 0; i < plan.getRowType().getFieldCount(); i++) {
            outs.add(new Stitch.Output.Passthrough(plan.getRowType().getFieldList().get(i).getName(), i));
        }
        return outs;
    }

    private static boolean hasObjectTypeColumns(RelNode root) {
        boolean[] hit = new boolean[1];
        new RelVisitor() {
            @Override
            public void visit(RelNode n, int ord, RelNode p) {
                if (hit[0]) return;
                if (n instanceof TableScan s) {
                    for (RelDataTypeField f : s.getRowType().getFieldList()) {
                        if (f.getType() instanceof ObjectType) {
                            hit[0] = true;
                            return;
                        }
                    }
                }
                super.visit(n, ord, p);
            }
        }.go(root);
        return hit[0];
    }

    /**
     * The output-shaping Project — the one whose column list is what the user sees as the
     * query result. Walks the unique-input chain from the root and returns the first
     * {@link LogicalProject}, but only if no {@link org.apache.calcite.rel.core.Aggregate}
     * sits between the root and that Project. The Project beneath an Aggregate is an
     * internal SQL-plugin artifact (e.g. the ROW_NUMBER projection emitted by {@code |
     * dedup}) — applying parent expansion there would emit dozens of {@code __stitch_*}
     * leaf passthroughs that the Aggregate doesn't carry, breaking the Stitch row indexing.
     */
    private static RelNode findTopProject(RelNode root) {
        for (RelNode c = root; c != null && c.getInputs().size() <= 1; c = c.getInputs().isEmpty() ? null : c.getInput(0)) {
            if (c instanceof org.apache.calcite.rel.core.Aggregate) return null;
            if (c instanceof LogicalProject) return c;
        }
        return null;
    }

    /** Bottom-up rewrite walker. Mutable: collects leaf indices, object types, and stitch outputs. */
    private static final class Rewriter {

        private final RexBuilder rex;
        private final RelNode topProject;
        /** Leaf column name → index in the post-strip scan row type. */
        private final Map<String, Integer> leafIndex = new LinkedHashMap<>();
        /** Stripped object-parent column name → its captured ObjectType descriptor. */
        private final Map<String, ObjectType> objectTypes = new LinkedHashMap<>();
        /** Stitch outputs from the top Project rewrite. */
        List<Stitch.Output> outputs = List.of();

        Rewriter(RexBuilder rex, RelNode topProject) {
            this.rex = rex;
            this.topProject = topProject;
        }

        // ── Walker entry point ────────────────────────────────────────────────────────

        RelNode visit(RelNode node) {
            if (node instanceof LogicalTableScan scan) return rewriteScan(scan);
            rejectIfMultiInput(node);

            List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
            for (RelNode in : node.getInputs()) newInputs.add(visit(in));
            RelNode newInput = newInputs.get(0);

            RemapContext ctx = remapContext(node.getInput(0).getRowType(), newInput.getRowType());

            if (node == topProject && node instanceof LogicalProject p) return rewriteTopProject(p, newInput, ctx);
            if (node instanceof LogicalProject p) return rewriteIntermediateProject(p, newInput, ctx);
            if (node instanceof LogicalSort s) return rewriteSort(s, newInput, ctx);
            if (node instanceof LogicalFilter f) return rewriteFilter(f, newInput, ctx);
            // Other operators: rebuild and sweep RexInputRefs through the shuttle.
            return node.copy(node.getTraitSet(), newInputs).accept(ctx.shuttle());
        }

        private void rejectIfMultiInput(RelNode node) {
            if (node instanceof Join || node instanceof SetOp) {
                throw new IllegalStateException(
                    "Multi-input operators (" + node.getRelTypeName() + ") with ObjectType columns are not supported"
                );
            }
        }

        // ── Per-operator rewrites ─────────────────────────────────────────────────────

        /** Drop ObjectType columns from the scan; capture each parent's descriptor for later expansion. */
        private RelNode rewriteScan(LogicalTableScan scan) {
            RelOptTable origTable = scan.getTable();
            List<RelDataTypeField> leaves = new ArrayList<>();
            boolean stripped = false;
            for (RelDataTypeField f : origTable.getRowType().getFieldList()) {
                if (f.getType() instanceof ObjectType ot) {
                    objectTypes.put(f.getName(), ot);
                    stripped = true;
                } else {
                    leafIndex.put(f.getName(), leaves.size());
                    leaves.add(new RelDataTypeFieldImpl(f.getName(), leaves.size(), f.getType()));
                }
            }
            if (!stripped) return scan;
            RelOptTable t = new RelOptAbstractTable(origTable.getRelOptSchema(), origTable.getQualifiedName().getLast(), new RelRecordType(leaves)) {};
            return LogicalTableScan.create(scan.getCluster(), t, scan.getHints());
        }

        /** Drop project items that pass through a stripped column; remap survivors. */
        private RelNode rewriteIntermediateProject(LogicalProject p, RelNode newInput, RemapContext ctx) {
            List<RexNode> exprs = new ArrayList<>();
            List<String> names = new ArrayList<>();
            for (int i = 0; i < p.getProjects().size(); i++) {
                RexNode e = p.getProjects().get(i);
                if (e instanceof RexInputRef ref && ctx.isDropped(ref.getIndex())) continue;
                exprs.add(e.accept(ctx.shuttle()));
                names.add(p.getRowType().getFieldList().get(i).getName());
            }
            return LogicalProject.create(newInput, p.getHints(), exprs, names, p.getVariablesSet());
        }

        /**
         * {@link LogicalSort}'s {@link org.apache.calcite.rel.RelCollation} stores field
         * indices outside RexNodes, so RexShuttle doesn't touch them. Remap explicitly.
         */
        private RelNode rewriteSort(LogicalSort sort, RelNode newInput, RemapContext ctx) {
            List<RelFieldCollation> remapped = new ArrayList<>(sort.collation.getFieldCollations().size());
            for (RelFieldCollation fc : sort.collation.getFieldCollations()) {
                if (ctx.isDropped(fc.getFieldIndex())) {
                    throw new IllegalStateException("Sort references stripped object-parent column; ObjectType cannot be sorted on");
                }
                remapped.add(fc.withFieldIndex(ctx.newIndex(fc.getFieldIndex())));
            }
            return LogicalSort.create(newInput, RelCollations.of(remapped), sort.offset, sort.fetch);
        }

        /**
         * Apply the shuttle to the Filter's condition BEFORE constructing the new Filter.
         * Calcite's {@link org.apache.calcite.rel.core.Filter} constructor runs RexChecker
         * on the condition, which would reject any input ref to a stripped column even
         * though our shuttle would have expanded {@code IS [NOT] NULL} into a leaf-level
         * predicate.
         */
        private RelNode rewriteFilter(LogicalFilter filter, RelNode newInput, RemapContext ctx) {
            RexNode newCondition = filter.getCondition().accept(ctx.shuttle());
            return LogicalFilter.create(newInput, newCondition);
        }

        /** Top Project: expand each ObjectType ref into leaf projections + a Stitch output. */
        private RelNode rewriteTopProject(LogicalProject p, RelNode newInput, RemapContext ctx) {
            List<RelDataTypeField> origFields = p.getInput().getRowType().getFieldList();
            List<RexNode> exprs = new ArrayList<>();
            List<String> names = new ArrayList<>();
            List<Stitch.Output> stitched = new ArrayList<>();

            for (int i = 0; i < p.getProjects().size(); i++) {
                RexNode expr = p.getProjects().get(i);
                String outName = p.getRowType().getFieldList().get(i).getName();
                ObjectType refParentType = parentObjectTypeIfRef(expr, origFields);
                if (refParentType != null) {
                    stitched.add(new Stitch.Output.ObjectMap(outName, expandObject(refParentType, exprs, names, outName, newInput.getRowType())));
                } else {
                    int idx = exprs.size();
                    exprs.add(expr.accept(ctx.shuttle()));
                    names.add(outName);
                    stitched.add(new Stitch.Output.Passthrough(outName, idx));
                }
            }
            this.outputs = stitched;
            return LogicalProject.create(newInput, p.getHints(), exprs, names, p.getVariablesSet());
        }

        /** Returns the captured ObjectType when {@code expr} is a passthrough InputRef to a parent column; else {@code null}. */
        private ObjectType parentObjectTypeIfRef(RexNode expr, List<RelDataTypeField> origFields) {
            if (!(expr instanceof RexInputRef ref)) return null;
            return objectTypes.get(origFields.get(ref.getIndex()).getName());
        }

        /**
         * Recursive: each child of an {@link ObjectType} is either a leaf engine column
         * (returning a {@link Stitch.MapSource.Leaf}) or a nested object (recurse). Leaf
         * projections are appended to the engine plan's project list as we go.
         */
        private Map<String, Stitch.MapSource> expandObject(ObjectType ot, List<RexNode> exprs, List<String> names, String namePrefix, RelDataType newRowType) {
            Map<String, Stitch.MapSource> children = new LinkedHashMap<>();
            for (Map.Entry<String, ObjectType.Child> e : ot.children().entrySet()) {
                if (e.getValue() instanceof ObjectType.Child.Leaf leaf) {
                    int leafIdx = leafIndexOrThrow(leaf.path());
                    int outIdx = exprs.size();
                    exprs.add(rex.makeInputRef(newRowType.getFieldList().get(leafIdx).getType(), leafIdx));
                    names.add("__stitch_" + namePrefix + "_" + leaf.path());
                    children.put(e.getKey(), new Stitch.MapSource.Leaf(outIdx));
                } else {
                    ObjectType nested = ((ObjectType.Child.Nested) e.getValue()).type();
                    children.put(e.getKey(), new Stitch.MapSource.Nested(expandObject(nested, exprs, names, namePrefix + "." + e.getKey(), newRowType)));
                }
            }
            return children;
        }

        // ── Remap context helper ──────────────────────────────────────────────────────

        /** Build the per-step remap context (index map + leaf-flatten map + lazy shuttle). */
        private RemapContext remapContext(RelDataType oldType, RelDataType newType) {
            int[] remap = IndexRemap.byName(oldType, newType);
            Map<Integer, List<Integer>> droppedToLeaves = leafIndicesForDropped(oldType, remap);
            return new RemapContext(remap, droppedToLeaves, newType, rex);
        }

        /** For each dropped old column that names a known parent, the ordered transitive leaf-index list in the new row. */
        private Map<Integer, List<Integer>> leafIndicesForDropped(RelDataType oldType, int[] remap) {
            Map<Integer, List<Integer>> out = new LinkedHashMap<>();
            for (int i = 0; i < remap.length; i++) {
                if (remap[i] >= 0) continue;
                ObjectType ot = objectTypes.get(oldType.getFieldList().get(i).getName());
                if (ot == null) continue;
                List<Integer> leaves = new ArrayList<>();
                collectLeafIndices(ot, leaves);
                out.put(i, leaves);
            }
            return out;
        }

        private void collectLeafIndices(ObjectType ot, List<Integer> leaves) {
            for (ObjectType.Child child : ot.children().values()) {
                if (child instanceof ObjectType.Child.Leaf leaf) {
                    leaves.add(leafIndexOrThrow(leaf.path()));
                } else {
                    collectLeafIndices(((ObjectType.Child.Nested) child).type(), leaves);
                }
            }
        }

        private int leafIndexOrThrow(String path) {
            Integer idx = leafIndex.get(path);
            if (idx == null) throw new IllegalStateException("ObjectType references missing leaf [" + path + "]");
            return idx;
        }

        /** Bundles the per-step remap data so individual rewrites only ask what they need. */
        private record RemapContext(int[] remap, Map<Integer, List<Integer>> droppedToLeaves, RelDataType newRowType, RexBuilder rex) {

            boolean isDropped(int oldIdx) {
                return remap[oldIdx] < 0;
            }

            int newIndex(int oldIdx) {
                return remap[oldIdx];
            }

            RexShuttle shuttle() {
                return IndexRemap.shuttle(remap, droppedToLeaves, newRowType, rex);
            }
        }
    }
}
