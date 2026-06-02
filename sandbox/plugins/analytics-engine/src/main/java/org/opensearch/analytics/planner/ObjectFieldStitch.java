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
 *       {@link ObjectType} columns. Capture each parent's descriptor in {@code objectTypes}
 *       so the topmost-Project rewriter can later expand it into leaves.</li>
 *   <li><b>Remap.</b> Walk upstream and remap every {@link RexInputRef} index through the
 *       post-strip name → index map. Drop intermediate-Project items that pass through a
 *       stripped column. {@link LogicalSort}'s {@link
 *       org.apache.calcite.rel.RelCollation} is remapped explicitly because RexShuttle does
 *       not sweep collation field indices.</li>
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
 * <h2>What we don't support</h2>
 *
 * <p>Filters, aggregates, sorts, and computed expressions over an {@link ObjectType} fail
 * fast with a clear error — there's no defined semantic for computing on an opaque map
 * placeholder. Multi-input operators ({@link Join}, {@link SetOp}) with ObjectType columns
 * crossing the boundary likewise reject up-front; the walker is single-input only.
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
        return Optional.of(new Rewrite(w.visit(root), new Stitch(w.outputs)));
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

    /** Topmost {@link LogicalProject} reachable through the unique-input chain, or {@code null}. */
    private static RelNode findTopProject(RelNode root) {
        for (RelNode c = root; c != null && c.getInputs().size() <= 1; c = c.getInputs().isEmpty() ? null : c.getInput(0)) {
            if (c instanceof LogicalProject) return c;
        }
        return null;
    }

    /** Bottom-up rewrite walker. Mutable: collects leaf indices, object types, and stitch outputs. */
    private static final class Rewriter {
        private final RexBuilder rex;
        private final RelNode topProject;
        // Leaf column name → index in the post-strip scan row type.
        private final Map<String, Integer> leafIndex = new LinkedHashMap<>();
        // Stripped object-parent column name → its captured ObjectType descriptor.
        private final Map<String, ObjectType> objectTypes = new LinkedHashMap<>();
        // Stitch outputs from the top Project rewrite.
        List<Stitch.Output> outputs = List.of();

        Rewriter(RexBuilder rex, RelNode topProject) {
            this.rex = rex;
            this.topProject = topProject;
        }

        RelNode visit(RelNode node) {
            if (node instanceof LogicalTableScan scan) return rewriteScan(scan);
            // Multi-input operators are out of scope: the walker only carries a single-input
            // index remap, and an ObjectType column crossing a join/union has no defined
            // expansion. Reject up-front rather than producing wrong results.
            if (node instanceof Join || node instanceof SetOp) {
                throw new IllegalStateException(
                    "Multi-input operators (" + node.getRelTypeName() + ") with ObjectType columns are not supported"
                );
            }
            List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
            for (RelNode in : node.getInputs()) newInputs.add(visit(in));

            RelNode newInput = newInputs.get(0);
            int[] remap = IndexRemap.byName(node.getInput(0).getRowType(), newInput.getRowType());

            if (node == topProject && node instanceof LogicalProject p) return rewriteTopProject(p, newInput, remap);
            if (node instanceof LogicalProject p) return rewriteIntermediate(p, newInput, remap);
            if (node instanceof LogicalSort s) return rewriteSort(s, newInput, remap);
            // Filter / Aggregate / etc.: rebuild and sweep RexInputRefs through the remap.
            // RexShuttle.apply does NOT touch RelCollation field indices — Sort handled above.
            return node.copy(node.getTraitSet(), newInputs).accept(IndexRemap.shuttle(remap, newInput.getRowType(), rex));
        }

        /** Drop ObjectType columns from the scan; capture each parent's ObjectType descriptor. */
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
        private RelNode rewriteIntermediate(LogicalProject p, RelNode newInput, int[] remap) {
            RexShuttle shuttle = IndexRemap.shuttle(remap, newInput.getRowType(), rex);
            List<RexNode> exprs = new ArrayList<>();
            List<String> names = new ArrayList<>();
            for (int i = 0; i < p.getProjects().size(); i++) {
                RexNode e = p.getProjects().get(i);
                if (e instanceof RexInputRef ref && remap[ref.getIndex()] < 0) continue;
                exprs.add(e.accept(shuttle));
                names.add(p.getRowType().getFieldList().get(i).getName());
            }
            return LogicalProject.create(newInput, p.getHints(), exprs, names, p.getVariablesSet());
        }

        /**
         * {@link LogicalSort}'s {@link org.apache.calcite.rel.RelCollation} stores field
         * indices outside RexNodes, so RexShuttle doesn't touch them. Remap explicitly.
         */
        private RelNode rewriteSort(LogicalSort sort, RelNode newInput, int[] remap) {
            List<RelFieldCollation> remapped = new ArrayList<>(sort.collation.getFieldCollations().size());
            for (RelFieldCollation fc : sort.collation.getFieldCollations()) {
                int newIdx = remap[fc.getFieldIndex()];
                if (newIdx < 0) {
                    throw new IllegalStateException("Sort references stripped object-parent column; ObjectType cannot be sorted on");
                }
                remapped.add(fc.withFieldIndex(newIdx));
            }
            return LogicalSort.create(newInput, RelCollations.of(remapped), sort.offset, sort.fetch);
        }

        /** Top Project: expand ObjectType refs into leaf projections + Stitch outputs. */
        private RelNode rewriteTopProject(LogicalProject p, RelNode newInput, int[] remap) {
            RexShuttle shuttle = IndexRemap.shuttle(remap, newInput.getRowType(), rex);
            List<RelDataTypeField> origFields = p.getInput().getRowType().getFieldList();
            List<RexNode> exprs = new ArrayList<>();
            List<String> names = new ArrayList<>();
            List<Stitch.Output> stitched = new ArrayList<>();

            for (int i = 0; i < p.getProjects().size(); i++) {
                RexNode expr = p.getProjects().get(i);
                String outName = p.getRowType().getFieldList().get(i).getName();
                if (expr instanceof RexInputRef ref && objectTypes.containsKey(origFields.get(ref.getIndex()).getName())) {
                    ObjectType ot = objectTypes.get(origFields.get(ref.getIndex()).getName());
                    stitched.add(new Stitch.Output.ObjectMap(outName, expandObject(ot, exprs, names, outName, newInput.getRowType())));
                } else {
                    int idx = exprs.size();
                    exprs.add(expr.accept(shuttle));
                    names.add(outName);
                    stitched.add(new Stitch.Output.Passthrough(outName, idx));
                }
            }
            this.outputs = stitched;
            return LogicalProject.create(newInput, p.getHints(), exprs, names, p.getVariablesSet());
        }

        /**
         * Recursive: each child of an {@link ObjectType} is either a leaf engine column
         * (returning a {@link Stitch.MapSource.Leaf}) or a nested object (recurse). Leaf
         * projections are appended to the engine plan's project list as we go.
         */
        private Map<String, Stitch.MapSource> expandObject(ObjectType ot, List<RexNode> exprs, List<String> names, String namePrefix, RelDataType inRowType) {
            Map<String, Stitch.MapSource> children = new LinkedHashMap<>();
            for (Map.Entry<String, ObjectType.Child> e : ot.children().entrySet()) {
                if (e.getValue() instanceof ObjectType.Child.Leaf leaf) {
                    Integer leafIdx = leafIndex.get(leaf.path());
                    if (leafIdx == null) throw new IllegalStateException("ObjectType references missing leaf [" + leaf.path() + "]");
                    int outIdx = exprs.size();
                    exprs.add(rex.makeInputRef(inRowType.getFieldList().get(leafIdx).getType(), leafIdx));
                    names.add("__stitch_" + namePrefix + "_" + leaf.path());
                    children.put(e.getKey(), new Stitch.MapSource.Leaf(outIdx));
                } else {
                    ObjectType nestedType = ((ObjectType.Child.Nested) e.getValue()).type();
                    children.put(e.getKey(), new Stitch.MapSource.Nested(expandObject(nestedType, exprs, names, namePrefix + "." + e.getKey(), inRowType)));
                }
            }
            return children;
        }
    }
}
