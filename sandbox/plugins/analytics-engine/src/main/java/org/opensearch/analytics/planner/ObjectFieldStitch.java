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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
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

/**
 * Rewrites a logical plan that references {@link ObjectType} parent columns into one the
 * storage engine can run, plus a coordinator-side stitch description.
 *
 * <p>The {@code OpenSearchSchemaBuilder} surfaces each object-parent as a synthetic
 * {@link ObjectType} column alongside the flat dotted leaves. This rewriter:
 * <ol>
 *   <li>Strips every {@link ObjectType} column from each {@link TableScan} in the tree.</li>
 *   <li>Remaps {@link RexInputRef} indices in every upstream operator so positions stay
 *       valid against the post-strip scan row type.</li>
 *   <li>Detects the topmost {@link LogicalProject} that references {@link ObjectType}
 *       columns and replaces those references with leaf-column projections, capturing a
 *       {@link StitchPlan} that re-assembles the leaves into a nested
 *       {@code Map<String,Object>} on the coordinator side.</li>
 * </ol>
 *
 * <p>Scope: this rewriter only EXPANDS ObjectType references at the topmost projection. Any
 * ObjectType reference in a filter, aggregate, or eval upstream of that projection survives
 * as a typed-NULL value because the synthetic Project above the scan replaces ObjectType
 * columns with NULL placeholders. So a {@code | where parent.object IS NULL} would silently
 * be true; a {@code | stats min(parent.object)} would aggregate NULLs. We don't support those
 * semantically — the schema only exposes the parent as an opaque MAP marker, and pushing it
 * into a predicate has no defined meaning without query-then-fetch.
 *
 * @opensearch.internal
 */
public final class ObjectFieldStitch {

    private ObjectFieldStitch() {}

    /** Description of a single output column produced by the rewritten plan. */
    public sealed interface OutputColumn permits OutputColumn.Passthrough, OutputColumn.Stitch {

        /** User-visible column name. */
        String name();

        /** Pass through one column from the engine's rewritten output unchanged. */
        record Passthrough(String name, int engineColumnIndex) implements OutputColumn {}

        /**
         * Build a nested {@code Map<String,Object>} from a set of engine columns according
         * to a recursive child structure.
         */
        record Stitch(String name, Map<String, ChildSource> children) implements OutputColumn {}

        /** Source for a child of a {@link Stitch}. */
        sealed interface ChildSource permits ChildSource.LeafColumn, ChildSource.NestedStitch {

            /** Pull the value from a column produced by the rewritten plan. */
            record LeafColumn(int engineColumnIndex) implements ChildSource {}

            /** Build a child {@code Map<String,Object>} from a nested {@link Stitch}. */
            record NestedStitch(Stitch stitch) implements ChildSource {}
        }
    }

    /**
     * Result of rewriting a plan: the engine-safe plan, and the description of how to
     * reassemble its output rows into the user-requested column shape.
     */
    public record StitchPlan(RelNode rewrittenPlan, List<OutputColumn> outputs) {

        /** True when the plan needs a coordinator-side stitch step. */
        public boolean needsStitch() {
            for (OutputColumn col : outputs) {
                if (col instanceof OutputColumn.Stitch) return true;
            }
            return false;
        }

        /** User-visible column names, in order. */
        public List<String> outputNames() {
            List<String> names = new ArrayList<>(outputs.size());
            for (OutputColumn col : outputs) names.add(col.name());
            return names;
        }
    }

    /**
     * Rewrite a plan whose top-level Project may select {@link ObjectType} columns. If no
     * ObjectType references are present at the topmost Project — including the no-Project
     * case — the StitchPlan's outputs are all passthroughs and the rewritten plan is the
     * input plan with ObjectType columns stripped from its TableScans (a no-op when none
     * are present, e.g. flat-leaf-only schemas).
     */
    public static StitchPlan rewrite(RelNode root) {
        RewriteContext ctx = new RewriteContext(root.getCluster().getRexBuilder(), findTopProject(root));
        RelNode rewritten = rewriteNode(root, ctx);
        return new StitchPlan(rewritten, ctx.outputs);
    }

    /**
     * Apply the {@link StitchPlan} to a single engine-output row. Returns a new row whose
     * length / column order matches {@link StitchPlan#outputs()}.
     */
    public static Object[] stitchRow(Object[] engineRow, List<OutputColumn> outputs) {
        Object[] out = new Object[outputs.size()];
        for (int i = 0; i < outputs.size(); i++) {
            OutputColumn col = outputs.get(i);
            if (col instanceof OutputColumn.Passthrough p) {
                out[i] = engineRow[p.engineColumnIndex()];
            } else {
                out[i] = buildMap(((OutputColumn.Stitch) col).children(), engineRow);
            }
        }
        return out;
    }

    private static Map<String, Object> buildMap(Map<String, OutputColumn.ChildSource> children, Object[] engineRow) {
        Map<String, Object> result = new LinkedHashMap<>(children.size());
        for (Map.Entry<String, OutputColumn.ChildSource> entry : children.entrySet()) {
            OutputColumn.ChildSource src = entry.getValue();
            Object value;
            if (src instanceof OutputColumn.ChildSource.LeafColumn leaf) {
                value = engineRow[leaf.engineColumnIndex()];
            } else {
                value = buildMap(((OutputColumn.ChildSource.NestedStitch) src).stitch().children(), engineRow);
            }
            result.put(entry.getKey(), value);
        }
        return result;
    }

    /**
     * Returns the topmost {@link LogicalProject} in {@code root}. ObjectType references can
     * only appear at the user-visible projection that the SQL plugin emits at the top of the
     * tree (the {@code | fields ...} step). Wrapping operators like Sort/SystemLimit are
     * uninteresting — the topmost LogicalProject is the one we may need to expand.
     */
    private static RelNode findTopProject(RelNode root) {
        RelNode current = root;
        while (current != null) {
            if (current instanceof LogicalProject) {
                return current;
            }
            if (current.getInputs().size() != 1) {
                return null;
            }
            current = current.getInput(0);
        }
        return null;
    }

    /** Per-rewrite mutable state: leaf-name → engine column index map and the captured outputs. */
    private static final class RewriteContext {
        final RexBuilder rexBuilder;
        // Field name → index in the row type of the synthetic Project we insert just above the
        // stripped TableScan. The synthetic Project re-introduces every ObjectType column as a
        // typed-NULL placeholder so upstream operators see the row type they were validated
        // against. Index positions match the original schema; consequently the leafIndex map's
        // size and ordering also matches the original.
        final Map<String, Integer> leafIndex = new LinkedHashMap<>();
        // Names of columns whose value is a typed-NULL placeholder (the ObjectType parents).
        // Used by the topmost-Project rewriter to know when an output expression is a
        // RexInputRef pointing at a synthetic ObjectType column that needs to be expanded into
        // a Stitch over leaves rather than passed through.
        final java.util.Set<String> objectColumns = new java.util.HashSet<>();
        // Column-name → original {@link ObjectType} captured during scan-strip. Calcite may
        // canonize the ObjectType subclass into a plain MapSqlType inside
        // {@code RexBuilder.makeNullLiteral}, so the synthetic Project's row type loses the
        // subclass identity. We keep the descriptor here so the topmost Project's rewriter
        // can still expand the column into the right child structure.
        final Map<String, ObjectType> objectTypeByName = new LinkedHashMap<>();
        List<OutputColumn> outputs = new ArrayList<>();
        // The topmost LogicalProject in the tree (or null if the plan has no Project at all).
        // Identity-matched so we know exactly when to expand ObjectType references vs. just
        // pass through.
        final RelNode topProject;

        RewriteContext(RexBuilder rexBuilder, RelNode topProject) {
            this.rexBuilder = rexBuilder;
            this.topProject = topProject;
        }
    }

    /**
     * Bottom-up rewrite. Returns the rewritten subtree rooted at {@code node} along with the
     * shape of the column-index map between the original input and the rewritten input.
     */
    private static RelNode rewriteNode(RelNode node, RewriteContext ctx) {
        if (node instanceof LogicalTableScan scan) {
            return rewriteScan(scan, ctx);
        }
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        for (RelNode input : node.getInputs()) {
            newInputs.add(rewriteNode(input, ctx));
        }
        if (node == ctx.topProject && node instanceof LogicalProject project) {
            return rewriteRootProject(project, newInputs.get(0), ctx);
        }

        RelNode newInput = newInputs.get(0);
        RelDataType newInputRowType = newInput.getRowType();
        // Build oldIdx → newIdx for the input. Any old-input field that's been stripped (because
        // it was an ObjectType column the scan removed) maps to -1, which causes the index-
        // remap shuttle to fail fast — non-Project operators with such a reference cannot be
        // safely rewritten because the column they read no longer exists.
        int[] oldToNew = buildIndexMap(node.getInput(0).getRowType(), newInputRowType);

        if (node instanceof LogicalProject project) {
            return rewriteIntermediateProject(project, newInput, oldToNew, ctx);
        }

        // Non-Project operators: rebuild via copy then sweep RexNodes through the index remap.
        RexShuttle remap = new IndexRemapShuttle(oldToNew, newInputRowType, ctx.rexBuilder);
        return node.copy(node.getTraitSet(), newInputs).accept(remap);
    }

    /**
     * Map from old input row type's field name → new input row type's field index. Fields whose
     * old name no longer exists in the new input are mapped to {@code -1} (i.e. stripped).
     */
    private static int[] buildIndexMap(RelDataType oldType, RelDataType newType) {
        List<RelDataTypeField> oldFields = oldType.getFieldList();
        int[] map = new int[oldFields.size()];
        Map<String, Integer> newByName = new LinkedHashMap<>();
        for (RelDataTypeField f : newType.getFieldList()) {
            newByName.put(f.getName(), f.getIndex());
        }
        for (int i = 0; i < oldFields.size(); i++) {
            Integer newIdx = newByName.get(oldFields.get(i).getName());
            map[i] = newIdx == null ? -1 : newIdx;
        }
        return map;
    }

    /**
     * Rewrite an intermediate {@link LogicalProject} (NOT the topmost). Drop each project
     * expression that resolves to a stripped column and remap RexInputRef indices for the
     * survivors. Non-input-ref expressions referencing a stripped column are an error — they
     * imply a filter/eval/agg uses an ObjectType, which is out of scope.
     */
    private static RelNode rewriteIntermediateProject(LogicalProject project, RelNode newInput, int[] oldToNew, RewriteContext ctx) {
        List<RexNode> origProjects = project.getProjects();
        List<RelDataTypeField> origFields = project.getRowType().getFieldList();
        List<RexNode> newProjects = new ArrayList<>();
        List<String> newNames = new ArrayList<>();
        RelDataType newInputRowType = newInput.getRowType();

        for (int i = 0; i < origProjects.size(); i++) {
            RexNode expr = origProjects.get(i);
            String outputName = origFields.get(i).getName();

            if (expr instanceof RexInputRef ref) {
                int newIdx = oldToNew[ref.getIndex()];
                if (newIdx < 0) {
                    // The referenced input column has been stripped (ObjectType). The Project
                    // is just passing it through; drop it from the rebuilt project list — the
                    // topmost-Project rewriter will reconstruct it via Stitch when needed.
                    continue;
                }
                RelDataType refType = newInputRowType.getFieldList().get(newIdx).getType();
                newProjects.add(ctx.rexBuilder.makeInputRef(refType, newIdx));
                newNames.add(outputName);
            } else {
                // Non-ref expression: walk and remap. If any child RexInputRef points at a
                // stripped column, the shuttle throws — we don't have a defined semantic for
                // computing on an opaque ObjectType placeholder.
                RexShuttle remap = new IndexRemapShuttle(oldToNew, newInputRowType, ctx.rexBuilder);
                newProjects.add(expr.accept(remap));
                newNames.add(outputName);
            }
        }

        return LogicalProject.create(newInput, project.getHints(), newProjects, newNames, project.getVariablesSet());
    }

    /** RexShuttle that remaps RexInputRef indices via {@code oldToNew}. */
    private static final class IndexRemapShuttle extends RexShuttle {
        private final int[] oldToNew;
        private final RelDataType newInputRowType;
        private final RexBuilder rexBuilder;

        IndexRemapShuttle(int[] oldToNew, RelDataType newInputRowType, RexBuilder rexBuilder) {
            this.oldToNew = oldToNew;
            this.newInputRowType = newInputRowType;
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexNode visitInputRef(RexInputRef ref) {
            int newIdx = oldToNew[ref.getIndex()];
            if (newIdx < 0) {
                throw new IllegalStateException(
                    "RexInputRef points at a stripped object-parent column; ObjectType cannot appear in filters / aggregates / evals"
                );
            }
            RelDataType expected = newInputRowType.getFieldList().get(newIdx).getType();
            if (newIdx == ref.getIndex() && expected.equals(ref.getType())) {
                return ref;
            }
            return rexBuilder.makeInputRef(expected, newIdx);
        }
    }

    /**
     * Strip every {@link ObjectType} column from the scan. The storage backend never sees
     * them; their position in the row type is simply removed. {@link RewriteContext} captures
     * the {@link ObjectType} descriptor for each stripped column so the topmost-Project
     * rewriter can expand a reference into a {@link OutputColumn.Stitch} over the underlying
     * leaves; intermediate operators have any RexInputRef-to-stripped-ObjectType silently
     * dropped (their projection list shrinks).
     */
    private static RelNode rewriteScan(LogicalTableScan scan, RewriteContext ctx) {
        RelOptTable origTable = scan.getTable();
        RelDataType origRowType = origTable.getRowType();
        List<RelDataTypeField> leafFields = new ArrayList<>();
        boolean hasObjectColumns = false;
        for (RelDataTypeField field : origRowType.getFieldList()) {
            if (field.getType() instanceof ObjectType objectType) {
                hasObjectColumns = true;
                ctx.objectColumns.add(field.getName());
                ctx.objectTypeByName.put(field.getName(), objectType);
                continue;
            }
            int newIdx = leafFields.size();
            leafFields.add(new RelDataTypeFieldImpl(field.getName(), newIdx, field.getType()));
            ctx.leafIndex.put(field.getName(), newIdx);
        }
        if (!hasObjectColumns) {
            return scan;
        }
        RelDataType strippedRowType = new RelRecordType(leafFields);
        RelOptTable strippedTable = new RelOptAbstractTable(
            origTable.getRelOptSchema(),
            origTable.getQualifiedName().getLast(),
            strippedRowType
        ) {};
        return LogicalTableScan.create(scan.getCluster(), strippedTable, scan.getHints());
    }

    /**
     * Rewrite the topmost Project: each output column becomes either a Passthrough (a leaf
     * column the SQL plugin asked for, with its RexInputRef index remapped to the post-strip
     * input), or a Stitch (when the output expression was a RexInputRef to an ObjectType
     * column the scan removed).
     */
    private static RelNode rewriteRootProject(LogicalProject project, RelNode rewrittenInput, RewriteContext ctx) {
        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        List<RexNode> newProjects = new ArrayList<>();
        List<String> newProjectNames = new ArrayList<>();
        List<OutputColumn> outputCols = new ArrayList<>();

        // Identify the per-column origin in the topmost Project's input by name. The input may
        // be the stripped scan (if there are no intermediate operators), or a chain of remapped
        // upstream operators — but every passthrough RexInputRef ultimately resolves to a name
        // in the original schema. We use that name for both the leafIndex lookup (passthroughs)
        // and the objectColumns membership check (Stitch expansion).
        List<RelDataTypeField> inputFields = project.getInput().getRowType().getFieldList();
        int[] oldToNew = buildIndexMap(project.getInput().getRowType(), rewrittenInput.getRowType());

        for (int i = 0; i < project.getProjects().size(); i++) {
            RexNode expr = project.getProjects().get(i);
            String outputName = project.getRowType().getFieldList().get(i).getName();

            if (expr instanceof RexInputRef ref) {
                String inputName = inputFields.get(ref.getIndex()).getName();
                if (ctx.objectColumns.contains(inputName)) {
                    ObjectType objectType = lookupObjectType(inputFields.get(ref.getIndex()).getType(), ctx, inputName);
                    OutputColumn.Stitch stitch = buildStitch(
                        outputName,
                        objectType,
                        ctx.leafIndex,
                        newProjects,
                        newProjectNames,
                        rexBuilder,
                        rewrittenInput
                    );
                    outputCols.add(stitch);
                    continue;
                }
                // Passthrough leaf — remap the index to the post-strip position.
                int newIdx = oldToNew[ref.getIndex()];
                RelDataType refType = rewrittenInput.getRowType().getFieldList().get(newIdx).getType();
                int idx = newProjects.size();
                newProjects.add(rexBuilder.makeInputRef(refType, newIdx));
                newProjectNames.add(outputName);
                outputCols.add(new OutputColumn.Passthrough(outputName, idx));
                continue;
            }

            // Non-RexInputRef expression: walk it through the index-remap shuttle. References to
            // ObjectType columns inside computed expressions throw — only direct RexInputRef
            // selection of an ObjectType is supported.
            RexShuttle remap = new IndexRemapShuttle(oldToNew, rewrittenInput.getRowType(), rexBuilder);
            int idx = newProjects.size();
            newProjects.add(expr.accept(remap));
            newProjectNames.add(outputName);
            outputCols.add(new OutputColumn.Passthrough(outputName, idx));
        }

        ctx.outputs = outputCols;
        return LogicalProject.create(rewrittenInput, project.getHints(), newProjects, newProjectNames, project.getVariablesSet());
    }

    /**
     * Resolve the {@link ObjectType} for an object-parent column. Prefer an exact instance check
     * — when the row type still carries the {@link ObjectType} subclass, the type already has
     * the children map. Otherwise fall back to the cached map keyed on column name (the
     * scan-strip phase recorded the original {@link ObjectType} for each column it stripped).
     */
    private static ObjectType lookupObjectType(RelDataType maybeObjectType, RewriteContext ctx, String columnName) {
        if (maybeObjectType instanceof ObjectType objectType) {
            return objectType;
        }
        ObjectType cached = ctx.objectTypeByName.get(columnName);
        if (cached == null) {
            throw new IllegalStateException(
                "Top-level projection references object column [" + columnName + "] but no ObjectType descriptor is available"
            );
        }
        return cached;
    }

    private static OutputColumn.Stitch buildStitch(
        String outputName,
        ObjectType objectType,
        Map<String, Integer> leafIdx,
        List<RexNode> newProjects,
        List<String> newProjectNames,
        RexBuilder rexBuilder,
        RelNode rewrittenInput
    ) {
        Map<String, OutputColumn.ChildSource> children = new LinkedHashMap<>();
        for (Map.Entry<String, ObjectType.Child> entry : objectType.children().entrySet()) {
            String childName = entry.getKey();
            ObjectType.Child child = entry.getValue();
            if (child instanceof ObjectType.Child.Leaf leaf) {
                Integer idxInRewrittenInput = leafIdx.get(leaf.path());
                if (idxInRewrittenInput == null) {
                    throw new IllegalStateException(
                        "ObjectType references leaf [" + leaf.path() + "] but the rewritten input has no such column"
                    );
                }
                int outIdx = newProjects.size();
                RelDataType leafType = rewrittenInput.getRowType().getFieldList().get(idxInRewrittenInput).getType();
                newProjects.add(rexBuilder.makeInputRef(leafType, idxInRewrittenInput));
                newProjectNames.add("__stitch_" + outputName + "_" + leaf.path());
                children.put(childName, new OutputColumn.ChildSource.LeafColumn(outIdx));
            } else {
                OutputColumn.ChildSource.NestedStitch nested = new OutputColumn.ChildSource.NestedStitch(
                    buildStitch(
                        outputName + "." + childName,
                        ((ObjectType.Child.Nested) child).type(),
                        leafIdx,
                        newProjects,
                        newProjectNames,
                        rexBuilder,
                        rewrittenInput
                    )
                );
                children.put(childName, nested);
            }
        }
        return new OutputColumn.Stitch(outputName, children);
    }

}
