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
import org.apache.calcite.rel.RelVisitor;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Lowers PPL {@code | fields parent.object} queries onto a leaf-only physical plan plus a
 * coordinator-side row stitcher.
 *
 * <p>{@code OpenSearchSchemaBuilder} surfaces every object-parent as a synthetic
 * {@link ObjectType} column alongside flat dotted leaves so PPL's name resolver can validate
 * a bare {@code parent} reference. The storage backend can't read that synthetic column, so
 * before planning we:
 * <ol>
 *   <li>Strip {@link ObjectType} columns from each {@link TableScan}.</li>
 *   <li>Drop upstream RexInputRef-to-stripped passthroughs and remap surviving indices.</li>
 *   <li>At the topmost {@link LogicalProject}, expand each {@link ObjectType} reference into
 *       leaf-column projections plus a {@link Stitch} description that re-assembles the
 *       leaves into a nested {@code Map<String,Object>} on the coordinator.</li>
 * </ol>
 *
 * <p>Only direct projection of an object parent is supported. Filters, aggregates, sorts, and
 * computed expressions over an {@link ObjectType} fail fast — there's no defined semantic for
 * computing on an opaque map placeholder.
 *
 * @opensearch.internal
 */
public final class ObjectFieldStitch {

    private ObjectFieldStitch() {}

    /**
     * Rewrite output: the engine-safe plan plus the row-level stitch description (if any
     * top-level Project actually selects an {@link ObjectType} column).
     */
    public record Rewrite(RelNode plan, Stitch stitch) {}

    /**
     * Coordinator-side row reshape: takes engine-output rows and returns user-visible rows.
     * Each entry in {@code outputs} either passes through a single engine column or stitches
     * a subset of engine columns into a nested {@code Map<String,Object>}.
     */
    public record Stitch(List<Output> outputs) {

        /** Apply the stitch to engine rows. */
        public List<Object[]> apply(Iterable<Object[]> engineRows) {
            List<Object[]> stitched = new ArrayList<>();
            for (Object[] row : engineRows) {
                Object[] out = new Object[outputs.size()];
                for (int i = 0; i < outputs.size(); i++) {
                    out[i] = outputs.get(i).read(row);
                }
                stitched.add(out);
            }
            return stitched;
        }

        /** Output column names, in order. */
        public List<String> names() {
            List<String> names = new ArrayList<>(outputs.size());
            for (Output col : outputs) names.add(col.name());
            return names;
        }
    }

    /** Single output column: either a direct passthrough or an object-stitch. */
    public sealed interface Output permits Output.Passthrough, Output.ObjectMap {

        String name();

        /** Read this output's value from an engine row. */
        Object read(Object[] engineRow);

        /** One engine column passed through unchanged. */
        record Passthrough(String name, int engineColumnIndex) implements Output {
            @Override
            public Object read(Object[] engineRow) {
                return engineRow[engineColumnIndex];
            }
        }

        /** Build a nested {@code Map} from a recursive child structure rooted at engine columns. */
        record ObjectMap(String name, Map<String, MapSource> children) implements Output {
            @Override
            public Object read(Object[] engineRow) {
                return buildMap(children, engineRow);
            }
        }
    }

    /** A child of an {@link Output.ObjectMap} — either a leaf engine column or a nested map. */
    public sealed interface MapSource permits MapSource.Leaf, MapSource.Nested {
        Object read(Object[] engineRow);

        record Leaf(int engineColumnIndex) implements MapSource {
            @Override
            public Object read(Object[] engineRow) {
                return engineRow[engineColumnIndex];
            }
        }

        record Nested(Map<String, MapSource> children) implements MapSource {
            @Override
            public Object read(Object[] engineRow) {
                return buildMap(children, engineRow);
            }
        }
    }

    private static Map<String, Object> buildMap(Map<String, MapSource> children, Object[] engineRow) {
        Map<String, Object> result = new LinkedHashMap<>(children.size());
        for (Map.Entry<String, MapSource> entry : children.entrySet()) {
            result.put(entry.getKey(), entry.getValue().read(engineRow));
        }
        return result;
    }

    /**
     * Rewrite the plan if any {@link TableScan} exposes an {@link ObjectType} column. Returns
     * {@link Optional#empty()} otherwise — the input plan is engine-safe as-is and no
     * coordinator stitch is needed. This short-circuit also avoids touching multi-input
     * operators (joins, unions) whose RexInputRef remap rules we don't model.
     */
    public static Optional<Rewrite> maybeRewrite(RelNode root) {
        if (!hasObjectTypeColumns(root)) {
            return Optional.empty();
        }
        Rewriter rewriter = new Rewriter(root.getCluster().getRexBuilder(), findTopProject(root));
        RelNode rewritten = rewriter.visit(root);
        return Optional.of(new Rewrite(rewritten, new Stitch(rewriter.outputs)));
    }

    private static boolean hasObjectTypeColumns(RelNode root) {
        boolean[] found = new boolean[1];
        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (found[0]) return;
                if (node instanceof TableScan scan) {
                    for (RelDataTypeField field : scan.getRowType().getFieldList()) {
                        if (field.getType() instanceof ObjectType) {
                            found[0] = true;
                            return;
                        }
                    }
                }
                super.visit(node, ordinal, parent);
            }
        }.go(root);
        return found[0];
    }

    /** Topmost {@link LogicalProject} reachable through the unique-input chain, or null. */
    private static RelNode findTopProject(RelNode root) {
        for (RelNode current = root; current != null; current = current.getInput(0)) {
            if (current instanceof LogicalProject) return current;
            if (current.getInputs().size() != 1) return null;
        }
        return null;
    }

    /** Bottom-up rewrite walker. */
    private static final class Rewriter {
        private final RexBuilder rexBuilder;
        private final RelNode topProject;
        // Leaf column name → its index in the post-strip scan row type.
        private final Map<String, Integer> leafIndex = new LinkedHashMap<>();
        // Stripped object-parent column name → its captured ObjectType descriptor (Calcite may
        // canonize the subclass to plain MapSqlType, so we keep the descriptor on the side).
        private final Map<String, ObjectType> objectTypes = new LinkedHashMap<>();
        // Captured stitch outputs from the top Project rewrite.
        List<Output> outputs = List.of();

        Rewriter(RexBuilder rexBuilder, RelNode topProject) {
            this.rexBuilder = rexBuilder;
            this.topProject = topProject;
        }

        RelNode visit(RelNode node) {
            if (node instanceof LogicalTableScan scan) return rewriteScan(scan);
            List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
            for (RelNode input : node.getInputs()) newInputs.add(visit(input));

            if (node == topProject && node instanceof LogicalProject project) {
                return rewriteTopProject(project, newInputs.get(0));
            }

            // Build oldIdx → newIdx for the (single) input. Stripped columns map to -1.
            int[] oldToNew = buildIndexMap(node.getInput(0).getRowType(), newInputs.get(0).getRowType());
            if (node instanceof LogicalProject project) {
                return rewriteIntermediateProject(project, newInputs.get(0), oldToNew);
            }
            // Filter/Sort/etc.: rebuild with new input then sweep RexInputRefs through the remap.
            return node.copy(node.getTraitSet(), newInputs).accept(new RemapShuttle(oldToNew, newInputs.get(0).getRowType(), rexBuilder));
        }

        /** Drop ObjectType columns from the scan and capture each parent's descriptor. */
        private RelNode rewriteScan(LogicalTableScan scan) {
            RelOptTable origTable = scan.getTable();
            List<RelDataTypeField> leafFields = new ArrayList<>();
            boolean stripped = false;
            for (RelDataTypeField field : origTable.getRowType().getFieldList()) {
                if (field.getType() instanceof ObjectType objectType) {
                    objectTypes.put(field.getName(), objectType);
                    stripped = true;
                } else {
                    int idx = leafFields.size();
                    leafFields.add(new RelDataTypeFieldImpl(field.getName(), idx, field.getType()));
                    leafIndex.put(field.getName(), idx);
                }
            }
            if (!stripped) return scan;
            RelOptTable strippedTable = new RelOptAbstractTable(
                origTable.getRelOptSchema(),
                origTable.getQualifiedName().getLast(),
                new RelRecordType(leafFields)
            ) {};
            return LogicalTableScan.create(scan.getCluster(), strippedTable, scan.getHints());
        }

        /**
         * Drop project items that pass through a stripped column; remap survivors. Computed
         * expressions over a stripped column throw via the shuttle.
         */
        private RelNode rewriteIntermediateProject(LogicalProject project, RelNode newInput, int[] oldToNew) {
            RemapShuttle remap = new RemapShuttle(oldToNew, newInput.getRowType(), rexBuilder);
            List<RexNode> newProjects = new ArrayList<>();
            List<String> newNames = new ArrayList<>();
            for (int i = 0; i < project.getProjects().size(); i++) {
                RexNode expr = project.getProjects().get(i);
                if (expr instanceof RexInputRef ref && oldToNew[ref.getIndex()] < 0) {
                    continue; // passthrough of stripped column — drop it
                }
                newProjects.add(expr.accept(remap));
                newNames.add(project.getRowType().getFieldList().get(i).getName());
            }
            return LogicalProject.create(newInput, project.getHints(), newProjects, newNames, project.getVariablesSet());
        }

        /**
         * Top-level Project: emit one engine output + one {@link Output} per user column. A
         * RexInputRef to an ObjectType produces an {@link Output.ObjectMap} (with leaves added
         * to the engine plan); everything else is a {@link Output.Passthrough}.
         */
        private RelNode rewriteTopProject(LogicalProject project, RelNode newInput) {
            RemapShuttle remap = new RemapShuttle(buildIndexMap(project.getInput().getRowType(), newInput.getRowType()), newInput.getRowType(), rexBuilder);
            List<RelDataTypeField> origInput = project.getInput().getRowType().getFieldList();
            List<RexNode> newProjects = new ArrayList<>();
            List<String> newNames = new ArrayList<>();
            List<Output> outputCols = new ArrayList<>();

            for (int i = 0; i < project.getProjects().size(); i++) {
                RexNode expr = project.getProjects().get(i);
                String outputName = project.getRowType().getFieldList().get(i).getName();
                if (expr instanceof RexInputRef ref) {
                    String name = origInput.get(ref.getIndex()).getName();
                    if (objectTypes.containsKey(name)) {
                        outputCols.add(
                            new Output.ObjectMap(
                                outputName,
                                buildChildren(objectTypes.get(name), newProjects, newNames, outputName, newInput.getRowType())
                            )
                        );
                        continue;
                    }
                }
                int idx = newProjects.size();
                newProjects.add(expr.accept(remap));
                newNames.add(outputName);
                outputCols.add(new Output.Passthrough(outputName, idx));
            }

            this.outputs = outputCols;
            return LogicalProject.create(newInput, project.getHints(), newProjects, newNames, project.getVariablesSet());
        }

        /**
         * Recursively build the child map for an {@link ObjectType}, adding leaf projections
         * to {@code newProjects} (with synthetic names in {@code newNames} so the engine
         * Project's row type is well-formed). The synthetic names are prefixed with
         * {@code __stitch_} to make them unambiguously internal.
         */
        private Map<String, MapSource> buildChildren(
            ObjectType objectType,
            List<RexNode> newProjects,
            List<String> newNames,
            String outputName,
            RelDataType newInputRowType
        ) {
            Map<String, MapSource> children = new LinkedHashMap<>();
            for (Map.Entry<String, ObjectType.Child> entry : objectType.children().entrySet()) {
                ObjectType.Child child = entry.getValue();
                if (child instanceof ObjectType.Child.Leaf leaf) {
                    Integer idx = leafIndex.get(leaf.path());
                    if (idx == null) {
                        throw new IllegalStateException("ObjectType references missing leaf [" + leaf.path() + "]");
                    }
                    int outIdx = newProjects.size();
                    newProjects.add(rexBuilder.makeInputRef(newInputRowType.getFieldList().get(idx).getType(), idx));
                    newNames.add("__stitch_" + outputName + "_" + leaf.path());
                    children.put(entry.getKey(), new MapSource.Leaf(outIdx));
                } else {
                    children.put(
                        entry.getKey(),
                        new MapSource.Nested(
                            buildChildren(
                                ((ObjectType.Child.Nested) child).type(),
                                newProjects,
                                newNames,
                                outputName + "." + entry.getKey(),
                                newInputRowType
                            )
                        )
                    );
                }
            }
            return children;
        }
    }

    /** old-input field name → new-input field index, with stripped fields → -1. */
    private static int[] buildIndexMap(RelDataType oldType, RelDataType newType) {
        Map<String, Integer> newByName = new HashMap<>();
        for (RelDataTypeField f : newType.getFieldList()) newByName.put(f.getName(), f.getIndex());
        int[] map = new int[oldType.getFieldCount()];
        for (int i = 0; i < map.length; i++) {
            Integer newIdx = newByName.get(oldType.getFieldList().get(i).getName());
            map[i] = newIdx == null ? -1 : newIdx;
        }
        return map;
    }

    /** Shifts {@link RexInputRef} indices via {@code oldToNew}; throws on stripped columns. */
    private static final class RemapShuttle extends RexShuttle {
        private final int[] oldToNew;
        private final RelDataType newInputRowType;
        private final RexBuilder rexBuilder;

        RemapShuttle(int[] oldToNew, RelDataType newInputRowType, RexBuilder rexBuilder) {
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
            if (newIdx == ref.getIndex() && expected.equals(ref.getType())) return ref;
            return rexBuilder.makeInputRef(expected, newIdx);
        }
    }
}
