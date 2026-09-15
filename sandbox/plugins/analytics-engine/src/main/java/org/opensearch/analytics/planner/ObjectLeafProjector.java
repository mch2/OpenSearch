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
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.GetFieldFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Rebuilds the flat leaf columns of an OpenSearch {@code object} from the struct the scan reads.
 *
 * <p>An object is stored as one Parquet struct, so {@code city} is a column and {@code city.name} is
 * not. The schema exposes both, because a query addresses a leaf by its dotted name and the frontend
 * resolves it as a column. So this drops the leaf columns from the scan and reconstructs them above
 * it, reaching into the struct:
 *
 * <pre>
 * LogicalProject(id=[$0], city=[$1], city.name=[get_field($1, 'name')],
 *                city.props.zone=[get_field(get_field($1, 'props'), 'zone')])
 *   LogicalTableScan(table=[[t]])      // id and city only
 * </pre>
 *
 * <p>The project reproduces the scan's original row type exactly, so every {@code RexInputRef} above
 * stays valid. Sub-objects nest the call, so any depth resolves in one pass.
 *
 * <p>One-shot pass rather than a HEP rule: a rule matching {@code TableScan} and producing
 * {@code Project(TableScan)} would re-match its own output.
 *
 * <p>Must run before {@code trimFields}, which drops the leaves no query referenced — otherwise a
 * query that only reads one leaf pays to project all of them. Leaf pushdown still works:
 * {@code FILTER_PROJECT_TRANSPOSE} moves a filter through this project, substituting the leaf
 * reference with the {@code get_field} call, which lands directly on the scan.
 *
 * <p>This replaced a pass in the other direction. Objects used to be stored as flat dotted columns
 * with no struct in the file, so the scan read the leaves and a project assembled the object with a
 * struct constructor. Nothing assembles a struct at query time any more.
 *
 * @opensearch.internal
 */
public final class ObjectLeafProjector {

    private ObjectLeafProjector() {}

    /**
     * Rewrites scans that expose both an object and its leaves into a scan without the leaves plus a
     * project that reads them out of the struct.
     *
     * @return the rewritten plan, or {@link Optional#empty()} when the plan has no object columns
     *         (callers keep the original plan unchanged)
     */
    public static Optional<RelNode> rewrite(RelNode root) {
        Projector projector = new Projector();
        RelNode rewritten = root.accept(projector);
        return projector.changed ? Optional.of(rewritten) : Optional.empty();
    }

    private static final class Projector extends RelShuttleImpl {

        private boolean changed = false;

        @Override
        public RelNode visit(TableScan scan) {
            RelDataType originalRowType = scan.getRowType();
            List<RelDataTypeField> originalFields = originalRowType.getFieldList();

            // A field-less struct is a shapeless object — `{"type": "object"}` before any document
            // gave it a shape — which has no column and no leaves. It is dropped from the scan and
            // projected as null, so a query naming it still resolves, as it does in vanilla.
            List<String> shapeless = new ArrayList<>();
            Map<String, RelDataTypeField> structs = new LinkedHashMap<>();
            for (RelDataTypeField field : originalFields) {
                if (field.getType().isStruct() == false) {
                    continue;
                }
                if (field.getType().getFieldCount() == 0) {
                    shapeless.add(field.getName());
                } else {
                    structs.put(field.getName(), field);
                }
            }
            if (structs.isEmpty() && shapeless.isEmpty()) {
                return scan;
            }

            // Only an outermost object is a column. The schema declares a sub-object under its own
            // dotted name too (`city.location` next to `city`), and that is no more a column than a
            // scalar leaf is: it has to be read out of its parent as well.
            Map<String, RelDataTypeField> objects = new LinkedHashMap<>();
            for (Map.Entry<String, RelDataTypeField> entry : structs.entrySet()) {
                if (enclosingStructOf(entry.getKey(), structs) == null) {
                    objects.put(entry.getKey(), entry.getValue());
                }
            }

            // The scan keeps every column that is physically there: the outermost objects, plus any
            // field that is not reachable inside one of them.
            RelDataTypeFactory typeFactory = scan.getCluster().getTypeFactory();
            RelDataTypeFactory.Builder storedTypeBuilder = typeFactory.builder();
            Map<String, Integer> storedIndexByName = new HashMap<>();
            List<Leaf> leaves = new ArrayList<>();
            for (RelDataTypeField field : originalFields) {
                if (shapeless.contains(field.getName())) {
                    continue;
                }
                Leaf leaf = objects.containsKey(field.getName()) ? null : resolveLeaf(field, objects);
                if (leaf != null) {
                    leaves.add(leaf);
                    continue;
                }
                storedIndexByName.put(field.getName(), storedTypeBuilder.getFieldCount());
                storedTypeBuilder.add(field.getName(), field.getType());
            }
            if (leaves.isEmpty() && shapeless.isEmpty()) {
                // Objects but no leaves to lift out of them — a scan whose row type was already
                // trimmed to the object alone. Rewriting would add a project that only copies input.
                return scan;
            }

            RelOptTable storedTable = new StoredOnlyTable(scan.getTable(), storedTypeBuilder.build());
            RelNode storedScan = LogicalTableScan.create(scan.getCluster(), storedTable, scan.getHints());

            // Rebuild the original row type: stored columns pass through, leaves read out of them.
            RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
            Map<String, Leaf> leafByName = new HashMap<>();
            for (Leaf leaf : leaves) {
                leafByName.put(leaf.name, leaf);
            }
            List<RexNode> projects = new ArrayList<>(originalFields.size());
            List<String> names = new ArrayList<>(originalFields.size());
            for (RelDataTypeField field : originalFields) {
                names.add(field.getName());
                Leaf leaf = leafByName.get(field.getName());
                if (shapeless.contains(field.getName())) {
                    projects.add(rexBuilder.makeNullLiteral(field.getType()));
                } else if (leaf == null) {
                    projects.add(rexBuilder.makeInputRef(storedScan, storedIndexByName.get(field.getName())));
                } else {
                    projects.add(readLeaf(rexBuilder, storedScan, leaf, storedIndexByName));
                }
            }

            changed = true;
            return LogicalProject.create(storedScan, List.of(), projects, names);
        }

        /**
         * One flat column that is really a path into a struct.
         *
         * @param name   the column's dotted name, as the schema and the frontend know it
         * @param object the name of the struct column it lives in
         * @param path   the field names to follow inside that struct
         * @param type   the leaf's type
         */
        private record Leaf(String name, String object, List<String> path, RelDataType type) {
        }

        /**
         * Decides whether {@code field} is a leaf of one of {@code objects}, and if so how to reach
         * it.
         *
         * <p>The walk through the struct is what confirms the schema and the storage agree. A name
         * that looks like a path into an object but does not resolve inside it stays a column of the
         * scan, where it will fail loudly against field storage rather than silently reading null.
         *
         * @return the resolved leaf, or null when the field is a column in its own right
         */
        private static Leaf resolveLeaf(RelDataTypeField field, Map<String, RelDataTypeField> objects) {
            RelDataTypeField object = enclosingStructOf(field.getName(), objects);
            if (object == null) {
                return null;
            }
            List<String> path = walk(object.getType(), field.getName().substring(object.getName().length() + 1));
            return path == null ? null : new Leaf(field.getName(), object.getName(), path, field.getType());
        }

        /**
         * Returns the innermost entry of {@code structs} whose name is a dotted prefix of
         * {@code name}, or null when nothing encloses it.
         *
         * <p>Matched against the declared struct columns rather than by splitting the name on dots: a
         * dot does not imply an object, and a name that merely looks like a path into one must stay a
         * column of its own, where it fails loudly against field storage rather than silently
         * reading null.
         */
        private static RelDataTypeField enclosingStructOf(String name, Map<String, RelDataTypeField> structs) {
            for (int dot = name.lastIndexOf('.'); dot > 0; dot = name.lastIndexOf('.', dot - 1)) {
                RelDataTypeField enclosing = structs.get(name.substring(0, dot));
                if (enclosing != null) {
                    return enclosing;
                }
            }
            return null;
        }

        /**
         * Follows {@code remainder} through {@code structType}, returning the field names to
         * dereference, or null when the path does not resolve.
         *
         * <p>Each step takes the longest child name that matches a prefix of what is left, so a
         * sub-object is preferred over nothing and a child whose own name contains a dot still
         * resolves.
         */
        private static List<String> walk(RelDataType structType, String remainder) {
            List<String> path = new ArrayList<>();
            RelDataType current = structType;
            String rest = remainder;
            while (true) {
                RelDataTypeField match = null;
                for (RelDataTypeField child : current.getFieldList()) {
                    String childName = child.getName();
                    boolean matches = rest.equals(childName) || rest.startsWith(childName + ".");
                    if (matches && (match == null || childName.length() > match.getName().length())) {
                        match = child;
                    }
                }
                if (match == null) {
                    return null;
                }
                path.add(match.getName());
                if (rest.equals(match.getName())) {
                    return path;
                }
                if (match.getType().isStruct() == false) {
                    return null;
                }
                current = match.getType();
                rest = rest.substring(match.getName().length() + 1);
            }
        }

        /** Builds the {@code get_field} chain that reads {@code leaf} out of its struct column. */
        private static RexNode readLeaf(RexBuilder rexBuilder, RelNode storedScan, Leaf leaf, Map<String, Integer> storedIndexByName) {
            RexNode value = rexBuilder.makeInputRef(storedScan, storedIndexByName.get(leaf.object));
            RelDataType currentType = value.getType();
            for (int step = 0; step < leaf.path.size(); step++) {
                String fieldName = leaf.path.get(step);
                RelDataTypeField child = currentType.getField(fieldName, true, false);
                RelDataType stepType = step == leaf.path.size() - 1 ? leaf.type : child.getType();
                value = GetFieldFunction.makeCall(rexBuilder, stepType, value, fieldName);
                currentType = stepType;
            }
            return value;
        }
    }

    /**
     * Wraps the scanned table with a row type stripped of the columns that are really paths into a
     * struct, so downstream physical resolution ({@code FieldStorageResolver}) only ever sees fields
     * that are columns of their own. Mirrors the {@code IndexNameTable} wrapper in
     * {@code OpenSearchTableScanRule}.
     */
    private static final class StoredOnlyTable extends RelOptAbstractTable {

        private final RelOptTable delegate;

        StoredOnlyTable(RelOptTable delegate, RelDataType storedRowType) {
            super(delegate.getRelOptSchema(), delegate.getQualifiedName().getLast(), storedRowType);
            this.delegate = delegate;
        }

        @Override
        public List<String> getQualifiedName() {
            // Preserve the original qualified name: OpenSearchTableScanRule resolves the index
            // from it, and RelOptAbstractTable would otherwise report a single-segment name.
            return delegate.getQualifiedName();
        }

        @Override
        public double getRowCount() {
            return delegate.getRowCount();
        }

        @Override
        public <T> T unwrap(Class<T> clazz) {
            T unwrapped = super.unwrap(clazz);
            return unwrapped != null ? unwrapped : delegate.unwrap(clazz);
        }
    }
}
