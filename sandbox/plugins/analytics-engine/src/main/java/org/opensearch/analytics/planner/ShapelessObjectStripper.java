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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Removes shapeless-object columns from a scan and projects a typed null in their place.
 *
 * <p>A shapeless object is {@code {"type": "object"}} before any document gave it a shape. The
 * schema declares it as a ROW with no fields so a query naming it still resolves, but there is no
 * such column in the file and nothing to read — so the scan must not ask for it, and the query must
 * see null, as it does in vanilla:
 *
 * <pre>
 * LogicalProject(id=[$0], attrs=[null:RecordType()])
 *   LogicalTableScan(table=[[t]])      // id only
 * </pre>
 *
 * <p>The project reproduces the scan's original row type exactly, so every {@code RexInputRef} above
 * stays valid.
 *
 * <p>One-shot pass rather than a HEP rule: a rule matching {@code TableScan} and producing
 * {@code Project(TableScan)} would re-match its own output.
 *
 * <p>This is what remains of a larger pass. The schema used to declare an object's leaves as flat
 * dotted columns as well, and that pass rebuilt each one above the scan by reaching into the struct.
 * The leaves are no longer declared: a dotted path resolves as field access on the struct instead
 * (see {@code OpenSearchNestedFieldRewriter.StructItemShuttle}), which is Calcite's own way to
 * reference a struct field and needs nothing lifted out of the scan.
 *
 * @opensearch.internal
 */
public final class ShapelessObjectStripper {

    private ShapelessObjectStripper() {}

    /**
     * Rewrites scans that expose a shapeless object into a scan without it plus a project supplying
     * null.
     *
     * @return the rewritten plan, or {@link Optional#empty()} when no scan has one (callers keep the
     *         original plan unchanged)
     */
    public static Optional<RelNode> rewrite(RelNode root) {
        Stripper stripper = new Stripper();
        RelNode rewritten = root.accept(stripper);
        return stripper.changed ? Optional.of(rewritten) : Optional.empty();
    }

    private static final class Stripper extends RelShuttleImpl {

        private boolean changed = false;

        @Override
        public RelNode visit(TableScan scan) {
            RelDataType originalRowType = scan.getRowType();
            List<RelDataTypeField> originalFields = originalRowType.getFieldList();

            List<String> shapeless = new ArrayList<>();
            for (RelDataTypeField field : originalFields) {
                if (field.getType().isStruct() && field.getType().getFieldCount() == 0) {
                    shapeless.add(field.getName());
                }
            }
            if (shapeless.isEmpty()) {
                return scan;
            }

            RelDataTypeFactory typeFactory = scan.getCluster().getTypeFactory();
            RelDataTypeFactory.Builder storedTypeBuilder = typeFactory.builder();
            Map<String, Integer> storedIndexByName = new HashMap<>();
            for (RelDataTypeField field : originalFields) {
                if (shapeless.contains(field.getName())) {
                    continue;
                }
                storedIndexByName.put(field.getName(), storedTypeBuilder.getFieldCount());
                storedTypeBuilder.add(field.getName(), field.getType());
            }

            RelOptTable storedTable = new StoredOnlyTable(scan.getTable(), storedTypeBuilder.build());
            RelNode storedScan = LogicalTableScan.create(scan.getCluster(), storedTable, scan.getHints());

            RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
            List<RexNode> projects = new ArrayList<>(originalFields.size());
            List<String> names = new ArrayList<>(originalFields.size());
            for (RelDataTypeField field : originalFields) {
                names.add(field.getName());
                if (shapeless.contains(field.getName())) {
                    projects.add(rexBuilder.makeNullLiteral(field.getType()));
                } else {
                    projects.add(rexBuilder.makeInputRef(storedScan, storedIndexByName.get(field.getName())));
                }
            }

            changed = true;
            return LogicalProject.create(storedScan, List.of(), projects, names);
        }
    }

    /** The scan's table, narrowed to the columns that are physically stored. */
    private static final class StoredOnlyTable extends RelOptAbstractTable {

        private final RelOptTable delegate;

        StoredOnlyTable(RelOptTable delegate, RelDataType storedRowType) {
            super(delegate.getRelOptSchema(), delegate.getQualifiedName().getLast(), storedRowType);
            this.delegate = delegate;
        }

        @Override
        public List<String> getQualifiedName() {
            return delegate.getQualifiedName();
        }

        @Override
        public double getRowCount() {
            return delegate.getRowCount();
        }

        @Override
        public <T> T unwrap(Class<T> clazz) {
            T unwrapped = delegate.unwrap(clazz);
            return unwrapped != null ? unwrapped : super.unwrap(clazz);
        }
    }
}
