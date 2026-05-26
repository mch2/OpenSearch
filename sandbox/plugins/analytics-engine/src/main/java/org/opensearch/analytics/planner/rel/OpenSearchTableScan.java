/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * OpenSearch custom TableScan carrying viable backend list and per-field storage metadata.
 *
 * @opensearch.internal
 */
public class OpenSearchTableScan extends TableScan implements OpenSearchRelNode {

    private final List<String> viableBackends;
    private final List<FieldStorageInfo> outputFieldStorage;

    public OpenSearchTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        List<String> viableBackends,
        List<FieldStorageInfo> outputFieldStorage
    ) {
        super(cluster, traitSet, List.of(), table);
        this.viableBackends = viableBackends;
        this.outputFieldStorage = outputFieldStorage;
    }

    /**
     * Creates an OpenSearchTableScan with {@code SHARD+SINGLETON} (1 shard) or
     * {@code SHARD+RANDOM} (N shards). Exchange insertion is CBO-driven: downstream cost
     * gates (root, Sort with collation, RexOver Project, Join, Union) demand
     * {@code COORDINATOR+SINGLETON}; Volcano materializes an ER via
     * {@link OpenSearchDistributionTraitDef#convert} wherever a demand can't be satisfied.
     *
     * <p>Join and Union split rules check the SHARD+SINGLETON+shardCount=1+matching-tableId
     * predicate to keep execution local when all inputs co-locate on one node.
     *
     * <p>{@code tableId} is derived from the table's qualified name, stable across plans for
     * the same index.
     */
    public static OpenSearchTableScan create(
        RelOptCluster cluster,
        RelOptTable table,
        List<String> viableBackends,
        List<FieldStorageInfo> outputFieldStorage,
        int shardCount,
        OpenSearchDistributionTraitDef distTraitDef
    ) {
        int tableId = table.getQualifiedName().hashCode();
        OpenSearchDistribution distribution = shardCount == 1
            ? distTraitDef.shardSingleton(tableId, shardCount)
            : distTraitDef.shardRandom(tableId, shardCount);
        RelTraitSet traitSet = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(distribution);
        return new OpenSearchTableScan(cluster, traitSet, table, viableBackends, outputFieldStorage);
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        return outputFieldStorage;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchTableScan(getCluster(), traitSet, getTable(), viableBackends, outputFieldStorage);
    }

    @Override
    public org.apache.calcite.plan.RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchTableScan(getCluster(), getTraitSet(), getTable(), List.of(backend), outputFieldStorage);
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        RelOptTable table = getTable();
        List<RelDataTypeField> fields = table.getRowType().getFieldList();

        // Detect alias columns: row type name differs from FieldStorageInfo physical name.
        // For aliases, the storage info points to the target column (which also exists in
        // the row type under its own name). Build a physical-only scan + wrapping Project
        // that re-adds alias columns as references to their targets.
        boolean hasAliases = false;
        for (int i = 0; i < fields.size() && i < outputFieldStorage.size(); i++) {
            if (!fields.get(i).getName().equals(outputFieldStorage.get(i).getFieldName())) {
                hasAliases = true;
                break;
            }
        }

        if (!hasAliases) {
            return LogicalTableScan.create(getCluster(), table, List.of());
        }

        // Build physical-only row type (no aliases) for the scan.
        RelDataTypeFactory tf = getCluster().getTypeFactory();
        RelDataTypeFactory.Builder scanBuilder = tf.builder();
        for (int i = 0; i < fields.size() && i < outputFieldStorage.size(); i++) {
            if (!fields.get(i).getName().equals(outputFieldStorage.get(i).getFieldName())) {
                continue;
            }
            scanBuilder.add(fields.get(i).getName(), fields.get(i).getType());
        }
        RelDataType physicalType = scanBuilder.build();
        RelOptTable physicalTable = new RelOptAbstractTable(
            table.getRelOptSchema(), table.getQualifiedName().getLast(), physicalType) {};
        RelNode scan = LogicalTableScan.create(getCluster(), physicalTable, List.of());

        // Build a Project above the scan that outputs ALL columns in the original order:
        // physical columns as pass-through, alias columns as references to their targets.
        org.apache.calcite.rex.RexBuilder rb = getCluster().getRexBuilder();
        List<org.apache.calcite.rex.RexNode> projects = new java.util.ArrayList<>(fields.size());
        List<String> names = new java.util.ArrayList<>(fields.size());
        for (int i = 0; i < fields.size() && i < outputFieldStorage.size(); i++) {
            String colName = fields.get(i).getName();
            String physicalName = outputFieldStorage.get(i).getFieldName();
            names.add(colName);
            if (colName.equals(physicalName)) {
                // Physical column — find its index in the physical scan.
                int physIdx = physicalType.getField(physicalName, true, false).getIndex();
                projects.add(rb.makeInputRef(physicalType.getFieldList().get(physIdx).getType(), physIdx));
            } else {
                // Alias column — reference the target in the physical scan.
                RelDataTypeField targetField = physicalType.getField(physicalName, true, false);
                if (targetField != null) {
                    projects.add(rb.makeInputRef(targetField.getType(), targetField.getIndex()));
                } else {
                    projects.add(rb.makeInputRef(fields.get(i).getType(), 0));
                }
            }
        }
        return org.apache.calcite.rel.logical.LogicalProject.create(
            scan, List.of(), projects, names, java.util.Set.of());
    }
}
