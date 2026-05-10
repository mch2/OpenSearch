/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
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
     * Creates an OpenSearchTableScan with RANDOM distribution regardless of shard count.
     *
     * <p>Single-shard scans report RANDOM rather than SINGLETON because the data still
     * lives on a data node, not the coordinator. The Calcite distribution trait can't
     * distinguish "one source on a data node" from "one source on the coord", so we
     * collapse to RANDOM and let {@code OpenSearchExchangeReducer} (the trait converter
     * for {@code RANDOM → SINGLETON}) be the sole producer of SINGLETON. This makes
     * trait propagation uniform across shard counts and lets gather rules ride the
     * normal {@code AbstractConverter.ExpandConversionRule} machinery instead of
     * special-casing the single-shard case.
     */
    public static OpenSearchTableScan create(
        RelOptCluster cluster,
        RelOptTable table,
        List<String> viableBackends,
        List<FieldStorageInfo> outputFieldStorage,
        OpenSearchDistributionTraitDef distTraitDef
    ) {
        RelTraitSet traitSet = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(distTraitDef.random());
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
        return LogicalTableScan.create(getCluster(), getTable(), List.of());
    }
}
