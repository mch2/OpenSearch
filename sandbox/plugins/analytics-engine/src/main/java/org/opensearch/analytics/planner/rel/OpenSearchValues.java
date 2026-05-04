/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * OpenSearch custom {@link Values} carrying a viable backend list.
 *
 * <p>Used when Calcite collapses a contradiction-filter (e.g. {@code WHERE 1=2}) into an
 * empty-relation {@link LogicalValues}. The marking pass must convert that leaf into an
 * {@link OpenSearchRelNode} so parent Project/Aggregate/Filter/Sort rules can see a
 * marked child; otherwise they throw {@code "... rule encountered unmarked child
 * [LogicalValues]"}.
 *
 * <p>Values is format-agnostic — a zero-row relation can be produced by any backend, so
 * {@link #getViableBackends()} returns the full registry-wide scan-capable list. The
 * downstream resolution picks a single backend per plan. Field storage is empty: Values
 * has no source columns, only projected literal tuples (zero tuples in the empty case).
 *
 * @opensearch.internal
 */
public class OpenSearchValues extends Values implements OpenSearchRelNode {

    private final List<String> viableBackends;

    public OpenSearchValues(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelDataType rowType,
        ImmutableList<ImmutableList<RexLiteral>> tuples,
        List<String> viableBackends
    ) {
        super(cluster, rowType, tuples, traitSet);
        this.viableBackends = viableBackends;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /**
     * Values has no physical storage — each output column is a derived (computed) column.
     * We surface one {@link FieldStorageInfo#derivedColumn} per output field so parent rules
     * (Aggregate, Project) can resolve by index without going out of bounds when they
     * annotate expressions referencing input columns. Typical path: a contradiction
     * {@code Filter(false)} over a TableScan gets reduced to a {@link LogicalValues} whose
     * rowType preserves the scan's columns; downstream {@code Aggregate(sum(col))} still
     * needs to look up col's field-type.
     */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        List<FieldStorageInfo> out = new java.util.ArrayList<>(getRowType().getFieldCount());
        for (org.apache.calcite.rel.type.RelDataTypeField field : getRowType().getFieldList()) {
            out.add(FieldStorageInfo.derivedColumn(field.getName(), field.getType().getSqlTypeName()));
        }
        return out;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new OpenSearchValues(getCluster(), traitSet, getRowType(), getTuples(), viableBackends);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchValues(getCluster(), getTraitSet(), getRowType(), getTuples(), List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        // Leaf node — no annotations, no children to strip; return a plain LogicalValues
        // so the backend's substrait converter sees standard Calcite.
        return LogicalValues.create(getCluster(), getRowType(), getTuples());
    }
}
