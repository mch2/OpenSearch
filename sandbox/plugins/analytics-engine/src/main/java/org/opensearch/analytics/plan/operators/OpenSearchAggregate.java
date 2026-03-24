/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan.operators;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * OpenSearch-specific aggregate operator.
 * Wraps a Calcite {@link Aggregate} and carries a backend tag.
 */
public final class OpenSearchAggregate extends Aggregate implements BackendTagged {

    private final String backendTag;

    public OpenSearchAggregate(RelOptCluster cluster, RelTraitSet traits,
                               RelNode input, ImmutableBitSet groupSet,
                               List<ImmutableBitSet> groupSets,
                               List<AggregateCall> aggCalls,
                               String backendTag) {
        super(cluster, traits, List.of(), input, groupSet, groupSets, aggCalls);
        this.backendTag = backendTag;
    }

    @Override
    public String getBackendTag() {
        return backendTag;
    }

    @Override
    public OpenSearchAggregate withBackendTag(String tag) {
        return new OpenSearchAggregate(getCluster(), getTraitSet(), getInput(),
            getGroupSet(), getGroupSets(), getAggCallList(), tag);
    }

    @Override
    public OpenSearchAggregate copy(RelTraitSet traitSet, RelNode input,
            ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        return new OpenSearchAggregate(getCluster(), traitSet, input,
            groupSet, groupSets, aggCalls, backendTag);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("backend", backendTag);
    }
}
