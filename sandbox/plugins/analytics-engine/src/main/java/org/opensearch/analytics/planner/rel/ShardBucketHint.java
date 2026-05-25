/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Pre-CBO hint attached to an {@link OpenSearchAggregate}(SINGLE); consumed at split time by
 * {@link org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule} to insert a
 * shard-side {@code Sort+Limit} so each shard ships at most {@code shardSize} groups.
 *
 * @param shardSize per-shard bucket count to ship to the coordinator
 * @param collation synthetic dense {@code [0..N)} field-index collation parallel to {@code sortExprs}; directions mirror the user's outer Sort
 * @param sortExprs RexNodes evaluated by the shard runtime to produce sort keys, against the shard-side aggregate's output row type
 *
 * @opensearch.internal
 */
public record ShardBucketHint(int shardSize, RelCollation collation, List<RexNode> sortExprs) {
}
