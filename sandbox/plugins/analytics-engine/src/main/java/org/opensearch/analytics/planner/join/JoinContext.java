/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.join;

import org.apache.calcite.rel.core.JoinRelType;

import java.util.List;

/**
 * Inputs available to a {@link JoinStrategy} when deciding how to distribute each
 * side of a join. Carries the equi-join keys (one index per side, paired by position),
 * the partition count for shuffle strategies, and the join type so strategies can
 * key their decisions off it (e.g. broadcast right-side build for INNER/LEFT but not
 * RIGHT).
 *
 * @param leftKeys      left-side equi-join key field indices (paired with rightKeys)
 * @param rightKeys     right-side equi-join key field indices (paired with leftKeys)
 * @param numPartitions intended fan-out for shuffle strategies; ignored by gather strategies
 * @param joinType      join shape (INNER / LEFT / RIGHT / etc.)
 * @opensearch.internal
 */
public record JoinContext(List<Integer> leftKeys, List<Integer> rightKeys, int numPartitions, JoinRelType joinType) {}
