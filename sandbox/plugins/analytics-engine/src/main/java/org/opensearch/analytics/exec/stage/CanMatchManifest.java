/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.stage.canmatch.CanMatchStage;
import org.opensearch.analytics.planner.dag.ExecutionTarget;

import java.util.List;

/**
 * Published by {@link CanMatchStage} when it completes; consumed by the parent
 * {@code ShardFragmentStageExecution} to decide which targets to actually scan.
 *
 * <p>{@code matchingTargets} is the subset of resolved targets whose data could
 * possibly contain rows matching the query's range predicates. On a fail-open path
 * (transport error, unknown backend, serialization failure) this list equals the
 * full resolved target set.
 *
 * @param matchingTargets the surviving target list (never null, may equal the input on fail-open)
 *
 * @opensearch.internal
 */
public record CanMatchManifest(List<ExecutionTarget> matchingTargets) implements StageMetadata {}
