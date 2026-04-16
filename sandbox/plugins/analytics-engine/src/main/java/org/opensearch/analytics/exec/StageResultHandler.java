/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.FragmentExecutionResponse;

/**
 * Strategy for processing per-batch responses within a {@link StageExecution}.
 * Selected by {@link StageExecutor} based on the stage type and passed to
 * the execution at construction time.
 *
 * <p>Implementations:
 * <ul>
 *   <li>{@link SinkFeedingHandler} — feeds batches to the root {@link ExchangeSink}</li>
 *   <li>{@link ManifestCollectingHandler} — collects shuffle partition manifests</li>
 * </ul>
 *
 * @opensearch.internal
 */
public interface StageResultHandler {

    /**
     * Process one response batch from a shard.
     *
     * @param response the fragment execution response (row data or metadata)
     * @param target   the shard that produced this response
     */
    void onBatch(FragmentExecutionResponse response, ShardTarget target);
}
