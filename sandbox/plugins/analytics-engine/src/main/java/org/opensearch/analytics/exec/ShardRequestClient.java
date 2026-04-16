/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.cluster.node.DiscoveryNode;

/**
 * Outbound client for sending fragment execution requests to data-node shards.
 * Created per-query by {@link Scheduler} binding the transport dispatcher
 * with the query's parent task and per-node concurrency state.
 *
 * <p>Not related to OpenSearch's Task framework.
 */
@FunctionalInterface
public interface ShardRequestClient {

    /**
     * Send a fragment execution request to the target node.
     *
     * @param request    the fragment execution request
     * @param targetNode the node hosting the target shard
     * @param listener   the streaming response listener to notify on each batch or failure
     */
    void send(FragmentExecutionRequest request, DiscoveryNode targetNode, StreamingResponseListener listener);
}
