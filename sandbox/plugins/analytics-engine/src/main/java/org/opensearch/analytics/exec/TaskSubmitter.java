/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;

/**
 * Callback interface from {@code PlanWalker} to {@code Scheduler} for dispatching
 * fragment execution requests to target nodes. Decouples the walker from transport
 * dispatch details.
 */
@FunctionalInterface
public interface TaskSubmitter {

    /**
     * Submit a fragment execution request to the target node.
     *
     * @param request    the fragment execution request
     * @param targetNode the node hosting the target shard
     * @param listener   the listener to notify on response or failure
     */
    void submit(FragmentExecutionRequest request, DiscoveryNode targetNode, ActionListener<FragmentExecutionResponse> listener);
}
