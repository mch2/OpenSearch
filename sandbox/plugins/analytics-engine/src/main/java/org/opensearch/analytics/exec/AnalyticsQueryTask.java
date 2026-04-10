/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.action.search.SearchTask;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;

import java.util.Map;

/**
 * Coordinator-level cancellable task representing a running analytics query.
 * Analogous to {@link SearchTask}.
 * Cancelling this task cascades cancellation to all child shard tasks.
 *
 * @opensearch.internal
 */
public class AnalyticsQueryTask extends CancellableTask {

    private final String queryId;

    public AnalyticsQueryTask(long id, String type, String action, String queryId, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, "queryId[" + queryId + "]", parentTaskId, headers);
        this.queryId = queryId;
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

    public String getQueryId() {
        return queryId;
    }
}
