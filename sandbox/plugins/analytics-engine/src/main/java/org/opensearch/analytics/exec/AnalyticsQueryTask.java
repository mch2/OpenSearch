/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.action.search.SearchTask;
import org.opensearch.common.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.SearchBackpressureTask;
import org.opensearch.wlm.WorkloadGroupTask;

import java.util.Map;

/**
 * Coordinator-level cancellable task representing a running analytics query.
 * Analogous to {@link SearchTask}.
 * Cancelling this task cascades cancellation to all child shard tasks.
 *
 * @opensearch.internal
 */
public class AnalyticsQueryTask extends CancellableTask implements SearchBackpressureTask {

    private final String queryId;
    private final TimeValue cancelAfterTimeInterval;

    public AnalyticsQueryTask(
        long id,
        String type,
        String action,
        String queryId,
        TaskId parentTaskId,
        Map<String, String> headers,
        @Nullable TimeValue cancelAfterTimeInterval
    ) {
        super(
            id,
            type,
            action,
            "queryId[" + queryId + "]",
            parentTaskId,
            headers,
            cancelAfterTimeInterval != null ? cancelAfterTimeInterval : TimeValue.MINUS_ONE
        );
        this.queryId = queryId;
        this.cancelAfterTimeInterval = cancelAfterTimeInterval;
    }

    public AnalyticsQueryTask(long id, String type, String action, String queryId, TaskId parentTaskId, Map<String, String> headers) {
        this(id, type, action, queryId, parentTaskId, headers, null);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

    public String getQueryId() {
        return queryId;
    }

    @Nullable
    public TimeValue getCancelAfterTimeInterval() {
        return cancelAfterTimeInterval;
    }
}
