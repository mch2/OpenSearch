/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.task;

import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;

import java.util.Map;

/**
 * Data-node shard task representing a single shard fragment execution.
 * Analogous to {@link org.opensearch.action.search.SearchShardTask}.
 *
 * <p>Uses plain {@link Task} (not {@code CancellableTask}) to avoid
 * interaction with {@code TaskManager.startTrackingCancellableChannelTask}
 * which conflicts with the streaming transport's
 * {@code completeStream()} lifecycle. Cancellation is handled at the
 * query level via {@link AnalyticsQueryTask}.
 *
 * @opensearch.internal
 */
public class AnalyticsShardTask extends Task {

    public AnalyticsShardTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
    }
}
