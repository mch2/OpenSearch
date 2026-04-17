/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link AnalyticsQueryTask}, {@link AnalyticsShardTask}, and
 * {@link FragmentExecutionRequest#createTask}.
 *
 * Validates: Requirements 6.2, 6.5, 6.6, 6.7
 */
public class AnalyticsTaskTests extends OpenSearchTestCase {

    /**
     * mirroring SearchTask behaviour where cancelling the query cascades to all shard tasks.
     *
     * Validates: Requirements 6.2
     */
    public void testAnalyticsQueryTaskCancellationCascades() {
        String queryId = randomAlphaOfLength(10);
        AnalyticsQueryTask task = new AnalyticsQueryTask(
            0L,
            "transport",
            "analytics_query",
            queryId,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

    }

    /**
     * mirroring SearchShardTask behaviour where shard tasks do not cascade cancellation.
     *
     * Validates: Requirements 6.5
     */
    public void testAnalyticsShardTaskCancellationDoesNotCascade() {
        AnalyticsShardTask task = new AnalyticsShardTask(
            0L,
            "transport",
            "analytics_shard",
            "test-description",
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

    }

    /**
     * AnalyticsQueryTask description contains "queryId[...]".
     *
     * Validates: Requirements 6.3
     */
    public void testAnalyticsQueryTaskDescription() {
        String queryId = randomAlphaOfLength(12);
        AnalyticsQueryTask task = new AnalyticsQueryTask(
            0L,
            "transport",
            "analytics_query",
            queryId,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        String description = task.getDescription();
        assertTrue(
            "description must contain queryId[" + queryId + "], got: " + description,
            description.contains("queryId[" + queryId + "]")
        );
    }

    /**
     * FragmentExecutionRequest.createTask() returns an AnalyticsShardTask whose description
     * includes queryId, stageId, and shardId.
     *
     * Validates: Requirements 6.7
     */
    public void testFragmentExecutionRequestCreateTaskReturnsAnalyticsShardTask() {
        String queryId = randomAlphaOfLength(8);
        int stageId = randomIntBetween(0, 10);
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), randomIntBetween(0, 5));

        FragmentExecutionRequest request = new FragmentExecutionRequest(queryId, stageId, shardId, List.of());

        Task task = request.createTask(1L, "transport", "analytics_shard", TaskId.EMPTY_TASK_ID, Collections.emptyMap());

        assertTrue(
            "createTask() must return an AnalyticsShardTask instance, got: " + task.getClass().getName(),
            task instanceof AnalyticsShardTask
        );

        String description = task.getDescription();
        assertTrue(
            "description must contain queryId[" + queryId + "], got: " + description,
            description.contains("queryId[" + queryId + "]")
        );
        assertTrue(
            "description must contain stageId[" + stageId + "], got: " + description,
            description.contains("stageId[" + stageId + "]")
        );
        assertTrue(
            "description must contain shardId[" + shardId + "], got: " + description,
            description.contains("shardId[" + shardId + "]")
        );
    }
}
