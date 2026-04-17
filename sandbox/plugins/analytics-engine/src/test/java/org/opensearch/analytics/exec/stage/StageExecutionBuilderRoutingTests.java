/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.exec.StreamingResponseListener;

import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

/**
 * Tests that {@link StageExecutionBuilder#buildExecution} and
 * {@link StageExecutionBuilder#buildExecution} route stages to the correct
 * concrete {@link StageExecution} type based on stage execution type.
 *
 * <p>Routing is an internal concern of {@link StageExecutionBuilder}; these tests
 * assert on the type of the returned execution rather than on the scheduler
 * that was picked.
 */
public class StageExecutionBuilderRoutingTests extends OpenSearchTestCase {

    private static AnalyticsSearchTransportService failingDispatcher() {
        return new AnalyticsSearchTransportService(mock(TransportService.class), mock(ClusterService.class)) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest r,
                DiscoveryNode n,
                StreamingResponseListener<FragmentExecutionResponse> l,
                Task t,
                PendingExecutions p
            ) {
                fail("should not be called");
            }
        };
    }

    /**
     * A LOCAL stage with a null fragment routes to {@link LocalStageScheduler},
     * which short-circuits to a {@link PassThroughStageExecution}.
     */
    public void testLocalPassThroughStageRoutesToPassThroughExecution() {
        ClusterService clusterService = mock(ClusterService.class);
        StageExecutionBuilder executor = new StageExecutionBuilder(clusterService, failingDispatcher(), null);

        Stage stage = new Stage(0, null, List.of(), null, StageExecutionType.LOCAL);
        QueryContext config = QueryContext.forTest("test-query", null);

        StageExecution exec = executor.buildExecution(stage, new PassThroughStageExecution(stage, new RowProducingSink()), config);
        assertNotNull(exec);
        assertTrue("LOCAL pass-through should produce a PassThroughStageExecution", exec instanceof PassThroughStageExecution);
    }

    /**
     * A DATA_NODE stage routes to the shard fan-out path, producing a
     * {@link ShardScanStageExecution}.
     */
    public void testDataNodeStageRoutesToShardScanExecution() {
        ClusterService clusterService = mock(ClusterService.class);
        StageExecutionBuilder executor = new StageExecutionBuilder(clusterService, failingDispatcher(), null);

        Stage stage = new Stage(0, null, List.of(), null, StageExecutionType.DATA_NODE);
        stage.setPlanAlternatives(List.of());
        QueryContext config = QueryContext.forTest("test-query", null);

        StageExecution exec = executor.buildExecution(stage, new PassThroughStageExecution(stage, new RowProducingSink()), config);
        assertNotNull(exec);
        assertTrue("DATA_NODE should produce a ShardScanStageExecution", exec instanceof ShardScanStageExecution);
    }

    /**
     * A {@code buildExecution} call with a non-row-receiving parent is a
     * planner bug — {@link StageExecutionBuilder} throws {@link IllegalStateException}
     * with a message naming the offending stage.
     */
    public void testBuildExecutionRejectsNonRowReceivingParent() {
        ClusterService clusterService = mock(ClusterService.class);
        StageExecutionBuilder executor = new StageExecutionBuilder(clusterService, failingDispatcher(), null);

        Stage child = new Stage(1, null, List.of(), null, StageExecutionType.DATA_NODE);
        child.setPlanAlternatives(List.of());
        QueryContext config = QueryContext.forTest("test-query", null);

        // A raw StageExecution mock does not implement SinkProvidingStageExecution.
        StageExecution badParent = mock(StageExecution.class);

        AtomicReference<Exception> captured = new AtomicReference<>();
        try {
            executor.buildExecution(child, badParent, config);
            fail("should have thrown");
        } catch (IllegalStateException e) {
            captured.set(e);
        }

        Exception e = captured.get();
        assertNotNull(e);
        assertTrue(
            "Message should mention the offending stage id, got: " + e.getMessage(),
            e.getMessage() != null && e.getMessage().contains("stage 1")
        );
    }
}

