/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.canmatch;

import org.opensearch.Version;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.canmatch.AnalyticsCanMatchAction;
import org.opensearch.analytics.exec.canmatch.AnalyticsCanMatchResponse;
import org.opensearch.analytics.exec.canmatch.CanMatchFilter;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.dag.TargetResolver;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportResponseHandler;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CanMatchStage}: target resolution, dispatch, fail-open paths,
 * and metadata publication. Verifies the stage lifecycle (CREATED → RUNNING → SUCCEEDED)
 * surfaces the filtered target manifest via {@code publishedMetadata()} on completion.
 */
public class CanMatchStageTests extends OpenSearchTestCase {

    private static final String BACKEND_ID = "datafusion";

    private Executor schedulerExecutor;

    @org.junit.Before
    public void setUpExecutor() {
        schedulerExecutor = Runnable::run; // run inline so assertions are immediate
    }

    public void testEmptyTargetsPublishesEmpty() {
        StreamTransportService transport = mock(StreamTransportService.class);
        ClusterService clusterService = mockClusterService();
        TargetResolver resolver = new TargetResolver() {
            @Override public List<ExecutionTarget> resolve(ClusterState s, Object m) { return List.of(); }
        };
        Stage stage = stage(resolver);

        CanMatchStage cm = build(stage, transport, clusterService, List.of(filter("c", 0, 10)));
        scheduleAndDispatch(cm);

        assertEquals(StageExecution.State.SUCCEEDED, cm.getState());
        assertEquals(List.of(), cm.publishedMetadata());
        verify(transport, never()).sendRequest(any(DiscoveryNode.class), any(String.class), any(), any());
    }

    public void testNoFiltersPublishesAllTargets() {
        StreamTransportService transport = mock(StreamTransportService.class);
        ClusterService clusterService = mockClusterService();
        List<ExecutionTarget> targets = List.of(target("a", 0), target("b", 1));
        TargetResolver resolver = constantResolver(targets);
        Stage stage = stage(resolver);

        CanMatchStage cm = build(stage, transport, clusterService, List.of());
        scheduleAndDispatch(cm);

        assertEquals(StageExecution.State.SUCCEEDED, cm.getState());
        assertEquals(targets, cm.publishedMetadata());
        verify(transport, never()).sendRequest(any(DiscoveryNode.class), any(String.class), any(), any());
    }

    public void testNullBackendIdPublishesAllTargets() {
        StreamTransportService transport = mock(StreamTransportService.class);
        ClusterService clusterService = mockClusterService();
        List<ExecutionTarget> targets = List.of(target("a", 0));
        TargetResolver resolver = constantResolver(targets);
        Stage stage = stage(resolver);

        CanMatchStage cm = new CanMatchStage(stage, mockConfig(), clusterService, mockDispatcher(transport), List.of(filter("c", 0, 10)), null);
        scheduleAndDispatch(cm);

        assertEquals(StageExecution.State.SUCCEEDED, cm.getState());
        assertEquals(targets, cm.publishedMetadata());
        verify(transport, never()).sendRequest(any(DiscoveryNode.class), any(String.class), any(), any());
    }

    public void testDispatchPrunesNonMatchingShards() {
        StreamTransportService transport = mock(StreamTransportService.class);
        ClusterService clusterService = mockClusterService();
        ExecutionTarget keep = target("keep", 0);
        ExecutionTarget drop = target("drop", 1);
        TargetResolver resolver = constantResolver(List.of(keep, drop));
        stubResponses(transport, Map.of("keep", true, "drop", false));
        Stage stage = stage(resolver);

        CanMatchStage cm = build(stage, transport, clusterService, List.of(filter("c", 0, 10)));
        scheduleAndDispatch(cm);

        assertEquals(StageExecution.State.SUCCEEDED, cm.getState());
        @SuppressWarnings("unchecked")
        List<ExecutionTarget> published = (List<ExecutionTarget>) cm.publishedMetadata();
        assertEquals(List.of(keep), published);
    }

    public void testTransportFailureFailsOpen() {
        StreamTransportService transport = mock(StreamTransportService.class);
        ClusterService clusterService = mockClusterService();
        ExecutionTarget a = target("a", 0);
        ExecutionTarget b = target("b", 1);
        TargetResolver resolver = constantResolver(List.of(a, b));
        doAnswer(inv -> {
            TransportResponseHandler<AnalyticsCanMatchResponse> handler = inv.getArgument(3);
            // One success, one failure — fail-open keeps both.
            DiscoveryNode node = inv.getArgument(0);
            if (node.getName().equals("a")) {
                handler.handleException(new org.opensearch.transport.NodeNotConnectedException(node, "boom"));
            } else {
                handler.handleResponse(AnalyticsCanMatchResponse.YES);
            }
            return null;
        }).when(transport).sendRequest(any(DiscoveryNode.class), eq(AnalyticsCanMatchAction.NAME), any(), any());
        Stage stage = stage(resolver);

        CanMatchStage cm = build(stage, transport, clusterService, List.of(filter("c", 0, 10)));
        scheduleAndDispatch(cm);

        assertEquals(StageExecution.State.SUCCEEDED, cm.getState());
        @SuppressWarnings("unchecked")
        List<ExecutionTarget> published = (List<ExecutionTarget>) cm.publishedMetadata();
        assertEquals(Set.of(a, b), Set.copyOf(published));
    }

    // ── helpers ────────────────────────────────────────────────────────────

    private CanMatchStage build(Stage stage, StreamTransportService transport, ClusterService clusterService, List<CanMatchFilter> filters) {
        return new CanMatchStage(stage, mockConfig(), clusterService, mockDispatcher(transport), filters, BACKEND_ID);
    }

    /** Mirrors QueryScheduler.scheduleStage: registers a one-shot dispatch listener, then starts the stage. */
    private static void scheduleAndDispatch(CanMatchStage cm) {
        cm.addStateListener((prev, target) -> {
            if (target == StageExecution.State.RUNNING) {
                cm.dispatchTasks((s, t) -> new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void unused) {
                        t.transitionTo(org.opensearch.analytics.exec.stage.StageTaskState.FINISHED);
                        cm.onTaskTerminal(t, null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        t.transitionTo(org.opensearch.analytics.exec.stage.StageTaskState.FINISHED);
                        cm.onTaskTerminal(t, e);
                    }
                });
            }
        });
        cm.start();
    }

    private QueryContext mockConfig() {
        QueryContext config = mock(QueryContext.class);
        when(config.queryId()).thenReturn("q1");
        when(config.operationListeners()).thenReturn(List.of());
        when(config.parentTask()).thenReturn(mock(AnalyticsQueryTask.class));
        when(config.schedulerExecutor()).thenReturn(schedulerExecutor);
        return config;
    }

    private AnalyticsSearchTransportService mockDispatcher(StreamTransportService transport) {
        AnalyticsSearchTransportService d = mock(AnalyticsSearchTransportService.class);
        when(d.streamTransportService()).thenReturn(transport);
        return d;
    }

    private ClusterService mockClusterService() {
        ClusterService cs = mock(ClusterService.class);
        when(cs.state()).thenReturn(mock(ClusterState.class));
        return cs;
    }

    private static TargetResolver constantResolver(List<ExecutionTarget> targets) {
        return new TargetResolver() {
            @Override public List<ExecutionTarget> resolve(ClusterState s, Object m) { return targets; }
        };
    }

    private static Stage stage(TargetResolver resolver) {
        return new Stage(0, null, List.of(), null, null, resolver, StageExecutionType.LOCAL_CANMATCH);
    }

    private static CanMatchFilter filter(String col, long lo, long hi) {
        return new CanMatchFilter(col, lo, hi);
    }

    private static ExecutionTarget target(String nodeName, int shardNum) {
        DiscoveryNode node = new DiscoveryNode(
            nodeName, nodeName,
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + shardNum),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        return new ShardExecutionTarget(node, new ShardId("idx", "_na_", shardNum));
    }

    private static void stubResponses(StreamTransportService transport, Map<String, Boolean> nodeNameToCanMatch) {
        doAnswer(inv -> {
            DiscoveryNode node = inv.getArgument(0);
            TransportResponseHandler<AnalyticsCanMatchResponse> handler = inv.getArgument(3);
            Boolean canMatch = nodeNameToCanMatch.get(node.getName());
            assertNotNull("no stub for node " + node.getName(), canMatch);
            handler.handleResponse(canMatch ? AnalyticsCanMatchResponse.YES : AnalyticsCanMatchResponse.NO);
            return null;
        }).when(transport).sendRequest(any(DiscoveryNode.class), eq(AnalyticsCanMatchAction.NAME), any(), any());
    }
}
