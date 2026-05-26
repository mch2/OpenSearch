/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.shard;

import org.opensearch.Version;
import org.opensearch.analytics.exec.canmatch.CanMatchFilter;
import org.opensearch.analytics.exec.canmatch.CanMatchPreFilterPhase;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link ShardFragmentStageExecution#applyCanMatchFilter(List, List, String,
 * CanMatchPreFilterPhase, TimeValue)} — the static glue used by {@code materializeTasks()}.
 * Drives every short-circuit path plus the happy path and phase failure.
 */
public class ShardFragmentStageExecutionCanMatchTests extends OpenSearchTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(5);

    public void testEmptyTargetsReturnsEmpty() {
        CanMatchPreFilterPhase phase = mock(CanMatchPreFilterPhase.class);
        List<ExecutionTarget> result = ShardFragmentStageExecution.applyCanMatchFilter(
            List.of(),
            List.of(new CanMatchFilter("c", 0, 10)),
            "datafusion",
            phase,
            TIMEOUT
        );
        assertTrue(result.isEmpty());
        verify(phase, never()).filter(any(), any(), any(), any());
    }

    public void testEmptyFiltersShortCircuits() {
        CanMatchPreFilterPhase phase = mock(CanMatchPreFilterPhase.class);
        List<ExecutionTarget> targets = List.of(target("a", 0));
        List<ExecutionTarget> result = ShardFragmentStageExecution.applyCanMatchFilter(targets, List.of(), "datafusion", phase, TIMEOUT);
        assertEquals(targets, result);
        verify(phase, never()).filter(any(), any(), any(), any());
    }

    public void testNullBackendIdShortCircuits() {
        CanMatchPreFilterPhase phase = mock(CanMatchPreFilterPhase.class);
        List<ExecutionTarget> targets = List.of(target("a", 0));
        List<ExecutionTarget> result = ShardFragmentStageExecution.applyCanMatchFilter(
            targets,
            List.of(new CanMatchFilter("c", 0, 10)),
            null,
            phase,
            TIMEOUT
        );
        assertEquals(targets, result);
        verify(phase, never()).filter(any(), any(), any(), any());
    }

    public void testHappyPathReturnsPhaseResult() {
        CanMatchPreFilterPhase phase = mock(CanMatchPreFilterPhase.class);
        ExecutionTarget keep = target("keep", 0);
        ExecutionTarget drop = target("drop", 1);
        // Stub filter() to invoke the listener synchronously with only "keep".
        doAnswer(inv -> {
            ActionListener<List<ExecutionTarget>> l = inv.getArgument(3);
            l.onResponse(List.of(keep));
            return null;
        }).when(phase).filter(any(), any(byte[].class), anyString(), any());

        List<ExecutionTarget> result = ShardFragmentStageExecution.applyCanMatchFilter(
            List.of(keep, drop),
            List.of(new CanMatchFilter("col", 0, 10)),
            "datafusion",
            phase,
            TIMEOUT
        );
        assertEquals(List.of(keep), result);
    }

    public void testPhaseFailureFallsBackToAllTargets() {
        CanMatchPreFilterPhase phase = mock(CanMatchPreFilterPhase.class);
        ExecutionTarget a = target("a", 0);
        ExecutionTarget b = target("b", 1);
        doAnswer(inv -> {
            ActionListener<List<ExecutionTarget>> l = inv.getArgument(3);
            l.onFailure(new RuntimeException("transport blew up"));
            return null;
        }).when(phase).filter(any(), any(byte[].class), anyString(), any());

        List<ExecutionTarget> targets = List.of(a, b);
        List<ExecutionTarget> result = ShardFragmentStageExecution.applyCanMatchFilter(
            targets,
            List.of(new CanMatchFilter("col", 0, 10)),
            "datafusion",
            phase,
            TIMEOUT
        );
        assertEquals(targets, result);
    }

    private static ExecutionTarget target(String nodeName, int shardNum) {
        DiscoveryNode node = new DiscoveryNode(
            nodeName,
            nodeName,
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + shardNum),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        return new ShardExecutionTarget(node, new ShardId("idx", "_na_", shardNum));
    }
}
