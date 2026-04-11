/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * Tests for {@link ShardFilterPhase} IDENTITY constant and
 * {@link TerminationDecider} DISPATCH_ALL constant.
 *
 * Validates: Requirements 1.1, 1.5, 2.1, 2.5
 */
public class ShardFilterPhaseAndTerminationDeciderTests extends OpenSearchTestCase {

    /**
     * ShardFilterPhase.IDENTITY returns the exact same list reference — no copy, no reorder.
     * Validates: Requirements 1.1, 1.5
     */
    public void testShardFilterPhaseIdentityReturnsInput() {
        int numTargets = randomIntBetween(1, 10);
        List<ShardTarget> targets = new ArrayList<>();
        for (int i = 0; i < numTargets; i++) {
            ShardId shardId = new ShardId(new Index(randomAlphaOfLength(8), "_na_"), i);
            DiscoveryNode node = mock(DiscoveryNode.class);
            targets.add(new ShardTarget(shardId, node));
        }

        Stage stage = mock(Stage.class);
        List<ShardTarget> result = ShardFilterPhase.IDENTITY.filter(targets, stage);

        assertSame("IDENTITY filter must return the exact same list reference", targets, result);
    }

    /**
     * TerminationDecider.DISPATCH_ALL.initialBatchSize returns totalTargets unchanged.
     * Validates: Requirements 2.1, 2.5
     */
    public void testTerminationDeciderDispatchAllReturnsTotal() {
        int totalTargets = randomIntBetween(1, 1000);
        int batchSize = TerminationDecider.DISPATCH_ALL.initialBatchSize(totalTargets);

        assertEquals("DISPATCH_ALL.initialBatchSize must return totalTargets", totalTargets, batchSize);
    }

    /**
     * TerminationDecider.DISPATCH_ALL.shouldTerminate always returns false,
     * regardless of sink state, completedTasks, or totalTasks.
     * Validates: Requirements 2.1, 2.5
     */
    public void testTerminationDeciderDispatchAllNeverTerminates() {
        ExchangeSink sink = mock(ExchangeSink.class);
        int completedTasks = randomIntBetween(0, 100);
        int totalTasks = randomIntBetween(completedTasks, 200);

        boolean result = TerminationDecider.DISPATCH_ALL.shouldTerminate(sink, completedTasks, totalTasks);

        assertFalse("DISPATCH_ALL.shouldTerminate must always return false", result);
    }
}
