/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.rel.ShuffleImpl;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link Stage#isShuffleWrite()} and {@link Stage#isShuffleRead()} helpers.
 *
 * Validates: Requirements 1.2, 1.3
 */
public class StageTests extends OpenSearchTestCase {

    private static RelNode mockFragment() {
        RelNode fragment = mock(RelNode.class);
        when(fragment.getInputs()).thenReturn(List.of());
        return fragment;
    }

    /**
     * A stage with HASH_DISTRIBUTED exchange returns true for isShuffleWrite().
     *
     * Validates: Requirements 1.2
     */
    public void testIsShuffleWriteTrueForHashDistributed() {
        ExchangeInfo hashExchange = new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, List.of(0), 4);
        Stage stage = new Stage(1, mockFragment(), List.of(), hashExchange, StageExecutionType.DATA_NODE);

        assertTrue("Stage with HASH_DISTRIBUTED exchange should be a shuffle write", stage.isShuffleWrite());
    }

    /**
     * A stage with SINGLETON exchange returns false for isShuffleWrite().
     *
     * Validates: Requirements 1.2
     */
    public void testIsShuffleWriteFalseForSingleton() {
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage stage = new Stage(1, mockFragment(), List.of(), singletonExchange, StageExecutionType.DATA_NODE);

        assertFalse("Stage with SINGLETON exchange should not be a shuffle write", stage.isShuffleWrite());
    }

    /**
     * A stage with null exchange returns false for isShuffleWrite().
     *
     * Validates: Requirements 1.2
     */
    public void testIsShuffleWriteFalseForNullExchange() {
        Stage stage = new Stage(1, mockFragment(), List.of(), null, StageExecutionType.LOCAL);

        assertFalse("Stage with null exchange should not be a shuffle write", stage.isShuffleWrite());
    }

    /**
     * A LOCAL stage with a shuffle-write child returns true for isShuffleRead().
     *
     * Validates: Requirements 1.3
     */
    public void testIsShuffleReadWhenChildIsShuffleWrite() {
        ExchangeInfo hashExchange = new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, List.of(0), 4);
        Stage childStage = new Stage(2, mockFragment(), List.of(), hashExchange, StageExecutionType.DATA_NODE);

        Stage parentStage = new Stage(1, mockFragment(), List.of(childStage), null, StageExecutionType.LOCAL);

        assertTrue("LOCAL stage with a shuffle-write child should be a shuffle read", parentStage.isShuffleRead());
    }

    /**
     * A DATA_NODE stage with a shuffle-write child returns false for isShuffleRead()
     * because isShuffleRead requires LOCAL execution type.
     *
     * Validates: Requirements 1.3
     */
    public void testIsShuffleReadRequiresLocalExecutionType() {
        ExchangeInfo hashExchange = new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, List.of(0), 4);
        Stage childStage = new Stage(2, mockFragment(), List.of(), hashExchange, StageExecutionType.DATA_NODE);

        Stage parentStage = new Stage(1, mockFragment(), List.of(childStage), null, StageExecutionType.DATA_NODE);

        assertFalse("DATA_NODE stage should not be a shuffle read even with shuffle-write child", parentStage.isShuffleRead());
    }

    /**
     * A LOCAL stage with no children returns false for isShuffleRead().
     *
     * Validates: Requirements 1.3
     */
    public void testIsShuffleReadFalseForNoChildren() {
        Stage stage = new Stage(1, mockFragment(), List.of(), null, StageExecutionType.LOCAL);

        assertFalse("LOCAL stage with no children should not be a shuffle read", stage.isShuffleRead());
    }

    /**
     * A LOCAL stage whose children are all non-shuffle-write returns false for isShuffleRead().
     *
     * Validates: Requirements 1.3
     */
    public void testIsShuffleReadFalseWhenNoChildIsShuffleWrite() {
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childStage = new Stage(2, mockFragment(), List.of(), singletonExchange, StageExecutionType.DATA_NODE);

        Stage parentStage = new Stage(1, mockFragment(), List.of(childStage), null, StageExecutionType.LOCAL);

        assertFalse("LOCAL stage with only non-shuffle-write children should not be a shuffle read", parentStage.isShuffleRead());
    }

    /**
     * Tests for {@link Stage#isBroadcastWrite()} and {@link Stage#isBroadcastRead()} helpers.
     *
     * Validates: Requirements 1.1, 1.2, 1.4
     */
    public void testBroadcastWriteAndReadHelpers() {
        // isBroadcastWrite: true for BROADCAST_DISTRIBUTED exchange
        ExchangeInfo broadcastExchange = new ExchangeInfo(RelDistribution.Type.BROADCAST_DISTRIBUTED, null, List.of());
        Stage broadcastWriter = new Stage(10, mockFragment(), List.of(), broadcastExchange, StageExecutionType.DATA_NODE);
        assertTrue("Stage with BROADCAST_DISTRIBUTED exchange should be a broadcast write", broadcastWriter.isBroadcastWrite());
        assertFalse("Broadcast write stage should not be a shuffle write", broadcastWriter.isShuffleWrite());

        // isBroadcastWrite: false for null exchange
        Stage noExchange = new Stage(11, mockFragment(), List.of(), null, StageExecutionType.LOCAL);
        assertFalse("Stage with null exchange should not be a broadcast write", noExchange.isBroadcastWrite());

        // isBroadcastWrite: false for HASH_DISTRIBUTED exchange
        ExchangeInfo hashExchange = new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, List.of(0), 4);
        Stage shuffleWriter = new Stage(12, mockFragment(), List.of(), hashExchange, StageExecutionType.DATA_NODE);
        assertFalse("Stage with HASH_DISTRIBUTED exchange should not be a broadcast write", shuffleWriter.isBroadcastWrite());

        // isBroadcastRead: true when any child is a broadcast write — no executionType constraint
        Stage dataNodeReader = new Stage(20, mockFragment(), List.of(broadcastWriter), null, StageExecutionType.DATA_NODE);
        assertTrue("DATA_NODE stage with broadcast-write child should be a broadcast read", dataNodeReader.isBroadcastRead());

        Stage localReader = new Stage(21, mockFragment(), List.of(broadcastWriter), null, StageExecutionType.LOCAL);
        assertTrue("LOCAL stage with broadcast-write child should also be a broadcast read", localReader.isBroadcastRead());

        // isBroadcastRead: false when no child is a broadcast write
        Stage noMatchReader = new Stage(22, mockFragment(), List.of(shuffleWriter), null, StageExecutionType.DATA_NODE);
        assertFalse("Stage with only shuffle-write children should not be a broadcast read", noMatchReader.isBroadcastRead());

        // Mixed-input: a stage MAY satisfy both isShuffleRead() and isBroadcastRead()
        Stage mixedParent = new Stage(30, mockFragment(), List.of(shuffleWriter, broadcastWriter), null, StageExecutionType.LOCAL);
        assertTrue("Mixed-input stage should be a shuffle read", mixedParent.isShuffleRead());
        assertTrue("Mixed-input stage should also be a broadcast read", mixedParent.isBroadcastRead());

        // isBroadcastRead: false for no children
        Stage leaf = new Stage(40, mockFragment(), List.of(), null, StageExecutionType.DATA_NODE);
        assertFalse("Stage with no children should not be a broadcast read", leaf.isBroadcastRead());
    }
}
