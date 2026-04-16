/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

/**
 * Tests for {@link StageSchedulerHelpers#walkChildrenWithSink}.
 *
 * Validates: Requirements 1.1
 */
public class StageSchedulerHelpersTests extends OpenSearchTestCase {

    /**
     * Three children all succeed — the final listener fires exactly once
     * with {@code onResponse(null)}.
     */
    public void testWalkChildrenWithSinkFansInOnAllSuccess() {
        Stage child1 = mock(Stage.class);
        Stage child2 = mock(Stage.class);
        Stage child3 = mock(Stage.class);
        List<Stage> children = List.of(child1, child2, child3);

        ExchangeSink sink = mock(ExchangeSink.class);
        ShardRequestClient client = (request, node, shardListener) -> fail("client should not be called");

        // ChildDispatcher that immediately succeeds for every child
        ChildDispatcher dispatcher = (stage, s, c, listener) -> listener.onResponse(null);

        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicReference<Exception> failureRef = new AtomicReference<>();
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                responseCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
            }
        };

        StageSchedulerHelpers.walkChildrenWithSink(children, sink, client, dispatcher, listener);

        assertTrue("listener.onResponse should have been called", responseCalled.get());
        assertNull("listener.onFailure should not have been called", failureRef.get());
    }

    /**
     * Two children: first succeeds, second fails with a RuntimeException.
     * The final listener fires with {@code onFailure} containing the exception.
     */
    public void testWalkChildrenWithSinkFansInOnFirstFailure() {
        Stage child1 = mock(Stage.class);
        Stage child2 = mock(Stage.class);
        List<Stage> children = List.of(child1, child2);

        ExchangeSink sink = mock(ExchangeSink.class);
        ShardRequestClient client = (request, node, shardListener) -> fail("client should not be called");

        RuntimeException expected = new RuntimeException("child2 failed");

        // First child succeeds, second child fails
        ChildDispatcher dispatcher = (stage, s, c, listener) -> {
            if (stage == child1) {
                listener.onResponse(null);
            } else {
                listener.onFailure(expected);
            }
        };

        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicReference<Exception> failureRef = new AtomicReference<>();
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                responseCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
            }
        };

        StageSchedulerHelpers.walkChildrenWithSink(children, sink, client, dispatcher, listener);

        assertFalse("listener.onResponse should not have been called", responseCalled.get());
        assertSame("listener.onFailure should carry the original exception", expected, failureRef.get());
    }

    /**
     * Empty children list — the listener fires immediately with
     * {@code onResponse(null)}.
     */
    public void testWalkChildrenWithSinkEmptyChildrenFiresImmediately() {
        List<Stage> children = List.of();

        ExchangeSink sink = mock(ExchangeSink.class);
        ShardRequestClient client = (request, node, shardListener) -> fail("client should not be called");
        ChildDispatcher dispatcher = (stage, s, c, listener) -> fail("dispatcher should not be called");

        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicReference<Exception> failureRef = new AtomicReference<>();
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                responseCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
            }
        };

        StageSchedulerHelpers.walkChildrenWithSink(children, sink, client, dispatcher, listener);

        assertTrue("listener.onResponse should have been called immediately", responseCalled.get());
        assertNull("listener.onFailure should not have been called", failureRef.get());
    }
}
