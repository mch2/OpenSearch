/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AbstractStageExecution}. Uses a minimal test-only
 * concrete subclass ({@link TestStageExecution}) that adds no behaviour
 * beyond what the base class provides.
 *
 * Validates: Requirements 1.3, 1.4, 1.5, 1.6, 1.7
 */
public class AbstractStageExecutionTests extends OpenSearchTestCase {

    // ─── test-only concrete subclass ────────────────────────────────────

    private static final class TestStageExecution extends AbstractStageExecution {

        TestStageExecution(Stage stage) {
            super(stage);
        }

        /** Expose protected helper for test access. */
        public void doTransitionTo(StageExecution.State target) {
            transitionTo(target);
        }

        /** Expose protected helper for test access. */
        public void doCaptureFailure(Exception e) {
            captureFailure(e);
        }

        @Override
        public void start() {
            // no-op
        }

        @Override
        public void cancel(String reason) {
            // no-op
        }
    }

    // ─── helpers ────────────────────────────────────────────────────────

    private static Stage mockStage(int stageId) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(stageId);
        return stage;
    }

    // ─── tests ─────────────────────────────────────────────────────────

    /**
     * A freshly constructed instance must report {@link StageExecution.State#CREATED}.
     *
     * Validates: Requirements 1.3
     */
    public void testInitialStateIsCreated() {
        TestStageExecution exec = new TestStageExecution(mockStage(0));
        assertEquals(StageExecution.State.CREATED, exec.getState());
    }

    /**
     * After adding a listener and transitioning to RUNNING, the listener
     * must receive {@code (CREATED, RUNNING)}.
     *
     * Validates: Requirements 1.4
     */
    public void testTransitionToFiresListenersWithFromAndTo() {
        TestStageExecution exec = new TestStageExecution(mockStage(1));

        List<StageExecution.State> fromCapture = new ArrayList<>();
        List<StageExecution.State> toCapture = new ArrayList<>();
        exec.addStateListener((from, to) -> {
            fromCapture.add(from);
            toCapture.add(to);
        });

        exec.doTransitionTo(StageExecution.State.RUNNING);

        assertEquals(1, fromCapture.size());
        assertEquals(StageExecution.State.CREATED, fromCapture.get(0));
        assertEquals(StageExecution.State.RUNNING, toCapture.get(0));
    }

    /**
     * Transitioning to the current state is a no-op — no listeners fire.
     *
     * Validates: Requirements 1.4
     */
    public void testTransitionToNoOpsOnSameState() {
        TestStageExecution exec = new TestStageExecution(mockStage(2));

        List<StageExecution.State> toCapture = new ArrayList<>();
        exec.addStateListener((from, to) -> toCapture.add(to));

        // Transition to CREATED (the current state) — should be a no-op
        exec.doTransitionTo(StageExecution.State.CREATED);

        assertTrue("Listener should not fire on same-state transition", toCapture.isEmpty());
    }

    /**
     * Three listeners registered in order must all fire in registration order.
     *
     * Validates: Requirements 1.4, 1.5
     */
    public void testMultipleListenersAllFireInRegistrationOrder() {
        TestStageExecution exec = new TestStageExecution(mockStage(3));

        List<Integer> callOrder = new ArrayList<>();
        exec.addStateListener((from, to) -> callOrder.add(1));
        exec.addStateListener((from, to) -> callOrder.add(2));
        exec.addStateListener((from, to) -> callOrder.add(3));

        exec.doTransitionTo(StageExecution.State.RUNNING);

        assertEquals(List.of(1, 2, 3), callOrder);
    }

    /**
     * If the first listener throws, the second and third must still fire,
     * the transition must complete, and a WARN must be logged.
     *
     * Validates: Requirements 1.4
     */
    public void testListenerExceptionIsSwallowedAndSubsequentListenersStillFire() {
        TestStageExecution exec = new TestStageExecution(mockStage(4));

        List<Integer> callOrder = new ArrayList<>();
        exec.addStateListener((from, to) -> {
            throw new RuntimeException("boom");
        });
        exec.addStateListener((from, to) -> callOrder.add(2));
        exec.addStateListener((from, to) -> callOrder.add(3));

        exec.doTransitionTo(StageExecution.State.RUNNING);

        assertEquals("Second and third listeners must still fire", List.of(2, 3), callOrder);
        assertEquals("State must have transitioned despite listener exception", StageExecution.State.RUNNING, exec.getState());
    }

    /**
     * Calling {@code captureFailure} twice with different exceptions must
     * retain only the first. {@code getFailure()} returns the first.
     *
     * Validates: Requirements 1.3
     */
    public void testCaptureFailureIsIdempotent() {
        TestStageExecution exec = new TestStageExecution(mockStage(5));

        Exception first = new RuntimeException("first");
        Exception second = new RuntimeException("second");
        exec.doCaptureFailure(first);
        exec.doCaptureFailure(second);

        assertSame("getFailure() must return the first captured exception", first, exec.getFailure());
    }

    /**
     * When {@code captureFailure} is never called, {@code getFailure()}
     * returns null — even after transitioning to CANCELLED.
     *
     * Validates: Requirements 1.3
     */
    public void testGetFailureNullWhenNoFailureCaptured() {
        TestStageExecution exec = new TestStageExecution(mockStage(6));

        exec.doTransitionTo(StageExecution.State.CANCELLED);

        assertNull("getFailure() must be null when captureFailure was never called", exec.getFailure());
    }
}
