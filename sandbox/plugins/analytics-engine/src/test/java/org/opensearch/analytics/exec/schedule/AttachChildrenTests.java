/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.schedule;

import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AbstractStageExecution#attachChildren} — the cascade's three behaviours:
 * scheduler dispatch on all-SUCCEEDED, direct propagation on FAILED / CANCELLED,
 * and the metadata channel that ferries each child's {@code publishedMetadata}
 * to the parent's {@code consumeChildMetadata} before scheduling.
 */
public class AttachChildrenTests extends OpenSearchTestCase {

    public void testSchedulesParentOnceAllChildrenSucceed() {
        RecordingParent parent = new RecordingParent(0);
        FakeChild childA = new FakeChild(1);
        FakeChild childB = new FakeChild(2);

        AtomicReference<AbstractStageExecution> scheduled = new AtomicReference<>();
        Consumer<AbstractStageExecution> scheduler = scheduled::set;

        parent.attachChildren(List.of(childA, childB), scheduler);
        childA.fireSucceeded();
        assertNull("not scheduled until last child succeeds", scheduled.get());
        childB.fireSucceeded();
        assertSame("parent scheduled after all-children-SUCCEEDED", parent, scheduled.get());
    }

    public void testHandsOffPublishedMetadataBeforeScheduling() {
        RecordingParent parent = new RecordingParent(0);
        FakeChild childA = new FakeChild(7, "broadcast-bytes-A");
        FakeChild childB = new FakeChild(8, "stats-from-B");
        FakeChild childC = new FakeChild(9, null);  // publishes nothing

        AtomicReference<AbstractStageExecution> scheduled = new AtomicReference<>();
        Consumer<AbstractStageExecution> scheduler = stage -> {
            // Verify the metadata handoff happened BEFORE this scheduler runs.
            assertNotNull("metadata should be handed off before scheduling", parent.consumedMetadata);
            assertEquals("broadcast-bytes-A", parent.consumedMetadata.get(7));
            assertEquals("stats-from-B", parent.consumedMetadata.get(8));
            assertFalse("null-publishing child must not appear in map", parent.consumedMetadata.containsKey(9));
            scheduled.set(stage);
        };

        parent.attachChildren(List.of(childA, childB, childC), scheduler);
        childA.fireSucceeded();
        childB.fireSucceeded();
        childC.fireSucceeded();
        assertNotNull("scheduler should have run", parent.consumedMetadata);
    }

    public void testFailedChildPropagatesDirectlyToParent() {
        RecordingParent parent = new RecordingParent(0);
        FakeChild failing = new FakeChild(1);
        failing.failure = new RuntimeException("kaboom");

        Consumer<AbstractStageExecution> scheduler = stage -> fail("must NOT schedule on child failure");

        parent.attachChildren(List.of(failing), scheduler);
        failing.fire(StageExecution.State.FAILED);

        assertSame("parent.failWithCause invoked with child's failure", failing.failure, parent.getFailure());
        assertEquals("parent reached FAILED terminal", StageExecution.State.FAILED, parent.getState());
        assertNull("metadata channel not invoked on failure path", parent.consumedMetadata);
    }

    /**
     * Early-termination contract: when a parent cancels its own child (e.g. coordinator-side
     * LIMIT satisfied, stop the shard stream), the child transitions to CANCELLED. The
     * cascade must NOT propagate that cancel back to the parent — the parent is the one
     * who issued the cancel and must stay RUNNING.
     */
    public void testCancelledChildIsNotPropagatedToParent() {
        RecordingParent parent = new RecordingParent(0);
        FakeChild cancelled = new FakeChild(5);  // no failure recorded

        Consumer<AbstractStageExecution> scheduler = stage -> fail("must NOT schedule on child cancellation");

        parent.attachChildren(List.of(cancelled), scheduler);
        cancelled.fire(StageExecution.State.CANCELLED);

        assertEquals("parent must stay CREATED — child CANCELLED is not propagated", StageExecution.State.CREATED, parent.getState());
        assertNull("failWithCause must not fire on child cancellation", parent.getFailure());
        assertNull("metadata channel not invoked on cancellation path", parent.consumedMetadata);
    }

    /**
     * Sibling-cancel sweep: when one child fails and the parent transitions to FAILED,
     * any siblings still running must be cancelled so they don't keep producing into a
     * sink whose owner has terminated.
     */
    public void testSiblingsAreCancelledWhenParentReachesFailedTerminal() {
        FakeChild failing = new FakeChild(1);
        failing.failure = new RuntimeException("kaboom");
        FakeChild stillRunning = new FakeChild(2);
        FakeChild alreadyDone = new FakeChild(3, null, StageExecution.State.SUCCEEDED);

        RecordingParent parent = new RecordingParent(99);
        Consumer<AbstractStageExecution> scheduler = stage -> {};

        parent.attachChildren(List.of(failing, stillRunning, alreadyDone), scheduler);
        failing.fire(StageExecution.State.FAILED);

        assertEquals("parent must reach FAILED from child failure", StageExecution.State.FAILED, parent.getState());
        assertNotNull("still-running sibling must have been cancelled", stillRunning.cancelReason);
        assertNull("already-terminal sibling must not be re-cancelled", alreadyDone.cancelReason);
    }

    /**
     * Eager (streaming) parents must be scheduled as soon as the first child transitions
     * to RUNNING — they need to run concurrently with their children's feeds (e.g. a
     * streaming reduce whose drain pulls native output while children push batches).
     * Waiting for all-children-SUCCEEDED would deadlock on a bounded input mpsc.
     */
    public void testEagerParentSchedulesOnFirstChildRunning() {
        RecordingParent parent = new RecordingParent(0, true);
        FakeChild childA = new FakeChild(1, null, StageExecution.State.CREATED);
        FakeChild childB = new FakeChild(2, null, StageExecution.State.CREATED);

        AtomicReference<AbstractStageExecution> scheduled = new AtomicReference<>();
        Consumer<AbstractStageExecution> scheduler = scheduled::set;

        parent.attachChildren(List.of(childA, childB), scheduler);

        assertNull("not scheduled until any child enters RUNNING", scheduled.get());
        childA.fire(StageExecution.State.RUNNING);
        assertSame("eager parent scheduled on first child RUNNING", parent, scheduled.get());

        // Subsequent RUNNING transitions on other children must not re-schedule.
        scheduled.set(null);
        childB.fire(StageExecution.State.RUNNING);
        assertNull("subsequent child RUNNING must not re-schedule", scheduled.get());
    }

    /**
     * Per-input EOF hook fires on every child SUCCEEDED, regardless of scheduling mode.
     * Backends without per-child resources inherit the default {@code closeChildInput}
     * no-op; this test guards against re-introducing an eager-mode gate that would
     * silently drop the signal for a future buffered multi-input backend.
     */
    public void testCloseChildInputFiresOnEveryChildSucceededRegardlessOfMode() {
        RecordingParent defaultParent = new RecordingParent(0);
        FakeChild a = new FakeChild(11);
        FakeChild b = new FakeChild(22);
        defaultParent.attachChildren(List.of(a, b), stage -> {});
        a.fireSucceeded();
        b.fireSucceeded();
        assertEquals("closeChildInput fired for every child id in default mode", List.of(11, 22), defaultParent.closedChildIds);

        RecordingParent eagerParent = new RecordingParent(0, true);
        FakeChild c = new FakeChild(33, null, StageExecution.State.CREATED);
        eagerParent.attachChildren(List.of(c), stage -> {});
        c.fire(StageExecution.State.RUNNING);
        c.fire(StageExecution.State.SUCCEEDED);
        assertEquals("closeChildInput fired in eager mode too", List.of(33), eagerParent.closedChildIds);
    }

    /**
     * Default (non-streaming) parents keep today's contract: scheduled only when all
     * children SUCCEEDED. A child reaching RUNNING must not trigger the parent.
     */
    public void testDefaultParentDoesNotScheduleOnChildRunning() {
        RecordingParent parent = new RecordingParent(0);
        // default schedulesEagerly() == false
        FakeChild child = new FakeChild(1, null, StageExecution.State.CREATED);

        AtomicReference<AbstractStageExecution> scheduled = new AtomicReference<>();
        Consumer<AbstractStageExecution> scheduler = scheduled::set;

        parent.attachChildren(List.of(child), scheduler);
        child.fire(StageExecution.State.RUNNING);

        assertNull("default-mode parent must NOT schedule on child RUNNING", scheduled.get());
        child.fire(StageExecution.State.SUCCEEDED);
        assertSame("default-mode parent scheduled on all-SUCCEEDED", parent, scheduled.get());
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private static Stage mockStage(int stageId) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(stageId);
        return stage;
    }

    /**
     * Minimal child stub extending {@link AbstractStageExecution}. Constructor seeds the
     * base state to {@link State#RUNNING} so the cascade observes the same default as the
     * old hand-rolled FakeChild. Tests that need a different pre-attach state pass it via
     * the two-arg constructor; tests then drive subsequent transitions via {@link #fire}.
     */
    private static final class FakeChild extends AbstractStageExecution {
        private final Object metadata;
        Exception failure;
        String cancelReason;

        FakeChild(int stageId) {
            this(stageId, null, State.RUNNING);
        }

        FakeChild(int stageId, Object metadata) {
            this(stageId, metadata, State.RUNNING);
        }

        FakeChild(int stageId, Object metadata, State initialState) {
            super(mockStage(stageId), "test-query", List.of(), mock(AnalyticsQueryTask.class));
            this.metadata = metadata;
            // Seed the base state. CREATED is the natural default — no listener fired —
            // anything else transitions explicitly so getState() agrees with the cascade.
            if (initialState != State.CREATED) {
                transitionTo(initialState);
            }
        }

        void fireSucceeded() {
            fire(State.SUCCEEDED);
        }

        void fire(State terminal) {
            if (terminal == State.FAILED && failure != null) {
                captureFailure(failure);
            }
            transitionTo(terminal);
        }

        @Override
        public void cancel(String reason) {
            cancelReason = reason;
            super.cancel(reason);
        }

        @Override
        protected Object publishedMetadata() {
            return metadata;
        }

        @Override
        protected List<StageTask> materializeTasks() {
            return List.of();
        }
    }

    /**
     * Parent stub extending {@link AbstractStageExecution}: records the eager-mode flag,
     * the per-child-EOF signal, and the metadata payload the cascade hands off before
     * scheduling. Failure / cancellation paths drive real {@code failWithCause} via the
     * inherited base, so the sibling-cancel sweep fires off the real listener loop.
     */
    private static final class RecordingParent extends AbstractStageExecution {
        private final boolean eager;
        final List<Integer> closedChildIds = new ArrayList<>();
        final AtomicInteger eagerScheduleInvocations = new AtomicInteger();
        Map<Integer, Object> consumedMetadata;

        RecordingParent(int stageId) {
            this(stageId, false);
        }

        RecordingParent(int stageId, boolean eager) {
            super(mockStage(stageId), "test-query", List.of(), mock(AnalyticsQueryTask.class));
            this.eager = eager;
        }

        @Override
        protected boolean schedulesEagerly() {
            return eager;
        }

        @Override
        protected void closeChildInput(int childStageId) {
            closedChildIds.add(childStageId);
        }

        @Override
        protected void consumeChildMetadata(Map<Integer, Object> metadataByChildStageId) {
            consumedMetadata = new HashMap<>(metadataByChildStageId);
        }

        @Override
        protected List<StageTask> materializeTasks() {
            return List.of();
        }
    }
}
