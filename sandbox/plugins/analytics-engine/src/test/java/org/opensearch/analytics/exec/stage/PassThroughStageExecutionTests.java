/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PassThroughStageExecution}.
 */
public class PassThroughStageExecutionTests extends OpenSearchTestCase {

    private static Stage mockStage(int stageId) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(stageId);
        return stage;
    }

    public void testStartTransitionsToSucceeded() {
        ExchangeSink sink = new SimpleExchangeSink();
        PassThroughStageExecution exec = new PassThroughStageExecution(mockStage(0), sink);

        List<StageExecution.State> states = new ArrayList<>();
        exec.addStateListener((from, to) -> states.add(to));

        exec.start();

        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        assertEquals(2, states.size());
        assertEquals(StageExecution.State.RUNNING, states.get(0));
        assertEquals(StageExecution.State.SUCCEEDED, states.get(1));
    }

    public void testCancelTransitionsToCancelled() {
        ExchangeSink sink = new SimpleExchangeSink();
        PassThroughStageExecution exec = new PassThroughStageExecution(mockStage(1), sink);

        exec.cancel("test cancel");

        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertNull(exec.getFailure());
    }

    public void testSinkIsSameInstancePassedIn() {
        SimpleExchangeSink sink = new SimpleExchangeSink();
        PassThroughStageExecution exec = new PassThroughStageExecution(mockStage(2), sink);

        assertSame(sink, exec.sink());
    }
}
