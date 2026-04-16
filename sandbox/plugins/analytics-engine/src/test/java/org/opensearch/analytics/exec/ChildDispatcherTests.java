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
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;

/**
 * Validates: Requirements 4.1
 */
public class ChildDispatcherTests extends OpenSearchTestCase {

    /**
     * Verify that a lambda can be assigned to {@link ChildDispatcher} and invoked.
     */
    public void testFunctionalInterface() {
        AtomicBoolean invoked = new AtomicBoolean(false);

        ChildDispatcher dispatcher = (stage, sink, client, listener) -> {
            invoked.set(true);
            listener.onResponse(null);
        };

        Stage stage = new Stage(0, null, List.of(), null, StageExecutionType.LOCAL);
        ExchangeSink sink = mock(ExchangeSink.class);
        ShardRequestClient client = mock(ShardRequestClient.class);

        dispatcher.dispatch(stage, sink, client, new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                // expected
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail");
            }
        });

        assertTrue("Lambda should have been invoked", invoked.get());
    }
}
