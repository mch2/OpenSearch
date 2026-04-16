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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Shared static helpers used by {@link StageScheduler} implementations.
 * Package-private, pure static, no instances.
 *
 * @opensearch.internal
 */
final class StageSchedulerHelpers {

    private StageSchedulerHelpers() {}

    /**
     * Fan out dispatch to N children concurrently, feeding each child the given sink,
     * and fan back in via a completion counter. Fires the listener once when all
     * children complete (success) or when the first failure propagates.
     *
     * <p>Moved verbatim from {@code StageExecutor.walkChildren}.
     */
    static void walkChildrenWithSink(
        List<Stage> children,
        ExchangeSink sink,
        ShardRequestClient client,
        ChildDispatcher childDispatcher,
        ActionListener<Void> listener
    ) {
        if (children.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        AtomicInteger remaining = new AtomicInteger(children.size());
        AtomicReference<Exception> failure = new AtomicReference<>();
        for (Stage child : children) {
            childDispatcher.dispatch(child, sink, client, new ActionListener<>() {
                @Override
                public void onResponse(Void v) {
                    if (remaining.decrementAndGet() == 0) {
                        Exception e = failure.get();
                        if (e != null) {
                            listener.onFailure(e);
                        } else {
                            listener.onResponse(null);
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    failure.compareAndSet(null, e);
                    if (remaining.decrementAndGet() == 0) {
                        listener.onFailure(failure.get());
                    }
                }
            });
        }
    }
}
