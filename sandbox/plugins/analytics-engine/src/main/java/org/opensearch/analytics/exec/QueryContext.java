/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.tasks.Task;

import java.util.concurrent.Executor;

/**
 * Immutable per-query configuration. Created once in {@link DefaultPlanExecutor#execute}
 * and threaded through execution components as a read-only reference.
 *
 * <p>Separate from {@link QueryState}, which holds mutable per-query state.
 * Components that only read configuration take {@code QueryContext};
 * components that mutate shared state also take {@code QueryState}.
 *
 * @opensearch.internal
 */
public record QueryContext(QueryDAG dag, Executor searchExecutor, Task parentTask) {

    public String queryId() {
        return dag.queryId();
    }

    /** Creates a test context with a synchronous executor. */
    public static QueryContext forTest(QueryDAG dag, Task parentTask) {
        return new QueryContext(dag, Runnable::run, parentTask);
    }

    /** Creates a test context with a stub DAG. */
    public static QueryContext forTest(String queryId, Task parentTask) {
        return new QueryContext(new QueryDAG(queryId, null), Runnable::run, parentTask);
    }
}
