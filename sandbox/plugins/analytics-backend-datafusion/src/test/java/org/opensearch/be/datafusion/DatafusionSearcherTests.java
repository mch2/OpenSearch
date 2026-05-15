/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Unit tests for {@link DatafusionSearcher#peelJdkAsyncWrappers}.
 *
 * <p>The peel helper exists so that the IOException raised by the searcher
 * surfaces the *actual* native error message to the user instead of the
 * generic "java.util.concurrent.CompletionException: ..." stack hop. Without
 * peeling, a planning error from the native side reaches the REST layer
 * three layers deep and the meaningful message is buried.
 */
public class DatafusionSearcherTests extends OpenSearchTestCase {

    public void testPeelStripsCompletionException() {
        IOException cause = new IOException("native planner: random expected zero argument");
        Throwable peeled = DatafusionSearcher.peelJdkAsyncWrappers(new CompletionException(cause));
        assertSame(cause, peeled);
    }

    public void testPeelStripsExecutionException() {
        IllegalStateException cause = new IllegalStateException("session closed");
        Throwable peeled = DatafusionSearcher.peelJdkAsyncWrappers(new ExecutionException(cause));
        assertSame(cause, peeled);
    }

    public void testPeelStripsNestedJdkWrappers() {
        IllegalArgumentException cause = new IllegalArgumentException("bad plan");
        Throwable peeled = DatafusionSearcher.peelJdkAsyncWrappers(new ExecutionException(new CompletionException(cause)));
        assertSame(cause, peeled);
    }

    public void testPeelLeavesNonWrapperUntouched() {
        IOException raw = new IOException("direct failure");
        assertSame(raw, DatafusionSearcher.peelJdkAsyncWrappers(raw));
    }

    public void testPeelStopsAtCauselessWrapper() {
        // CompletionException with no cause — pathological but defined; do not infinite-loop or NPE.
        CompletionException causeless = new CompletionException(null);
        assertSame(causeless, DatafusionSearcher.peelJdkAsyncWrappers(causeless));
    }
}
