/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SqlTransportAction} using the test-only constructor to inject a
 * mock {@link SqlQueryService}, isolating the transport action's listener contract.
 */
public class SqlTransportActionTests extends OpenSearchTestCase {

    private SqlQueryService mockService;
    private SqlTransportAction action;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockService = mock(SqlQueryService.class);
        threadPool = new TestThreadPool(getTestName());
        action = new SqlTransportAction(mock(TransportService.class), new ActionFilters(Collections.emptySet()), mockService, threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testSuccessPathCallsOnResponse() throws Exception {
        SqlResponse expected = new SqlResponse(List.of("a"), List.<Object[]>of(new Object[] { 1 }));
        when(mockService.execute("SELECT a FROM t")).thenReturn(expected);

        AtomicReference<SqlResponse> captured = new AtomicReference<>();
        action.execute(null, new SqlRequest("SELECT a FROM t"), new ActionListener<>() {
            @Override
            public void onResponse(SqlResponse r) {
                captured.set(r);
            }

            @Override
            public void onFailure(Exception e) {
                fail("should not fail: " + e.getMessage());
            }
        });

        assertBusy(() -> assertNotNull(captured.get()));
        assertSame(expected, captured.get());
        verify(mockService).execute("SELECT a FROM t");
    }

    public void testExplainRoutesToProfile() throws Exception {
        SqlResponse expected = new SqlResponse(List.of("a"), List.of());
        when(mockService.executeWithProfile("SELECT a FROM t")).thenReturn(expected);

        AtomicReference<SqlResponse> captured = new AtomicReference<>();
        action.execute(null, new SqlRequest("SELECT a FROM t", true), new ActionListener<>() {
            @Override
            public void onResponse(SqlResponse r) {
                captured.set(r);
            }

            @Override
            public void onFailure(Exception e) {
                fail("should not fail: " + e.getMessage());
            }
        });

        assertBusy(() -> assertNotNull(captured.get()));
        verify(mockService).executeWithProfile("SELECT a FROM t");
    }

    public void testFailurePathCallsOnFailure() throws Exception {
        RuntimeException boom = new RuntimeException("SQL execution failed");
        when(mockService.execute(any(String.class))).thenThrow(boom);

        AtomicReference<Exception> captured = new AtomicReference<>();
        action.execute(null, new SqlRequest("SELECT bad"), new ActionListener<>() {
            @Override
            public void onResponse(SqlResponse r) {
                fail("should not succeed");
            }

            @Override
            public void onFailure(Exception e) {
                captured.set(e);
            }
        });

        assertBusy(() -> assertNotNull(captured.get()));
        assertSame(boom, captured.get());
    }
}
