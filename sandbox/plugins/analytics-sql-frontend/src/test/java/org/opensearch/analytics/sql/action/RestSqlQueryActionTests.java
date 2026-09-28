/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql.action;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

public class RestSqlQueryActionTests extends OpenSearchTestCase {

    private final RestSqlQueryAction action = new RestSqlQueryAction();

    public void testName() {
        assertEquals("analytics_sql_query", action.getName());
    }

    public void testRoutes() {
        assertEquals(2, action.routes().size());
        assertEquals(RestRequest.Method.POST, action.routes().get(0).getMethod());
        assertEquals("/_analytics/sql", action.routes().get(0).getPath());
        assertEquals(RestRequest.Method.POST, action.routes().get(1).getMethod());
        assertEquals("/_analytics/sql/_explain", action.routes().get(1).getPath());
    }

    public void testPrepareRequestHappyPath() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_analytics/sql")
            .withContent(new BytesArray("{\"query\":\"SELECT 1\"}"), XContentType.JSON)
            .build();
        // Should not throw; a consumer is returned (execution deferred to the node client).
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestMissingQuery() {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_analytics/sql")
            .withContent(new BytesArray("{\"other\":\"value\"}"), XContentType.JSON)
            .build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertTrue(ex.getMessage().contains("query"));
    }

    public void testPrepareRequestEmptyQuery() {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_analytics/sql")
            .withContent(new BytesArray("{\"query\":\"\"}"), XContentType.JSON)
            .build();
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }
}
