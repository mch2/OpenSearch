/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SqlResponseTests extends OpenSearchTestCase {

    public void testToXContentEmptyResponse() throws IOException {
        SqlResponse response = new SqlResponse(List.of(), List.of());
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"columns\":[],\"rows\":[]}", builder.toString());
    }

    public void testToXContentWithData() throws IOException {
        List<String> columns = List.of("name", "age");
        List<Object[]> rows = List.of(new Object[] { "Alice", 30 }, new Object[] { "Bob", 25 });
        SqlResponse response = new SqlResponse(columns, rows);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"columns\":[\"name\",\"age\"],\"rows\":[[\"Alice\",30],[\"Bob\",25]]}", builder.toString());
    }

    public void testToXContentWithNullValues() throws IOException {
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { null });
        SqlResponse response = new SqlResponse(List.of("col1"), rows);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"columns\":[\"col1\"],\"rows\":[[null]]}", builder.toString());
    }

    public void testStreamRoundTrip() throws Exception {
        List<String> columns = List.of("host", "status");
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { "server-1", 200 });
        rows.add(new Object[] { "server-2", 500 });
        SqlResponse original = new SqlResponse(columns, rows);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                SqlResponse copy = new SqlResponse(in);
                assertEquals(columns, copy.getColumns());
                assertEquals(2, copy.getRows().size());
                assertArrayEquals(new Object[] { "server-1", 200 }, copy.getRows().get(0));
                assertArrayEquals(new Object[] { "server-2", 500 }, copy.getRows().get(1));
            }
        }
    }
}
