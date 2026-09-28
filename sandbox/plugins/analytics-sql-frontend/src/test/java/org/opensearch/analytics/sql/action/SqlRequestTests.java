/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class SqlRequestTests extends OpenSearchTestCase {

    public void testValidateRejectsNullSql() {
        SqlRequest request = new SqlRequest((String) null);
        assertNotNull(request.validate());
        assertTrue(request.validate().getMessage().contains("sql is missing or empty"));
    }

    public void testValidateRejectsEmptySql() {
        SqlRequest request = new SqlRequest("");
        assertNotNull(request.validate());
    }

    public void testValidatePassesForNonEmptySql() {
        SqlRequest request = new SqlRequest("SELECT 1");
        assertNull(request.validate());
    }

    public void testExplainDefaultsFalse() {
        assertFalse(new SqlRequest("SELECT 1").isExplain());
    }

    public void testStreamRoundTrip() throws Exception {
        SqlRequest original = new SqlRequest("SELECT a FROM t WHERE b > 1", true);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                SqlRequest copy = new SqlRequest(in);
                assertEquals(original.getSql(), copy.getSql());
                assertEquals(original.isExplain(), copy.isExplain());
            }
        }
    }
}
