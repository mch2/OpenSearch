/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.Map;

/**
 * Reproduction of failing JSON-builtin methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLJsonBuiltinFunctionIT} on the analytics-engine
 * route. The JSON args are literals, so any &ge;1-row source works; we reuse {@code date_formats}.
 *
 * <p>Note: PPL string literals embed double-quotes; {@link #executePpl} JSON-escapes the whole query
 * for the request body, so we write the PPL with raw {@code "} (Java {@code \"}) here.
 */
public class CalcitePPLJsonBuiltinFunctionReproIT extends CalciteReproTestCase {

    private static final Dataset SRC = new Dataset("date_formats", "repro_json_src");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), SRC);
        provisioned = true;
    }

    private String src() { return "source=" + SRC.indexName; }

    public void testJson() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval a = json('[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]'),"
            + " b = json('{\"invalid\": \"json\"') | fields a, b | head 1");
        verifySchema(actual, schema("a", "string"), schema("b", "string"));
        verifyDataRows(actual, rows("[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]", null));
    }

    public void testJsonDeleteWithDollarPrefixedPath() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval a = json_delete('{\"name\":\"alice\",\"scores\":[90,85,92]}', '$.name')"
            + " | fields a | head 1");
        verifySchema(actual, schema("a", "string"));
        verifyDataRows(actual, rows("{\"scores\":[90,85,92]}"));
    }

    public void testJsonSetWithDollarPrefixedPath() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval a = json_set('{\"name\":\"alice\",\"scores\":[90,85,92]}', '$.name', 'modified_alice')"
            + " | fields a | head 1");
        verifySchema(actual, schema("a", "string"));
        verifyDataRows(actual, rows("{\"name\":\"modified_alice\",\"scores\":[90,85,92]}"));
    }

    public void testJsonAppend() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval a = json_append('{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',"
            + " 'student', json_object(\"name\", \"Tomy\", \"rank\", 5)) | fields a | head 1");
        verifySchema(actual, schema("a", "string"));
        verifyDataRowsSome(actual, rows(
            "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2},{\"name\":\"Tomy\",\"rank\":5}]}"));
    }

    public void testJsonExtend() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval a = json_extend('{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',"
            + " 'teacher', 'Tom', 'teacher', 'Walt') | fields a | head 1");
        verifySchema(actual, schema("a", "string"));
        verifyDataRowsSome(actual, rows(
            "{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}"));
    }
}
