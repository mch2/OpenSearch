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
 * Reproduction of failing {@code concat}/{@code regexp} methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalciteTextFunctionIT} on the analytics-engine route.
 * Uses {@code strings} (3 rows: hello, world, helloworld).
 */
public class CalciteTextFunctionReproIT extends CalciteReproTestCase {

    private static final Dataset STRINGS = new Dataset("strings", "repro_text_strings");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), STRINGS);
        provisioned = true;
    }

    private String src() { return "source=" + STRINGS.indexName; }

    public void testConcat() throws IOException {
        Map<String, Object> r = executePpl(src()
            + " | eval f=concat(name, 'there', 'all', '!') | fields f");
        verifySchema(r, schema("f", "string"));
        verifyDataRows(r, rows("hellothereall!"), rows("worldthereall!"), rows("helloworldthereall!"));
    }

    public void testRegexp() throws IOException {
        // Calcite path: name regexp 'pattern' yields a boolean column.
        Map<String, Object> r1 = executePpl(src() + " | eval f=name regexp 'hello' | fields f");
        verifySchema(r1, schema("f", "boolean"));
        verifyDataRows(r1, rows(true), rows(false), rows(true));

        Map<String, Object> r2 = executePpl(src() + " | eval f=name regexp '.*' | fields f");
        verifySchema(r2, schema("f", "boolean"));
        verifyDataRows(r2, rows(true), rows(true), rows(true));
    }
}
