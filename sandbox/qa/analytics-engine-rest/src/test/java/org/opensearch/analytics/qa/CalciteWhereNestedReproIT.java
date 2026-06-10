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
 * Reproduction of {@code CalciteWhereCommandIT#testFilterOnNestedFields} (the self-contained
 * nested_simple variant) on the analytics-engine route. The deep_nested variants
 * (testFilterOnNestedAndRootFields, testFilterOnComputedNestedFields) need the {@code deep_nested}
 * dataset and are deferred.
 */
public class CalciteWhereNestedReproIT extends CalciteReproTestCase {

    private static final Dataset NS = new Dataset("nested_simple", "repro_nested_simple");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), NS);
        provisioned = true;
    }

    public void testFilterOnNestedFields() throws IOException {
        Map<String, Object> result1 = executePpl("source=" + NS.indexName
            + " | where address.city = 'New york city' | fields address.city");
        verifySchema(result1, schema("address.city", "string"));
        verifyDataRows(result1, rows("New york city"));

        Map<String, Object> result2 = executePpl("source=" + NS.indexName
            + " | where address.city in ('Miami', 'san diego') | fields address.city");
        verifyDataRows(result2, rows("Miami"), rows("san diego"));
    }
}
