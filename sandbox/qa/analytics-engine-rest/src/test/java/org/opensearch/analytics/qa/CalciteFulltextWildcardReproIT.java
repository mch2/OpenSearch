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
 * Reproduction of the wildcard full-text tests from upstream {@code CalciteMultiMatchIT},
 * {@code CalciteQueryStringIT}, {@code CalciteSimpleQueryStringIT} on the analytics-engine route.
 * Uses the {@code beer} (stackexchange) dataset. Each exercises a field-name wildcard
 * (e.g. {@code ['T*']}, {@code ['*Date']}) inside multi_match / query_string / simple_query_string.
 */
public class CalciteFulltextWildcardReproIT extends CalciteReproTestCase {

    private static final Dataset BEER = new Dataset("beer", "repro_beer");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), BEER);
        provisioned = true;
    }

    private int total(String ppl) throws IOException {
        Map<String, Object> r = executePpl(ppl);
        Object t = r.get("total");
        if (t instanceof Number) {
            return ((Number) t).intValue();
        }
        // Real opensearch-sql response has no "total"; fall back to datarows count.
        return dataRowsOf(r).size();
    }

    public void test_wildcard_multi_match() throws IOException {
        int t1 = total("SOURCE=" + BEER.indexName + " | WHERE multi_match(['Tags'], 'taste') | fields Id");
        int t2 = total("SOURCE=" + BEER.indexName + " | WHERE multi_match(['T*'], 'taste') | fields Id");
        assertNotEquals("['T*'] wildcard must widen the match vs ['Tags']", t2, t1);
    }

    public void testWildcardQueryString() throws IOException {
        int t1 = total("source=" + BEER.indexName + " | where query_string(['Tags'], 'taste')");
        int t2 = total("source=" + BEER.indexName + " | where query_string(['T*'], 'taste')");
        assertNotEquals("['T*'] wildcard must widen the match vs ['Tags']", t1, t2);
    }

    public void test_wildcard_simple_query_string() throws IOException {
        int t1 = total("SOURCE=" + BEER.indexName + " | WHERE simple_query_string(['Tags'], 'taste') | fields Id");
        int t2 = total("SOURCE=" + BEER.indexName + " | WHERE simple_query_string(['T*'], 'taste') | fields Id");
        assertNotEquals("['T*'] wildcard must widen the match vs ['Tags']", t2, t1);
    }
}
