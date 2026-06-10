/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Reproduction of failing higher-order array-function methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalciteArrayFunctionIT} on the analytics-engine route:
 * {@code exists/filter/forall} with lambda predicates over an {@code array(...)} literal. Uses
 * {@code bank} (the array is constructed in eval; only one row is needed via {@code head 1}).
 */
public class CalciteArrayFunctionReproIT extends CalciteReproTestCase {

    private static final Dataset BANK = new Dataset("bank", "repro_arr_bank");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), BANK);
        provisioned = true;
    }

    private String src() { return "source=" + BANK.indexName; }

    public void testExists() throws IOException {
        Map<String, Object> r = executePpl(src()
            + " | eval array = array(1, -1, 2), result = exists(array, x -> x > 0) | fields result | head 1");
        verifySchema(r, schema("result", "boolean"));
        verifyDataRows(r, rows(true));
    }

    public void testFilter() throws IOException {
        Map<String, Object> r = executePpl(src()
            + " | eval array = array(1, -1, 2), result = filter(array, x -> x > 0) | fields result | head 1");
        verifySchema(r, schema("result", "array"));
        verifyDataRows(r, rows(List.of(1, 2)));
    }

    public void testForAll() throws IOException {
        Map<String, Object> r = executePpl(src()
            + " | eval array = array(1, -1, 2), result = forall(array, x -> x > 0) | fields result | head 1");
        verifySchema(r, schema("result", "boolean"));
        verifyDataRows(r, rows(false));
    }
}
