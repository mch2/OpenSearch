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
 * Reproduction of failing methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLDedupIT} on the analytics-engine route.
 * Uses {@code duplication_nullable}. Note: dedup keeps the first occurrence, so the exact-row
 * variants are sensitive to scan/insertion order — a divergence here may reflect AE's parquet
 * read order, which is itself part of what the dedup tests pin down.
 */
public class CalcitePPLDedupReproIT extends CalciteReproTestCase {

    private static final Dataset DN = new Dataset("duplication_nullable", "repro_dedup_dn");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), DN);
        provisioned = true;
    }

    private String src() { return "source=" + DN.indexName; }

    public void testDedupComplex() throws IOException {
        Map<String, Object> actual = executePpl(src() + " | dedup 1 name");
        verifyDataRows(actual,
            rows("X", "A", 1), rows("Z", "B", 1), rows("X", "C", 1), rows("Z", "D", 1), rows(null, "E", 1));
    }

    public void testDedupExpr() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval new_name = lower(name) | dedup 1 new_name");
        verifyDataRows(actual,
            rows("X", "A", 1, "a"), rows("Z", "B", 1, "b"), rows("X", "C", 1, "c"),
            rows("Z", "D", 1, "d"), rows(null, "E", 1, "e"));
    }

    public void testConsecutiveImplicitFallbackV2() throws IOException {
        verifyNumOfRows(executePpl(src() + " | dedup 1 name CONSECUTIVE=true | fields name"), 8);
        verifyNumOfRows(executePpl(src() + " | dedup 1 name KEEPEMPTY=true CONSECUTIVE=true | fields name"), 12);
        verifyNumOfRows(executePpl(src() + " | dedup 2 name CONSECUTIVE=true | fields name"), 12);
        verifyNumOfRows(executePpl(src() + " | dedup 2 name KEEPEMPTY=true CONSECUTIVE=true | fields name"), 16);
    }
}
