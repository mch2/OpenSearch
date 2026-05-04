/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;

import java.util.List;

/**
 * End-to-end tests for the PPL {@code bin} command routed through PPL → Calcite →
 * Substrait → DataFusion. The {@code bin} command expands into UDF calls
 * ({@code SPAN_BUCKET}, {@code WIDTH_BUCKET}, {@code RANGE_BUCKET},
 * {@code MINSPAN_BUCKET}) whose Calcite-side Java impls format a range string
 * like {@code "0-10000"}. Rust UDFs ({@code span_bucket}, {@code width_bucket} —
 * see {@code rust/src/udf/}) format the label bit-exactly; isthmus routes the
 * substrait call by name via {@code opensearch_scalar.yaml}.
 *
 * <p>These tests mirror a subset of
 * {@code org.opensearch.sql.calcite.remote.CalciteBinCommandIT} from the SQL
 * repo, adapted to the bank fixture provided by {@link BaseScalarFunctionIT}.
 *
 * @opensearch.internal
 */
public class BinCommandIT extends BaseScalarFunctionIT {

    /**
     * Numeric span via SPAN_BUCKET. Bank row 1 has {@code balance=39225}; {@code span=10000}
     * should produce the bucket {@code "30000-40000"}.
     */
    public void testBinWithNumericSpan() {
        assertBinResult(
            "source=" + BANK_INDEX
                + " | where account_number = 1"
                + " | bin balance span=10000"
                + " | fields balance"
                + " | head 1",
            "balance",
            "30000-40000"
        );
    }

    // ---- Helper ----

    private void assertBinResult(String ppl, String columnName, String expected) {
        PPLRequest request = new PPLRequest(ppl);
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, request).actionGet();
        assertNotNull("PPLResponse must not be null", response);
        assertEquals("schema columns", List.of(columnName), response.getColumns());
        assertEquals("head 1 → exactly 1 row", 1, response.getRows().size());
        Object cell = response.getRows().get(0)[0];
        assertNotNull("bin result must not be null", cell);
        assertEquals(ppl, expected, cell.toString());
    }
}
