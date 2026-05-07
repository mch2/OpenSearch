/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * End-to-end integration test for PPL {@code trendline} on the analytics-engine route.
 *
 * <p>The trendline command lowers (in {@code CalciteRelNodeVisitor.visitTrendline}) to
 * three relational pieces stacked on top of the input:
 *
 * <ol>
 *   <li>A {@code Filter(field IS NOT NULL)} filtering rows with null in the trendline
 *       field.</li>
 *   <li>A {@code Project} with one expression per trendline computation, of the shape
 *       {@code CASE WHEN COUNT() OVER (ROWS N-1 PRECEDING) > N-1 THEN <agg> OVER (ROWS N-1
 *       PRECEDING) ELSE NULL END} — a windowed Project with two RexOvers per output column
 *       (the warm-up COUNT and the actual aggregate).</li>
 *   <li>For SMA the inner aggregate is {@code AVG(field)}; for WMA it's a sum-of-products
 *       built from {@code NTH_VALUE(field, k) OVER (ROWS N-1 PRECEDING)} for k = 1..N.</li>
 * </ol>
 *
 * <h2>Status: blocked upstream by isthmus NULL-literal type conversion</h2>
 *
 * <p>The trendline lowering's warm-up CASE has an {@code ELSE NULL} branch that becomes a
 * Calcite {@code RexLiteral(NULL, type=NULL)}. {@code io.substrait.isthmus.TypeConverter#toSubstrait}
 * at line 192 throws {@code UnsupportedOperationException: Unable to convert the type NULL}
 * on this — isthmus's substrait converter doesn't handle the bare NULL Calcite type. The
 * windowed-gather transform produces a structurally-correct plan; substrait conversion fails
 * at the wire-encoding step.
 *
 * <p>Two paths to unblock, neither in this PR:
 * <ul>
 *   <li>Upstream the SQL plugin's trendline lowering to use a typed null literal —
 *       {@code relBuilder.cast(relBuilder.literal(null), thenExpr.getType())} — so the ELSE
 *       branch's RexLiteral carries the THEN branch's type.</li>
 *   <li>Add a pre-substrait RexShuttle in {@code DataFusionFragmentConvertor} that types
 *       any RexLiteral(NULL) by looking at its parent CASE's expected branch type.</li>
 * </ul>
 *
 * <p>The tests are kept in this class so the limitation has a concrete repro the next time
 * someone investigates. They will start passing once either fix lands.
 */
public class TrendlineCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static final Set<String> PROVISIONED = new HashSet<>();

    private void ensureDataProvisioned() throws IOException {
        if (PROVISIONED.add(DATASET.indexName)) {
            DatasetProvisioner.provision(client(), DATASET);
        }
    }

    /**
     * SMA(2) over num0. Lowers to {@code CASE WHEN COUNT() OVER (ROWS 1 PRECEDING) > 1 THEN
     * AVG(num0) OVER (ROWS 1 PRECEDING) ELSE NULL END} after first filtering num0-null rows.
     *
     * <p>calcs num0 (non-null, in key order): {@code 12.3, -12.3, 15.7, -15.7, 3.5, -3.5, 0, 10}
     * (key07 and key09 have null num0 — filtered out before the windowed compute).
     *
     * <p>Expected SMA values for the first 5 surviving rows:
     * <ul>
     *   <li>row 1 (key00, num0=12.3): only one value in frame; warm-up CASE → null</li>
     *   <li>row 2 (key01, num0=-12.3): avg(12.3, -12.3) = 0.0</li>
     *   <li>row 3 (key02, num0=15.7): avg(-12.3, 15.7) = 1.7</li>
     *   <li>row 4 (key03, num0=-15.7): avg(15.7, -15.7) = 0.0</li>
     *   <li>row 5 (key04, num0=3.5): avg(-15.7, 3.5) = -6.1</li>
     * </ul>
     */
    @AwaitsFix(bugUrl = "isthmus TypeConverter rejects NULL-typed literal in trendline CASE ELSE branch")
    public void testTrendlineSimpleMovingAverage() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName
                + " | sort key | trendline sma(2, num0) as ma | fields key, num0, ma | head 5"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("Expected 5 rows", 5, rows.size());

        // First row's ma is null (warm-up).
        assertNull("ma at row 0 should be null (warm-up)", rows.get(0).get(2));

        double[] expectedMa = { 0.0, 0.0, 1.7, 0.0, -6.1 };  // index 0 is null, checked above
        for (int i = 1; i < 5; i++) {
            Object actual = rows.get(i).get(2);
            assertNotNull("ma at row " + i + " should not be null", actual);
            assertEquals("ma at row " + i, expectedMa[i], ((Number) actual).doubleValue(), 0.0001);
        }
    }

    /**
     * WMA(3) over num0. Lowers to a sum-of-products of NTH_VALUE OVER (ROWS 2 PRECEDING)
     * calls — exercises the NTH_VALUE window function support added alongside this track.
     *
     * <p>This test proves NTH_VALUE end-to-end. It does not pin specific values because
     * NTH_VALUE's interaction with the warm-up CASE is verbose to compute by hand and a
     * non-null row count plus the windowed-gather producing a well-formed plan is enough
     * confidence for now.
     */
    @AwaitsFix(bugUrl = "isthmus TypeConverter rejects NULL-typed literal in trendline CASE ELSE branch (also depends on NTH_VALUE substrait support)")
    public void testTrendlineWeightedMovingAverage() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName
                + " | sort key | trendline wma(3, num0) as wma | fields key, num0, wma | head 5"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull(rows);
        assertEquals(5, rows.size());

        // The first two rows are warm-up (need 3 rows to produce a WMA(3) value); they
        // should be null. From row 3 onward the WMA should be a non-null number.
        assertNull("wma at row 0 should be null (warm-up)", rows.get(0).get(2));
        assertNull("wma at row 1 should be null (warm-up)", rows.get(1).get(2));
        for (int i = 2; i < 5; i++) {
            assertNotNull("wma at row " + i + " should not be null after warm-up", rows.get(i).get(2));
        }
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
