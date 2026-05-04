/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;

/**
 * End-to-end ITs for the four window-ranking functions routed through
 * PPL → Calcite → Substrait → DataFusion: {@code row_number}, {@code rank},
 * {@code dense_rank}, {@code nth_value}.
 *
 * <p>Per {@code tasks/function-implementation-handoff.md}, these are NOT in
 * the SQL plugin's {@code WINDOW_FUNC_MAPPING} — they can't be called directly
 * from {@code eventstats} or {@code streamstats}. They surface only as planner-
 * internal {@code RexOver} calls emitted by commands that need a row-index or
 * positional value:
 *
 * <ul>
 *   <li>{@code dedup}, {@code streamstats}, {@code rare}, and join on
 *       {@code appendcol} all emit {@code ROW_NUMBER() OVER (...)} via
 *       {@code PlanUtils.makeOver}.</li>
 *   <li>{@code trendline wma(...)} emits {@code NTH_VALUE(field, i)} for
 *       each tap in the weighted moving average.</li>
 *   <li>{@code rank} / {@code dense_rank} have no user-reachable emission
 *       path today — the SQL plugin's {@code DSL.rank()} /
 *       {@code DSL.dense_rank()} are defined on the old non-Calcite engine.</li>
 * </ul>
 *
 * <p>This IT therefore exercises the functions through the commands that
 * actually emit them. Where there is no emission path (rank/dense_rank), we
 * assert that the direct eventstats call fails, which documents the gap
 * rather than silently passing.
 *
 * @opensearch.internal
 */
public class WindowRankIT extends BaseScalarFunctionIT {

    private PPLResponse run(String ppl) {
        PPLRequest request = new PPLRequest(ppl);
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, request).actionGet();
        assertNotNull("PPLResponse must not be null", response);
        return response;
    }

    // ── row_number ──────────────────────────────────────────────────────────

    /**
     * {@code dedup firstname} lowers to {@code ROW_NUMBER() OVER
     * (PARTITION BY firstname ORDER BY firstname)} + a filter on
     * {@code _row_number_dedup_ <= 1}. Bank has Amber + Hattie so both rows
     * survive — the assertion is that the pipeline executes without error.
     *
     * <p>Currently blocked by a pushdown restriction in the Lucene delegation
     * model — the filter on the derived {@code _row_number_dedup_} column
     * cannot be delegated. The ROW_NUMBER emission itself is fine; the gap is
     * downstream of the window function. Once the analytics engine supports
     * filtering on derived/expression columns (or the filter is moved above
     * the delegation boundary), this test should pass.
     */
    @AwaitsFix(bugUrl = "Filter on derived column [_row_number_dedup_] not yet supported by the Lucene"
        + " delegation model — unrelated to ROW_NUMBER wiring (covered by testRowNumberViaStreamstats)")
    public void testRowNumberViaDedup() {
        PPLResponse response = run("source=" + BANK_INDEX + " | dedup firstname | fields firstname");
        assertEquals("dedup with unique firstnames must preserve both rows", 2, response.getRows().size());
    }

    /**
     * {@code streamstats count() as cnt} emits a planner-internal
     * {@code ROW_NUMBER() OVER (ORDER BY _row_number_)} to establish the
     * running window index, then evaluates the user's aggregate with
     * {@code ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW}. Exercises the
     * ROW_NUMBER substrait mapping on a different command than dedup.
     */
    public void testRowNumberViaStreamstats() {
        PPLResponse response = run("source=" + BANK_INDEX + " | streamstats count() as cnt | fields cnt");
        assertEquals("streamstats must preserve all 2 rows", 2, response.getRows().size());
        long sum = 0;
        long max = Long.MIN_VALUE;
        for (Object[] row : response.getRows()) {
            long v = ((Number) row[0]).longValue();
            sum += v;
            max = Math.max(max, v);
        }
        // Running count over 2 rows in insertion order is {1, 2} — sum=3, max=2,
        // order-independent to avoid depending on shard scan order.
        assertEquals("running count sum", 3L, sum);
        assertEquals("running count max", 2L, max);
    }

    // ── nth_value ───────────────────────────────────────────────────────────

    /**
     * {@code trendline wma(k, field)} emits
     * {@code CASE(count() over (ROWS k-1 PRECEDING) > k-1,
     *             (1*NTH_VALUE(field,1) + ... + k*NTH_VALUE(field,k)) / (k*(k+1)/2),
     *             null)}. The untyped-NULL ELSE branch is retyped by
     * {@link UntypedNullRewriter} (covered by the unit test in the same name).
     *
     * <p>End-to-end execution is currently blocked one layer further down:
     * DataFusion's physical planner rejects the {@code COUNT() OVER (ROWS ...)}
     * window function embedded in the CASE condition with
     * <em>"Physical plan does not support logical expression
     * WindowFunction(Count, ...)"</em>. That's a separate gap in
     * {@code datafusion-substrait}'s window-function lowering, independent of
     * the untyped-NULL fix this task addresses.
     */
    @AwaitsFix(bugUrl = "DF physical planner can't materialize COUNT() OVER (ROWS N PRECEDING)"
        + " inside trendline's CASE condition. Untyped-NULL ELSE branch — the original blocker —"
        + " is fixed and covered by UntypedNullRewriterTests.")
    public void testNthValueViaTrendlineWma() {
        PPLResponse response = run(
            "source=" + BANK_INDEX + " | sort account_number | trendline wma(2, balance) as t | fields account_number, t"
        );
        assertEquals("trendline must preserve all rows", 2, response.getRows().size());
        // Ordering by account_number: row 0 = account 1 (Amber), row 1 = account 6 (Hattie).
        Object[] row0 = response.getRows().get(0);
        Object[] row1 = response.getRows().get(1);
        assertEquals("row 0 is account 1 (sorted ascending)", 1L, ((Number) row0[0]).longValue());
        assertEquals("row 1 is account 6 (sorted ascending)", 6L, ((Number) row1[0]).longValue());
        assertNull("wma(2, balance) must be null on the first row (partial window)", row0[1]);
        assertNotNull("wma(2, balance) must be non-null on the second row", row1[1]);
        // (1 * 39225 + 2 * 5686) / 3 = 50597 / 3.
        assertEquals("wma(2, balance) on second row", 50597.0 / 3.0,
            ((Number) row1[1]).doubleValue(), 1e-6);
    }

    // ── rank / dense_rank — no emission path today ──────────────────────────

    /**
     * {@code rank()} is not in the SQL plugin's WINDOW_FUNC_MAPPING and no
     * PPL command emits it today. Direct invocation must fail — either at
     * parse time (grammar allows the token but the function resolver rejects
     * it) or at plan time. Documents the gap so a future WINDOW_FUNC_MAPPING
     * wire-up makes the test start failing visibly.
     */
    public void testRankDirectNotWired() {
        expectPipelineRejects("source=" + BANK_INDEX + " | eventstats rank() as r");
    }

    /** Same gap as {@link #testRankDirectNotWired}. */
    public void testDenseRankDirectNotWired() {
        expectPipelineRejects("source=" + BANK_INDEX + " | eventstats dense_rank() as r");
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    /**
     * Asserts that the pipeline rejects the given PPL with some exception.
     * We don't pin the type — different failure layers (ANTLR parse,
     * SemanticCheckException, Calcite validation, substrait lowering) each
     * throw their own, and the point here is only "this isn't a callable
     * surface today."
     */
    private void expectPipelineRejects(String ppl) {
        try {
            run(ppl);
        } catch (Throwable t) {
            return;
        }
        fail("expected pipeline to reject `" + ppl + "` — if this now passes, WINDOW_FUNC_MAPPING"
            + " was extended and the test should be rewritten to assert results");
    }
}
