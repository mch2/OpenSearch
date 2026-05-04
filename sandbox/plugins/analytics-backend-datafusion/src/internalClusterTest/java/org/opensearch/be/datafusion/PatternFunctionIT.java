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
 * End-to-end tests for PPL pattern / parse / regex functions routed through
 * Calcite → Substrait → DataFusion.
 *
 * <p>The DIRECT trio ({@code regexp_replace_5}, {@code regexp_replace_pg_4},
 * {@code translate3}) are internal Calcite library operators emitted only
 * when the {@code rex mode=sed} command rewrites a sed expression
 * ({@code s/pat/repl/flags} or {@code y/from/to/}) in
 * {@code CalciteRelNodeVisitor.createOptimizedSedCall}. They are not reachable
 * from an {@code eval} scalar call, so these tests drive them via {@code rex mode=sed}.
 *
 * <p>Bank fixture row 1: firstname='Amber'. We project {@code firstname} through
 * {@code rex mode=sed} and assert the rewritten cell.
 */
public class PatternFunctionIT extends BaseScalarFunctionIT {

    /** Runs a PPL pipeline that projects a single `firstname` column for account 1. */
    private String runFirstname(String pipeline) {
        PPLRequest request = new PPLRequest(
            "source=" + "bank" + " | where account_number = 1 " + pipeline + " | fields firstname | head 1"
        );
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, request).actionGet();
        assertNotNull("PPLResponse must not be null", response);
        assertEquals("schema columns", List.of("firstname"), response.getColumns());
        assertEquals("head 1 → exactly 1 row", 1, response.getRows().size());
        Object cell = response.getRows().get(0)[0];
        assertNotNull("firstname result must not be null", cell);
        return cell.toString();
    }

    // ── regexp_replace_3 (plain 3-arg) — covered by ScalarStringFunctionIT.testRegexpReplace ──
    // (eval regexp_replace(s, pat, repl) resolves via BuiltinFunctionName.REPLACE → REGEXP_REPLACE_3)

    /**
     * 4-arg Postgres-style form emitted by {@code rex mode=sed "s/pat/repl/flags"}.
     * The 'g' flag means replace all; case-insensitive 'i' is also routed through PG_4.
     * Calcite's operator name is "REGEXP_REPLACE"; YAML declares a 4-arg variant so
     * isthmus resolves it; DataFusion's native {@code regexp_replace} accepts the flags.
     */
    public void testRegexpReplacePg4FlagG() {
        // "Amber" has only one lowercased vowel ('e'); 'A' is uppercase so [aeiou] misses it.
        // Use a pattern with multiple matches to prove 'g' replaces globally.
        assertEquals("XXXXr", runFirstname("| rex field=firstname mode=sed \"s/[Ambe]/X/g\""));
    }

    /** Case-insensitive 'gi' via PG_4. 'A' and 'a' both match. */
    public void testRegexpReplacePg4FlagGi() {
        assertEquals("Xmber", runFirstname("| rex field=firstname mode=sed \"s/A/X/gi\""));
    }

    /**
     * 5-arg occurrence form emitted by {@code rex mode=sed "s/pat/repl/N"} where N is
     * a positive integer specifying only the Nth occurrence. Substrait core declares
     * {@code regexp_replace(str, pat, repl, i64 position, i64 occurrence)} but
     * DataFusion's built-in {@code regexp_replace} only accepts 3 or 4 args — it has
     * no 5-arg impl. Lowering this requires either a Rust UDF or a plan-side rewrite
     * that loops until the Nth match. Deferred.
     */
    public void testRegexpReplace5Occurrence_BLOCKED_DF_NATIVE() {
        // Expected if lowered: assertEquals("AmbXr",
        //   runFirstname("| rex field=firstname mode=sed \"s/[aeiou]/X/2\""));
    }

    /**
     * {@code translate3} emitted by {@code rex mode=sed "y/from/to/"}. Maps each char
     * in {@code from} to the same-indexed char in {@code to}. YAML declares a
     * {@code translate} variant + a {@code translate3 → translate} alias.
     */
    public void testTranslate3() {
        // y/Amber/Xmbor/ → A→X, m→m, b→b, e→o, r→r
        assertEquals("Xmbor", runFirstname("| rex field=firstname mode=sed \"y/Amber/Xmbor/\""));
    }

    /**
     * {@code rex field=firstname "(?<head>A)(?<tail>.+)"} emits one REX_EXTRACT UDF per named
     * group. The call is resolved at runtime by the {@code rex_extract} Rust UDF
     * (see {@code rust/src/udf/rex_extract.rs}), which compiles the pattern and looks up
     * the named group directly — no plan-time index resolution required.
     */
    public void testRexExtract() {
        PPLRequest request = new PPLRequest(
            "source=bank | where account_number = 1 "
                + "| rex field=firstname \"(?<head>A)(?<tail>.+)\" | fields head, tail | head 1"
        );
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, request).actionGet();
        assertNotNull("PPLResponse must not be null", response);
        assertEquals("schema columns", List.of("head", "tail"), response.getColumns());
        assertEquals("head 1 → exactly 1 row", 1, response.getRows().size());
        Object[] row = response.getRows().get(0);
        assertEquals("head group matches 'A'", "A", row[0].toString());
        assertEquals("tail group matches 'mber'", "mber", row[1].toString());
    }

    // parse returns Map<String,String>, which substrait can't serialize — BLOCKED BY DESIGN.
    // grok / pattern_parser need bespoke Rust crates.
}
