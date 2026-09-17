/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;

import java.util.List;
import java.util.Map;

/**
 * Drives {@link PlannerImpl#runAllOptimizations} with profiling enabled across multiple
 * SQL query shapes and asserts on the resulting {@link RuleProfilingListener.PlannerProfile}:
 * phases ran in order, durations are non-negative, the rule-set matches exactly, and each
 * rule's production count matches the expected value.
 *
 * <p>Plan-shape correctness is intentionally <strong>not</strong> asserted here — that's
 * {@code *PlanShapeTests}' job. These tests only validate the profiling listener.
 *
 * <p>Productions (count of successful {@code transformTo} calls) are deterministic enough
 * to assert exactly. Attempts and elapsed time depend on Volcano's exploration order and
 * are only sanity-checked ({@code productions <= attempts}, {@code totalNanos >= 0}).
 */
public class RuleProfilingListenerTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(RuleProfilingListenerTests.class);

    /**
     * Every phase, in the order {@link PlannerImpl#runAllOptimizations} declares them. A phase only
     * registers with the listener when it actually runs, and {@code subquery-remove} is conditional —
     * it is skipped for a plan that has neither a {@code RexSubQuery} nor a {@code Correlate} needing
     * decorrelation, which is every query here. So the assertion is that the phases that ran are an
     * in-order subsequence of this list, with the unconditional ones all present.
     */
    private static final List<String> DECLARED_PHASES = List.of(
        "subquery-remove",
        "literal-agg-extract",
        "reduce-expressions",
        "pushdown-rules",
        "aggregate-decompose",
        "marking",
        "agg-literal-arg-split",
        "cbo"
    );

    /** Phases that run for every query, regardless of its shape. */
    private static final List<String> UNCONDITIONAL_PHASES = DECLARED_PHASES.stream()
        .filter(phase -> "subquery-remove".equals(phase) == false)
        .toList();

    public void testProfilePureScan() {
        runAndAssertRules(
            1,
            "SELECT URL FROM hits",
            Map.of(
                "ReduceExpressionsRule(Project)",
                0L,
                "OpenSearchProjectRule",
                1L,
                "OpenSearchTableScanRule",
                1L,
                "ExpandConversionRule",
                1L
            )
        );
    }

    public void testProfileFilterOverScan() {
        runAndAssertRules(
            1,
            "SELECT URL FROM hits WHERE CounterID = 100",
            Map.of(
                "ReduceExpressionsRule(Filter)",
                0L,
                "ReduceExpressionsRule(Project)",
                0L,
                "OpenSearchFilterRule",
                1L,
                "OpenSearchProjectRule",
                1L,
                "OpenSearchTableScanRule",
                1L,
                "ExpandConversionRule",
                1L,
                // trim-first enables the pushdown cascade: Filter pushed past Project, then merged.
                "FilterProjectTransposeRule",
                1L,
                "ProjectMergeRule",
                1L
            )
        );
    }

    public void testProfileAggregateOverFilterMultiShard() {
        runAndAssertRules(
            5,
            "SELECT CounterID, SUM(ParamPrice) AS total FROM hits WHERE AdvEngineID = 5 GROUP BY CounterID",
            Map.ofEntries(
                Map.entry("ExtractLiteralAggRule", 0L),
                Map.entry("ReduceExpressionsRule(Filter)", 0L),
                Map.entry("ReduceExpressionsRule(Project)", 0L),
                Map.entry("OpenSearchFilterRule", 1L),
                Map.entry("OpenSearchProjectRule", 1L),
                Map.entry("OpenSearchTableScanRule", 1L),
                Map.entry("OpenSearchAggregateRule", 1L),
                Map.entry("OpenSearchAggregateSplitRule", 1L),
                Map.entry("OpenSearchAggLiteralArgProjectSplitRule", 0L),
                Map.entry("OpenSearchDistributionDeriveRule", 3L),
                Map.entry("ExpandConversionRule", 5L),
                // trim-first pushdown cascade: Filter pushed past Project, then merged.
                Map.entry("FilterProjectTransposeRule", 1L),
                Map.entry("ProjectMergeRule", 1L),
                // Calcite built-in: attempted on the decomposed aggregate but produces nothing
                // here (no constant group keys), so productions == 0.
                Map.entry("AggregateProjectPullUpConstantsRule", 0L)
            )
        );
    }

    /** Self-join with aggregate on top — exercises {@code OpenSearchJoinRule} + {@code OpenSearchJoinSplitRule}. */
    public void testProfileJoinWithAggregateMultiShard() {
        runAndAssertRules(
            5,
            "SELECT l.CounterID, COUNT(*) AS cnt FROM hits l JOIN hits r ON l.CounterID = r.CounterID GROUP BY l.CounterID",
            // Trim-first narrows both join arms (and the top output) to [CounterID]: extra narrowing
            // Projects → ProjectRule 1→2, ExpandConversionRule 2→3, and DistributionDerive now fires.
            // Verified against the captured optimized plan (join key correctly reindexed =($0,$1)).
            Map.ofEntries(
                Map.entry("ExtractLiteralAggRule", 0L),
                Map.entry("ReduceExpressionsRule(Project)", 0L),
                Map.entry("OpenSearchTableScanRule", 1L),
                // 2, not 1: RelFieldTrimmer column pruning introduces a narrowing Project above the
                // scan, so the marking rule fires once per Project.
                Map.entry("OpenSearchProjectRule", 2L),
                Map.entry("OpenSearchJoinRule", 1L),
                Map.entry("OpenSearchAggregateRule", 1L),
                Map.entry("OpenSearchAggregateSplitRule", 1L),
                Map.entry("OpenSearchJoinSplitRule", 1L),
                Map.entry("OpenSearchAggLiteralArgProjectSplitRule", 0L),
                Map.entry("OpenSearchDistributionDeriveRule", 1L),
                // 3, not 2: OpenSearchDistributionDeriveRule adds a SINGLETON spine variant, so Volcano
                // runs one more trait conversion.
                Map.entry("ExpandConversionRule", 3L),
                // Calcite built-in: attempted on the decomposed aggregate but produces nothing
                // here (no constant group keys), so productions == 0.
                Map.entry("AggregateProjectPullUpConstantsRule", 0L)
            )
        );
    }

    public void testProfilingDisabledLeavesContextNull() {
        ClusterState state = clickBenchClusterState(1);
        PlannerContext context = context(state, false);
        RelNode parsed = SqlPlannerTestFixture.parseSql("SELECT URL FROM hits", state);

        PlannerImpl.runAllOptimizations(parsed, context);

        assertNull("Profiling disabled — getProfilingResults() must return null", context.getProfilingResults());
    }

    // ---- Shared assertion helper ----

    /**
     * Asserts the phases that ran are an in-order subsequence of {@link #DECLARED_PHASES} and include
     * every {@link #UNCONDITIONAL_PHASES} entry. A conditional phase that legitimately did not run
     * (see {@link #DECLARED_PHASES}) is allowed to be absent, but no phase may run out of order or
     * under an unknown name — either would be a real regression in the phase wiring.
     */
    private static void assertPhasesInDeclaredOrder(List<String> actual) {
        int next = 0;
        for (String phase : actual) {
            int at = DECLARED_PHASES.subList(next, DECLARED_PHASES.size()).indexOf(phase);
            assertTrue(
                "Phase '" + phase + "' is unknown or ran out of declared order; ran " + actual + ", declared " + DECLARED_PHASES,
                at >= 0
            );
            next += at + 1;
        }
        assertTrue("Every unconditional phase must run; ran " + actual, actual.containsAll(UNCONDITIONAL_PHASES));
    }

    /**
     * Runs the SQL through {@link PlannerImpl#runAllOptimizations} with profiling enabled,
     * then asserts:
     * <ul>
     *   <li>All optimization phases ran in declared order with non-negative durations.</li>
     *   <li>The set of fired rules matches {@code expectedProductionsByRule.keySet()} exactly.</li>
     *   <li>Each rule's {@code productions} equals the expected value in the map.</li>
     *   <li>Per-rule sanity: {@code attempts > 0}, {@code productions <= attempts},
     *       {@code totalNanos >= 0}.</li>
     * </ul>
     */
    private void runAndAssertRules(int shardCount, String sql, Map<String, Long> expectedProductionsByRule) {
        ClusterState state = clickBenchClusterState(shardCount);
        PlannerContext context = context(state, true);
        RelNode parsed = SqlPlannerTestFixture.parseSql(sql, state);

        PlannerImpl.runAllOptimizations(parsed, context);

        RuleProfilingListener.PlannerProfile profile = context.getProfilingResults();
        assertNotNull("Profiling enabled — profile must be recorded", profile);

        assertPhasesInDeclaredOrder(profile.phases());

        for (String phase : profile.phases()) {
            Long durationNs = profile.phaseDurationsNs().get(phase);
            assertNotNull("Phase '" + phase + "' must have a duration recorded", durationNs);
            assertTrue("Phase '" + phase + "' duration must be non-negative", durationNs >= 0);
        }

        // Rule-set match.
        assertEquals("Profile rule-set must match expected rule-set exactly", expectedProductionsByRule.keySet(), profile.rules().keySet());

        // Per-rule productions + sanity.
        profile.rules().forEach((ruleName, metrics) -> {
            long expectedProductions = expectedProductionsByRule.get(ruleName);
            assertEquals("Rule '" + ruleName + "' production count must match expected", expectedProductions, metrics.productions());
            assertTrue("Rule '" + ruleName + "' attempts must be > 0", metrics.attempts() > 0);
            assertTrue("Rule '" + ruleName + "' productions must be <= attempts", metrics.productions() <= metrics.attempts());
            assertTrue("Rule '" + ruleName + "' totalNanos must be >= 0", metrics.totalNanos() >= 0);
        });

        LOGGER.info("Profile:\n{}", profile.format());
    }

    // ---- Context wiring ----

    private static ClusterState clickBenchClusterState(int shardCount) {
        return SqlPlannerTestFixture.clusterStateWith(ClickBench.INDEX, ClickBench.BASIC_FIELDS, "parquet", shardCount);
    }

    private PlannerContext context(ClusterState state, boolean profilingEnabled) {
        // Default test settings: MPP off — pins COORDINATOR_CENTRIC plan shape (the existing
        // test fixture's expected rule firings assume this). MPP-on cases live in the rule-
        // specific test files (OpenSearchBroadcastJoinSplitRuleTests etc.).
        org.opensearch.common.settings.Settings settings = org.opensearch.common.settings.Settings.builder()
            .put("analytics.mpp.enabled", false)
            .build();
        return new PlannerContext(
            new CapabilityRegistry(List.of(DATAFUSION, LUCENE), FieldStorageResolver::new),
            state,
            settings,
            profilingEnabled
        );
    }
}
