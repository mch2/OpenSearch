/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.common.settings.Settings;

import java.util.List;
import java.util.Map;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rules.OpenSearchAggregateShardBucketRule}.
 *
 * <p>Verifies the post-CBO insertion of a shard-side Sort+Limit above a shard-local
 * merge aggregate when {@code index.analytics.shard_bucket_oversampling_factor > 0}
 * for all involved indices. {@code shardSize = ceil(max(LIMIT, 10) * factor) + 10}.
 */
public class AggregateShardBucketTests extends PlanShapeTestBase {

    /**
     * Default factor 1.5 with multi-shard {@code GROUP BY k ORDER BY COUNT(*) LIMIT 100}.
     * Inserts a shard-side Sort with {@code fetch = ceil(100 * 1.5) + 10 = 160} above a
     * shard-local FINAL aggregate. The shard-local FINAL fully aggregates each shard's
     * scan into one row per group before the Sort+Limit truncates groups. The shard Sort
     * carries an expression collation (sortExprs={@code [$1]}) and a synthetic dense
     * field-index collation; the convertor patches substrait SortField.expr at lowering.
     */
    public void testDefaultFactor_multiShard() {
        RelNode plan = makeSortLimitOverGroupByCount(/* limit */ 100);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$1], dir0=[ASC], fetch=[100], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[160], viableBackends=[[mock-parquet]], sortExprs=[[$1]])
                        OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                          OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Factor 0 disables the optimization — pre-existing FINAL+ER+PARTIAL shape, no shard-local merge. */
    public void testFactorZero_disabled() {
        RelNode plan = makeSortLimitOverGroupByCount(100);
        RelNode result = runPlanner(plan, multiShardContextWithFactor(0.0));
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$1], dir0=[ASC], fetch=[100], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Factor 1.0 — no oversampling buffer; {@code shardSize = max(LIMIT, 10) * 1.0 + 10 = 110}. */
    public void testFactorOne_pureTopK() {
        RelNode plan = makeSortLimitOverGroupByCount(100);
        RelNode result = runPlanner(plan, multiShardContextWithFactor(1.0));
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$1], dir0=[ASC], fetch=[100], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[110], viableBackends=[[mock-parquet]], sortExprs=[[$1]])
                        OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                          OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Factor 3.0 — aggressive oversampling; {@code shardSize = max(100, 10) * 3.0 + 10 = 310}. */
    public void testFactorThree() {
        RelNode plan = makeSortLimitOverGroupByCount(100);
        RelNode result = runPlanner(plan, multiShardContextWithFactor(3.0));
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$1], dir0=[ASC], fetch=[100], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[310], viableBackends=[[mock-parquet]], sortExprs=[[$1]])
                        OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                          OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Single-shard: aggregate is SINGLE (no FINAL/PARTIAL split), rule pattern doesn't match. */
    public void testSingleShard_noRewrite() {
        RelNode plan = makeSortLimitOverGroupByCount(100);
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchSort(sort0=[$1], dir0=[ASC], fetch=[100], viableBackends=[[mock-parquet]])
              OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[SINGLE], viableBackends=[[mock-parquet]])
                OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    /**
     * No LIMIT in the query: rule still fires using the default {@code head}-style fallback
     * (10) as the implicit coordinator limit, so the shard ships
     * {@code ceil(max(10, 10) * 1.5) + 10 = 25} rows.
     */
    public void testNoLimit_defaultFallback() {
        RelNode agg = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countStarCall());
        RelNode plan = org.apache.calcite.rel.logical.LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$1], dir0=[ASC], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[25], viableBackends=[[mock-parquet]], sortExprs=[[$1]])
                        OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                          OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Sort+Limit over GROUP BY shape: ORDER BY agg ASC LIMIT N. Field 0 = group key,
     * field 1 = COUNT(*).
     */
    private RelNode makeSortLimitOverGroupByCount(int limit) {
        RelNode agg = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countStarCall());
        return org.apache.calcite.rel.logical.LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(limit, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
    }

    /**
     * AVG-by-AVG: outer Sort references the recompose Project's output column. The rule
     * walks through the Project, identifies the recompose RexNode {@code SUM/COUNT}, and
     * uses it as the shard-side sort expression. Plan-shape: outer Sort + Project (recompose)
     * + coord FINAL Aggregate(SUM, COUNT) + ER + shard-side Sort with sortExprs containing
     * the recompose RexNode + shard-local FINAL Aggregate(SUM, COUNT).
     *
     * <p>Locks the engine-native-merge SPI plumbing for the decomposed-aggregate path:
     * primitive decomposition produces a Project whose RexNode for the AVG output is the
     * sort key; the rule's {@code EngineNativeMergeRewriter} passes column refs through
     * unchanged (since SUM/COUNT are not engine-native merge), so the Project's expression
     * already references the shard aggregate's output and is reused as the sortExprs entry.
     */
    public void testAvg_byAvg_shardLocalSortExpression() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall avg = AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            false,
            false,
            List.of(),
            List.of(1),
            -1,
            null,
            RelCollations.EMPTY,
            1,
            scan,
            null,
            "avg_size"
        );
        RelNode agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(avg));
        RelNode plan = org.apache.calcite.rel.logical.LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(50, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(plan, multiShardContext());
        // shardSize = ceil(max(50, 10) * 1.5) + 10 = 85.
        // Inner shard Sort sortExprs reference the recompose RexNode CAST(SUM/COUNT) over the
        // shard-side FINAL aggregate's outputs ($1=SUM, $2=COUNT). The synthetic dense
        // collation has a single entry at index 0.
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$1], dir0=[DESC], fetch=[50], viableBackends=[[mock-parquet]])
                  OpenSearchProject(status=[$0], avg_size=[ANNOTATED_PROJECT_EXPR(id=3, backends=[mock-parquet], CAST(ANNOTATED_PROJECT_EXPR(id=2, backends=[mock-parquet], /($1, $2))):INTEGER NOT NULL)], viableBackends=[[mock-parquet]])
                    OpenSearchAggregate(group=[{0}], agg#0=[SUM($1)], agg#1=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                      OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                        OpenSearchSort(sort0=[$0], dir0=[DESC], fetch=[85], viableBackends=[[mock-parquet]], sortExprs=[[ANNOTATED_PROJECT_EXPR(id=3, backends=[mock-parquet], CAST(ANNOTATED_PROJECT_EXPR(id=2, backends=[mock-parquet], /($1, $2))):INTEGER NOT NULL)]])
                          OpenSearchAggregate(group=[{0}], agg#0=[SUM($1)], agg#1=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                            OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * DC-by-DC: outer Sort references {@code APPROX_COUNT_DISTINCT(field)}'s output. The rule
     * detects engine-native merge and replaces the column ref with
     * {@code AggregateFunction.APPROX_COUNT_DISTINCT.finalizeOperator()} =
     * {@code hll_estimate($state)}. The shard side switches to {@code AggregateMode.SHARD_MERGE}
     * with the aggCall's return type overridden to the intermediate {@code VARBINARY} state
     * shape so the sketch ships across the wire instead of the per-shard cardinality scalar.
     * Coord FINAL stays as {@code APPROX_COUNT_DISTINCT(state)} via {@code DistributedAggregateRewriter},
     * merging sketches and emitting the global cardinality.
     */
    public void testDc_byDc_shardLocalSortExpressionUsesHllEstimate() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall dc = AggregateCall.create(
            SqlStdOperatorTable.APPROX_COUNT_DISTINCT,
            false,
            List.of(1),
            -1,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "dc_size"
        );
        RelNode agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(dc));
        RelNode plan = org.apache.calcite.rel.logical.LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(50, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(plan, multiShardContext());
        // shardSize = ceil(max(50, 10) * 1.5) + 10 = 85.
        // Inner shard Sort sortExprs is `hll_estimate($1)`; the shard SHARD_MERGE aggregate
        // declares dc_size as VARBINARY (intermediate HLL sketch) so the wire ships state
        // rather than the BIGINT scalar. Coord FINAL stays APPROX_COUNT_DISTINCT(state).
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$1], dir0=[DESC], fetch=[50], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], dc_size=[APPROX_COUNT_DISTINCT($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchSort(sort0=[$0], dir0=[DESC], fetch=[85], viableBackends=[[mock-parquet]], sortExprs=[[hll_estimate($1)]])
                        OpenSearchAggregate(group=[{0}], dc_size=[APPROX_COUNT_DISTINCT($1)], mode=[SHARD_MERGE], viableBackends=[[mock-parquet]])
                          OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Helper: build a multi-shard context with a custom oversampling factor on test_index. */
    PlannerContext multiShardContextWithFactor(double factor) {
        return buildContextPerIndex(
            "parquet",
            Map.of("test_index", 2),
            Map.of("test_index", Settings.builder().put("index.analytics.shard_bucket_oversampling_factor", factor).build()),
            intFields(),
            List.of(DATAFUSION, LUCENE)
        );
    }
}
