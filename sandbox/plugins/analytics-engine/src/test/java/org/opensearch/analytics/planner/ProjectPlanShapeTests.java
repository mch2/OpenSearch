/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rel.OpenSearchProject} —
 * passthrough projection (field refs only) and {@code eval}-style scalar expressions.
 */
public class ProjectPlanShapeTests extends PlanShapeTestBase {

    public void testFieldsProject_1shard() {
        RelNode plan = identityFieldsProject();
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testFieldsProject_2shard() {
        RelNode plan = identityFieldsProject();
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testProjectWithScalarExpression_2shard() {
        // status + size — primitive arithmetic. PLUS now goes through the capability
        // registry so it gets wrapped in ANNOTATED_PROJECT_EXPR.
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RexNode plus = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, rexBuilder.makeInputRef(scan, 0), rexBuilder.makeInputRef(scan, 1));
        RelNode plan = LogicalProject.create(scan, List.of(), List.of(rexBuilder.makeInputRef(scan, 0), plus), List.of("status", "sum"));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchProject(status=[$0], sum=[ANNOTATED_PROJECT_EXPR(id=0, backends=[mock-parquet], +($0, $1))], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Field trimming discovers that only {@code status} is read from a wider index
     * ([status, size]) and materializes a narrowing {@code OpenSearchProject(status)}. The
     * post-CBO project-into-scan fold then collapses that Project into the scan itself (seeing
     * through the exchange), so the scan declares only [status] — the native read prunes the
     * unused column at the source instead of relying on a separate Project to do it on the shard.
     */
    public void testProjectSubset_foldsNarrowingIntoScan_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalProject.create(scan, List.of(), List.of(rexBuilder.makeInputRef(scan, 0)), List.of("status"));
        RelNode result = runPlanner(plan, multiShardContext());
        // The scan's fields=[[status]] term (rendered only when the scan is narrowed) shows the fold
        // reached the scan through the exchange and dropped the redundant Project.
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchTableScan(table=[[test_index]], fields=[[status]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Narrowing to a single column deep in a wide index (the {@code appendcol} regression used a
     * column at index 20). The fold must remap the parent's $-ref through the projected position and
     * narrow the scan to exactly that one column — proving the fold isn't limited to a leading prefix.
     */
    public void testProjectSingleDeepColumn_foldsIntoScan_1shard() {
        RelNode scan = stubScan(mockTable("wide_idx", "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "deep"));
        RelNode plan = LogicalProject.create(scan, List.of(), List.of(rexBuilder.makeInputRef(scan, 10)), List.of("deep"));
        RelNode result = runPlanner(
            plan,
            buildContextPerIndex("parquet", java.util.Map.of("wide_idx", 1), wideFields(), java.util.List.of(DATAFUSION, LUCENE))
        );
        assertPlanShape("""
            OpenSearchTableScan(table=[[wide_idx]], fields=[[deep]], viableBackends=[[mock-parquet]])
            """, result);
    }

    /**
     * Narrowing projection on top of a shard-delegated filter. The Filter (delegated to Lucene)
     * keeps its place at the shard, and the fold still narrows the scan below it to the read column —
     * delegation and the fold compose.
     */
    public void testProjectSubsetOverDelegatedFilter_foldsIntoScan_1shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode filter = makeFilter(scan, makeEquals(0, SqlTypeName.INTEGER, 200));
        RelNode plan = LogicalProject.create(filter, List.of(), List.of(rexBuilder.makeInputRef(scan, 0)), List.of("status"));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], viableBackends=[[mock-parquet]])
                  OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    private static java.util.Map<String, java.util.Map<String, Object>> wideFields() {
        java.util.Map<String, Object> intType = java.util.Map.of("type", "integer");
        java.util.Map<String, java.util.Map<String, Object>> fields = new java.util.LinkedHashMap<>();
        for (int i = 0; i < 10; i++) {
            fields.put("c" + i, intType);
        }
        fields.put("deep", intType);
        return fields;
    }

    private RelNode identityFieldsProject() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        return LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(scan, 0), rexBuilder.makeInputRef(scan, 1)),
            List.of("status", "size")
        );
    }
}
