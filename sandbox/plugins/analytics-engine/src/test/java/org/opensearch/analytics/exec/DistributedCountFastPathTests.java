/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the distributed COUNT fast-path reshape ({@link DistributedCountFastPath}).
 * Verifies the reshape recognizes {@code count(*)} shapes, rejects {@code count(col)}/grouped/
 * distinct, and produces a {@code sum($0)} aggregate over a single NULLABLE-BIGINT Lucene scan
 * with the injected {@code delegated_predicate} marker (the #1 silent-hang hazard is a NOT-NULL
 * mismatch, so the nullability assertion below is load-bearing).
 */
public class DistributedCountFastPathTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(HepProgram.builder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    /** Builds an OpenSearchTableScan over a mock table with the given column names (all BIGINT). */
    private OpenSearchTableScan scan(String... cols) {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        for (String c : cols) {
            b.add(c, typeFactory.createSqlType(SqlTypeName.BIGINT));
        }
        RelDataType rowType = b.build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getRowType()).thenReturn(rowType);
        when(table.getQualifiedName()).thenReturn(List.of("clickbench"));
        List<FieldStorageInfo> storage = new java.util.ArrayList<>();
        for (String c : cols) {
            storage.add(FieldStorageInfo.derivedColumn(c, SqlTypeName.BIGINT));
        }
        return new OpenSearchTableScan(cluster, cluster.traitSet(), table, List.of("datafusion"), storage);
    }

    private AggregateCall countStar(RelNode input) {
        return AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            input,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "c"
        );
    }

    private AggregateCall countCol(RelNode input, int col) {
        return AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(col),
            -1,
            input,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "c"
        );
    }

    private OpenSearchAggregate agg(RelNode input, ImmutableBitSet groupSet, AggregateCall... calls) {
        return new OpenSearchAggregate(
            cluster,
            cluster.traitSet(),
            input,
            groupSet,
            List.of(groupSet),
            List.of(calls),
            AggregateMode.SINGLE,
            List.of("datafusion"),
            Map.of()
        );
    }

    public void testMatchesUnfilteredCountStar() {
        OpenSearchTableScan s = scan("a", "b");
        RelNode marked = agg(s, ImmutableBitSet.of(), countStar(s));
        assertTrue("count(*) over bare scan should match", DistributedCountFastPath.matches(marked));
    }

    public void testReshapeProducesNullableBigintSumOverLuceneScan() {
        OpenSearchTableScan s = scan("a", "b");
        RelNode marked = agg(s, ImmutableBitSet.of(), countStar(s));

        RelNode reshaped = DistributedCountFastPath.tryReshape(marked);
        assertNotNull("count(*) should reshape", reshaped);

        // Top is an OpenSearchAggregate with a single SUM call, empty group set, Lucene backend.
        assertTrue(reshaped instanceof OpenSearchAggregate);
        OpenSearchAggregate sumAgg = (OpenSearchAggregate) reshaped;
        assertTrue("group set must stay empty", sumAgg.getGroupSet().isEmpty());
        assertEquals(1, sumAgg.getAggCallList().size());
        assertEquals("SUM", sumAgg.getAggCallList().get(0).getAggregation().getName());
        assertEquals(List.of("lucene"), sumAgg.getViableBackends());

        // The aggregate output column MUST be nullable BIGINT (the #1 silent-hang hazard).
        RelDataType outType = sumAgg.getRowType().getFieldList().get(0).getType();
        assertEquals(SqlTypeName.BIGINT, outType.getSqlTypeName());
        assertTrue("count fast-path column must be NULLABLE", outType.isNullable());

        // Below: an OpenSearchFilter carrying the delegated_predicate marker, over the Lucene scan.
        RelNode filter = sumAgg.getInput(0);
        assertTrue(
            "expected OpenSearchFilter with marker, got " + filter.getClass().getSimpleName(),
            filter instanceof org.opensearch.analytics.planner.rel.OpenSearchFilter
        );
        assertTrue(filter.toString().contains("delegated_predicate"));

        // The scan is a single NULLABLE-BIGINT column named the count column.
        RelNode luceneScan = filter.getInput(0);
        assertTrue(luceneScan instanceof OpenSearchTableScan);
        RelDataType scanRow = luceneScan.getRowType();
        assertEquals(1, scanRow.getFieldCount());
        assertEquals(DistributedCountFastPath.COUNT_COLUMN, scanRow.getFieldList().get(0).getName());
        assertEquals(SqlTypeName.BIGINT, scanRow.getFieldList().get(0).getType().getSqlTypeName());
        assertTrue("leaf count column must be NULLABLE", scanRow.getFieldList().get(0).getType().isNullable());
    }

    public void testDoesNotMatchCountCol() {
        OpenSearchTableScan s = scan("a", "b");
        RelNode marked = agg(s, ImmutableBitSet.of(), countCol(s, 0));
        assertFalse("count(col) must NOT take the fast-path (null semantics differ)", DistributedCountFastPath.matches(marked));
        assertNull(DistributedCountFastPath.tryReshape(marked));
    }

    public void testDoesNotMatchGroupedCount() {
        OpenSearchTableScan s = scan("a", "b");
        RelNode marked = agg(s, ImmutableBitSet.of(0), countStar(s));
        assertFalse("grouped count must NOT take the unfiltered fast-path", DistributedCountFastPath.matches(marked));
    }
}
