/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql.action;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit-level fail-closed compatibility check (R2, R4) — the steering's requirement that these
 * checks be unit-level, not IT-level. Drives the real parse → validate → convert →
 * capability-walk path (no full lowering, no cluster, no Arrow) and asserts:
 * <ul>
 *   <li>a standard function AE cannot execute is rejected with {@link UnsupportedFunctionException}
 *       naming the function, and the executor is NEVER invoked (reject is at plan time); and</li>
 *   <li>a query using only AE-supported operators plans and DOES invoke the executor.</li>
 * </ul>
 */
@SuppressWarnings("unchecked")
public class FailClosedFunctionTests extends OpenSearchTestCase {

    private QueryPlanExecutor<RelNode, Iterable<Object[]>> executor;
    private SqlQueryService service;
    private QueryRequestContext ctx;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        executor = mock(QueryPlanExecutor.class);
        EngineContextProvider provider = mock(EngineContextProvider.class);
        // The published table (AE operators) chained ahead of std — same as production wiring —
        // so a std-only function still VALIDATES (proving the fail-closed walk, not the validator,
        // is what rejects it).
        when(provider.operatorTable()).thenReturn(
            org.apache.calcite.sql.util.SqlOperatorTables.chain(
                org.opensearch.analytics.spi.AnalyticsOperatorTables.build(),
                SqlStdOperatorTable.instance()
            )
        );
        ctx = new QueryRequestContext(null, inMemorySchema(), null);
        when(provider.getContext()).thenReturn(ctx);
        service = new SqlQueryService(executor, provider);
    }

    /** BIT_XOR is a standard SQL aggregate with no AggregateFunction mapping — must fail closed. */
    public void testUnsupportedAggregateRejectedBeforeExecution() throws Exception {
        UnsupportedFunctionException e = expectThrows(
            UnsupportedFunctionException.class,
            () -> service.plan("SELECT BIT_XOR(age) FROM users", ctx)
        );
        assertTrue(e.getMessage(), e.getMessage().toUpperCase(java.util.Locale.ROOT).contains("BIT_XOR"));
        // Reject is at plan time — the walk runs inside plan(), which execute() calls before the executor.
        verify(executor, never()).execute(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any()
        );
    }

    /** The bitwise-XOR operator '^' is a standard operator with no ScalarFunction mapping — must fail closed. */
    public void testUnsupportedScalarRejected() {
        UnsupportedFunctionException e = expectThrows(
            UnsupportedFunctionException.class,
            () -> service.plan("SELECT age ^ 1 FROM users", ctx)
        );
        assertFalse(e.functions().isEmpty());
    }

    /** A supported query (COUNT + comparison) plans cleanly and reaches the executor. */
    public void testSupportedQueryReachesExecutor() {
        org.mockito.Mockito.doAnswer(inv -> {
            org.opensearch.action.support.PlainActionFuture<Iterable<Object[]>> f = inv.getArgument(2);
            f.onResponse(java.util.List.of());
            return null;
        })
            .when(executor)
            .execute(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
        service.execute("SELECT dept, COUNT(*) FROM users GROUP BY dept");
        verify(executor).execute(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any()
        );
    }

    private static SchemaPlus inMemorySchema() {
        AbstractSchema schema = new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                Map<String, Table> tables = new java.util.HashMap<>();
                tables.put("users", table(cols -> {
                    cols.put("id", SqlTypeName.INTEGER);
                    cols.put("name", SqlTypeName.VARCHAR);
                    cols.put("age", SqlTypeName.INTEGER);
                    cols.put("dept", SqlTypeName.VARCHAR);
                    cols.put("tags", SqlTypeName.VARCHAR);
                }));
                return tables;
            }
        };
        return CalciteSchema.createRootSchema(true, false, "", schema).plus();
    }

    private interface ColumnSpec {
        void define(Map<String, SqlTypeName> columns);
    }

    private static Table table(ColumnSpec spec) {
        Map<String, SqlTypeName> columns = new LinkedHashMap<>();
        spec.define(columns);
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder b = typeFactory.builder();
                columns.forEach((n, t) -> b.add(n, typeFactory.createTypeWithNullability(typeFactory.createSqlType(t), true)));
                return b.build();
            }
        };
    }
}
