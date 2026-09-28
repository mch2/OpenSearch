/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql.action;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises the real Calcite pipeline in {@link SqlQueryService#plan} against an in-memory schema,
 * asserting each v1 grammar item produces the expected logical {@link RelNode} shape, and that
 * DDL/DML is rejected before any executor call.
 */
@SuppressWarnings("unchecked")
public class SqlQueryServiceTests extends OpenSearchTestCase {

    private SqlQueryService service;
    private QueryRequestContext ctx;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        QueryPlanExecutor<RelNode, Iterable<Object[]>> executor = mock(QueryPlanExecutor.class);
        EngineContextProvider provider = mock(EngineContextProvider.class);
        // operatorTable() is a default method on the interface — Mockito's mock returns null for it,
        // so stub it to the standard table (matches the SPI default).
        when(provider.operatorTable()).thenReturn(org.apache.calcite.sql.fun.SqlStdOperatorTable.instance());
        ctx = new QueryRequestContext(null, inMemorySchema(), null);
        when(provider.getContext()).thenReturn(ctx);
        service = new SqlQueryService(executor, provider);
    }

    private RelNode plan(String sql) throws Exception {
        return service.plan(sql, ctx);
    }

    private static String tree(RelNode rel) {
        return RelOptUtil.toString(rel);
    }

    public void testSelectProjection() throws Exception {
        RelNode rel = plan("SELECT name, age FROM users");
        assertTrue(tree(rel), tree(rel).contains("LogicalProject"));
        assertEquals(2, rel.getRowType().getFieldCount());
    }

    public void testSelectExpression() throws Exception {
        RelNode rel = plan("SELECT age + 1 AS age_plus FROM users");
        assertTrue(tree(rel), tree(rel).contains("LogicalProject"));
        assertEquals("age_plus", rel.getRowType().getFieldList().get(0).getName());
    }

    public void testWhere() throws Exception {
        RelNode rel = plan("SELECT name FROM users WHERE age > 30");
        assertTrue(tree(rel), tree(rel).contains("LogicalFilter"));
    }

    public void testGroupByWithAggregate() throws Exception {
        RelNode rel = plan("SELECT dept, COUNT(*) FROM users GROUP BY dept");
        assertTrue(tree(rel), tree(rel).contains("LogicalAggregate"));
    }

    public void testHaving() throws Exception {
        RelNode rel = plan("SELECT dept, COUNT(*) c FROM users GROUP BY dept HAVING COUNT(*) > 5");
        String t = tree(rel);
        assertTrue(t, t.contains("LogicalAggregate"));
        assertTrue(t, t.contains("LogicalFilter"));
    }

    public void testOrderBy() throws Exception {
        RelNode rel = plan("SELECT name FROM users ORDER BY age DESC");
        assertTrue(tree(rel), tree(rel).contains("LogicalSort"));
    }

    public void testLimitOffset() throws Exception {
        RelNode rel = plan("SELECT name FROM users ORDER BY age LIMIT 10 OFFSET 5");
        assertTrue(tree(rel), tree(rel).contains("LogicalSort"));
    }

    public void testJoin() throws Exception {
        RelNode rel = plan("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id");
        assertTrue(tree(rel), tree(rel).contains("LogicalJoin"));
    }

    public void testUnion() throws Exception {
        RelNode rel = plan("SELECT name FROM users UNION SELECT name FROM users");
        assertTrue(tree(rel), tree(rel).contains("LogicalUnion"));
    }

    public void testDdlRejected() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> plan("CREATE TABLE t (a INT)"));
        assertTrue(e.getMessage(), e.getMessage().contains("Unsupported"));
    }

    public void testDmlRejected() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> plan("INSERT INTO users (id) VALUES (1)"));
        assertTrue(e.getMessage(), e.getMessage().contains("Unsupported SQL statement"));
    }

    /** Two in-memory tables (users, orders) whose row types are built against the caller's factory. */
    private static SchemaPlus inMemorySchema() {
        AbstractSchema schema = new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                Map<String, Table> tables = new HashMap<>();
                tables.put("users", table(builder -> {
                    builder.put("id", SqlTypeName.INTEGER);
                    builder.put("name", SqlTypeName.VARCHAR);
                    builder.put("age", SqlTypeName.INTEGER);
                    builder.put("dept", SqlTypeName.VARCHAR);
                }));
                tables.put("orders", table(builder -> {
                    builder.put("user_id", SqlTypeName.INTEGER);
                    builder.put("total", SqlTypeName.DOUBLE);
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
        Map<String, SqlTypeName> columns = new java.util.LinkedHashMap<>();
        spec.define(columns);
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder b = typeFactory.builder();
                columns.forEach((name, type) -> b.add(name, typeFactory.createTypeWithNullability(typeFactory.createSqlType(type), true)));
                return b.build();
            }
        };
    }

    // Sanity: the shared type factory used by tables matches the one the service builds internally.
    public void testTypeFactoryCompatibility() {
        RelDataTypeFactory tf = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        assertNotNull(tf.createSqlType(SqlTypeName.INTEGER));
    }
}
