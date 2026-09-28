/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql.action;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.exec.profile.ProfiledResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Core orchestrator: SQL text → logical {@link RelNode} → {@link QueryPlanExecutor} → {@link SqlResponse}.
 *
 * <p>Unlike the PPL front-end (which delegates parsing to the SQL plugin's {@code unified-query}
 * planner), this service drives Calcite's own {@code SqlParser → SqlValidator → SqlToRelConverter}
 * pipeline directly, against Analytics Engine's injected {@link SchemaPlus}, type factory, and
 * operator table. There is no {@code org.opensearch.sql.*} dependency anywhere on this path.
 *
 * <p>The logical {@link RelNode} produced is a standard Calcite tree
 * ({@code LogicalProject}/{@code LogicalFilter}/{@code LogicalAggregate}/{@code LogicalSort}/
 * {@code LogicalJoin}/{@code LogicalUnion}) — the same node classes AE's planner already converts
 * to physical {@code OpenSearch*} nodes, so no intermediate rewrite is needed.
 */
public class SqlQueryService {

    private static final Logger logger = LogManager.getLogger(SqlQueryService.class);

    private final QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor;
    private final EngineContextProvider contextProvider;

    public SqlQueryService(QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor, EngineContextProvider contextProvider) {
        this.planExecutor = planExecutor;
        this.contextProvider = contextProvider;
    }

    /** Executes a SQL query: SQL text → RelNode → planExecutor.execute() → SqlResponse. */
    public SqlResponse execute(String sql) {
        return execute(sql, false);
    }

    /** Executes a SQL query with profiling, returning stage timings alongside results. */
    public SqlResponse executeWithProfile(String sql) {
        return execute(sql, true);
    }

    private SqlResponse execute(String sql, boolean profile) {
        try {
            QueryRequestContext baseCtx = contextProvider.getContext();
            RelNode logicalPlan = plan(sql, baseCtx);

            List<RelDataTypeField> fields = logicalPlan.getRowType().getFieldList();
            List<String> columns = new ArrayList<>(fields.size());
            for (RelDataTypeField field : fields) {
                columns.add(field.getName());
            }

            QueryRequestContext queryCtx = new QueryRequestContext(baseCtx.clusterState(), baseCtx.schema(), sql);

            if (profile) {
                PlainActionFuture<ProfiledResult> future = new PlainActionFuture<>();
                planExecutor.executeWithProfile(logicalPlan, queryCtx, future);
                ProfiledResult result = future.actionGet();
                if (result.isSuccess() == false) {
                    Throwable failure = result.failure();
                    if (failure instanceof RuntimeException re) throw re;
                    throw new RuntimeException("Query failed: " + failure.getMessage(), failure);
                }
                List<Object[]> rows = new ArrayList<>();
                for (Object[] row : result.rows()) {
                    rows.add(row);
                }
                return new SqlResponse(columns, rows, result.profile());
            }

            PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
            planExecutor.execute(logicalPlan, queryCtx, future);
            Iterable<Object[]> results = future.actionGet();
            List<Object[]> rows = new ArrayList<>();
            for (Object[] row : results) {
                rows.add(row);
            }
            return new SqlResponse(columns, rows);
        } catch (Exception e) {
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException("Failed to execute SQL query: " + e.getMessage(), e);
        }
    }

    /**
     * Parses and validates the SQL text against AE's schema, returning a logical {@link RelNode}.
     * Visible for unit testing with a mocked executor and in-memory schema.
     *
     * <p>Uses Calcite's {@code Frameworks}/{@code Planner} tools API — the same recipe AE's own
     * SQL test runner uses — rather than hand-wiring {@code SqlValidatorUtil}/{@code SqlToRelConverter}.
     * Critically, unquoted identifiers keep their original case ({@code Casing.UNCHANGED}) instead of
     * forcing case-insensitivity: the AE schema is lazy and case-insensitive on {@code getTable(name)},
     * and a case-insensitive validator would instead enumerate {@code getTableNames()} (empty under the
     * lazy schema) and fail to resolve tables.
     */
    RelNode plan(String sql, QueryRequestContext baseCtx) throws Exception {
        FrameworkConfig frameworkConfig = frameworkConfig(baseCtx.schema());
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        try {
            SqlNode parsed;
            try {
                parsed = planner.parse(sql);
            } catch (org.apache.calcite.sql.parser.SqlParseException e) {
                // DDL and other statements outside the default query grammar do not parse at all.
                // Surface them as an unsupported-statement error, consistent with the DML rejection
                // below (R5).
                throw new IllegalArgumentException("Unsupported or invalid SQL statement: " + e.getMessage(), e);
            }
            // v1 accepts only read queries — reject parseable-but-unsupported statements, e.g. DML (R5).
            if (isSupportedQuery(parsed) == false) {
                throw new IllegalArgumentException(
                    "Unsupported SQL statement: only SELECT / query statements are supported (got " + parsed.getKind() + ")"
                );
            }
            SqlNode validated = planner.validate(parsed);
            RelRoot root = planner.rel(validated);
            RelNode logicalPlan = root.project();
            // Fail closed (R2/R3): reject any function that validated against the standard operator
            // table but has no AE implementation, BEFORE the plan reaches the executor. Resolves
            // through AE's real resolution path, so it cannot drift from what AE executes.
            FunctionCapabilityWalker.check(logicalPlan);
            logger.debug("[AnalyticsSql] planned SQL to logical RelNode: {}", sql);
            return logicalPlan;
        } finally {
            planner.close();
        }
    }

    /**
     * Builds the Calcite framework config for planning against AE's schema, mirroring AE's own
     * SqlPlanRunner: preserve identifier case ({@code Casing.UNCHANGED}) so the lazy schema's
     * {@code getTable(name)} resolution is used, and chain AE's operator table ahead of the standard
     * one so AE functions resolve (R4).
     */
    private FrameworkConfig frameworkConfig(SchemaPlus schema) {
        SchemaPlus rootSchema = wrapLazySchema(schema);
        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), "UNCHANGED");
        CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(properties);
        // The provider's operatorTable() already includes the standard Calcite operators (the SPI
        // default IS the standard table; AE's override chains its own operators ahead of it), so it
        // is used as-is — re-chaining SqlStdOperatorTable here can disturb COUNT(*) star handling.
        SqlOperatorTable operatorTable = contextProvider.operatorTable();
        return Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.config().withUnquotedCasing(Casing.UNCHANGED))
            .defaultSchema(rootSchema)
            .operatorTable(operatorTable)
            .context(Contexts.of(connectionConfig))
            .build();
    }

    /** True only for read queries AE supports in v1 (SELECT and set operations). Rejects DDL/DML. */
    private static boolean isSupportedQuery(SqlNode sqlNode) {
        SqlKind kind = sqlNode.getKind();
        return kind == SqlKind.SELECT
            || kind == SqlKind.ORDER_BY
            || kind == SqlKind.UNION
            || kind == SqlKind.INTERSECT
            || kind == SqlKind.EXCEPT
            || kind == SqlKind.WITH
            || kind == SqlKind.VALUES;
    }

    /**
     * Wraps the AE {@link SchemaPlus} in a delegating non-caching {@link AbstractSchema} so the
     * validator's {@code getTable(name)} lookups still drive AE's lazy wildcard/alias/exclusion
     * resolution. Mirrors the PPL front-end's wrapper.
     */
    private static SchemaPlus wrapLazySchema(SchemaPlus schemaPlus) {
        AbstractSchema delegating = new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return new HashMap<>() {
                    {
                        for (String tableName : schemaPlus.getTableNames()) {
                            super.put(tableName, schemaPlus.getTable(tableName));
                        }
                    }

                    @Override
                    public Table get(Object key) {
                        Table t = super.get(key);
                        if (t == null && key instanceof String name) {
                            t = schemaPlus.getTable(name);
                            if (t != null) super.put(name, t);
                        }
                        return t;
                    }
                };
            }
        };
        return CalciteSchema.createRootSchema(true, false, "", delegating).plus();
    }
}
