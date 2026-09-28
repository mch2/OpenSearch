/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql.action;

import org.opensearch.action.ActionType;

/**
 * Action singleton for analytics SQL query execution.
 * Registered as a transport action in {@code AnalyticsSqlPlugin}.
 */
public class SqlExecuteAction extends ActionType<SqlResponse> {
    public static final String NAME = "cluster:internal/qe/analytics_sql_execute";
    public static final SqlExecuteAction INSTANCE = new SqlExecuteAction();

    private SqlExecuteAction() {
        super(NAME, SqlResponse::new);
    }
}
