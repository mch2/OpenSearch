/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport-layer request carrying regular SQL query text for the analytics SQL endpoint.
 */
public class SqlRequest extends ActionRequest {

    private final String sql;
    private final boolean explain;

    public SqlRequest(String sql) {
        this(sql, false);
    }

    public SqlRequest(String sql, boolean explain) {
        this.sql = sql;
        this.explain = explain;
    }

    public SqlRequest(StreamInput in) throws IOException {
        super(in);
        this.sql = in.readString();
        this.explain = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sql);
        out.writeBoolean(explain);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sql == null || sql.isEmpty()) {
            validationException = addValidationError("sql is missing or empty", validationException);
        }
        return validationException;
    }

    public String getSql() {
        return sql;
    }

    public boolean isExplain() {
        return explain;
    }
}
