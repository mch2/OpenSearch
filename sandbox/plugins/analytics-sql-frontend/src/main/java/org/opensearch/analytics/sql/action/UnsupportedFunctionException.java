/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql.action;

import java.util.Collection;
import java.util.List;

/**
 * Thrown when a parsed-and-validated SQL query references a function that Calcite's standard
 * operator table resolves but Analytics Engine has no implementation for. Extends
 * {@link IllegalArgumentException} so it inherits the HTTP-400 mapping the REST layer already
 * applies, while remaining a distinct type so tests (and callers) can tell it apart from the
 * unsupported-<em>statement</em> (DDL/DML) error and the parse error.
 *
 * <p>This is the fail-closed guard (R2): rather than letting an unsupported standard function
 * validate and then blow up opaquely deep in execution, the query is rejected up front, naming
 * the offending functions.
 */
public class UnsupportedFunctionException extends IllegalArgumentException {

    private final List<String> functions;

    public UnsupportedFunctionException(Collection<String> functions) {
        super(message(functions));
        this.functions = List.copyOf(functions);
    }

    /** The functions that AE cannot execute, in the order encountered. */
    public List<String> functions() {
        return functions;
    }

    private static String message(Collection<String> functions) {
        return "Unsupported function(s) for the analytics engine: "
            + String.join(", ", functions)
            + ". The analytics engine has no implementation for these standard SQL functions.";
    }
}
