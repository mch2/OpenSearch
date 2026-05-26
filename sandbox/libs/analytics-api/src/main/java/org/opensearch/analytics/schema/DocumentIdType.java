/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Locale;

/**
 * Calcite type marker for the OpenSearch {@code _id} metadata field. Backed by
 * {@link SqlTypeName#VARBINARY} matching the on-disk Uid-encoded binary representation;
 * the subclass exists as an {@code instanceof}-dispatch marker so the SQL plugin's
 * response conversion layer can apply {@code Uid.decodeId(bytes)} to produce the
 * human-readable document ID string.
 */
public final class DocumentIdType extends AbstractSqlType {

    public static final String NAME = "_id";

    public DocumentIdType(boolean nullable) {
        super(SqlTypeName.VARBINARY, nullable, null);
        computeDigest();
    }

    public static DocumentIdType nullable() {
        return new DocumentIdType(true);
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(NAME.toUpperCase(Locale.ROOT));
    }
}
