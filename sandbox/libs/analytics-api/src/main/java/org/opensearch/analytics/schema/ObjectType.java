/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Calcite type marker for an OpenSearch {@code object} parent column. Carries the
 * recursive child structure (leaf path or nested {@code ObjectType}) needed to expand
 * a {@code Project} reference to this column into a leaf projection plus a
 * coordinator-side stitch back into a nested {@code Map<String,Object>}.
 *
 * <p>Backed by {@link SqlTypeName#MAP} so the column survives the SQL plugin's
 * RelDataType→ExprType conversion (MAP → STRUCT, which dispatches the Java {@code Map}
 * value through {@code tupleValue} in the response). The plan rewriter strips the column
 * before it reaches the storage backend; DataFusion never sees it.
 *
 * <p>The PPL frontend's qualified-name resolver attempts longest-match first, so the
 * {@code city.name} flat leaf is found before the {@code city} parent — leaf projections
 * continue to work with the existing flat-column path. Only a bare reference to
 * {@code city} hits this UDT.
 */
public final class ObjectType extends MapSqlType {

    /** Shared key/value types for the Map super-class — VARCHAR keys, ANY values. */
    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();

    /** Immediate-child name → child descriptor (leaf path or nested ObjectType). */
    private final Map<String, Child> children;

    /** Discriminated child: either a leaf path string or a nested object. */
    public sealed interface Child permits Child.Leaf, Child.Nested {

        /** Leaf descriptor — wraps the dotted leaf path that exists as a flat column. */
        record Leaf(String path) implements Child {}

        /** Nested-object descriptor — wraps another {@link ObjectType}. */
        record Nested(ObjectType type) implements Child {}
    }

    public ObjectType(boolean nullable, Map<String, Child> children) {
        super(
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
            TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.ANY), true),
            nullable
        );
        this.children = Collections.unmodifiableMap(new LinkedHashMap<>(children));
        // Recompute the digest now that {@code children} is set; the super-class constructor
        // already invoked generateTypeString once with a placeholder digest, which is harmless
        // (the digest is overwritten here) but means we must re-compute.
        computeDigest();
    }

    public Map<String, Child> children() {
        return children;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        // Called both by the super-class constructor (before {@code children} is set) and by
        // our own {@link #computeDigest()} call afterwards. Guard the null case so the
        // super-call doesn't NPE; the post-construction call produces the real digest.
        sb.append("OBJECT");
        if (children != null) {
            sb.append(children.keySet().toString().toUpperCase(Locale.ROOT));
        }
    }
}
