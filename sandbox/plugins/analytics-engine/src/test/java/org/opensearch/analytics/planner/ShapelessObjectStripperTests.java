/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;

/**
 * Plan-shape coverage for {@link ShapelessObjectStripper}: a shapeless object —
 * {@code {"type": "object"}} before any document gave it a shape — is a field-less struct. There is
 * no such column in the file, so it is dropped from the scan and projected as a typed NULL, which is
 * what vanilla OpenSearch returns for it.
 *
 * <p>An object with a shape needs no rewrite at all: it is one Parquet struct column, read straight
 * off the scan, and a dotted path into it resolves as field access on that struct.
 */
public class ShapelessObjectStripperTests extends PlanShapeTestBase {

    /**
     * The index mapping the object came from. Nested, as OpenSearch stores it — which is what gives
     * {@code FieldStorageResolver} both the object's own storage and its leaves'.
     */
    private Map<String, Map<String, Object>> objectFieldMappings() {
        return Map.of(
            "id",
            Map.of("type", "integer"),
            "nested_metadata",
            Map.of(
                "properties",
                Map.of(
                    "top",
                    Map.of("type", "keyword"),
                    "properties",
                    Map.of("properties", Map.of("name", Map.of("type", "keyword"), "value", Map.of("type", "keyword")))
                )
            )
        );
    }

    /** No object spec ⇒ no rewrite, so plans without objects are untouched. */
    public void testNoOpWithoutAShapelessObject() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        assertFalse(ShapelessObjectStripper.rewrite(scan).isPresent());
    }

    /**
     * A shapeless object — {@code {"type": "object"}} before any document gave it a shape — is a
     * field-less struct. There is no column to read it from, so it is dropped from the scan and
     * projected as a typed NULL, which is what vanilla OpenSearch returns for it.
     */
    public void testEmitsNullForAShapelessObject() {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
        builder.add("shapeless", typeFactory.createStructType(List.of(), List.of()));
        RelNode scan = stubScan(mockTable("test_index", builder.build()));

        RelNode rewritten = ShapelessObjectStripper.rewrite(scan).orElseThrow();

        String shape = RelOptUtil.toString(rewritten);
        assertTrue("expected a typed NULL for the shapeless object, got:\n" + shape, shape.contains("null:RecordType"));
        assertFalse("the shapeless object has no column to scan, got:\n" + shape, shape.contains("shapeless=[$1]"));
    }

}
