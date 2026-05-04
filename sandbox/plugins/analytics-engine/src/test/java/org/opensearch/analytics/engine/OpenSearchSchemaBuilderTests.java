/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.engine;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.Version;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class OpenSearchSchemaBuilderTests extends OpenSearchTestCase {

    /**
     * Test that buildSchema produces a table for each index with correct column types.
     * Type mapping: keyword->VARCHAR, long->BIGINT, double->DOUBLE
     */
    public void testBuildSchemaWithKeywordLongDouble() throws Exception {
        ClusterState clusterState = buildClusterState(Map.of("test_index", Map.of("name", "keyword", "age", "long", "score", "double")));

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Table table = schema.getTable("test_index");
        assertNotNull("Table test_index should exist in schema", table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertEquals(3, rowType.getFieldCount());

        assertFieldType(rowType, "name", SqlTypeName.VARCHAR);
        assertFieldType(rowType, "age", SqlTypeName.BIGINT);
        assertFieldType(rowType, "score", SqlTypeName.DOUBLE);
    }

    /**
     * Test integer, float, boolean type mappings.
     */
    public void testBuildSchemaWithIntegerFloatBoolean() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("types_index", Map.of("count", "integer", "ratio", "float", "active", "boolean"))
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Table table = schema.getTable("types_index");
        assertNotNull("Table types_index should exist in schema", table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(rowType, "count", SqlTypeName.INTEGER);
        // OpenSearch "float" is IEEE 754 binary32 (32-bit, Parquet Float32).
        // Calcite REAL = 32-bit; Calcite FLOAT = 64-bit (double-precision). Mapping to
        // REAL so substrait emits Float32 and agrees with the parquet runtime schema;
        // using FLOAT produces "different type (Float64) than ... (Float32)".
        assertFieldType(rowType, "ratio", SqlTypeName.REAL);
        assertFieldType(rowType, "active", SqlTypeName.BOOLEAN);
    }

    /**
     * Test date, ip, text, short, byte type mappings.
     */
    public void testBuildSchemaWithDateIpTextShortByte() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("more_types", Map.of("created", "date", "address", "ip", "content", "text", "small_num", "short", "tiny_num", "byte"))
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Table table = schema.getTable("more_types");
        assertNotNull("Table more_types should exist in schema", table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertFieldType(rowType, "created", SqlTypeName.TIMESTAMP);
        assertFieldType(rowType, "address", SqlTypeName.VARBINARY);
        assertFieldType(rowType, "content", SqlTypeName.VARCHAR);
        assertFieldType(rowType, "small_num", SqlTypeName.SMALLINT);
        assertFieldType(rowType, "tiny_num", SqlTypeName.TINYINT);
    }

    /**
     * Test that multiple indices produce multiple tables.
     */
    public void testMultipleIndicesProduceMultipleTables() throws Exception {
        IndexMetadata idx1 = buildIndexMetadata("index_a", Map.of("col1", "keyword"));
        IndexMetadata idx2 = buildIndexMetadata("index_b", Map.of("col2", "long"));

        Metadata metadata = Metadata.builder().put(idx1, false).put(idx2, false).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        assertNotNull("Table index_a should exist", schema.getTable("index_a"));
        assertNotNull("Table index_b should exist", schema.getTable("index_b"));
    }

    /**
     * Test that bare {@code object}/{@code nested} fields (no {@code properties}) are skipped —
     * there are no leaves to emit. Only the flat leaf remains.
     */
    public void testBareObjectAndNestedFieldsSkipped() throws Exception {
        ClusterState clusterState = buildClusterState(
            Map.of("nested_index", Map.of("name", "keyword", "address", "object", "tags", "nested"))
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);

        Table table = schema.getTable("nested_index");
        assertNotNull(table);

        RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        assertEquals("Should only have 'name' field, skipping childless object/nested", 1, rowType.getFieldCount());
        assertFieldType(rowType, "name", SqlTypeName.VARCHAR);
    }

    /**
     * OpenSearch strips {@code "type": "object"} when {@code properties} is present on the same
     * node, so an object container is typically recognized by the {@code properties} key alone.
     * Its children must flatten to dotted-path columns.
     */
    public void testObjectWithImplicitTypeFlattensToDotPath() throws Exception {
        ClusterState clusterState = buildClusterStateFromJson(
            "impl_obj_index",
            "{\"properties\":{\"agent\":{\"properties\":{\"name\":{\"type\":\"keyword\"}}}}}"
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataType rowType = schema.getTable("impl_obj_index").getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());

        assertEquals(1, rowType.getFieldCount());
        assertFieldType(rowType, "agent.name", SqlTypeName.VARCHAR);
    }

    /** Explicit {@code "type": "object"} containers should also recurse into their properties. */
    public void testObjectWithExplicitTypeFlattensToDotPath() throws Exception {
        ClusterState clusterState = buildClusterStateFromJson(
            "expl_obj_index",
            "{\"properties\":{\"agent\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"keyword\"}}}}}"
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataType rowType = schema.getTable("expl_obj_index").getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());

        assertEquals(1, rowType.getFieldCount());
        assertFieldType(rowType, "agent.name", SqlTypeName.VARCHAR);
    }

    /**
     * {@code "type": "nested"} containers with a properties map should flatten to dotted
     * leaves — the parquet backend stores nested-child fields as flat dotted-name columns.
     */
    public void testNestedTypeContainerFlattensToDotPath() throws Exception {
        ClusterState clusterState = buildClusterStateFromJson(
            "nested_container_index",
            "{\"properties\":{\"events\":{\"type\":\"nested\",\"properties\":{\"name\":{\"type\":\"keyword\"}}}}}"
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataType rowType = schema.getTable("nested_container_index").getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());

        assertEquals(1, rowType.getFieldCount());
        assertFieldType(rowType, "events.name", SqlTypeName.VARCHAR);
    }

    /** Deep nesting must flatten all levels, e.g. resource.attributes.telemetry.sdk.version. */
    public void testDeeplyNestedObjectFlattens() throws Exception {
        ClusterState clusterState = buildClusterStateFromJson(
            "deep_index",
            "{\"properties\":{"
                + "\"resource\":{\"properties\":{"
                + "\"attributes\":{\"properties\":{"
                + "\"telemetry\":{\"properties\":{"
                + "\"sdk\":{\"properties\":{"
                + "\"version\":{\"type\":\"keyword\"},"
                + "\"language\":{\"type\":\"keyword\"}"
                + "}}}}}}}}}}"
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataType rowType = schema.getTable("deep_index").getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());

        assertEquals(2, rowType.getFieldCount());
        assertFieldType(rowType, "resource.attributes.telemetry.sdk.version", SqlTypeName.VARCHAR);
        assertFieldType(rowType, "resource.attributes.telemetry.sdk.language", SqlTypeName.VARCHAR);
    }

    /**
     * Mixed flat + nested mapping (the common real-world shape for big5 / OTel-style indices)
     * should expose both the flat leaf and the nested leaves under their dotted paths.
     */
    public void testMixedFlatAndNestedMappingExposesAllLeaves() throws Exception {
        ClusterState clusterState = buildClusterStateFromJson(
            "mixed_index",
            "{\"properties\":{"
                + "\"@timestamp\":{\"type\":\"date\"},"
                + "\"agent\":{\"properties\":{"
                + "\"name\":{\"type\":\"keyword\"},"
                + "\"id\":{\"type\":\"keyword\"}"
                + "}}}}"
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataType rowType = schema.getTable("mixed_index").getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());

        assertEquals(3, rowType.getFieldCount());
        assertFieldType(rowType, "@timestamp", SqlTypeName.TIMESTAMP);
        assertFieldType(rowType, "agent.name", SqlTypeName.VARCHAR);
        assertFieldType(rowType, "agent.id", SqlTypeName.VARCHAR);
    }

    /** An object container marked {@code enabled: false} should be skipped entirely. */
    public void testDisabledObjectSubtreeIsSkipped() throws Exception {
        ClusterState clusterState = buildClusterStateFromJson(
            "disabled_index",
            "{\"properties\":{"
                + "\"kept\":{\"type\":\"keyword\"},"
                + "\"metadata\":{\"enabled\":false,\"properties\":{"
                + "\"ignored\":{\"type\":\"keyword\"}"
                + "}}}}"
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataType rowType = schema.getTable("disabled_index").getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());

        assertEquals("Only the enabled leaf should be exposed", 1, rowType.getFieldCount());
        assertFieldType(rowType, "kept", SqlTypeName.VARCHAR);
        assertNull("Disabled subtree leaf must not be exposed", rowType.getField("metadata.ignored", true, false));
    }

    /**
     * Test that an empty ClusterState produces an empty schema.
     */
    public void testEmptyClusterStateProducesEmptySchema() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        assertNotNull(schema);
        assertTrue("Schema should have no tables", schema.getTableNames().isEmpty());
    }

    /**
     * Test mapFieldType for all supported types.
     */
    public void testMapFieldTypeForAllSupportedTypes() {
        assertEquals(SqlTypeName.VARCHAR, OpenSearchSchemaBuilder.mapFieldType("keyword"));
        assertEquals(SqlTypeName.VARCHAR, OpenSearchSchemaBuilder.mapFieldType("text"));
        assertEquals(SqlTypeName.BIGINT, OpenSearchSchemaBuilder.mapFieldType("long"));
        assertEquals(SqlTypeName.INTEGER, OpenSearchSchemaBuilder.mapFieldType("integer"));
        assertEquals(SqlTypeName.SMALLINT, OpenSearchSchemaBuilder.mapFieldType("short"));
        assertEquals(SqlTypeName.TINYINT, OpenSearchSchemaBuilder.mapFieldType("byte"));
        assertEquals(SqlTypeName.DOUBLE, OpenSearchSchemaBuilder.mapFieldType("double"));
        // OpenSearch "float" is IEEE 754 binary32 → Calcite REAL (32-bit), not FLOAT (64-bit).
        // See mapFieldType javadoc for the substrait/parquet type-agreement rationale.
        assertEquals(SqlTypeName.REAL, OpenSearchSchemaBuilder.mapFieldType("float"));
        assertEquals(SqlTypeName.BOOLEAN, OpenSearchSchemaBuilder.mapFieldType("boolean"));
        assertEquals(SqlTypeName.TIMESTAMP, OpenSearchSchemaBuilder.mapFieldType("date"));
        // date_nanos must map to TIMESTAMP to match the Parquet-side schema
        // emitted by DateNanosParquetField (Timestamp(NANOSECOND)). If this maps to
        // VARCHAR (the pre-fix default), substrait declares the column as Utf8 and
        // DataFusion rejects the plan with "different type (Utf8) than ... (Timestamp(ns))".
        assertEquals(SqlTypeName.TIMESTAMP, OpenSearchSchemaBuilder.mapFieldType("date_nanos"));
        assertEquals(SqlTypeName.VARBINARY, OpenSearchSchemaBuilder.mapFieldType("ip"));
        // half_float / scaled_float Calcite-side declarations.
        //
        // half_float: OpenSearch stores IEEE 754 binary16 (Float16) on disk. Calcite's
        // SqlTypeName has no Float16; declaring REAL (Float32) + widening the parquet
        // writer to emit Float32 keeps the substrait Read schema in agreement with the
        // runtime parquet schema. A FLOAT (Float64) declaration would break on
        // "different type (Float64) than ... (Float32)" mid-plan.
        //
        // scaled_float: stored as Long on disk (value * scalingFactor). Declaring BIGINT
        // mirrors the on-disk shape; per-query unscaling (multiplying by 1/scalingFactor
        // at read time) is a follow-up that requires a backend Cast-above-scan rule and
        // API-type metadata propagation — tracked alongside the deferred half_float
        // coercion task.
        assertEquals(SqlTypeName.REAL, OpenSearchSchemaBuilder.mapFieldType("half_float"));
        assertEquals(SqlTypeName.BIGINT, OpenSearchSchemaBuilder.mapFieldType("scaled_float"));
    }

    /**
     * Index-level test: a mapping with {@code half_float} and {@code scaled_float} fields
     * must produce REAL and BIGINT Calcite columns respectively so the substrait Read
     * schema matches the parquet runtime schema. Before this mapping, both types fell
     * through to VARCHAR (Utf8) and DataFusion rejected the plan with
     * "different type (Utf8) than ... (Float16/Int64)".
     */
    public void testHalfFloatAndScaledFloatFieldsMapToExpectedCalciteTypes() throws Exception {
        ClusterState clusterState = buildClusterStateFromJson(
            "datatypes_numeric_like",
            "{\"properties\":{"
                + "\"half_float_number\":{\"type\":\"half_float\"},"
                + "\"scaled_float_number\":{\"type\":\"scaled_float\",\"scaling_factor\":100}"
                + "}}"
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataType rowType = schema.getTable("datatypes_numeric_like").getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());

        assertFieldType(rowType, "half_float_number", SqlTypeName.REAL);
        assertFieldType(rowType, "scaled_float_number", SqlTypeName.BIGINT);
    }

    /**
     * {@code date}/{@code date_nanos} fields without a {@code format:} header stay
     * TIMESTAMP — that's the OpenSearch default (accepts full datetime values). A
     * time-only format on a {@code date_nanos} field would ideally narrow to TIME
     * but Calcite's {@link SqlTypeName#TIME} max precision is 3 (ms), so we widen
     * back to TIMESTAMP(ns) for the date_nanos case. Only {@code date} (millisecond
     * resolution) narrows to TIME(3) — see
     * {@link #testDateFieldNarrowsToDateOrTimeByFormat}.
     */
    public void testDateNanosFieldWidensToTimestampForTimeOnlyFormat() throws Exception {
        ClusterState clusterState = buildClusterStateFromJson(
            "date_nanos_index",
            "{\"properties\":{"
                + "\"hour_minute_second_OR_t_time\":{\"type\":\"date_nanos\",\"format\":\"hour_minute_second||t_time\"},"
                + "\"plain_date\":{\"type\":\"date\"}"
                + "}}"
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataType rowType = schema.getTable("date_nanos_index").getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());

        assertEquals(2, rowType.getFieldCount());
        // date_nanos + time-only format would want Time64(NANO), but Calcite TIME
        // can't carry ns precision, so we widen back to TIMESTAMP. Parquet writer
        // mirrors this by keeping Timestamp(NANO) for DateNanosParquetField's
        // TIME_ONLY case.
        assertFieldType(rowType, "hour_minute_second_OR_t_time", SqlTypeName.TIMESTAMP);
        // No format on this field → default datetime acceptance → TIMESTAMP.
        assertFieldType(rowType, "plain_date", SqlTypeName.TIMESTAMP);
    }

    /**
     * Regression for the 6 Class-C {@code CalcitePPLAggregationIT} tests that fail
     * because fields declared {@code type: date} with a date-only {@code format:}
     * (e.g. {@code basic_date}, {@code year_month_day}) produce {@code type=timestamp}
     * response headers instead of {@code type=date}. With {@link
     * OpenSearchSchemaBuilder#classifyDateFormat} wired into {@code buildTable}, these
     * declarations now map to Calcite DATE at the schema layer and flow through span
     * rewrites as DATE — paired with the parquet writer's narrowed vector emission so
     * DataFusion's substrait consumer sees matching Arrow types.
     */
    public void testDateFieldNarrowsToDateOrTimeByFormat() throws Exception {
        ClusterState clusterState = buildClusterStateFromJson(
            "date_formats_index",
            "{\"properties\":{"
                + "\"basic_date\":{\"type\":\"date\",\"format\":\"basic_date\"},"
                + "\"strict_date\":{\"type\":\"date\",\"format\":\"strict_date\"},"
                + "\"year_month_day\":{\"type\":\"date\",\"format\":\"year_month_day\"},"
                + "\"hour_minute_second\":{\"type\":\"date\",\"format\":\"hour_minute_second\"},"
                + "\"t_time\":{\"type\":\"date\",\"format\":\"t_time\"},"
                + "\"epoch_millis\":{\"type\":\"date\",\"format\":\"epoch_millis\"},"
                + "\"basic_date_time\":{\"type\":\"date\",\"format\":\"basic_date_time\"},"
                + "\"yyyy_mm_dd_or_epoch_millis\":{\"type\":\"date\",\"format\":\"yyyy-MM-dd||epoch_millis\"},"
                + "\"plain_date\":{\"type\":\"date\"}"
                + "}}"
        );

        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataType rowType = schema.getTable("date_formats_index").getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());

        // Date-only formats → DATE
        assertFieldType(rowType, "basic_date", SqlTypeName.DATE);
        assertFieldType(rowType, "strict_date", SqlTypeName.DATE);
        assertFieldType(rowType, "year_month_day", SqlTypeName.DATE);
        // Time-only formats → TIME(3) (ms) for date-typed fields so the substrait
        // schema emits Time32(MILLISECOND), matching parquet's Time vector.
        assertFieldType(rowType, "hour_minute_second", SqlTypeName.TIME);
        assertEquals(3, rowType.getField("hour_minute_second", true, false).getType().getPrecision());
        assertFieldType(rowType, "t_time", SqlTypeName.TIME);
        // Full-datetime / numeric / combined-cross-bucket / no-format → TIMESTAMP
        assertFieldType(rowType, "epoch_millis", SqlTypeName.TIMESTAMP);
        assertFieldType(rowType, "basic_date_time", SqlTypeName.TIMESTAMP);
        assertFieldType(rowType, "yyyy_mm_dd_or_epoch_millis", SqlTypeName.TIMESTAMP);
        assertFieldType(rowType, "plain_date", SqlTypeName.TIMESTAMP);
    }

    /**
     * Unit coverage for {@link OpenSearchSchemaBuilder#classifyDateFormat}. The
     * classifier is not yet wired into {@code mapFieldType}; exposing its result
     * through the Calcite schema requires matching narrowing on the parquet writer
     * side. The logic itself needs to be correct for the follow-up that wires it in.
     */
    public void testClassifyDateFormatForNamedFormats() {
        // Date-only named formats → DATE
        assertEquals(SqlTypeName.DATE, OpenSearchSchemaBuilder.classifyDateFormat("basic_date"));
        assertEquals(SqlTypeName.DATE, OpenSearchSchemaBuilder.classifyDateFormat("year_month_day"));
        assertEquals(SqlTypeName.DATE, OpenSearchSchemaBuilder.classifyDateFormat("week_date"));
        assertEquals(SqlTypeName.DATE, OpenSearchSchemaBuilder.classifyDateFormat("strict_date"));

        // Time-only named formats → TIME
        assertEquals(SqlTypeName.TIME, OpenSearchSchemaBuilder.classifyDateFormat("basic_t_time"));
        assertEquals(SqlTypeName.TIME, OpenSearchSchemaBuilder.classifyDateFormat("hour_minute_second"));
        assertEquals(SqlTypeName.TIME, OpenSearchSchemaBuilder.classifyDateFormat("hour"));
        assertEquals(SqlTypeName.TIME, OpenSearchSchemaBuilder.classifyDateFormat("t_time"));

        // Full datetime / epoch → TIMESTAMP
        assertEquals(SqlTypeName.TIMESTAMP, OpenSearchSchemaBuilder.classifyDateFormat("basic_date_time"));
        assertEquals(SqlTypeName.TIMESTAMP, OpenSearchSchemaBuilder.classifyDateFormat("epoch_millis"));
        assertEquals(SqlTypeName.TIMESTAMP, OpenSearchSchemaBuilder.classifyDateFormat("date_optional_time"));

        // Combined (cross-bucket) → TIMESTAMP
        assertEquals(SqlTypeName.TIMESTAMP, OpenSearchSchemaBuilder.classifyDateFormat("yyyy-MM-dd||epoch_millis"));
        assertEquals(SqlTypeName.TIMESTAMP, OpenSearchSchemaBuilder.classifyDateFormat("hour_minute_second||basic_date"));

        // Combined same-bucket — all time-only → TIME
        assertEquals(SqlTypeName.TIME, OpenSearchSchemaBuilder.classifyDateFormat("hour_minute_second||t_time"));
        // Combined same-bucket — all date-only → DATE
        assertEquals(SqlTypeName.DATE, OpenSearchSchemaBuilder.classifyDateFormat("basic_date||year_month_day"));

        // Null/empty/custom → TIMESTAMP
        assertEquals(SqlTypeName.TIMESTAMP, OpenSearchSchemaBuilder.classifyDateFormat(null));
        assertEquals(SqlTypeName.TIMESTAMP, OpenSearchSchemaBuilder.classifyDateFormat(""));
        assertEquals(SqlTypeName.TIMESTAMP, OpenSearchSchemaBuilder.classifyDateFormat("yyyy-MM-dd HH:mm:ss"));
    }

    /**
     * Test that unknown field types default to VARCHAR.
     */
    public void testUnknownFieldTypeDefaultsToVarchar() {
        assertEquals(SqlTypeName.VARCHAR, OpenSearchSchemaBuilder.mapFieldType("unknown_type"));
        assertEquals(SqlTypeName.VARCHAR, OpenSearchSchemaBuilder.mapFieldType("geo_point"));
    }

    // --- helpers ---

    private void assertFieldType(RelDataType rowType, String fieldName, SqlTypeName expectedType) {
        RelDataTypeField field = rowType.getField(fieldName, true, false);
        assertNotNull("Field '" + fieldName + "' should exist", field);
        assertEquals("Field '" + fieldName + "' should have type " + expectedType, expectedType, field.getType().getSqlTypeName());
    }

    private ClusterState buildClusterState(Map<String, Map<String, String>> indices) throws Exception {
        Metadata.Builder metadataBuilder = Metadata.builder();
        for (Map.Entry<String, Map<String, String>> entry : indices.entrySet()) {
            metadataBuilder.put(buildIndexMetadata(entry.getKey(), entry.getValue()), false);
        }
        return ClusterState.builder(new ClusterName("test")).metadata(metadataBuilder.build()).build();
    }

    /**
     * Builds a ClusterState with a single index whose mapping is the raw JSON provided.
     * Use this for tests that need nested/object/enabled-false structures that can't be
     * expressed as a flat {@code name -> type} map.
     */
    private ClusterState buildClusterStateFromJson(String indexName, String mappingJson) throws Exception {
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(mappingJson)
            .build();
        Metadata metadata = Metadata.builder().put(indexMetadata, false).build();
        return ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
    }

    private IndexMetadata buildIndexMetadata(String indexName, Map<String, String> fieldTypes) throws Exception {
        StringBuilder mappingJson = new StringBuilder("{\"properties\":{");
        boolean first = true;
        for (Map.Entry<String, String> field : fieldTypes.entrySet()) {
            if (!first) mappingJson.append(",");
            mappingJson.append("\"").append(field.getKey()).append("\":{\"type\":\"").append(field.getValue()).append("\"}");
            first = false;
        }
        mappingJson.append("}}");

        return IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(mappingJson.toString())
            .build();
    }
}
