/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;

import java.util.Map;

/**
 * Builds a Calcite {@link SchemaPlus} from OpenSearch {@link ClusterState} index mappings.
 *
 * <p>One Calcite table per index. Reads field types from index mapping properties.
 * Navigates: IndexMetadata -> MappingMetadata -> sourceAsMap() -> "properties" -> per-field "type".
 * // TODO: This is for illustation - use version sql plugin has built and re-purpose to not call node-client
 */
public class OpenSearchSchemaBuilder {

    private OpenSearchSchemaBuilder() {}

    /**
     * Builds a Calcite SchemaPlus from the given ClusterState.
     * Each index becomes a table; each mapped field becomes a column.
     *
     * @param clusterState the current cluster state to derive schema from
     */
    public static SchemaPlus buildSchema(ClusterState clusterState) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        SchemaPlus schemaPlus = rootSchema.plus();

        for (Map.Entry<String, IndexMetadata> entry : clusterState.metadata().indices().entrySet()) {
            String indexName = entry.getKey();
            IndexMetadata indexMetadata = entry.getValue();
            MappingMetadata mapping = indexMetadata.mapping();
            if (mapping == null) {
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> sourceMap = mapping.sourceAsMap();
            @SuppressWarnings("unchecked")
            Map<String, Object> properties = (Map<String, Object>) sourceMap.get("properties");
            if (properties == null) {
                continue;
            }

            schemaPlus.add(indexName, buildTable(properties));
        }

        return schemaPlus;
    }

    /**
     * Maps an OpenSearch field type string to a Calcite SqlTypeName, or {@code null} when the type
     * has no scalar Calcite representation here (geo_point, geo_shape, nested, completion, …) or
     * is unrecognized. Callers omit the column from the schema so a query referencing it surfaces
     * a Calcite "column not found" via the validator rather than a planner-time crash.
     *
     * <p>Type mapping:
     * <ul>
     *   <li>keyword/text/match_only_text -> VARCHAR</li>
     *   <li>long/unsigned_long/scaled_float -> BIGINT</li>
     *   <li>integer -> INTEGER, short -> SMALLINT, byte -> TINYINT</li>
     *   <li>double -> DOUBLE, float/half_float -> REAL</li>
     *   <li>boolean -> BOOLEAN</li>
     *   <li>date/date_nanos -> TIMESTAMP</li>
     *   <li>ip/binary -> VARBINARY</li>
     *   <li>everything else (geo_point, geo_shape, nested, object, flat_object, completion,
     *       constant_keyword, wildcard, alias, dense_vector, sparse_vector, percolator,
     *       *_range, token_count, version, plus genuinely unknown plugin types) -> {@code null}</li>
     * </ul>
     *
     * @param opensearchType the OpenSearch field type string
     */
    public static SqlTypeName mapFieldType(String opensearchType) {
        if (opensearchType == null) {
            return null;
        }
        switch (opensearchType) {
            case "keyword":
            case "text":
            case "match_only_text":
                return SqlTypeName.VARCHAR;
            case "long":
            case "unsigned_long":
                // unsigned_long: values above 2^63 - 1 wrap into negatives because BIGINT is
                // signed and Substrait has no unsigned integer types. Smaller values are safe.
                // TODO: values above 2^63 - 1 wrap into negatives. Drop the UInt64 → Int64 narrowing
                // (see schema_coerce.rs) when we have a proper solution.
            case "scaled_float":
                return SqlTypeName.BIGINT;
            case "integer":
                return SqlTypeName.INTEGER;
            case "short":
                return SqlTypeName.SMALLINT;
            case "byte":
                return SqlTypeName.TINYINT;
            case "double":
                return SqlTypeName.DOUBLE;
            case "float":
            case "half_float":
                // half_float lands as Arrow Float16 on disk. Calcite has no fp16 type; widen to
                // REAL so the planner sees the same shape as a regular float column. The parquet
                // reader's SchemaAdapter casts Float16 → Float32 per batch.
                // TODO: every record batch goes through a Float16 → Float32 cast (see
                // schema_coerce.rs) and downstream operators see Float32. Drop the widening when
                // we have a proper solution.
                return SqlTypeName.REAL;
            case "boolean":
                return SqlTypeName.BOOLEAN;
            case "date":
            case "date_nanos":
                return SqlTypeName.TIMESTAMP;
            case "ip":
            case "binary":
                return SqlTypeName.VARBINARY;
            default:
                return null;
        }
    }

    /**
     * Builds the Calcite {@link RelDataType} for a leaf column given the OpenSearch field-type
     * string. For {@code ip} and {@code binary} this returns an {@link IpType} or
     * {@link BinaryType} UDT (both backed by {@link SqlTypeName#VARBINARY}); for everything
     * else it returns the {@link SqlTypeName} from {@link #mapFieldType} as a nullable basic
     * SQL type. Returns {@code null} for unrecognized / unsupported field types.
     *
     * <p>Operator dispatch on the UDTs is unaffected because both extend
     * {@link org.apache.calcite.sql.type.AbstractSqlType} with VARBINARY — the cidrmatch
     * byte-range rewrite, equality / IN / BETWEEN coercion, and Substrait conversion all see
     * the same shape they did before.
     */
    public static RelDataType buildLeafType(String opensearchType, RelDataTypeFactory typeFactory) {
        if (opensearchType == null) {
            return null;
        }
        if (IpType.NAME.equals(opensearchType)) {
            return IpType.nullable();
        }
        if (BinaryType.NAME.equals(opensearchType)) {
            return BinaryType.nullable();
        }
        SqlTypeName sqlType = mapFieldType(opensearchType);
        if (sqlType == null) {
            return null;
        }
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlType), true);
    }

    private static AbstractTable buildTable(Map<String, Object> properties) {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();
                addLeafFields(builder, typeFactory, properties, "");
                // TODO: re-enable once _id BinaryView/Binary type mismatch at the
                // coordinator reduce StageInputScan is resolved. Currently causes
                // "Field '_id' has different type (Binary) than table schema (BinaryView)"
                // on every query regardless of whether _id is projected.
                // appendMetadataFields(builder, typeFactory);
                return builder.build();
            }
        };
    }

    /**
     * Appends system metadata fields that exist in parquet storage but are not declared
     * in user index mappings. These are per-document fields written by the parquet data
     * format plugin ({@code MetadataFieldPlugin}) and should be queryable via PPL/SQL.
     *
     * <p>The set of fields here mirrors what {@code MetadataFieldPlugin.getParquetFields()}
     * registers as parquet-resident columns. Types match the Arrow types declared by each
     * field's {@code getArrowType()} → Calcite equivalent:
     * <ul>
     *   <li>{@code _id} — Binary in parquet → VARBINARY. Content is always valid UTF-8
     *       (doc IDs are base64/UUID). Implicit coercion handles
     *       {@code WHERE _id = 'abc'} (VARCHAR literal vs VARBINARY column).</li>
     *   <li>{@code _routing} — Utf8 in parquet → VARCHAR.</li>
     * </ul>
     *
     * <p>Adding a metadata field here requires a corresponding entry in
     * {@code MetadataFieldPlugin} so the parquet writer materializes the column.
     */
    private static void appendMetadataFields(RelDataTypeFactory.Builder builder, RelDataTypeFactory typeFactory) {
        builder.add("_id", DocumentIdType.nullable());
        builder.add("_routing", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true));
    }

    @SuppressWarnings("unchecked")
    private static void addLeafFields(
        RelDataTypeFactory.Builder builder,
        RelDataTypeFactory typeFactory,
        Map<String, Object> properties,
        String pathPrefix
    ) {
        addLeafFields(builder, typeFactory, properties, pathPrefix, properties);
    }

    @SuppressWarnings("unchecked")
    private static void addLeafFields(
        RelDataTypeFactory.Builder builder,
        RelDataTypeFactory typeFactory,
        Map<String, Object> properties,
        String pathPrefix,
        Map<String, Object> rootProperties
    ) {
        for (Map.Entry<String, Object> fieldEntry : properties.entrySet()) {
            String fieldName = pathPrefix.isEmpty() ? fieldEntry.getKey() : pathPrefix + "." + fieldEntry.getKey();
            Map<String, Object> fieldProps = (Map<String, Object>) fieldEntry.getValue();
            String fieldType = (String) fieldProps.get("type");
            if (fieldType == null || "object".equals(fieldType)) {
                Map<String, Object> nested = (Map<String, Object>) fieldProps.get("properties");
                if (nested != null) {
                    addLeafFields(builder, typeFactory, nested, fieldName, rootProperties);
                }
                continue;
            }
            if ("nested".equals(fieldType)) {
                continue;
            }
            if ("alias".equals(fieldType)) {
                RelDataType aliasType = resolveAliasType((String) fieldProps.get("path"), rootProperties, typeFactory);
                if (aliasType != null) {
                    builder.add(fieldName, aliasType);
                }
                continue;
            }
            RelDataType columnType = buildLeafType(fieldType, typeFactory);
            if (columnType == null) {
                continue;
            }
            builder.add(fieldName, columnType);
        }
    }

    /**
     * Resolves an alias field's target type by walking the root properties map along
     * the dotted path. Returns {@code null} (alias dropped) when:
     * <ul>
     *   <li>path is null or empty (malformed mapping)</li>
     *   <li>target field doesn't exist</li>
     *   <li>target resolves to an unsupported type</li>
     *   <li>target is itself an alias (chain) — resolved transitively up to depth 5</li>
     * </ul>
     */
    @SuppressWarnings("unchecked")
    private static RelDataType resolveAliasType(String path, Map<String, Object> rootProperties, RelDataTypeFactory typeFactory) {
        if (path == null || path.isEmpty()) {
            return null;
        }
        Map<String, Object> current = rootProperties;
        String[] segments = path.split("\\.");
        for (int i = 0; i < segments.length; i++) {
            Object entry = current.get(segments[i]);
            if (!(entry instanceof Map)) {
                return null;
            }
            Map<String, Object> fieldProps = (Map<String, Object>) entry;
            if (i < segments.length - 1) {
                Object nested = fieldProps.get("properties");
                if (!(nested instanceof Map)) {
                    return null;
                }
                current = (Map<String, Object>) nested;
            } else {
                String targetType = (String) fieldProps.get("type");
                if ("alias".equals(targetType)) {
                    return resolveAliasType((String) fieldProps.get("path"), rootProperties, typeFactory);
                }
                return buildLeafType(targetType, typeFactory);
            }
        }
        return null;
    }
}
