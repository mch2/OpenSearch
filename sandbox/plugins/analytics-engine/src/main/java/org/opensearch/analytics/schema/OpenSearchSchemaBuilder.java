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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
     * Maps an OpenSearch field type string to a Calcite SqlTypeName.
     *
     * <p>Type mapping:
     * <ul>
     *   <li>keyword/text -> VARCHAR</li>
     *   <li>long -> BIGINT</li>
     *   <li>integer -> INTEGER</li>
     *   <li>short -> SMALLINT</li>
     *   <li>byte -> TINYINT</li>
     *   <li>double -> DOUBLE</li>
     *   <li>float -> REAL (Calcite's 32-bit float; see case body)</li>
     *   <li>half_float -> REAL (widened; parquet writer emits Float32)</li>
     *   <li>scaled_float -> BIGINT (parquet writer stores scaled Long)</li>
     *   <li>boolean -> BOOLEAN</li>
     *   <li>date / date_nanos -> TIMESTAMP (format-aware narrowing is a follow-up —
     *       see {@link #classifyDateFormat(String)})</li>
     *   <li>ip -> VARBINARY</li>
     *   <li>nested/object -> skip (not mapped)</li>
     *   <li>unknown -> VARCHAR (default)</li>
     * </ul>
     *
     * @param opensearchType the OpenSearch field type string
     */
    public static SqlTypeName mapFieldType(String opensearchType) {
        switch (opensearchType) {
            case "keyword":
            case "text":
                return SqlTypeName.VARCHAR;
            case "ip":
                // IP fields are stored as 16-byte InetAddressPoint binaries in parquet
                // (see IpParquetField), so the Calcite schema declares them as
                // VARBINARY so the runtime parquet schema matches when DataFusion
                // reads it. The sql repo's OpenSearchTypeFactory maps VARBINARY -> IP
                // in ExprType so PPL's IP-overload dispatch works on this column.
                return SqlTypeName.VARBINARY;
            case "binary":
                // Generic `binary` fields are written to parquet by BinaryParquetField
                // as Arrow Binary. Declaring VARBINARY here keeps the substrait Read
                // schema (Binary) in agreement with the parquet runtime schema —
                // DataFusion rejects plans whose column types disagree ("different
                // type (Utf8) than ... (Binary)"). Shares the VARBINARY encoding with
                // `ip`; downstream IP-aware adapters only fire when the query actually
                // uses an IP operator, so generic binary columns pass through unchanged.
                return SqlTypeName.VARBINARY;
            case "long":
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
                // OpenSearch "float" is IEEE 754 binary32 (32-bit). The parquet writer
                // stores it as Arrow Float32. Calcite's SqlTypeName vocabulary is
                // counter-intuitive: REAL = 32-bit, FLOAT = double-precision (64-bit).
                // Using SqlTypeName.FLOAT here would make substrait emit Float64 and
                // DataFusion would reject the plan with:
                // "different type (Float64) than ... (Float32)".
                return SqlTypeName.REAL;
            case "half_float":
                // OpenSearch "half_float" is IEEE 754 binary16 on disk (Float16).
                // Calcite's SqlTypeName vocabulary has no Float16 variant; declaring
                // any narrower / wider type here only works if the parquet writer
                // emits the matching Arrow vector. The HalfFloatParquetField is
                // therefore widened to emit Float32 (Float4Vector) so the substrait
                // Read schema agrees with the parquet runtime schema. On-disk size
                // increases from 16 → 32 bits per value; accepted as the trade-off
                // for not extending Calcite's type vocabulary. A future "Cast-above-
                // scan" branch that preserves Float16 on disk is tracked as a
                // follow-up (see tasks/fixes/deferred-half-scaled-float.md).
                return SqlTypeName.REAL;
            case "scaled_float":
                // OpenSearch "scaled_float" is stored on disk as Long = value *
                // scaling_factor. The parquet writer uses LongParquetField (Int64),
                // so the Calcite declaration must be BIGINT to match the substrait
                // Read schema. The logical (API) type is Double — per-query unscaling
                // via a backend "Cast-above-scan" rule that multiplies by 1/scalingFactor
                // is deferred; tracked alongside the half_float follow-up in
                // tasks/fixes/deferred-half-scaled-float.md. Until that lands, raw
                // scaled integer values flow through — acceptable for schema-shape
                // tests like testAggByByteNumberWithScript where scaled_float_number
                // is present in the mapping but isn't the aggregated column.
                return SqlTypeName.BIGINT;
            case "boolean":
                return SqlTypeName.BOOLEAN;
            case "date":
            case "date_nanos":
                // date/date_nanos fields are written to parquet as Timestamp(MILLI) /
                // Timestamp(NANOSECOND) respectively (DateParquetField /
                // DateNanosParquetField). Declaring TIMESTAMP in the Calcite schema keeps
                // the substrait Read schema in agreement with the runtime parquet schema —
                // DataFusion rejects substrait plans whose column types don't match the
                // table schema exactly (no implicit cast).
                //
                // Format-aware narrowing to DATE/TIME is a follow-up that requires the
                // parquet writer path to *also* emit Date32/Time64 vectors for date-only/
                // time-only formats. Kept as TIMESTAMP until that lands.
                return SqlTypeName.TIMESTAMP;
            default:
                return SqlTypeName.VARCHAR;
        }
    }

    private static AbstractTable buildTable(Map<String, Object> properties) {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();
                addProperties(properties, "", builder, typeFactory);
                return builder.build();
            }
        };
    }

    /**
     * Recursively walks a {@code properties} map, flattening object/nested containers into
     * dotted leaf field names (e.g. {@code agent.name},
     * {@code resource.attributes.telemetry.sdk.version}).
     *
     * <p>OpenSearch's {@link MappingMetadata#sourceAsMap()} normalizes object fields: when a
     * field has a {@code "properties"} subkey, the explicit {@code "type": "object"} is
     * stripped. Containers are therefore identified by the presence of {@code "properties"}
     * rather than by an explicit type. {@code "type": "nested"} containers are flattened the
     * same way — the parquet backend stores nested-child fields as flat dotted-name columns,
     * not as Arrow struct types. Containers with {@code "enabled": false} are skipped entirely —
     * they are excluded from the index and cannot be queried.
     *
     * <p>This mirrors {@code FieldStorageResolver.addProperties} so that every field exposed
     * in the Calcite schema is also resolvable by the planner's field-storage layer.
     */
    @SuppressWarnings("unchecked")
    private static void addProperties(
        Map<String, Object> properties,
        String prefix,
        RelDataTypeFactory.Builder builder,
        RelDataTypeFactory typeFactory
    ) {
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String fieldName = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object rawProps = entry.getValue();
            if (!(rawProps instanceof Map)) {
                continue;
            }
            Map<String, Object> fieldProps = (Map<String, Object>) rawProps;

            // Object/nested container: either no type at all (type stripped on reserialization)
            // or an explicit object/nested type with a nested properties map.
            Object nestedProps = fieldProps.get("properties");
            if (nestedProps instanceof Map) {
                if (Boolean.FALSE.equals(fieldProps.get("enabled"))) {
                    continue;
                }
                addProperties((Map<String, Object>) nestedProps, fieldName, builder, typeFactory);
                continue;
            }

            String fieldType = (String) fieldProps.get("type");
            if (fieldType == null) {
                // No type and no properties: nothing to expose (e.g. a disabled leaf).
                // Skip silently rather than throw; matches FieldStorageResolver behavior.
                continue;
            }
            // Defensive: a container type without a properties map would have been handled
            // above. If we somehow see a bare object/nested type with no children, skip it —
            // there are no leaves to emit.
            if ("nested".equals(fieldType) || "object".equals(fieldType)) {
                continue;
            }
            // OpenSearch types without a parquet-data-format representation. Declaring them
            // in the Calcite schema would make DataFusion's Substrait read fail with
            // "No field named X" because the parquet-writer layer never materializes a
            // column for these types. Skip silently — queries that project these fields
            // will fail at the analyzer with an unknown-column error, which is the
            // correct behavior until parquet support lands for them.
            if ("geo_point".equals(fieldType)
                || "geo_shape".equals(fieldType)
                || "point".equals(fieldType)
                || "shape".equals(fieldType)
                || "flat_object".equals(fieldType)
                || "completion".equals(fieldType)) {
                continue;
            }
            SqlTypeName sqlType;
            boolean isDateNanos = "date_nanos".equals(fieldType);
            if (("date".equals(fieldType) || isDateNanos) && fieldProps.get("format") instanceof String format) {
                // Narrow a `date`/`date_nanos` field to DATE or TIME when the mapping
                // declares a date-only or time-only `format:` (e.g. "basic_date",
                // "hour_minute_second"). Full-datetime, numeric (epoch_*), and
                // cross-bucket combined formats fall through to TIMESTAMP.
                //
                // Requires the parquet writer's per-format branch in DateParquetField
                // and DateNanosParquetField to emit matching Arrow Date32 / Time64
                // vectors; otherwise DataFusion rejects the substrait plan with
                // "different type (Date) than ... (Timestamp(ms))".
                sqlType = classifyDateFormat(format);
                // Calcite's SqlTypeName.TIME maxPrecision is 3 (ms). `date_nanos` with
                // a time-only format would need ns precision, which Calcite can't
                // represent — widen back to TIMESTAMP and let the default Timestamp
                // writer handle it (i.e. skip the narrowing for this edge case).
                if (sqlType == SqlTypeName.TIME && isDateNanos) {
                    sqlType = SqlTypeName.TIMESTAMP;
                }
            } else {
                sqlType = mapFieldType(fieldType);
            }
            // TIME needs explicit precision = 3 (ms) so substrait emits Time32(MILLI),
            // matching the Arrow Time vector the parquet writer emits for time-only
            // `date` fields. Default Calcite TIME is precision 0 (seconds) →
            // Time32(SECOND), which would mismatch. DataFusion rejects substrait plans
            // whose column type differs from parquet runtime schema (cf. task #14's
            // Float64/Float32 fix).
            RelDataType relType = sqlType == SqlTypeName.TIME
                ? typeFactory.createSqlType(SqlTypeName.TIME, 3)
                : typeFactory.createSqlType(sqlType);
            builder.add(fieldName, typeFactory.createTypeWithNullability(relType, true));
        }
    }

    // ---- OpenSearch date-format classification ----
    //
    // Mirrors the named-format buckets defined in the sql repo's
    // {@code OpenSearchDateType.SUPPORTED_NAMED_*} lists. We duplicate the lists
    // (rather than depend on sql-repo from sandbox) to keep the dependency direction
    // clean: sandbox is a backend for sql/PPL, not the other way around.
    //
    // If a format string names exactly one bucket → narrow to that bucket's
    // Calcite type (DATE/TIME). If it names full-datetime or numeric (epoch)
    // formats → widen to TIMESTAMP. Combined formats (a||b) that span buckets
    // also widen to TIMESTAMP.

    /** Formats producing a full date+time value — classify as TIMESTAMP. */
    private static final Set<String> DATETIME_FORMATS = Set.of(
        "iso8601",
        "basic_date_time",
        "basic_date_time_no_millis",
        "basic_ordinal_date_time",
        "basic_ordinal_date_time_no_millis",
        "basic_week_date_time",
        "strict_basic_week_date_time",
        "basic_week_date_time_no_millis",
        "strict_basic_week_date_time_no_millis",
        "date_optional_time",
        "strict_date_optional_time",
        "strict_date_optional_time_nanos",
        "date_time",
        "strict_date_time",
        "date_time_no_millis",
        "strict_date_time_no_millis",
        "date_hour_minute_second_fraction",
        "strict_date_hour_minute_second_fraction",
        "date_hour_minute_second_millis",
        "strict_date_hour_minute_second_millis",
        "date_hour_minute_second",
        "strict_date_hour_minute_second",
        "date_hour_minute",
        "strict_date_hour_minute",
        "date_hour",
        "strict_date_hour",
        "ordinal_date_time",
        "strict_ordinal_date_time",
        "ordinal_date_time_no_millis",
        "strict_ordinal_date_time_no_millis",
        "week_date_time",
        "strict_week_date_time",
        "week_date_time_no_millis",
        "strict_week_date_time_no_millis"
    );

    /** Numeric epoch formats — carry full timestamp. */
    private static final Set<String> NUMERIC_FORMATS = Set.of("epoch_millis", "epoch_second", "epoch_micros");

    /** Date-only formats — classify as DATE. */
    private static final Set<String> DATE_ONLY_FORMATS = Set.of(
        "basic_date",
        "basic_ordinal_date",
        "date",
        "strict_date",
        "year_month_day",
        "strict_year_month_day",
        "ordinal_date",
        "strict_ordinal_date",
        "week_date",
        "strict_week_date",
        "weekyear_week_day",
        "strict_weekyear_week_day",
        // basic_week_date covers year+week+weekday → date-only (no time component)
        "basic_week_date",
        "strict_basic_week_date"
    );

    /** Time-only formats — classify as TIME. */
    private static final Set<String> TIME_ONLY_FORMATS = Set.of(
        "basic_time",
        "basic_time_no_millis",
        "basic_t_time",
        "basic_t_time_no_millis",
        "time",
        "strict_time",
        "time_no_millis",
        "strict_time_no_millis",
        "hour_minute_second_fraction",
        "strict_hour_minute_second_fraction",
        "hour_minute_second_millis",
        "strict_hour_minute_second_millis",
        "hour_minute_second",
        "strict_hour_minute_second",
        "hour_minute",
        "strict_hour_minute",
        "hour",
        "strict_hour",
        "t_time",
        "strict_t_time",
        "t_time_no_millis",
        "strict_t_time_no_millis"
    );

    /**
     * Classifies an OpenSearch {@code format:} string to {@link SqlTypeName#DATE},
     * {@link SqlTypeName#TIME}, or {@link SqlTypeName#TIMESTAMP}.
     *
     * <p><b>Not yet wired into {@link #mapFieldType(String)}</b> — exposing this via the
     * Calcite schema requires the parquet write path to *also* emit Date32/Time64
     * vectors when the format is date-only/time-only, otherwise DataFusion rejects the
     * substrait plan with "different type (Date32) than ... (Timestamp(ms))". Tracked
     * as a follow-up task. The logic itself is complete and unit-tested so the
     * follow-up can wire it in once the writer-side variants exist.
     *
     * <p>Rules:
     * <ul>
     *   <li>{@code null}, empty, or unrecognized custom format → TIMESTAMP.</li>
     *   <li>Single named format in the date-only bucket → DATE.</li>
     *   <li>Single named format in the time-only bucket → TIME.</li>
     *   <li>Any format in the full-datetime or numeric (epoch) bucket → TIMESTAMP.</li>
     *   <li>Combined ({@code a||b}): if all alternatives sit in the same date-only /
     *       time-only bucket, narrow to that bucket; otherwise TIMESTAMP.</li>
     * </ul>
     *
     * @param format the {@code format:} metadata from the OpenSearch mapping; may be
     *     {@code null} or a pipe-pair-separated list of named formats and/or custom
     *     patterns.
     */
    public static SqlTypeName classifyDateFormat(String format) {
        if (format == null || format.isEmpty()) {
            return SqlTypeName.TIMESTAMP;
        }
        // Strip a leading "8" (OpenSearch marks Joda-vs-Java patterns with an "8"
        // prefix; see DateFormatter.strip8Prefix). For our lookup we just ignore it.
        String stripped = format.startsWith("8") ? format.substring(1) : format;
        List<String> parts = Arrays.stream(stripped.split("\\|\\|")).map(String::trim).filter(p -> !p.isEmpty()).toList();
        if (parts.isEmpty()) {
            return SqlTypeName.TIMESTAMP;
        }
        boolean sawDate = false;
        boolean sawTime = false;
        boolean sawTimestamp = false;
        for (String part : parts) {
            String lower = part.toLowerCase(java.util.Locale.ROOT);
            if (DATETIME_FORMATS.contains(lower) || NUMERIC_FORMATS.contains(lower)) {
                sawTimestamp = true;
            } else if (DATE_ONLY_FORMATS.contains(lower)) {
                sawDate = true;
            } else if (TIME_ONLY_FORMATS.contains(lower)) {
                sawTime = true;
            } else {
                // Custom pattern / unrecognized named format — safest widening is TIMESTAMP
                // so PPL operations that expect a full timestamp remain well-typed.
                sawTimestamp = true;
            }
            // Early out: any bucket mix collapses to TIMESTAMP.
            if ((sawDate && sawTime) || sawTimestamp) {
                return SqlTypeName.TIMESTAMP;
            }
        }
        if (sawDate) {
            return SqlTypeName.DATE;
        }
        if (sawTime) {
            return SqlTypeName.TIME;
        }
        return SqlTypeName.TIMESTAMP;
    }
}
