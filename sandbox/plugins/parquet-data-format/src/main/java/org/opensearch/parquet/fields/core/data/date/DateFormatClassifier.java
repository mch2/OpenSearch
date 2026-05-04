/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.date;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Narrows an OpenSearch {@code format:} string on a {@code date}/{@code date_nanos}
 * field mapping into one of three buckets — {@link Bucket#DATE_ONLY},
 * {@link Bucket#TIME_ONLY}, {@link Bucket#DATETIME} — that the parquet writer uses
 * to pick an Arrow column type ({@code Date32} / {@code Time(MS)} / {@code Timestamp(MS|NS)}).
 *
 * <p>This mirrors the identical classifier used by the analytics-engine's Calcite
 * schema builder in {@code OpenSearchSchemaBuilder.classifyDateFormat}. The logic
 * is duplicated rather than shared because the parquet-data-format module doesn't
 * depend on analytics-engine (and vice versa). Keeping the two in sync is a
 * documented maintenance requirement: if either drifts, substrait/parquet schema
 * types will diverge and DataFusion will reject the plan at runtime.
 *
 * <p>The classifier returns {@link Bucket#DATETIME} for:
 * <ul>
 *   <li>{@code null} or empty format,
 *   <li>any format listed in {@link #DATETIME_FORMATS} or {@link #NUMERIC_FORMATS},
 *   <li>custom (unrecognized) patterns, and
 *   <li>pipe-combined formats whose components span multiple buckets.
 * </ul>
 *
 * @opensearch.internal
 */
public final class DateFormatClassifier {

    private DateFormatClassifier() {}

    /** Classification result — determines which Arrow column type the writer picks. */
    public enum Bucket {
        /** Date-only format (e.g. {@code basic_date}, {@code year_month_day}). Arrow Date32. */
        DATE_ONLY,
        /** Time-only format (e.g. {@code hour_minute_second}, {@code t_time}). Arrow Time. */
        TIME_ONLY,
        /** Full datetime, epoch, custom, or cross-bucket combined. Arrow Timestamp. */
        DATETIME
    }

    /** Formats producing a full date+time value → DATETIME bucket. */
    private static final Set<String> DATETIME_FORMATS = Set.of(
        "iso8601", "basic_date_time", "basic_date_time_no_millis",
        "basic_ordinal_date_time", "basic_ordinal_date_time_no_millis",
        "basic_week_date_time", "strict_basic_week_date_time",
        "basic_week_date_time_no_millis", "strict_basic_week_date_time_no_millis",
        "date_optional_time", "strict_date_optional_time",
        "strict_date_optional_time_nanos",
        "date_time", "strict_date_time",
        "date_time_no_millis", "strict_date_time_no_millis",
        "date_hour_minute_second_fraction", "strict_date_hour_minute_second_fraction",
        "date_hour_minute_second_millis", "strict_date_hour_minute_second_millis",
        "date_hour_minute_second", "strict_date_hour_minute_second",
        "date_hour_minute", "strict_date_hour_minute",
        "date_hour", "strict_date_hour",
        "ordinal_date_time", "strict_ordinal_date_time",
        "ordinal_date_time_no_millis", "strict_ordinal_date_time_no_millis",
        "week_date_time", "strict_week_date_time",
        "week_date_time_no_millis", "strict_week_date_time_no_millis"
    );

    /** Numeric epoch formats — carry full timestamp. */
    private static final Set<String> NUMERIC_FORMATS = Set.of(
        "epoch_millis", "epoch_second", "epoch_micros"
    );

    /** Date-only formats → DATE_ONLY bucket. */
    private static final Set<String> DATE_ONLY_FORMATS = Set.of(
        "basic_date", "basic_ordinal_date",
        "date", "strict_date",
        "year_month_day", "strict_year_month_day",
        "ordinal_date", "strict_ordinal_date",
        "week_date", "strict_week_date",
        "weekyear_week_day", "strict_weekyear_week_day",
        "basic_week_date", "strict_basic_week_date"
    );

    /** Time-only formats → TIME_ONLY bucket. */
    private static final Set<String> TIME_ONLY_FORMATS = Set.of(
        "basic_time", "basic_time_no_millis",
        "basic_t_time", "basic_t_time_no_millis",
        "time", "strict_time",
        "time_no_millis", "strict_time_no_millis",
        "hour_minute_second_fraction", "strict_hour_minute_second_fraction",
        "hour_minute_second_millis", "strict_hour_minute_second_millis",
        "hour_minute_second", "strict_hour_minute_second",
        "hour_minute", "strict_hour_minute",
        "hour", "strict_hour",
        "t_time", "strict_t_time",
        "t_time_no_millis", "strict_t_time_no_millis"
    );

    /** Classifies the given OpenSearch {@code format:} string. See class javadoc for rules. */
    public static Bucket classify(String format) {
        if (format == null || format.isEmpty()) {
            return Bucket.DATETIME;
        }
        // Strip a leading "8" (OpenSearch marks Joda-vs-Java patterns with an "8"
        // prefix; see DateFormatter.strip8Prefix). For our lookup we just ignore it.
        String stripped = format.startsWith("8") ? format.substring(1) : format;
        List<String> parts = Arrays.stream(stripped.split("\\|\\|"))
            .map(String::trim)
            .filter(p -> !p.isEmpty())
            .toList();
        if (parts.isEmpty()) {
            return Bucket.DATETIME;
        }
        boolean sawDate = false;
        boolean sawTime = false;
        for (String part : parts) {
            String lower = part.toLowerCase(Locale.ROOT);
            if (DATETIME_FORMATS.contains(lower) || NUMERIC_FORMATS.contains(lower)) {
                return Bucket.DATETIME;
            } else if (DATE_ONLY_FORMATS.contains(lower)) {
                sawDate = true;
            } else if (TIME_ONLY_FORMATS.contains(lower)) {
                sawTime = true;
            } else {
                // Custom pattern / unrecognized — safest widening.
                return Bucket.DATETIME;
            }
            if (sawDate && sawTime) {
                return Bucket.DATETIME;
            }
        }
        if (sawDate) return Bucket.DATE_ONLY;
        if (sawTime) return Bucket.TIME_ONLY;
        return Bucket.DATETIME;
    }
}
