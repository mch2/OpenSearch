/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Transport response carrying field names and result rows from a shard
 * fragment execution.
 *
 * <p>Each cell value is serialized via {@link StreamOutput#writeGenericValue(Object)} /
 * {@link StreamInput#readGenericValue()}, which handle common Java types
 * (String, Long, Double, Integer, null, byte[], etc.).
 *
 * <p>Cells are coerced at construction time (see {@link #coerce}). This
 * normalizes values into types {@code writeGenericValue} can serialize and
 * — critically — formats {@link java.time} values as OpenSearch SQL canonical
 * MySQL-style strings ({@code "yyyy-MM-dd HH:mm:ss"}) rather than the ISO
 * format that {@link LocalDateTime#toString()} produces. Without this,
 * a single-node test using {@code DirectResponseChannel} bypasses serialization
 * entirely, so the ISO-formatted cell would leak to downstream consumers
 * (e.g. Arrow {@code VarCharVector.setSafe(toString())} in the row codec)
 * and never see a proper formatter.
 *
 * <p>Wire format: {@code fieldNames (string list) + rowCount (vint) + per-row (colCount (vint) + cells)}.
 *
 * @opensearch.internal
 */
public class FragmentExecutionResponse extends ActionResponse {

    // OpenSearch SQL canonical MySQL-style formatters (mirror
    // org.opensearch.sql.utils.DateTimeFormatters.DATE_TIME_FORMATTER_WITHOUT_NANO and
    // org.opensearch.sql.data.model.ExprDateValue/ExprTimeValue formats). Inlined
    // here because the sandbox analytics-engine module cannot depend on the sql repo.
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT);
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT);

    private final List<String> fieldNames;
    private final List<Object[]> rows;

    public FragmentExecutionResponse(List<String> fieldNames, List<Object[]> rows) {
        this.fieldNames = fieldNames;
        this.rows = coerceRows(rows);
    }

    public FragmentExecutionResponse(StreamInput in) throws IOException {
        super(in);
        this.fieldNames = in.readStringList();
        int rowCount = in.readVInt();
        this.rows = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            int colCount = in.readVInt();
            Object[] row = new Object[colCount];
            for (int c = 0; c < colCount; c++) {
                row[c] = in.readGenericValue();
            }
            rows.add(row);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(fieldNames);
        out.writeVInt(rows.size());
        for (Object[] row : rows) {
            out.writeVInt(row.length);
            for (Object cell : row) {
                // Cells are pre-coerced at construction; no further coercion needed.
                out.writeGenericValue(cell);
            }
        }
    }

    private static List<Object[]> coerceRows(List<Object[]> rows) {
        List<Object[]> coerced = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            Object[] newRow = new Object[row.length];
            for (int i = 0; i < row.length; i++) {
                newRow[i] = coerce(row[i]);
            }
            coerced.add(newRow);
        }
        return coerced;
    }

    /**
     * Coerces a cell value into a type that {@link StreamOutput#writeGenericValue}
     * can serialize. Arrow's materializer can produce types (Text, JsonStringArrayList)
     * that writeGenericValue doesn't handle — convert them to standard Java types here
     * so serialization failures don't cascade into framework-level assertion errors.
     */
    private static Object coerce(Object cell) {
        if (cell == null) return null;
        if (cell instanceof List<?> list) {
            List<Object> coerced = new ArrayList<>(list.size());
            for (Object elem : list) {
                coerced.add(coerce(elem));
            }
            return coerced;
        }
        if (cell instanceof Number || cell instanceof String || cell instanceof Boolean || cell instanceof byte[]) {
            return cell;
        }
        // java.time.* values default to ISO-8601 via toString() (e.g. LocalDateTime
        // emits "2026-01-15T10:30" — with a T separator, and elides :00 seconds).
        // OpenSearch SQL's canonical wire format is MySQL-style
        // "yyyy-MM-dd HH:mm:ss". Format java.time types explicitly so downstream
        // ExprValue wrapping and test assertions see the expected format.
        if (cell instanceof LocalDateTime ldt) {
            return TIMESTAMP_FORMATTER.format(ldt);
        }
        if (cell instanceof Instant instant) {
            return TIMESTAMP_FORMATTER.withZone(ZoneOffset.UTC).format(instant);
        }
        if (cell instanceof LocalDate ld) {
            return DATE_FORMATTER.format(ld);
        }
        if (cell instanceof LocalTime lt) {
            return TIME_FORMATTER.format(lt);
        }
        return cell.toString();
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<Object[]> getRows() {
        return rows;
    }
}
