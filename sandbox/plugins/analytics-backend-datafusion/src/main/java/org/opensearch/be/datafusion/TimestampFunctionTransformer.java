/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Folds {@code TIMESTAMP(varchar_literal)} into a {@code TIMESTAMP} literal with
 * precision derived from the field's mapping type ({@code date}→3, {@code date_nanos}→9).
 *
 * <p>The SQL plugin emits {@code TIMESTAMP('2024-01-01T00:00:00Z')} as a function call
 * that isthmus can't convert (it's a UDF, not a standard Calcite literal). This
 * transformer folds it to a real timestamp literal before substrait conversion.
 *
 * @opensearch.internal
 */
class TimestampFunctionTransformer implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) return original;
        if (!(original.getOperands().get(0) instanceof RexLiteral literal)) return original;
        if (literal.getType().getSqlTypeName() != SqlTypeName.VARCHAR) return original;
        String value = literal.getValueAs(String.class);
        if (value == null) return original;

        int precision = resolveTimestampPrecision(original, fieldStorage);
        if (precision < 0) return original;

        return cluster.getRexBuilder().makeTimestampLiteral(parseTimestamp(value), precision);
    }

    private int resolveTimestampPrecision(RexNode node, List<FieldStorageInfo> fieldStorage) {
        Set<Integer> fieldIndices = new HashSet<>();
        collectFieldIndices(node, fieldIndices);
        for (int idx : fieldIndices) {
            if (idx < 0 || idx >= fieldStorage.size()) continue;
            String mappingType = fieldStorage.get(idx).getMappingType();
            if ("date".equals(mappingType)) return 3;
            if ("date_nanos".equals(mappingType)) return 9;
        }
        return -1;
    }

    private void collectFieldIndices(RexNode node, Set<Integer> result) {
        if (node instanceof RexInputRef inputRef) {
            result.add(inputRef.getIndex());
        } else if (node instanceof RexCall rexCall) {
            for (RexNode operand : rexCall.getOperands()) {
                collectFieldIndices(operand, result);
            }
        }
    }

    static TimestampString parseTimestamp(String input) {
        try {
            LocalDate date = LocalDate.parse(input);
            return toTimestampString(date.atStartOfDay());
        } catch (DateTimeParseException ignored) {}

        try {
            OffsetDateTime odt = OffsetDateTime.parse(input);
            return toTimestampString(LocalDateTime.ofInstant(odt.toInstant(), ZoneOffset.UTC));
        } catch (DateTimeParseException ignored) {}

        try {
            Instant instant = Instant.parse(input);
            return toTimestampString(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
        } catch (DateTimeParseException ignored) {}

        try {
            LocalDateTime ldt = LocalDateTime.parse(input);
            return toTimestampString(ldt);
        } catch (DateTimeParseException ignored) {}

        return new TimestampString(input);
    }

    private static TimestampString toTimestampString(LocalDateTime ldt) {
        TimestampString ts = new TimestampString(
            ldt.getYear(),
            ldt.getMonthValue(),
            ldt.getDayOfMonth(),
            ldt.getHour(),
            ldt.getMinute(),
            ldt.getSecond()
        );
        int nanos = ldt.getNano();
        if (nanos > 0) {
            ts = ts.withNanos(nanos);
        }
        return ts;
    }
}
