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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.NlsString;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Rewrites {@code GET_FORMAT(type, region)} → constant string literal.
 * Both args must be literals; DataFusion never sees the call.
 */
class GetFormatAdapter implements ScalarFunctionAdapter {

    private static final Map<String, String> FORMATS = Map.ofEntries(
        Map.entry("date|usa", "%m.%d.%Y"),
        Map.entry("date|jis", "%Y-%m-%d"),
        Map.entry("date|iso", "%Y-%m-%d"),
        Map.entry("date|eur", "%d.%m.%Y"),
        Map.entry("date|internal", "%Y%m%d"),
        Map.entry("time|usa", "%h:%i:%s %p"),
        Map.entry("time|jis", "%H:%i:%s"),
        Map.entry("time|iso", "%H:%i:%s"),
        Map.entry("time|eur", "%H.%i.%s"),
        Map.entry("time|internal", "%H%i%s"),
        Map.entry("timestamp|usa", "%Y-%m-%d %H.%i.%s"),
        Map.entry("timestamp|jis", "%Y-%m-%d %H:%i:%s"),
        Map.entry("timestamp|iso", "%Y-%m-%d %H:%i:%s"),
        Map.entry("timestamp|eur", "%Y-%m-%d %H.%i.%s"),
        Map.entry("timestamp|internal", "%Y%m%d%H%i%s")
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            throw new IllegalArgumentException("GET_FORMAT expects 2 operands, got " + original.getOperands().size());
        }
        String type = stringOf(original.getOperands().get(0));
        String region = stringOf(original.getOperands().get(1));
        // Non-literal args are a legitimate guard — a later stage (isthmus) will surface the
        // unfolded call as an error. Return original so the stack trace points at that stage
        // rather than this adapter.
        if (type == null || region == null) return original;
        String format = FORMATS.get(type.toLowerCase(Locale.ROOT) + "|" + region.toLowerCase(Locale.ROOT));
        if (format == null) {
            throw new IllegalArgumentException(
                "GET_FORMAT has no known format for type='" + type + "' region='" + region + "'"
            );
        }
        return cluster.getRexBuilder().makeLiteral(format, original.getType(), false);
    }

    private static String stringOf(RexNode node) {
        if (!(node instanceof RexLiteral lit)) return null;
        Object v = lit.getValue();
        if (v instanceof NlsString) return ((NlsString) v).getValue();
        if (v instanceof String) return (String) v;
        return null;
    }
}
