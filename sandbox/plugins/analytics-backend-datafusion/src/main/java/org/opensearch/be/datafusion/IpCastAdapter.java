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
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Strips PPL's {@code IP(string)} cast wrapper. Calcite inserts this cast on
 * literals when they appear on an IP comparison operator (e.g.
 * {@code where ip_field = '1.2.3.4'} becomes
 * {@code equals_ip(ip_field, IP('1.2.3.4'))}). The cast is Calcite-enumerable
 * only — substrait's converter has no mapping for it. The downstream Rust IP
 * comparison UDFs ({@code equals_ip}, {@code less_ip}, ...) accept Utf8
 * operands and canonicalize internally, so the wrapper is a no-op and we can
 * just unwrap to the inner string operand.
 *
 * <p>The IP field column on the other side comes through as VARBINARY → Arrow
 * Binary (16-byte InetAddressPoint). The Rust UDF handles both Binary and
 * Utf8 operands via {@code Signature::one_of} over the four combinations.
 *
 * @opensearch.internal
 */
class IpCastAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) return original;
        return original.getOperands().get(0);
    }
}
