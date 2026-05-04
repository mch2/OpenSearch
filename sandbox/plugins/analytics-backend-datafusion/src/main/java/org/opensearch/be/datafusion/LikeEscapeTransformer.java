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
 * Drops the 3rd (escape) operand from LIKE calls. Calcite's grammar always emits
 * {@code LIKE(input, pattern, escape)}; the substrait spec declares only a 2-arg
 * variant. DataFusion's substrait consumer expects 2 args.
 *
 * @opensearch.internal
 */
class LikeEscapeTransformer implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 3) return original;
        return original.clone(original.getType(), original.getOperands().subList(0, 2));
    }
}
