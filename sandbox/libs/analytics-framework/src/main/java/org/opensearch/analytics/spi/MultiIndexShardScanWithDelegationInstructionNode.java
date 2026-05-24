/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Instruction node for multi-index shard scan with filter delegation. Combines the multi-index
 * concern (logical table name, schema widening) with the delegation concern (tree shape,
 * predicate count).
 *
 * @opensearch.internal
 */
public class MultiIndexShardScanWithDelegationInstructionNode extends ShardScanWithDelegationInstructionNode {

    private final String logicalTableName;

    public MultiIndexShardScanWithDelegationInstructionNode(
        FilterTreeShape treeShape,
        int delegatedPredicateCount,
        String logicalTableName
    ) {
        super(treeShape, delegatedPredicateCount);
        this.logicalTableName = logicalTableName;
    }

    public MultiIndexShardScanWithDelegationInstructionNode(StreamInput in) throws IOException {
        super(in);
        this.logicalTableName = in.readString();
    }

    @Override
    public InstructionType type() {
        return InstructionType.SETUP_MULTI_INDEX_SHARD_SCAN_WITH_DELEGATION;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(logicalTableName);
    }

    public String getLogicalTableName() {
        return logicalTableName;
    }
}
