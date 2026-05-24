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
 * Instruction node for multi-index shard scan (alias/pattern queries). Extends the base scan
 * with the logical table name the coordinator planned against. The data node registers the table
 * under this name (so the Substrait consumer can bind) and widens the inferred schema from the
 * plan's base_schema to null-fill columns this shard doesn't have.
 *
 * @opensearch.internal
 */
public class MultiIndexShardScanInstructionNode extends ShardScanInstructionNode {

    private final String logicalTableName;

    public MultiIndexShardScanInstructionNode(String logicalTableName) {
        this.logicalTableName = logicalTableName;
    }

    public MultiIndexShardScanInstructionNode(StreamInput in) throws IOException {
        super(in);
        this.logicalTableName = in.readString();
    }

    @Override
    public InstructionType type() {
        return InstructionType.SETUP_MULTI_INDEX_SHARD_SCAN;
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
