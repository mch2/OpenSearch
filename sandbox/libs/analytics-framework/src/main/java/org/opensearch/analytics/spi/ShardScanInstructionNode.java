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
 * Instruction node for base shard scan setup — reader acquisition, SessionContext creation,
 * default table provider registration.
 *
 * <p>Carries the logical table name (alias/pattern the coordinator planned against) so the
 * data node registers the table under the correct name for Substrait plan binding.
 *
 * @opensearch.internal
 */
public class ShardScanInstructionNode implements InstructionNode {

    private final String logicalTableName;

    public ShardScanInstructionNode() {
        this((String) null);
    }

    public ShardScanInstructionNode(String logicalTableName) {
        this.logicalTableName = logicalTableName;
    }

    public ShardScanInstructionNode(StreamInput in) throws IOException {
        this.logicalTableName = in.readOptionalString();
    }

    @Override
    public InstructionType type() {
        return InstructionType.SETUP_SHARD_SCAN;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(logicalTableName);
    }

    public String getLogicalTableName() {
        return logicalTableName;
    }
}
