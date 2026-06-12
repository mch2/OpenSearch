/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.analytics.spi.DelegationDescriptor;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.InstructionType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Transport request carrying plan fragment alternatives to a data node for shard-level execution.
 *
 * <p>Each {@link PlanAlternative} represents a backend-specific serialized plan produced by
 * {@code FragmentConversionDriver}. The data node selects the best alternative based on
 * available backend capabilities.
 *
 * @opensearch.internal
 */
public class FragmentExecutionRequest extends ActionRequest implements ShardInvocationRequest {

    /** Sentinel for {@link #planFormatVersion} on a legacy (Substrait + side-channel) request. */
    public static final int PLAN_FORMAT_VERSION_LEGACY = 0;

    /**
     * Current DF_PROTO plan format version (df-proto migration D8). Bumped when the
     * {@code datafusion-proto} wire encoding or codec changes incompatibly. The data node
     * compares this against its own and throws {@link PlanFormatMismatchException} on skew.
     */
    public static final int PLAN_FORMAT_VERSION_CURRENT = 1;

    /**
     * The workspace DataFusion version the proto plans are encoded against (D8). Must match
     * the Rust crate's pinned `datafusion = "=54.0.0"`. Carried in DF_PROTO requests so the
     * data node can reject a plan from a coordinator on a different DataFusion.
     */
    public static final String DATAFUSION_VERSION = "54.0.0";

    private final String queryId;
    private final int stageId;
    private final ShardId shardId;
    private final List<PlanAlternative> planAlternatives;

    // DF_PROTO form (df-proto migration D8/D14). When {@code planFormatVersion > 0} this request
    // carries one finalized DataFusion physical plan instead of {@code planAlternatives}.
    private final int planFormatVersion;
    private final String dataFusionVersion;
    private final byte[] planBytes;

    /** Legacy constructor: Substrait fragment alternatives + side channels. */
    public FragmentExecutionRequest(String queryId, int stageId, ShardId shardId, List<PlanAlternative> planAlternatives) {
        this.queryId = queryId;
        this.stageId = stageId;
        this.shardId = shardId;
        this.planAlternatives = planAlternatives;
        this.planFormatVersion = PLAN_FORMAT_VERSION_LEGACY;
        this.dataFusionVersion = null;
        this.planBytes = null;
    }

    /**
     * DF_PROTO constructor (D14): exactly one finalized plan for this stage — no
     * {@code PlanAlternative} list, no instructions, no delegation descriptor.
     */
    public FragmentExecutionRequest(
        String queryId,
        int stageId,
        ShardId shardId,
        int planFormatVersion,
        String dataFusionVersion,
        byte[] planBytes
    ) {
        this.queryId = queryId;
        this.stageId = stageId;
        this.shardId = shardId;
        this.planAlternatives = List.of();
        this.planFormatVersion = planFormatVersion;
        this.dataFusionVersion = dataFusionVersion;
        this.planBytes = planBytes;
    }

    public FragmentExecutionRequest(StreamInput in) throws IOException {
        super(in);
        this.queryId = in.readString();
        this.stageId = in.readInt();
        this.shardId = new ShardId(in);
        int numAlternatives = in.readVInt();
        this.planAlternatives = new ArrayList<>(numAlternatives);
        for (int i = 0; i < numAlternatives; i++) {
            planAlternatives.add(new PlanAlternative(in));
        }
        // DF_PROTO trailer (backward-compatible: a legacy peer writes false).
        if (in.readBoolean()) {
            this.planFormatVersion = in.readVInt();
            this.dataFusionVersion = in.readString();
            this.planBytes = in.readByteArray();
        } else {
            this.planFormatVersion = PLAN_FORMAT_VERSION_LEGACY;
            this.dataFusionVersion = null;
            this.planBytes = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(queryId);
        out.writeInt(stageId);
        shardId.writeTo(out);
        out.writeVInt(planAlternatives.size());
        for (PlanAlternative alt : planAlternatives) {
            alt.writeTo(out);
        }
        // DF_PROTO trailer.
        if (planFormatVersion != PLAN_FORMAT_VERSION_LEGACY) {
            out.writeBoolean(true);
            out.writeVInt(planFormatVersion);
            out.writeString(dataFusionVersion != null ? dataFusionVersion : "");
            out.writeByteArray(planBytes != null ? planBytes : new byte[0]);
        } else {
            out.writeBoolean(false);
        }
    }

    /** True if this is a DF_PROTO request carrying one finalized physical plan (D14). */
    public boolean isProtoFormat() {
        return planFormatVersion != PLAN_FORMAT_VERSION_LEGACY;
    }

    public int getPlanFormatVersion() {
        return planFormatVersion;
    }

    public String getDataFusionVersion() {
        return dataFusionVersion;
    }

    public byte[] getPlanBytes() {
        return planBytes;
    }

    public String getQueryId() {
        return queryId;
    }

    public int getStageId() {
        return stageId;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public List<PlanAlternative> getPlanAlternatives() {
        return planAlternatives;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        String desc = "queryId[" + queryId + "] stageId[" + stageId + "] shardId[" + shardId + "]";
        return new AnalyticsShardTask(id, type, action, desc, parentTaskId, headers);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * A single plan alternative: a backend ID paired with its serialized fragment bytes
     * and ordered instruction nodes for data-node execution.
     * Produced by {@code FragmentConversionDriver.convertAll()} using the backend's
     * {@code FragmentConvertor}.
     */
    public static class PlanAlternative {
        private final String backendId;
        private final byte[] fragmentBytes;
        private final List<InstructionNode> instructions;
        private final DelegationDescriptor delegationDescriptor;

        public PlanAlternative(String backendId, byte[] fragmentBytes, List<InstructionNode> instructions) {
            this(backendId, fragmentBytes, instructions, null);
        }

        public PlanAlternative(
            String backendId,
            byte[] fragmentBytes,
            List<InstructionNode> instructions,
            DelegationDescriptor delegationDescriptor
        ) {
            this.backendId = backendId;
            this.fragmentBytes = fragmentBytes;
            this.instructions = instructions;
            this.delegationDescriptor = delegationDescriptor;
        }

        public PlanAlternative(StreamInput in) throws IOException {
            this.backendId = in.readString();
            byte[] bytes = in.readByteArray();
            this.fragmentBytes = (bytes.length == 0) ? null : bytes;
            int instructionCount = in.readVInt();
            List<InstructionNode> nodes = new ArrayList<>(instructionCount);
            for (int i = 0; i < instructionCount; i++) {
                InstructionType type = in.readEnum(InstructionType.class);
                nodes.add(type.readNode(in));
            }
            this.instructions = nodes;
            this.delegationDescriptor = in.readBoolean() ? new DelegationDescriptor(in) : null;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(backendId);
            out.writeByteArray(fragmentBytes != null ? fragmentBytes : new byte[0]);
            out.writeVInt(instructions.size());
            for (InstructionNode node : instructions) {
                out.writeEnum(node.type());
                node.writeTo(out);
            }
            if (delegationDescriptor != null) {
                out.writeBoolean(true);
                delegationDescriptor.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }

        public String getBackendId() {
            return backendId;
        }

        public byte[] getFragmentBytes() {
            return fragmentBytes;
        }

        public List<InstructionNode> getInstructions() {
            return instructions;
        }

        public DelegationDescriptor getDelegationDescriptor() {
            return delegationDescriptor;
        }
    }
}
