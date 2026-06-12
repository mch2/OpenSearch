/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag.proto;

import java.util.ArrayList;
import java.util.List;

/**
 * Coordinator-side encoder for the {@code FinalizeRequest} / {@code StageMeta}
 * protobuf messages and decoder for {@code FinalizeResponse} (df-proto migration §5).
 *
 * <p>Field numbers MUST match {@code rust/src/proto.rs} (and the canonical
 * {@code proto/stage.proto}) exactly — they are the wire contract with the native
 * finalizer. Changing one requires changing all three in lockstep.
 *
 * @opensearch.internal
 */
public final class StageMetaCodec {

    private StageMetaCodec() {}

    // ---- AggMode / LeafKind enum ordinals (match proto/stage.proto) ----
    public static final int AGG_MODE_NONE = 0;
    public static final int AGG_MODE_PARTIAL = 1;
    public static final int AGG_MODE_FINAL = 2;

    public static final int LEAF_KIND_SHARD_SCAN = 0;
    public static final int LEAF_KIND_STAGE_INPUT = 1;
    public static final int LEAF_KIND_VALUES = 2;
    public static final int LEAF_KIND_LM_OUTPUT = 3;

    /** One delegated predicate payload — mirrors {@code proto::DelegatedExpr}. */
    public record DelegatedExpr(int annotationId, String backendId, byte[] payload) {
        byte[] encode() {
            return new ProtoWriter().int32(1, annotationId).string(2, backendId).bytes(3, payload).toByteArray();
        }
    }

    /** Per-stage metadata — mirrors {@code proto::StageMeta}. */
    public record StageMeta(
        int stageId,
        int[] childStageIds,
        int aggMode,
        int leafKind,
        int treeShape,
        boolean requestsRowIds,
        List<DelegatedExpr> delegated,
        List<byte[]> declaredInputRowTypeIpc,
        byte[] lmOutputRowTypeIpc,
        List<byte[]> childPartialSubstrait
    ) {
        byte[] encode() {
            ProtoWriter w = new ProtoWriter().int32(1, stageId)
                .packedInt32(2, childStageIds)
                .enumValue(3, aggMode)
                .enumValue(4, leafKind)
                .int32(5, treeShape)
                .bool(6, requestsRowIds);
            if (delegated != null) {
                for (DelegatedExpr d : delegated) {
                    w.message(7, d.encode());
                }
            }
            if (declaredInputRowTypeIpc != null) {
                for (byte[] ipc : declaredInputRowTypeIpc) {
                    w.message(8, serializedSchema(ipc));
                }
            }
            if (lmOutputRowTypeIpc != null) {
                w.message(9, serializedSchema(lmOutputRowTypeIpc));
            }
            if (childPartialSubstrait != null) {
                // repeated bytes (parallel to childStageIds): each element a length-delimited
                // field 10. Empty elements are still emitted to keep positional alignment.
                for (byte[] b : childPartialSubstrait) {
                    w.bytesAllowEmpty(10, b != null ? b : new byte[0]);
                }
            }
            return w.toByteArray();
        }
    }

    /** One stage in a {@code FinalizeRequest}. */
    public record FinalizeStage(byte[] substraitBytes, StageMeta meta) {
        byte[] encode() {
            return new ProtoWriter().bytes(1, substraitBytes).message(2, meta.encode()).toByteArray();
        }
    }

    /** Encode a whole {@code FinalizeRequest} (all stages) for the FFM finalize call. */
    public static byte[] encodeFinalizeRequest(List<FinalizeStage> stages) {
        ProtoWriter w = new ProtoWriter();
        for (FinalizeStage s : stages) {
            w.message(1, s.encode());
        }
        return w.toByteArray();
    }

    /** {@code SerializedSchema { bytes ipc = 1; }} */
    private static byte[] serializedSchema(byte[] ipc) {
        return new ProtoWriter().bytes(1, ipc).toByteArray();
    }

    // ---- FinalizeResponse decode ----

    /** One finalized stage's plan bytes — mirrors {@code proto::FinalizedStageProto}. */
    public record FinalizedStage(int stageId, byte[] planBytes) {}

    /**
     * Decode a {@code FinalizeResponse} (repeated FinalizedStageProto, field 1) into a list.
     * Implements just enough of the protobuf reader: length-delimited sub-messages with an
     * int32 field 1 (stage_id) and bytes field 2 (plan_bytes).
     */
    public static List<FinalizedStage> decodeFinalizeResponse(byte[] bytes) {
        List<FinalizedStage> result = new ArrayList<>();
        ProtoReader r = new ProtoReader(bytes);
        while (r.hasRemaining()) {
            int key = (int) r.readVarint();
            int field = key >>> 3;
            int wire = key & 0x7;
            if (field == 1 && wire == 2) {
                byte[] sub = r.readLengthDelimited();
                result.add(decodeFinalizedStage(sub));
            } else {
                r.skip(wire);
            }
        }
        return result;
    }

    private static FinalizedStage decodeFinalizedStage(byte[] bytes) {
        ProtoReader r = new ProtoReader(bytes);
        int stageId = 0;
        byte[] planBytes = new byte[0];
        while (r.hasRemaining()) {
            int key = (int) r.readVarint();
            int field = key >>> 3;
            int wire = key & 0x7;
            if (field == 1 && wire == 0) {
                stageId = (int) r.readVarint();
            } else if (field == 2 && wire == 2) {
                planBytes = r.readLengthDelimited();
            } else {
                r.skip(wire);
            }
        }
        return new FinalizedStage(stageId, planBytes);
    }
}
