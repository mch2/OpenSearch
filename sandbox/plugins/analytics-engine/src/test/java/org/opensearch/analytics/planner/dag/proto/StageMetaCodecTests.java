/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag.proto;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Wire-format parity tests for {@link StageMetaCodec} / {@link ProtoWriter}.
 *
 * <p>The reference hex strings are produced by the Rust prost encoder in
 * {@code rust/src/proto.rs} for the same logical message. Asserting byte equality
 * here guarantees the hand-written Java encoder and the prost structs agree on the
 * wire — the contract the native finalizer depends on.
 */
public class StageMetaCodecTests extends OpenSearchTestCase {

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * A FINAL reduce stage (id=2, child=[1], aggMode=FINAL, leaf=STAGE_INPUT, all
     * other fields default). prost reference: {@code 080212010118022001}.
     */
    public void testStageMetaMatchesProstReference() {
        StageMetaCodec.StageMeta meta = new StageMetaCodec.StageMeta(
            2,
            new int[] { 1 },
            StageMetaCodec.AGG_MODE_FINAL,
            StageMetaCodec.LEAF_KIND_STAGE_INPUT,
            0,
            false,
            List.of(),
            List.of(),
            null,
            List.of()
        );
        // StageMeta.encode() is package-private; this test lives in the same package.
        byte[] metaBytes = meta.encode();
        assertEquals("080212010118022001", hex(metaBytes));
    }

    /** A defaults-only StageMeta encodes to empty bytes (proto3 default elision). */
    public void testEmptyStageMetaIsEmpty() {
        StageMetaCodec.StageMeta meta = new StageMetaCodec.StageMeta(
            0,
            new int[0],
            StageMetaCodec.AGG_MODE_NONE,
            StageMetaCodec.LEAF_KIND_SHARD_SCAN,
            0,
            false,
            List.of(),
            List.of(),
            null,
            List.of()
        );
        assertEquals("", hex(meta.encode()));
    }

    /** A DelegatedExpr round-trips its three fields. prost ref for {id=42,"lucene",[de ad]}. */
    public void testDelegatedExprFields() {
        // 08 2a            field1 int32 = 42
        // 12 06 6c756365 6e65   field2 string "lucene"
        // 1a 02 dead       field3 bytes [de ad]
        StageMetaCodec.DelegatedExpr d = new StageMetaCodec.DelegatedExpr(42, "lucene", new byte[] { (byte) 0xde, (byte) 0xad });
        assertEquals("082a12066c7563656e651a02dead", hex(d.encode()));
    }

    /** FinalizeResponse decode reads {stage_id, plan_bytes} pairs. */
    public void testDecodeFinalizeResponse() {
        // Build a response: one FinalizedStageProto { stage_id=2, plan_bytes=[01 02 03] }.
        // 0a 07              field1 (FinalizedStageProto), len 7
        //   08 02            field1 stage_id = 2
        //   12 03 010203     field2 plan_bytes = [01 02 03]
        byte[] response = new byte[] { 0x0a, 0x07, 0x08, 0x02, 0x12, 0x03, 0x01, 0x02, 0x03 };
        List<StageMetaCodec.FinalizedStage> plans = StageMetaCodec.decodeFinalizeResponse(response);
        assertEquals(1, plans.size());
        assertEquals(2, plans.get(0).stageId());
        assertEquals("010203", hex(plans.get(0).planBytes()));
    }
}
