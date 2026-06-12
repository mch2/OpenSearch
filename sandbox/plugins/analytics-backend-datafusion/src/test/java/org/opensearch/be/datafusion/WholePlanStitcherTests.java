/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.test.OpenSearchTestCase;

import com.google.protobuf.Any;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;

import java.util.Map;

/**
 * Unit tests for {@link WholePlanStitcher} — proves the per-stage Substrait plans stitch into
 * one whole-query plan with {@code os_stage_boundary} {@link io.substrait.proto.ExtensionSingleRel}
 * markers in place of each {@code input-<childId>} read, byte-compatible with the Rust consumer.
 */
public class WholePlanStitcherTests extends OpenSearchTestCase {

    /** A stage plan: Aggregate over a named-table read (real table or {@code input-<id>}). */
    private static byte[] aggOverRead(String tableName, String... names) {
        ReadRel read = ReadRel.newBuilder()
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames(tableName).build())
            .build();
        Rel readRel = Rel.newBuilder().setRead(read).build();
        Rel aggRel = Rel.newBuilder().setAggregate(AggregateRel.newBuilder().setInput(readRel).build()).build();
        RelRoot root = RelRoot.newBuilder().setInput(aggRel).addAllNames(java.util.Arrays.asList(names)).build();
        return Plan.newBuilder().addRelations(PlanRel.newBuilder().setRoot(root).build()).build().toByteArray();
    }

    /** Two-stage SUM: stage 0 (shard) partial-aggregates the real table; stage 1 (root) reads input-0. */
    public void testStitchesTwoStageBoundary() throws Exception {
        byte[] shard = aggOverRead("http_logs", "k", "s");   // stage 0: real table scan leaf
        byte[] reduce = aggOverRead("input-0", "k", "s");     // stage 1: reads child stage 0

        byte[] stitchedBytes = WholePlanStitcher.stitch(1, Map.of(0, shard, 1, reduce));
        Plan stitched = Plan.parseFrom(stitchedBytes);

        // Root is the reduce aggregate; its input is the os_stage_boundary marker (not a read).
        assertEquals(1, stitched.getRelationsCount());
        RelRoot root = stitched.getRelations(0).getRoot();
        assertEquals("result column names preserved", java.util.List.of("k", "s"), root.getNamesList());

        Rel rootRel = root.getInput();
        assertEquals(Rel.RelTypeCase.AGGREGATE, rootRel.getRelTypeCase());
        Rel reduceInput = rootRel.getAggregate().getInput();

        // The input-0 read became an os_stage_boundary marker for boundary_id=0.
        assertEquals(Rel.RelTypeCase.EXTENSION_SINGLE, reduceInput.getRelTypeCase());
        Any detail = reduceInput.getExtensionSingle().getDetail();
        assertEquals(WholePlanStitcher.STAGE_BOUNDARY_TYPE_URL, detail.getTypeUrl());
        String json = detail.getValue().toStringUtf8();
        assertTrue("boundary_id is the child stage id: " + json, json.contains("\"boundary_id\":0"));
        assertTrue("exchange GATHER: " + json, json.contains("\"exchange_type\":\"GATHER\""));

        // Below the marker is the child stage's own tree: an aggregate over the REAL table scan.
        Rel childTree = reduceInput.getExtensionSingle().getInput();
        assertEquals(Rel.RelTypeCase.AGGREGATE, childTree.getRelTypeCase());
        Rel childLeaf = childTree.getAggregate().getInput();
        assertEquals(Rel.RelTypeCase.READ, childLeaf.getRelTypeCase());
        assertEquals("http_logs", childLeaf.getRead().getNamedTable().getNames(0));
        // The real scan leaf is NOT wrapped — it stays a read for the Rust scan-leaf swap.
    }

    /** Three-stage tree: root reads input-1, stage 1 reads input-0, stage 0 scans the table. */
    public void testStitchesThreeStageChain() throws Exception {
        byte[] s0 = aggOverRead("logs", "v");
        byte[] s1 = aggOverRead("input-0", "v");
        byte[] s2 = aggOverRead("input-1", "v");

        Plan stitched = Plan.parseFrom(WholePlanStitcher.stitch(2, Map.of(0, s0, 1, s1, 2, s2)));
        Rel root = stitched.getRelations(0).getRoot().getInput();

        // root agg -> boundary(1) -> agg -> boundary(0) -> agg -> read(logs)
        Rel b1 = root.getAggregate().getInput();
        assertEquals(Rel.RelTypeCase.EXTENSION_SINGLE, b1.getRelTypeCase());
        assertTrue(b1.getExtensionSingle().getDetail().getValue().toStringUtf8().contains("\"boundary_id\":1"));

        Rel b0 = b1.getExtensionSingle().getInput().getAggregate().getInput();
        assertEquals(Rel.RelTypeCase.EXTENSION_SINGLE, b0.getRelTypeCase());
        assertTrue(b0.getExtensionSingle().getDetail().getValue().toStringUtf8().contains("\"boundary_id\":0"));

        Rel leaf = b0.getExtensionSingle().getInput().getAggregate().getInput();
        assertEquals(Rel.RelTypeCase.READ, leaf.getRelTypeCase());
        assertEquals("logs", leaf.getRead().getNamedTable().getNames(0));
    }

    /** A single-stage plan (no child reads) stitches to itself — no markers. */
    public void testSingleStageNoBoundary() throws Exception {
        byte[] only = aggOverRead("idx", "c");
        Plan stitched = Plan.parseFrom(WholePlanStitcher.stitch(0, Map.of(0, only)));
        Rel leaf = stitched.getRelations(0).getRoot().getInput().getAggregate().getInput();
        assertEquals(Rel.RelTypeCase.READ, leaf.getRelTypeCase());
        assertEquals("idx", leaf.getRead().getNamedTable().getNames(0));
    }
}
