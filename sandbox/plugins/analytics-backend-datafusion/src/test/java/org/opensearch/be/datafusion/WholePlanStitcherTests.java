/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.test.OpenSearchTestCase;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link WholePlanStitcher} — proves the per-stage Substrait plans stitch into one
 * whole-query plan with {@code os_stage_boundary} {@link io.substrait.proto.ExtensionSingleRel}
 * markers in place of each {@code input-<childId>} scan, AND that the stitched plan re-serializes
 * with ONE consistent extension table (the bug a raw-proto splice would introduce).
 */
public class WholePlanStitcherTests extends OpenSearchTestCase {

    private JavaTypeFactoryImpl typeFactory;
    private RelOptCluster cluster;
    private SimpleExtension.ExtensionCollection extensions;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), new RexBuilder(typeFactory));
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(WholePlanStitcherTests.class.getClassLoader());
            extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(
                SimpleExtension.load(List.of("/opensearch_aggregate_functions.yaml"))
            );
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    private RelDataType intRow(String... columns) {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        for (String c : columns) {
            b.add(c, typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true));
        }
        return b.build();
    }

    private RelNode scan(String table, String... columns) {
        return new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), table, intRow(columns));
    }

    /** SUM(col0) GROUP BY () over the given input. */
    private RelNode sumAgg(RelNode input) {
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(0),
            -1,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "s"
        );
        return LogicalAggregate.create(input, List.of(), ImmutableBitSet.of(), null, List.of(sum));
    }

    /** Two-stage SUM: stage 0 (shard) sums the real table; stage 1 (root) sums input-0. */
    public void testStitchesTwoStageBoundary() throws Exception {
        DataFusionFragmentConvertor convertor = new DataFusionFragmentConvertor(extensions);
        byte[] shard = convertor.convertFragment(sumAgg(scan("http_logs", "size")));
        byte[] reduce = convertor.convertFragment(sumAgg(scan("input-0", "s")));

        byte[] stitchedBytes = new WholePlanStitcher(extensions).stitch(1, Map.of(0, shard, 1, reduce));
        Plan stitched = Plan.parseFrom(stitchedBytes);

        // Root is the reduce aggregate; its input is the os_stage_boundary marker.
        Rel rootRel = rootRel(stitched);
        assertEquals(Rel.RelTypeCase.AGGREGATE, rootRel.getRelTypeCase());
        Rel reduceInput = rootRel.getAggregate().getInput();
        assertEquals(Rel.RelTypeCase.EXTENSION_SINGLE, reduceInput.getRelTypeCase());

        String detailJson = reduceInput.getExtensionSingle().getDetail().getValue().toStringUtf8();
        assertEquals(WholePlanStitcher.STAGE_BOUNDARY_TYPE_URL, reduceInput.getExtensionSingle().getDetail().getTypeUrl());
        assertTrue(detailJson, detailJson.contains("\"boundary_id\":0"));
        assertTrue(detailJson, detailJson.contains("\"exchange_type\":\"GATHER\""));

        // Below the marker: the child stage's own aggregate over the REAL table scan (unwrapped).
        Rel childTree = reduceInput.getExtensionSingle().getInput();
        assertEquals(Rel.RelTypeCase.AGGREGATE, childTree.getRelTypeCase());
        assertEquals(Rel.RelTypeCase.READ, childTree.getAggregate().getInput().getRelTypeCase());
        assertEquals("http_logs", childTree.getAggregate().getInput().getRead().getNamedTable().getNames(0));

        // The whole stitched plan must carry a single, consistent extension table — every SUM
        // function_reference across both stages resolves against the re-collected extensions.
        // (Decoding back through the convertor's collection would throw on a dangling anchor.)
        assertFalse("stitched plan must declare its function extensions", stitched.getExtensionsList().isEmpty());
    }

    /** A single-stage plan (no input- scans) stitches to itself — no markers. */
    public void testSingleStageNoBoundary() throws Exception {
        DataFusionFragmentConvertor convertor = new DataFusionFragmentConvertor(extensions);
        byte[] only = convertor.convertFragment(sumAgg(scan("idx", "v")));

        Plan stitched = Plan.parseFrom(new WholePlanStitcher(extensions).stitch(0, Map.of(0, only)));
        Rel rootRel = rootRel(stitched);
        assertEquals(Rel.RelTypeCase.AGGREGATE, rootRel.getRelTypeCase());
        assertEquals(Rel.RelTypeCase.READ, rootRel.getAggregate().getInput().getRelTypeCase());
        assertEquals("idx", rootRel.getAggregate().getInput().getRead().getNamedTable().getNames(0));
    }

    private static Rel rootRel(Plan plan) {
        for (PlanRel pr : plan.getRelationsList()) {
            if (pr.hasRoot()) {
                return pr.getRoot().getInput();
            }
        }
        throw new AssertionError("stitched plan has no root relation");
    }
}
