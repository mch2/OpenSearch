/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * End-to-end plan-shape coverage for {@link ObjectLeafProjector}: an OpenSearch {@code object} field
 * is read from the scan as one struct column, and each of its flat dotted leaf columns is recovered
 * above the scan with a field access.
 *
 * <p>The scan carries the object and <em>not</em> its leaves: the object is what is physically
 * stored, as a Parquet struct, while {@code nested_metadata.top} is a field inside it and not a
 * column. The schema still declares both, because a query addresses a leaf by its dotted name:
 *
 * <pre>
 * id                               INTEGER
 * nested_metadata.top              VARCHAR   ← projected, not scanned
 * nested_metadata.properties.name  VARCHAR   ← projected, not scanned
 * nested_metadata.properties.value VARCHAR   ← projected, not scanned
 * nested_metadata                  ROW       ← scanned
 * </pre>
 */
public class ObjectStructPlanShapeTests extends PlanShapeTestBase {

    /**
     * Table as the schema builder produces it for an {@code object} mapping: the struct-typed parent
     * column PLUS a flat dotted column per leaf.
     */
    private RelOptTable objectTable() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType properties = typeFactory.createStructType(List.of(varchar, varchar), List.of("name", "value"));
        RelDataType meta = typeFactory.createStructType(List.of(varchar, properties), List.of("top", "properties"));

        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
        builder.add("nested_metadata.top", varchar);
        builder.add("nested_metadata.properties.name", varchar);
        builder.add("nested_metadata.properties.value", varchar);
        builder.add("nested_metadata", meta);
        return mockTable("test_index", builder.build());
    }

    /**
     * The index mapping the object came from. Nested, as OpenSearch stores it — which is what gives
     * {@code FieldStorageResolver} both the object's own storage and its leaves'.
     */
    private Map<String, Map<String, Object>> objectFieldMappings() {
        return Map.of(
            "id",
            Map.of("type", "integer"),
            "nested_metadata",
            Map.of(
                "properties",
                Map.of(
                    "top",
                    Map.of("type", "keyword"),
                    "properties",
                    Map.of("properties", Map.of("name", Map.of("type", "keyword"), "value", Map.of("type", "keyword")))
                )
            )
        );
    }

    /**
     * The core rewrite: the scan keeps the object, and a project above it reads each leaf out of the
     * struct, nesting a second access for the {@code properties} sub-object.
     */
    public void testProjectorReadsLeavesOutOfTheStructAboveTheScan() {
        RelNode scan = stubScan(objectTable());

        Optional<RelNode> rewritten = ObjectLeafProjector.rewrite(scan);

        assertTrue("projector should fire when an object and its leaves are both present", rewritten.isPresent());
        assertPlanShape(
            """
                LogicalProject(id=[$0], nested_metadata.top=[$1.top], nested_metadata.properties.name=[$1.properties.name], nested_metadata.properties.value=[$1.properties.value], nested_metadata=[$1])
                  LogicalTableScan(table=[[test_index]])
                """,
            rewritten.get()
        );
    }

    /** No object spec ⇒ no rewrite, so plans without objects are untouched. */
    public void testProjectorNoOpWithoutObjectSpec() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        assertFalse(ObjectLeafProjector.rewrite(scan).isPresent());
    }

    /**
     * A scan already trimmed to the object alone needs no rewrite: there are no leaf columns to
     * recover, and a project that only copied its input would be pure overhead.
     */
    public void testProjectorNoOpWhenOnlyTheObjectIsScanned() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType meta = typeFactory.createStructType(List.of(varchar), List.of("top"));
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
        builder.add("nested_metadata", meta);

        assertFalse(ObjectLeafProjector.rewrite(stubScan(mockTable("test_index", builder.build()))).isPresent());
    }

    /**
     * A shapeless object — {@code {"type": "object"}} before any document gave it a shape — is a
     * field-less struct. There is no column to read it from, so it is dropped from the scan and
     * projected as a typed NULL, which is what vanilla OpenSearch returns for it.
     */
    public void testProjectorEmitsNullForAShapelessObject() {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
        builder.add("shapeless", typeFactory.createStructType(List.of(), List.of()));
        RelNode scan = stubScan(mockTable("test_index", builder.build()));

        RelNode rewritten = ObjectLeafProjector.rewrite(scan).orElseThrow();

        String shape = RelOptUtil.toString(rewritten);
        assertTrue("expected a typed NULL for the shapeless object, got:\n" + shape, shape.contains("null:RecordType"));
        assertFalse("the shapeless object has no column to scan, got:\n" + shape, shape.contains("shapeless=[$1]"));
    }

    /**
     * Projecting the object returns the whole object: it comes straight off the scan and survives the
     * full planner (marking, CBO) with no reassembly.
     */
    public void testProjectOnObjectReturnsWholeObjectThroughPlanner() {
        RelNode scan = stubScan(objectTable());
        RelNode projected = ObjectLeafProjector.rewrite(scan).orElseThrow();
        int objectIndex = projected.getRowType().getFieldCount() - 1;
        RelNode plan = LogicalProject.create(
            projected,
            List.of(),
            List.of(rexBuilder.makeInputRef(projected, 0), rexBuilder.makeInputRef(projected, objectIndex)),
            List.of("id", "nested_metadata")
        );

        RelNode result = runPlanner(plan, buildContext("parquet", 1, objectFieldMappings()));

        // The object is a column, so asking for it needs no expression at all: the project reduces
        // to the identity and the planner trims it away, leaving a bare scan. Under the previous
        // flat-column storage this same query assembled a struct over three leaf references.
        assertPlanShape("""
            OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    /**
     * Projecting one leaf reads only that leaf out of the struct — the other two are trimmed away, so
     * a query that names one sub-field does not pay for the rest of the object's fields.
     */
    public void testProjectOnOneLeafReadsOnlyThatLeaf() {
        RelNode scan = stubScan(objectTable());
        RelNode projected = ObjectLeafProjector.rewrite(scan).orElseThrow();
        RelNode plan = LogicalProject.create(
            projected,
            List.of(),
            List.of(rexBuilder.makeInputRef(projected, 1)),
            List.of("nested_metadata.top")
        );

        RelNode result = runPlanner(plan, buildContext("parquet", 1, objectFieldMappings()));

        String shape = RelOptUtil.toString(result);
        assertTrue("expected the leaf to be read out of the struct, got:\n" + shape, shape.contains("$1.top"));
        assertFalse("the other leaves should have been trimmed, got:\n" + shape, shape.contains("properties"));
    }

    /**
     * {@code stats count() by nested_metadata} — the aggregate groups on the object column directly.
     *
     * <p>Grouping (rather than {@code count(nested_metadata)}) is the meaningful probe: counting a
     * non-nullable column is equivalent to {@code count(*)}, so Calcite drops the column reference
     * and trims the project away — correctly, but it proves nothing. A group key genuinely needs the
     * object's value.
     */
    public void testAggregateOnObjectGroupsOnTheScannedColumn() {
        RelNode scan = stubScan(objectTable());
        RelNode projected = ObjectLeafProjector.rewrite(scan).orElseThrow();
        int objectIndex = projected.getRowType().getFieldCount() - 1;
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            projected,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        RelNode plan = makeAggregate(projected, ImmutableBitSet.of(objectIndex), count);

        RelNode result = runPlanner(plan, buildContext("parquet", 1, objectFieldMappings()));

        String shape = RelOptUtil.toString(result);
        int aggAt = shape.indexOf("Aggregate");
        assertTrue("expected an aggregate over the scan, got:\n" + shape, aggAt >= 0 && aggAt < shape.indexOf("TableScan"));
    }

    /**
     * Multi-shard: grouping on the object still splits into PARTIAL/FINAL. The group set is
     * {@code {0}} over a single-column input, so it satisfies
     * {@code OpenSearchAggregateSplitRule.shouldSkipPartialFinalSplit}'s prefix check and the
     * aggregate distributes normally — the struct as a group key costs throughput (DataFusion has no
     * {@code Struct} specialization in its columnar group-values path, so it row-encodes) but does
     * not cost distribution. Pinned because the reduce path is unreachable at one shard.
     */
    public void testAggregateOnObject_2shard() {
        RelNode scan = stubScan(objectTable());
        RelNode projected = ObjectLeafProjector.rewrite(scan).orElseThrow();
        int objectIndex = projected.getRowType().getFieldCount() - 1;
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            projected,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        RelNode plan = makeAggregate(projected, ImmutableBitSet.of(objectIndex), count);

        RelNode result = runPlanner(plan, buildContext("parquet", 2, objectFieldMappings()));

        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[], partitionCount=0]])
                    OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchProject(nested_metadata=[$1], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }
}
