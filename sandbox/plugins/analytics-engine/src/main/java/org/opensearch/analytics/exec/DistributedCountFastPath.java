/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;
import java.util.Map;

/**
 * Recognizes the distributed COUNT fast-path shape and reshapes the marked Calcite tree so the
 * per-shard count is served by Lucene ({@code IndexSearcher.count}) at the data node instead of a
 * full parquet scan + DataFusion count.
 *
 * <p><b>Why a reshape is needed.</b> The distributed marked tree carries a single logical
 * {@code OpenSearchAggregate[count()]} over a real table scan. The native datafusion-distributed
 * planner puts {@code AggregateExec(Partial,count)} ABOVE the leaf. If the leaf emits ONE
 * pre-counted row per shard (Lucene's count), a {@code count(*)} partial over that single row
 * returns {@code 1}, not the shard count. So the aggregate above the leaf must be a {@code sum} of
 * the per-shard counts. This class rebuilds:
 * <pre>
 *   OpenSearchAggregate[count()] groupSet={}
 *     OpenSearchTableScan(real index, wide rowType)
 * </pre>
 * into
 * <pre>
 *   OpenSearchAggregate[sum($0)] groupSet={}                    (nullable BIGINT out)
 *     OpenSearchFilter[delegated_predicate(0)]                  (marker → descriptor reaches leaf)
 *       OpenSearchTableScan'(same RelOptTable, viableBackends=[lucene],
 *                            overrideRowType = 1 × BIGINT NULLABLE "count fast-path column")
 * </pre>
 *
 * <p>The synthetic {@code delegated_predicate(0)} marker is required even for an UNFILTERED count:
 * the Rust coordinator only attaches the leaf DelegationDescriptor (which carries the count-mode
 * signal + Lucene wire bytes) when a delegation marker is among the filters pushed into the scan
 * (see {@code coordinator.rs} {@code has_marker}). The reshaped scan's single output column is
 * <b>nullable BIGINT</b> end-to-end — the leaf's Lucene count VSR is nullable Int64, and a NOT-NULL
 * mismatch silently stalls the partition stream, so nullability MUST agree across the reshaped
 * scan schema, the Substrait base_schema, and the leaf VSR.
 *
 * <p><b>Guards (v1):</b> only {@code count(*)}/{@code count(1)}/{@code count(literal)} (EMPTY
 * argList) qualify — {@code count(col)} has different null semantics ({@code IndexSearcher.count}
 * is a matching-DOC count = {@code count(*)}), so it is NOT reshaped. v1 also requires NO filter
 * (the filtered-count path is a follow-on). Non-qualifying queries return {@code null} from
 * {@link #tryReshape} and run the normal distributed scan+aggregate.
 *
 * @opensearch.internal
 */
final class DistributedCountFastPath {

    private static final Logger LOGGER = LogManager.getLogger(DistributedCountFastPath.class);

    /** Output column name of the synthetic single-column Lucene count scan. */
    static final String COUNT_COLUMN = "__count_fast_path__";

    /** Lucene is the driving backend for the reshaped scan (it serves the count). */
    private static final List<String> LUCENE_BACKENDS = List.of("lucene");

    private DistributedCountFastPath() {}

    /**
     * True when {@code marked} is the distributed count fast-path shape: a single
     * {@code OpenSearchAggregate}, empty group set, every call {@code count(*)} (COUNT with EMPTY
     * argList), directly over an {@code OpenSearchTableScan} with NO intervening filter (v1).
     */
    static boolean matches(RelNode marked) {
        if (marked instanceof OpenSearchAggregate == false) {
            return false;
        }
        OpenSearchAggregate agg = (OpenSearchAggregate) marked;
        if (agg.getGroupSet().isEmpty() == false) {
            return false;
        }
        if (agg.getAggCallList().isEmpty()) {
            return false;
        }
        for (AggregateCall call : agg.getAggCallList()) {
            // count(*)/count(1)/count(literal) only — EMPTY argList. count(col) excludes nulls and
            // is NOT a matching-doc count, so it must not take the Lucene count fast-path.
            if (call.getAggregation().getKind() != SqlKind.COUNT) {
                return false;
            }
            if (call.getArgList().isEmpty() == false) {
                return false;
            }
            if (call.isDistinct()) {
                return false;
            }
        }
        // v1: unfiltered only. The child must be a bare table scan (no OpenSearchFilter).
        RelNode child = unwrap(agg.getInput());
        return child instanceof TableScan;
    }

    /**
     * If {@code marked} is the count fast-path shape, returns the reshaped tree
     * ({@code sum($0)} over the synthetic single-column Lucene scan with the injected marker);
     * otherwise returns {@code null} (caller runs the normal distributed path).
     */
    static RelNode tryReshape(RelNode marked) {
        if (matches(marked) == false) {
            return null;
        }
        OpenSearchAggregate agg = (OpenSearchAggregate) marked;
        TableScan origScan = (TableScan) unwrap(agg.getInput());

        RexBuilder rexBuilder = agg.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = agg.getCluster().getTypeFactory();

        // Synthetic scan output: exactly one NULLABLE BIGINT column carrying the per-shard count.
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RelDataType nullableBigint = typeFactory.createTypeWithNullability(bigint, true);
        RelDataType scanRowType = typeFactory.builder().add(COUNT_COLUMN, nullableBigint).build();

        // One FieldStorageInfo aligned 1:1 with scanRowType (the count column is a synthetic/derived
        // BIGINT — the leaf produces it from Lucene, no physical-column dependency).
        List<FieldStorageInfo> fieldStorage = List.of(FieldStorageInfo.derivedColumn(COUNT_COLUMN, SqlTypeName.BIGINT));

        // Reuse the ORIGINAL RelOptTable so DfShardRouting still resolves the index + shards.
        OpenSearchTableScan luceneScan = new OpenSearchTableScan(
            origScan.getCluster(),
            origScan.getTraitSet(),
            origScan.getTable(),
            LUCENE_BACKENDS,
            fieldStorage,
            scanRowType
        );

        // Inject a delegated_predicate(0) marker filter so the coordinator attaches the leaf
        // DelegationDescriptor (which carries the count-mode signal + Lucene wire bytes). The
        // marker is a synthetic boolean predicate over the scan; it evaluates to true at the leaf.
        RexNode marker = DelegatedPredicateFunction.makeCall(rexBuilder, 0);
        org.opensearch.analytics.planner.rel.OpenSearchFilter markerFilter = new org.opensearch.analytics.planner.rel.OpenSearchFilter(
            luceneScan.getCluster(),
            luceneScan.getTraitSet(),
            luceneScan,
            marker,
            LUCENE_BACKENDS
        );

        // sum($0) over the single count column. isDistinct=false, not approximate, not ignoreNulls.
        // Overload: create(agg, distinct, approximate, ignoreNulls, rexList, argList, filterArg,
        // distinctKeys, collation, type, name).
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            /* distinct */ false,
            /* approximate */ false,
            /* ignoreNulls */ false,
            /* rexList */ List.of(),
            /* argList */ List.of(0),
            /* filterArg */ -1,
            /* distinctKeys */ null,
            /* collation */ org.apache.calcite.rel.RelCollations.EMPTY,
            /* type */ nullableBigint,
            /* name */ agg.getAggCallList().get(0).getName()
        );

        OpenSearchAggregate sumAgg = new OpenSearchAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            markerFilter,
            ImmutableBitSet.of(),
            List.of(ImmutableBitSet.of()),
            List.of(sumCall),
            AggregateMode.SINGLE,
            LUCENE_BACKENDS,
            Map.of()
        );

        LOGGER.debug(
            "[count-fast-path] reshaped count() -> sum($0) over Lucene count scan (table={})",
            origScan.getTable().getQualifiedName()
        );
        return sumAgg;
    }

    /** Peels HEP/subset wrappers so shape checks see the concrete rel. */
    private static RelNode unwrap(RelNode n) {
        return org.opensearch.analytics.planner.RelNodeUtils.unwrapHep(n);
    }
}
