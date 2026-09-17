/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;

/**
 * Gives each element of a LIST-valued {@code GROUP BY} key its own bucket, by expanding the key
 * before the plan is split into stages.
 *
 * <p>A {@code multi_value} column groups per element rather than per document — Lucene terms-agg
 * parity — so {@code stats count() by tags} over {@code ["prod","us-east"]} contributes to two
 * buckets. This rewrites
 *
 * <pre>
 * Aggregate(group=[{tags}])
 *   input
 * </pre>
 *
 * into the same {@code Correlate} + {@code Uncollect} shape the PPL frontend emits for an explicit
 * {@code mvexpand}, with a Project restoring the original column order so the aggregate's group set
 * still addresses {@code tags} by its original index:
 *
 * <pre>
 * Aggregate(group=[{tags}])
 *   Project(..., tags=[$&lt;uncollected&gt;], ...)
 *     Correlate(inner, requiredColumns=[{tags}])
 *       input
 *       Uncollect
 *         Project(tags=[$cor0.tags])
 *           Values(1 row)
 * </pre>
 *
 * <p><b>Why here and not in fragment conversion.</b> The equivalent backend rewriter
 * (since deleted) ran per fragment, i.e. after the aggregate has been split into a
 * per-shard PARTIAL and a coordinator FINAL. The shard then emits the element type while the
 * coordinator's {@code OpenSearchStageInputScan} still declares the LIST, and the reduce sink fails:
 *
 * <pre>
 * Substrait error: Field 'tags' in Substrait schema has a different type (List(Utf8))
 * than the corresponding field in the table schema (Utf8View).
 * </pre>
 *
 * <p>Running before the split means Calcite's own type derivation carries the element type into both
 * fragments, so no separate reconciliation is needed — which is exactly why an explicit
 * {@code mvexpand … | stats … by …} already worked across shards while the implicit form did not.
 * Typing the stage input from the wire instead was tried and rejected: the Substrait round-trip's
 * record type is not a faithful substitute for the Calcite row type once struct columns are in play,
 * and it broke grouping on an {@code object}.
 */
public final class MultiValueGroupKeyExpander {

    private static final Logger LOGGER = LogManager.getLogger(MultiValueGroupKeyExpander.class);

    private MultiValueGroupKeyExpander() {}

    /**
     * Expands every LIST-valued group key in the plan, or {@link Optional#empty()} when there is
     * none — letting the caller skip the walk's cost and keep the original tree identity.
     */
    public static Optional<RelNode> rewrite(RelNode root, RelBuilder relBuilder) {
        boolean[] fired = { false };
        RelNode rewritten = root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                if (visited instanceof Aggregate aggregate) {
                    RelNode expanded = expand(aggregate, relBuilder);
                    if (expanded != aggregate) {
                        fired[0] = true;
                        return expanded;
                    }
                }
                return visited;
            }
        });
        return fired[0] ? Optional.of(rewritten) : Optional.empty();
    }

    private static RelNode expand(Aggregate aggregate, RelBuilder relBuilder) {
        RelNode input = aggregate.getInput();
        // Group keys and aggregate arguments both need expanding, and both address the input by index,
        // so one ordered, deduplicated set covers them. Deduplication is load-bearing, not hygiene:
        // `stats dc(tags) by tags` names the same index twice, and expanding it twice would build a
        // nested Correlate that cross-products a document's array with itself.
        NavigableSet<Integer> listCols = new TreeSet<>();
        List<RelDataTypeField> fields = input.getRowType().getFieldList();
        for (int fieldIndex : aggregate.getGroupSet()) {
            if (fields.get(fieldIndex).getType().getComponentType() != null) {
                listCols.add(fieldIndex);
            }
        }
        for (AggregateCall call : aggregate.getAggCallList()) {
            for (int argIndex : call.getArgList()) {
                if (argIndex < fields.size() && fields.get(argIndex).getType().getComponentType() != null) {
                    listCols.add(argIndex);
                }
            }
        }
        if (listCols.isEmpty()) {
            return aggregate;
        }
        RelNode expandedInput = input;
        for (int fieldIndex : listCols) {
            expandedInput = expandOneColumn(expandedInput, fieldIndex, relBuilder);
            if (expandedInput == null) {
                LOGGER.debug("Multi-value expansion skipped for field index {}", fieldIndex);
                return aggregate;
            }
        }
        return aggregate.copy(
            aggregate.getTraitSet(),
            expandedInput,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            reinferCalls(aggregate, expandedInput, listCols)
        );
    }

    /**
     * Rebuilds every aggregate call that reads an expanded column so Calcite re-infers its return
     * type against the narrowed input.
     *
     * <p>An aggregate's own return type can depend on its argument's: {@code MIN}/{@code MAX} are
     * {@code ARG0}, so a call left holding {@code ARRAY<BIGINT>} over a now-{@code BIGINT} argument
     * trips {@code Aggregate}'s per-call {@code typeMatchesInferred} assertion. Passing a null type
     * asks Calcite to infer it, exactly as {@code OpenSearchDistinctCountRule} does. Calls whose type
     * does not depend on the argument ({@code COUNT}, {@code APPROX_COUNT_DISTINCT} → BIGINT) come
     * back identical.
     */
    private static List<AggregateCall> reinferCalls(Aggregate aggregate, RelNode expandedInput, NavigableSet<Integer> expandedCols) {
        List<AggregateCall> calls = new ArrayList<>(aggregate.getAggCallList().size());
        for (AggregateCall call : aggregate.getAggCallList()) {
            if (call.getArgList().stream().noneMatch(expandedCols::contains)) {
                calls.add(call);
                continue;
            }
            calls.add(
                AggregateCall.create(
                    call.getAggregation(),
                    call.isDistinct(),
                    call.isApproximate(),
                    call.ignoreNulls(),
                    call.rexList,
                    call.getArgList(),
                    call.filterArg,
                    call.distinctKeys,
                    call.collation,
                    aggregate.getGroupSet().cardinality(),
                    expandedInput,
                    null,
                    call.getName()
                )
            );
        }
        return calls;
    }

    /**
     * Replaces column {@code fieldIndex} with one row per element, leaving every other column — and
     * the column order — untouched, so callers keep addressing fields by their original index.
     * Returns {@code null} when the shape can't be built, so the caller can leave the plan alone
     * rather than emit something the backend will reject.
     */
    private static RelNode expandOneColumn(RelNode input, int fieldIndex, RelBuilder relBuilder) {
        RelOptCluster cluster = input.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType inputRowType = input.getRowType();
        RelDataTypeField listField = inputRowType.getFieldList().get(fieldIndex);
        CorrelationId correlationId = cluster.createCorrel();

        try {
            // Right arm: Uncollect over a one-row Values projecting the correlated LIST column —
            // byte-for-byte the shape the frontend builds for `mvexpand <field>`.
            LogicalValues singleRow = (LogicalValues) relBuilder.values(new String[] { "ZERO" }, 0).build();
            RexNode correlatedList = rexBuilder.makeFieldAccess(rexBuilder.makeCorrel(inputRowType, correlationId), fieldIndex);
            RelNode listProject = LogicalProject.create(
                singleRow,
                List.of(),
                List.of(correlatedList),
                List.of(listField.getName()),
                java.util.Set.of()
            );
            RelNode uncollect = Uncollect.create(listProject.getTraitSet(), listProject, false, List.of());

            RelNode correlate = relBuilder.push(input)
                .push(uncollect)
                .correlate(JoinRelType.INNER, correlationId, rexBuilder.makeInputRef(input, fieldIndex))
                .build();

            // Correlate appends the uncollected column; project back onto the original shape so the
            // aggregate's group set and every agg call keep their existing indices.
            int uncollectedIndex = inputRowType.getFieldCount();
            List<RexNode> projects = new ArrayList<>(inputRowType.getFieldCount());
            for (int i = 0; i < inputRowType.getFieldCount(); i++) {
                RelDataType type = correlate.getRowType().getFieldList().get(i == fieldIndex ? uncollectedIndex : i).getType();
                projects.add(rexBuilder.makeInputRef(type, i == fieldIndex ? uncollectedIndex : i));
            }
            RelNode restored = LogicalProject.create(correlate, List.of(), projects, inputRowType.getFieldNames(), java.util.Set.of());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Expanded multi-value group key [{}]:\n{}", listField.getName(), RelOptUtil.toString(restored));
            }
            return restored;
        } catch (RuntimeException e) {
            LOGGER.debug("Could not build the expansion for column [{}]", listField.getName(), e);
            return null;
        }
    }
}
