/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Optionality;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.ArrayList;
import java.util.List;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.NameBasedAggregateFunctionConverter;
import io.substrait.isthmus.expression.NameBasedScalarFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Sort;

/**
 * Converts Calcite RelNode fragments to Substrait protobuf bytes
 * for the DataFusion Rust runtime.
 *
 * <p>Dispatch summary:
 * <ul>
 *   <li>{@link #convertShardScanFragment(String, RelNode)} and
 *       {@link #convertFinalAggFragment(RelNode)} — full-fragment conversions via
 *       {@link #convertToSubstrait(RelNode)}.</li>
 *   <li>{@link #attachPartialAggOnTop(RelNode, byte[])} and
 *       {@link #attachFragmentOnTop(RelNode, byte[])} — convert the wrapping
 *       operator standalone, then rewire its input to the decoded inner plan's
 *       root via {@link #rewire(Plan, Rel)}.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class DataFusionFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(DataFusionFragmentConvertor.class);

    private final SimpleExtension.ExtensionCollection extensions;

    public DataFusionFragmentConvertor(SimpleExtension.ExtensionCollection extensions) {
        this.extensions = extensions;
    }

    @Override
    public byte[] convertShardScanFragment(String tableName, RelNode fragment) {
        LOGGER.debug("Converting shard scan fragment for table [{}]", tableName);
        return convertToSubstrait(fragment);
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        LOGGER.info("Attaching PARTIAL aggregate. Calcite fragment:\n{}", org.apache.calcite.plan.RelOptUtil.toString(partialAggFragment));
        Plan inner = decodePlan(innerBytes);
        Rel wrapper = convertStandalone(partialAggFragment);
        // Wrapper output names may change column count vs inner plan (e.g. an aggregate adds
        // measure columns), so override Plan.Root names with the wrapper's actual output.
        List<String> wrapperNames = partialAggFragment.getRowType().getFieldList().stream()
            .map(RelDataTypeField::getName)
            .toList();
        // No AggregationPhase set here. The native data-node executor's physical-plan walker
        // sets AggregateExec.mode = Partial after from_substrait_plan, which is what actually
        // drives partial behavior — substrait's AggregationPhase field is silently ignored by
        // datafusion-substrait's consumer.
        Plan rewired = rewire(inner, wrapper, wrapperNames);
        LOGGER.info("PARTIAL substrait plan:\n{}", rewired);
        return serializePlan(rewired);
    }

    @Override
    public byte[] convertFinalAggFragment(RelNode fragment) {
        LOGGER.info("Converting FINAL aggregate. Calcite fragment:\n{}", org.apache.calcite.plan.RelOptUtil.toString(fragment));
        RelNode rewritten = rewriteStageInputScans(fragment);
        rewritten = remapAggregates(rewritten);
        rewritten = rewriteCountWindow(rewritten);
        rewritten = rewritePercentileApprox(rewritten);
        rewritten = SymbolLiteralRewriter.rewrite(rewritten);
        rewritten = UntypedNullRewriter.rewrite(rewritten);
        // Pin every state column above the StageInput by inserting an identity Project that
        // textually references each one. Without this, isthmus/Calcite emit substrait `Read`
        // with a `projection` mask that drops state columns the FINAL aggregate's argList
        // doesn't reference (e.g. avg(col0) only mentions a[count], so a[sum] gets pruned).
        // The Final-mode `AggregateExec` reads state positionally from its input and needs
        // every state column present at runtime — the Project keeps them all alive at the
        // logical level so the optimizer can't strip any.
        rewritten = pinStageInputStateColumns(rewritten);
        SubstraitRelVisitor visitor = createVisitor(rewritten);
        Rel substraitRel = visitor.apply(rewritten);

        // No AggregationPhase set here. The coordinator-side LocalSession's physical-plan
        // walker sets AggregateExec.mode = Final after from_substrait_plan — that's what
        // drives Final behavior at runtime. The substrait phase field is dead.
        List<String> fieldNames = RelRoot.of(rewritten, SqlKind.SELECT).fields.stream().map(field -> field.getValue()).toList();
        Plan.Root substraitRoot = Plan.Root.builder().input(substraitRel).names(fieldNames).build();
        Plan plan = Plan.builder().addRoots(substraitRoot).build();
        plan = SubstraitPlanRewriter.rewrite(plan);
        LOGGER.info("FINAL substrait plan:\n{}", plan);
        return serializePlan(plan);
    }

    @Override
    public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        LOGGER.debug("Attaching generic fragment [{}] on top of {} inner bytes", fragment.getClass().getSimpleName(), innerBytes.length);
        Plan inner = decodePlan(innerBytes);
        // Rewrite StageInputScan first — generic fragments above a final aggregate
        // (e.g. the Project introduced by AggregateReduceFunctionsRule when AVG is
        // decomposed into SUM/COUNT) reference the stage's input.
        RelNode rewritten = rewriteStageInputScans(fragment);
        rewritten = rewriteCountWindow(rewritten);
        Rel wrapper = convertStandalone(rewritten);
        // Use the wrapper's actual output column names — wrappers like Project change column
        // count, so inheriting inner plan's names would mismatch.
        List<String> wrapperNames = rewritten.getRowType().getFieldList().stream()
            .map(RelDataTypeField::getName)
            .toList();
        return serializePlan(rewire(inner, wrapper, wrapperNames));
    }

    // ── Core conversion helpers ─────────────────────────────────────────────────

    private byte[] convertToSubstrait(RelNode fragment) {
        // Remap aggregate functions whose Calcite identity isthmus can't resolve to its own
        // bindings (e.g. AVG, future ARG_MIN/MAX). Driven by ISTHMUS_AGG_REMAP — adding new
        // entries is a one-line table change, not a new rewriter.
        fragment = remapAggregates(fragment);
        fragment = rewriteCountWindow(fragment);
        fragment = rewritePercentileApprox(fragment);
        fragment = SymbolLiteralRewriter.rewrite(fragment);
        fragment = UntypedNullRewriter.rewrite(fragment);
        RelRoot root = RelRoot.of(fragment, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(fragment);
        Rel substraitRel = visitor.apply(root.rel);

        List<String> fieldNames = root.fields.stream().map(field -> field.getValue()).toList();

        Plan.Root substraitRoot = Plan.Root.builder().input(substraitRel).names(fieldNames).build();
        Plan plan = Plan.builder().addRoots(substraitRoot).build();

        plan = SubstraitPlanRewriter.rewrite(plan);

        byte[] bytes = serializePlan(plan);
        LOGGER.debug("Substrait plan: {} bytes", bytes.length);
        return bytes;
    }

    private Rel convertStandalone(RelNode operator) {
        operator = remapAggregates(operator);
        operator = rewriteCountWindow(operator);
        operator = rewritePercentileApprox(operator);
        operator = SymbolLiteralRewriter.rewrite(operator);
        operator = UntypedNullRewriter.rewrite(operator);
        SubstraitRelVisitor visitor = createVisitor(operator);
        return visitor.apply(operator);
    }

    /**
     * SqlKind → isthmus SqlAggFunction. isthmus's {@code AggregateFunctionConverter} only
     * recognizes its own variants for some aggregates; Calcite's standard impls aren't
     * resolved. Add an entry here when a new aggregate hits the same problem.
     *
     * <p>Each entry says: "if you see an aggregate with this {@link SqlKind} whose function
     * is NOT this isthmus variant, swap it." The walker below handles the rest generically —
     * no per-function methods needed.
     */
    private static final java.util.Map<SqlKind, org.apache.calcite.sql.SqlAggFunction> ISTHMUS_AGG_REMAP = java.util.Map.of(
        SqlKind.AVG, io.substrait.isthmus.AggregateFunctions.AVG
        // Future entries — add only when isthmus rejects Calcite's variant. SUM0/sum0 mapping
        // happens at substrait proto level (see SUBSTRAIT_FN_NAME_REMAP) to avoid Calcite's
        // type validation rejecting nullability changes.
    );

    /**
     * Substrait function-name remap applied to the proto plan AFTER isthmus emits it. Used
     * for renames where the Calcite-level type system would reject the substitution due to
     * nullability differences. Currently empty — kept as the hook for future renames where
     * DataFusion's substrait consumer rejects an isthmus-emitted name.
     */
    private static final java.util.Map<String, String> SUBSTRAIT_FN_NAME_REMAP = java.util.Map.of(
        "approx_count_distinct", "approx_distinct"
    );

    /**
     * Walks {@code node} and remaps any {@link AggregateCall} whose {@link SqlKind} appears
     * in {@link #ISTHMUS_AGG_REMAP}. Single recursive walk; adding new aggregates is a
     * one-line addition to the table, not a new method.
     */
    private static RelNode remapAggregates(RelNode node) {
        if (node instanceof org.apache.calcite.rel.core.Aggregate agg) {
            boolean changed = false;
            // Track which agg-call output columns changed type so we can cast-restore them below.
            List<RelDataType> originalAggTypes = new ArrayList<>(agg.getAggCallList().size());
            List<AggregateCall> newCalls = new ArrayList<>(agg.getAggCallList().size());
            for (AggregateCall call : agg.getAggCallList()) {
                originalAggTypes.add(call.getType());
                org.apache.calcite.sql.SqlAggFunction target = ISTHMUS_AGG_REMAP.get(call.getAggregation().getKind());
                if (target != null && call.getAggregation() != target) {
                    // Re-infer the return type using the new function. Calcite's Aggregate
                    // re-validates aggCall.type against target.inferReturnType during copy(),
                    // so we must align the type with whatever the target function infers
                    // (e.g. isthmus AVG returns ARG0; Calcite's standard AVG returns DOUBLE).
                    RelDataType inferred = target.inferReturnType(call.createBinding(agg));
                    newCalls.add(
                        AggregateCall.create(
                            target,
                            call.isDistinct(),
                            call.isApproximate(),
                            call.ignoreNulls(),
                            call.rexList,
                            call.getArgList(),
                            call.filterArg,
                            call.distinctKeys,
                            call.collation,
                            inferred,
                            call.name
                        )
                    );
                    changed = true;
                } else {
                    newCalls.add(call);
                }
            }
            if (changed) {
                org.apache.calcite.rel.core.Aggregate newAgg = (org.apache.calcite.rel.core.Aggregate) agg.copy(
                    agg.getTraitSet(), remapAggregates(agg.getInput()), agg.getGroupSet(), agg.getGroupSets(), newCalls);
                // If any agg-call output type differs from what the original Aggregate advertised,
                // wrap in a Project that casts those columns back to the original types. This keeps
                // parent RelNodes' RexInputRef types valid without needing a deep rewrite of the
                // whole subtree (e.g., Calcite AVG → DOUBLE, isthmus AVG → BIGINT: parent projects
                // reference column as DOUBLE and would fail isValid otherwise).
                int groupCount = newAgg.getGroupCount();
                org.apache.calcite.rex.RexBuilder rb = newAgg.getCluster().getRexBuilder();
                boolean needsCast = false;
                List<org.apache.calcite.rex.RexNode> projects = new ArrayList<>();
                List<String> names = new ArrayList<>();
                List<RelDataTypeField> fields = newAgg.getRowType().getFieldList();
                for (int i = 0; i < groupCount; i++) {
                    projects.add(rb.makeInputRef(newAgg, i));
                    names.add(fields.get(i).getName());
                }
                for (int i = 0; i < originalAggTypes.size(); i++) {
                    int colIdx = groupCount + i;
                    RelDataType origType = originalAggTypes.get(i);
                    RelDataType newType = newAgg.getRowType().getFieldList().get(colIdx).getType();
                    org.apache.calcite.rex.RexNode ref = rb.makeInputRef(newAgg, colIdx);
                    if (!origType.equals(newType)) {
                        ref = rb.makeCast(origType, ref);
                        needsCast = true;
                    }
                    projects.add(ref);
                    names.add(fields.get(colIdx).getName());
                }
                node = needsCast
                    ? org.apache.calcite.rel.logical.LogicalProject.create(newAgg, java.util.List.of(), projects, names, java.util.Set.of())
                    : newAgg;
            }
        }
        // Recurse into children
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean childChanged = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = remapAggregates(input);
            newInputs.add(rewritten);
            if (rewritten != input) childChanged = true;
        }
        if (childChanged) {
            node = node.copy(node.getTraitSet(), newInputs);
        }
        return node;
    }

    /**
     * Remap bits in an {@link org.apache.calcite.util.ImmutableBitSet} through {@code idxRemap}.
     * Bits whose key is absent from the map keep their original index. No-op when idxRemap is
     * identity.
     */
    private static org.apache.calcite.util.ImmutableBitSet remapBitSet(
            org.apache.calcite.util.ImmutableBitSet set,
            java.util.Map<Integer, Integer> idxRemap) {
        org.apache.calcite.util.ImmutableBitSet.Builder builder = org.apache.calcite.util.ImmutableBitSet.builder();
        for (int bit : set) {
            Integer r = idxRemap.get(bit);
            builder.set(r == null ? bit : r);
        }
        return builder.build();
    }

    /**
     * Historic Calcite-level rewrite for PPL's pre-task-#6 {@code percentile_approx(field,
     * pct_0_100_int, SYMBOL)} shape. That shape is no longer emitted — the PPL visitor now
     * emits the clean 2-arg {@code percentile_approx(field, fraction_DOUBLE_literal)} form
     * directly. Retained as a no-op (deprecated) so nothing breaks if an old code path
     * happens to call it; the actual literal-inline fix lives post-isthmus in
     * SubstraitPlanRewriter's Aggregate visitor, which splices the fraction back into the
     * Aggregate measure after Calcite's {@code AggCall.Registrar} hoists it into the child
     * Project.
     *
     * @deprecated Kept only so callers compile. Returns the input unchanged.
     */
    @Deprecated
    private static RelNode rewritePercentileApprox(RelNode node) {
        return node;
    }

    /**
     * Three-part rewrite for window functions in {@link org.apache.calcite.rel.core.Project}
     * nodes that DataFusion's physical planner would otherwise reject:
     *
     * <ol>
     *   <li><b>Zero-arg COUNT</b> — DataFusion rejects {@code COUNT(col) OVER (…)} but
     *       accepts {@code COUNT(*) OVER (…)}. Rewrite every {@code COUNT(col) OVER} in
     *       the project expressions to {@code COUNT(*) OVER}. Exact when the column is
     *       non-nullable; slightly lossy otherwise (null rows would not contribute to the
     *       per-column count but do contribute to count-of-rows). Acceptable for
     *       eventstats because the decomposition PPL uses for {@code avg/stddev_pop/
     *       var_pop} treats count as row-count anyway.</li>
     *   <li><b>Lift nested windows</b> — DataFusion's physical planner lifts top-level
     *       {@code RexOver}s in a projection into a {@code Window} operator below, then
     *       replaces them with column refs in the projection. If a {@code RexOver} is
     *       nested inside another call (e.g. {@code CASE WHEN count() = 0 THEN null ELSE
     *       sum()/count() END}), the lift doesn't recurse and physical planning fails.
     *       Flatten by inserting a lower projection that computes each window expression
     *       as its own column, and rewriting the outer projection to reference those
     *       columns via {@code RexInputRef}.</li>
     *   <li><b>Dedup identical top-level windows</b> — DataFusion's substrait consumer
     *       auto-names each emitted {@code WindowFunctionInvocation} from its canonical
     *       form (e.g. {@code count(Int64(1)) PARTITION BY [...] ROWS ...}). Two textually
     *       identical invocations in the same Project collide with
     *       {@code "Schema contains duplicate unqualified field name"}. Common shape:
     *       {@code streamstats count(), avg(age)} becomes four window functions —
     *       {@code count()}, {@code sum(age)}, {@code count(age)}, {@code min/max(age)} —
     *       then step (1) collapses {@code count(age)} → {@code count()}, creating a
     *       duplicate with the user's original {@code count()}. Likewise chained
     *       streamstats emit two {@code row_number()} helpers with the same frame. Walk
     *       {@code liftedExprs}, canonicalize by digest, and for each group of identical
     *       RexOvers push one copy into the lower projection and replace duplicates with
     *       a shared {@link org.apache.calcite.rex.RexInputRef}.</li>
     * </ol>
     */
    private static RelNode rewriteCountWindow(RelNode node) {
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean childChanged = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = rewriteCountWindow(input);
            newInputs.add(rewritten);
            if (rewritten != input) childChanged = true;
        }
        if (childChanged) {
            node = node.copy(node.getTraitSet(), newInputs);
        }
        if (node instanceof org.apache.calcite.rel.core.Project project) {
            RexBuilder rexBuilder = project.getCluster().getRexBuilder();
            CountWindowRewriter countShuttle = new CountWindowRewriter(rexBuilder);
            List<RexNode> afterCount = countShuttle.apply(project.getProjects());
            List<RexNode> projectExprs = afterCount != project.getProjects() ? afterCount : project.getProjects();

            WindowLifter lifter = new WindowLifter(rexBuilder, project.getInput().getRowType());
            List<RexNode> liftedExprs = new ArrayList<>(projectExprs.size());
            for (RexNode expr : projectExprs) {
                liftedExprs.add(lifter.liftTopLevel(expr));
            }
            // Dedup identical top-level RexOvers — runs AFTER the lift so we dedupe the
            // full projection space (both originally top-level windows left in place by the
            // lifter and any lower-Project entries the lifter introduced).
            List<RexNode> dedupedExprs = dedupTopLevelWindows(rexBuilder, liftedExprs, lifter);
            boolean dedupChanged = dedupedExprs != liftedExprs;

            RelNode newInput = project.getInput();
            if (lifter.hasAdditions()) {
                List<RexNode> lowerProjects = lifter.buildLowerProjects();
                newInput = org.apache.calcite.rel.logical.LogicalProject.create(
                    project.getInput(),
                    List.of(),
                    lowerProjects,
                    (List<String>) null
                );
            }
            List<RexNode> finalExprs = dedupChanged ? dedupedExprs : liftedExprs;
            if (newInput != project.getInput() || finalExprs != projectExprs) {
                node = project.copy(project.getTraitSet(), newInput, finalExprs, project.getRowType());
            } else if (afterCount != project.getProjects()) {
                node = project.copy(project.getTraitSet(), project.getInput(), finalExprs, project.getRowType());
            }
        }
        return node;
    }

    /**
     * If {@code liftedExprs} contains top-level {@link RexOver}s that are textually identical
     * (same op, same operands, same partition / order / frame), push one copy into the lower
     * projection via {@code lifter} and rewrite the duplicates to {@link org.apache.calcite.rex.RexInputRef} entries
     * pointing at that single lower column. Leaves all other expressions untouched.
     *
     * <p>Canonical form is {@link RexNode#toString()} which encodes op + operands + window
     * spec — Calcite's built-in digest used for RexCall equality. If no duplicates are found,
     * returns {@code liftedExprs} unchanged so the surrounding logic can skip the copy.
     */
    private static List<RexNode> dedupTopLevelWindows(
        RexBuilder rexBuilder,
        List<RexNode> liftedExprs,
        WindowLifter lifter
    ) {
        // First pass — count occurrences.
        java.util.Map<String, Integer> counts = new java.util.HashMap<>();
        for (RexNode expr : liftedExprs) {
            if (expr instanceof RexOver) {
                counts.merge(expr.toString(), 1, Integer::sum);
            }
        }
        boolean anyDuplicates = counts.values().stream().anyMatch(c -> c > 1);
        if (!anyDuplicates) {
            return liftedExprs;
        }
        // Second pass — for each duplicated key, stash the first occurrence in the lower
        // Project (via lifter.lowerProject) and replace both that entry and all subsequent
        // occurrences with a RexInputRef to the shared lower column.
        java.util.Map<String, RexNode> sharedRefs = new java.util.HashMap<>();
        List<RexNode> out = new ArrayList<>(liftedExprs.size());
        for (RexNode expr : liftedExprs) {
            if (expr instanceof RexOver over && counts.getOrDefault(expr.toString(), 0) > 1) {
                RexNode shared = sharedRefs.computeIfAbsent(expr.toString(), k -> lifter.lowerProject(over));
                out.add(shared);
            } else {
                out.add(expr);
            }
        }
        return out;
    }

    private static final class CountWindowRewriter extends RexShuttle {
        private final RexBuilder rexBuilder;

        CountWindowRewriter(RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexNode visitOver(RexOver over) {
            RexOver recursed = (RexOver) super.visitOver(over);
            if (recursed.getAggOperator().getKind() != SqlKind.COUNT) {
                return recursed;
            }
            if (recursed.getOperands().isEmpty()) {
                return recursed;
            }
            RexWindow window = recursed.getWindow();
            return rexBuilder.makeOver(
                recursed.getType(),
                recursed.getAggOperator(),
                List.of(),
                window.partitionKeys,
                window.orderKeys,
                window.getLowerBound(),
                window.getUpperBound(),
                window.isRows(),
                true,
                false,
                recursed.isDistinct(),
                recursed.ignoreNulls()
            );
        }
    }

    /**
     * Replaces each nested {@link RexOver} — i.e. one that is not the outermost expression
     * of a projection entry — with an input-ref into a lower projection that exposes all
     * of the input columns plus one computed column per lifted window. Top-level
     * {@code RexOver}s are left alone; DataFusion's physical planner handles those
     * directly.
     */
    private static final class WindowLifter {
        private final RexBuilder rexBuilder;
        private final RelDataType inputRowType;
        private final List<RexNode> extraWindows = new ArrayList<>();
        // Dedup cache — keyed by RexOver.toString() (Calcite's canonical digest including op,
        // operands, partition, order, frame, DISTINCT, IGNORE NULLS). Lets repeat calls to
        // {@link #lowerProject} return the SAME input-ref column for an identical RexOver
        // seen elsewhere. Prevents duplicate WindowFunctionInvocations in the emitted
        // substrait plan (which DataFusion rejects with a "duplicate unqualified field name"
        // schema error).
        private final java.util.Map<String, RexNode> loweredByDigest = new java.util.HashMap<>();

        WindowLifter(RexBuilder rexBuilder, RelDataType inputRowType) {
            this.rexBuilder = rexBuilder;
            this.inputRowType = inputRowType;
        }

        RexNode liftTopLevel(RexNode expr) {
            if (expr instanceof RexOver) {
                return expr;
            }
            return expr.accept(new NestedWindowShuttle());
        }

        /**
         * Pushes {@code over} into the lower projection and returns a
         * {@link org.apache.calcite.rex.RexInputRef} pointing at its new column. Repeated
         * calls for a textually identical RexOver return the SAME input-ref, so duplicate
         * window computations collapse into a single lower-project entry.
         */
        RexNode lowerProject(RexOver over) {
            String digest = over.toString();
            RexNode cached = loweredByDigest.get(digest);
            if (cached != null) {
                return cached;
            }
            int newIndex = inputRowType.getFieldCount() + extraWindows.size();
            extraWindows.add(over);
            RexNode ref = rexBuilder.makeInputRef(over.getType(), newIndex);
            loweredByDigest.put(digest, ref);
            return ref;
        }

        boolean hasAdditions() {
            return !extraWindows.isEmpty();
        }

        List<RexNode> buildLowerProjects() {
            int inputFieldCount = inputRowType.getFieldCount();
            List<RexNode> out = new ArrayList<>(inputFieldCount + extraWindows.size());
            for (int i = 0; i < inputFieldCount; i++) {
                out.add(rexBuilder.makeInputRef(inputRowType.getFieldList().get(i).getType(), i));
            }
            out.addAll(extraWindows);
            return out;
        }

        private final class NestedWindowShuttle extends RexShuttle {
            @Override
            public RexNode visitOver(RexOver over) {
                // Go through lowerProject so two nested RexOvers with identical signatures
                // collapse to a single lower-project entry (see {@link #loweredByDigest}).
                return lowerProject(over);
            }
        }
    }

    static Plan rewire(Plan inner, Rel wrapper) {
        return rewire(inner, wrapper, null);
    }

    /**
     * @param overrideNames if non-null, used as Plan.Root names instead of the inner plan's
     *                      names. Necessary when the wrapper changes column count
     *                      (e.g. an Aggregate with multiple measures over a single-column input).
     */
    static Plan rewire(Plan inner, Rel wrapper, List<String> overrideNames) {
        if (inner.getRoots().isEmpty()) {
            throw new IllegalArgumentException("Inner Substrait plan has no root relation to rewire under wrapper");
        }
        Plan.Root innerRoot = inner.getRoots().get(0);
        Rel innerRel = innerRoot.getInput();
        Rel rewired = replaceInput(wrapper, innerRel);
        List<String> names = overrideNames != null ? overrideNames : innerRoot.getNames();
        return Plan.builder().addRoots(Plan.Root.builder().input(rewired).names(names).build()).build();
    }

    private static Rel replaceInput(Rel wrapper, Rel newInput) {
        if (wrapper instanceof Aggregate agg) {
            return Aggregate.builder().from(agg).input(newInput).build();
        }
        if (wrapper instanceof Sort sort) {
            return Sort.builder().from(sort).input(newInput).build();
        }
        if (wrapper instanceof Fetch fetch) {
            // Calcite Sort(fetch, offset, collation=EMPTY) lowers to substrait Fetch, not Sort —
            // substrait splits the concept: Sort carries collation, Fetch carries limit+offset.
            // PPL emits a default top-level Sort(fetch=10000) to cap result sets, so every
            // coord-side fragment ends with a Fetch wrapper. Rewire preserves offset+count
            // metadata via {@code .from(fetch)} and only swaps the input pointer.
            return Fetch.builder().from(fetch).input(newInput).build();
        }
        if (wrapper instanceof Filter filter) {
            return Filter.builder().from(filter).input(newInput).build();
        }
        if (wrapper instanceof Project project) {
            return Project.builder().from(project).input(newInput).build();
        }
        throw new UnsupportedOperationException(
            "Cannot attach-on-top a Substrait Rel of type " + wrapper.getClass().getSimpleName() + " — no single-input rewire defined"
        );
    }

    /**
     * Rewrites every {@link OpenSearchStageInputScan} in {@code node} to a
     * {@link StageInputTableScan} whose qualified name is {@code "input-" + i} where
     * {@code i} is the dense index assigned to the scan's {@code childStageId} based
     * on encounter order in pre-order traversal.
     *
     * <p>Single-input shapes (today's only shape outside joins) collapse to one
     * distinct {@code childStageId}, which always gets index {@code 0} → name
     * {@code "input-0"} — matching {@link DatafusionReduceSink#INPUT_ID} byte-for-byte.
     *
     * <p>Join coord fragments have two distinct {@code childStageId}s; the left
     * subtree is encountered first (giving its scan name {@code "input-0"}) and the
     * right subtree second ({@code "input-1"}). This matches the dense
     * {@code childStageId → indexInChildStages} convention emitted by the DAG
     * builder, and the same convention the reduce sink uses when registering
     * partition streams against the local DataFusion session.
     */
    static RelNode rewriteStageInputScans(RelNode node) {
        java.util.LinkedHashMap<Integer, Integer> childStageIdToInputIndex = new java.util.LinkedHashMap<>();
        collectStageInputIds(node, childStageIdToInputIndex);
        return rewriteStageInputScans(node, childStageIdToInputIndex);
    }

    private static void collectStageInputIds(RelNode node, java.util.LinkedHashMap<Integer, Integer> out) {
        if (node instanceof OpenSearchStageInputScan scan) {
            int childStageId = scan.getChildStageId();
            if (!out.containsKey(childStageId)) {
                out.put(childStageId, out.size());
            }
            return;
        }
        for (RelNode input : node.getInputs()) {
            collectStageInputIds(input, out);
        }
    }

    private static RelNode rewriteStageInputScans(RelNode node, java.util.Map<Integer, Integer> childStageIdToInputIndex) {
        if (node instanceof OpenSearchStageInputScan scan) {
            int idx = childStageIdToInputIndex.get(scan.getChildStageId());
            String inputId = "input-" + idx;
            return new StageInputTableScan(scan.getCluster(), scan.getTraitSet(), inputId, scan.getRowType());
        }
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = rewriteStageInputScans(input, childStageIdToInputIndex);
            newInputs.add(rewritten);
            if (rewritten != input) {
                changed = true;
            }
        }
        if (changed) {
            return node.copy(node.getTraitSet(), newInputs);
        }
        return node;
    }

    /** Walks {@code node} and wraps each {@link StageInputTableScan} in an identity
     *  {@link org.apache.calcite.rel.logical.LogicalProject} that selects every input
     *  column. The Project pins every state column at the logical level so isthmus's
     *  substrait emission keeps the full state schema in the {@code Read}'s output —
     *  otherwise unused-column pruning drops state cols the FINAL aggregate's argList
     *  doesn't textually reference, leaving Final-mode {@code AggregateExec} unable
     *  to read its accumulator state at runtime. */
    private static RelNode pinStageInputStateColumns(RelNode node) {
        if (node instanceof StageInputTableScan scan) {
            org.apache.calcite.rex.RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
            List<org.apache.calcite.rex.RexNode> projects = new ArrayList<>(scan.getRowType().getFieldCount());
            List<String> names = new ArrayList<>(scan.getRowType().getFieldCount());
            for (int i = 0; i < scan.getRowType().getFieldCount(); i++) {
                RelDataTypeField f = scan.getRowType().getFieldList().get(i);
                projects.add(rexBuilder.makeInputRef(f.getType(), i));
                names.add(f.getName());
            }
            return org.apache.calcite.rel.logical.LogicalProject.create(
                scan,
                java.util.List.of(),
                projects,
                names,
                java.util.Set.of()
            );
        }
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = pinStageInputStateColumns(input);
            newInputs.add(rewritten);
            if (rewritten != input) {
                changed = true;
            }
        }
        if (changed) {
            return node.copy(node.getTraitSet(), newInputs);
        }
        return node;
    }

    // ── Visitor wiring ──────────────────────────────────────────────────────────

    private SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        TypeConverter typeConverter = TypeConverter.DEFAULT;

        List<FunctionMappings.Sig> additionalAggSigs = List.of(FunctionMappings.s(stubAgg("take"), "take"));
        AggregateFunctionConverter aggConverter = new NameBasedAggregateFunctionConverter(
            extensions.aggregateFunctions(),
            additionalAggSigs,
            typeFactory,
            typeConverter
        );
        ScalarFunctionConverter scalarConverter = new NameBasedScalarFunctionConverter(
            extensions.scalarFunctions(),
            List.of(),
            typeFactory,
            typeConverter
        );
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(extensions.windowFunctions(), typeFactory);

        ConverterProvider provider = new ConverterProvider(
            typeFactory,
            extensions,
            scalarConverter,
            aggConverter,
            windowConverter,
            typeConverter
        );
        return new SubstraitRelVisitor(provider);
    }

    private static SqlAggFunction stubAgg(String name) {
        return new SqlAggFunction(
            name,
            null,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0,
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            false,
            false,
            Optionality.FORBIDDEN
        ) {};
    }

    // ── Plan serde helpers ──────────────────────────────────────────────────────

    private Plan decodePlan(byte[] bytes) {
        try {
            io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(bytes);
            return new ProtoPlanConverter(extensions).from(proto);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to decode Substrait plan bytes", e);
        }
    }

    private static byte[] serializePlan(Plan plan) {
        io.substrait.proto.Plan proto = new PlanProtoConverter().toProto(plan);
        return remapFunctionNames(proto).toByteArray();
    }

    /**
     * Walks the proto plan's extension function declarations and renames any function whose
     * name (the part before {@code :}) appears as a key in {@link #SUBSTRAIT_FN_NAME_REMAP}.
     * Substrait function refs are by anchor; DataFusion's consumer resolves the name from
     * the extension table, so renaming here is sufficient.
     */
    private static io.substrait.proto.Plan remapFunctionNames(io.substrait.proto.Plan proto) {
        if (SUBSTRAIT_FN_NAME_REMAP.isEmpty()) return proto;
        io.substrait.proto.Plan.Builder b = proto.toBuilder();
        boolean changed = false;
        for (int i = 0; i < b.getExtensionsCount(); i++) {
            io.substrait.proto.SimpleExtensionDeclaration ext = b.getExtensions(i);
            if (!ext.hasExtensionFunction()) continue;
            String name = ext.getExtensionFunction().getName();
            // Names are formatted "fn_name:type_signature" — only rewrite the fn_name part.
            int colon = name.indexOf(':');
            String fnName = colon < 0 ? name : name.substring(0, colon);
            String replacement = SUBSTRAIT_FN_NAME_REMAP.get(fnName);
            if (replacement == null) continue;
            String newName = colon < 0 ? replacement : replacement + name.substring(colon);
            b.setExtensions(
                i,
                ext.toBuilder().setExtensionFunction(ext.getExtensionFunction().toBuilder().setName(newName)).build()
            );
            changed = true;
        }
        return changed ? b.build() : proto;
    }

    // ── Calcite TableScan wrappers for OpenSearchStageInputScan rewrite ─────────

    static final class StageInputTableScan extends TableScan {
        StageInputTableScan(RelOptCluster cluster, RelTraitSet traitSet, String stageInputId, RelDataType rowType) {
            super(cluster, traitSet, List.of(), new StageInputRelOptTable(stageInputId, rowType));
        }
    }

    static final class StageInputRelOptTable implements RelOptTable {
        private final List<String> qualifiedName;
        private final RelDataType rowType;

        StageInputRelOptTable(String stageInputId, RelDataType rowType) {
            this.qualifiedName = List.of(stageInputId);
            this.rowType = rowType;
        }

        @Override
        public List<String> getQualifiedName() {
            return qualifiedName;
        }

        @Override
        public RelDataType getRowType() {
            return rowType;
        }

        @Override
        public double getRowCount() {
            return 100;
        }

        @Override
        public RelOptSchema getRelOptSchema() {
            return null;
        }

        @Override
        public RelNode toRel(ToRelContext context) {
            throw new UnsupportedOperationException("StageInputRelOptTable.toRel not supported");
        }

        @Override
        public List<ColumnStrategy> getColumnStrategies() {
            return List.of();
        }

        @Override
        public <C> C unwrap(Class<C> aClass) {
            return null;
        }

        @Override
        public boolean isKey(ImmutableBitSet columns) {
            return false;
        }

        @Override
        public List<ImmutableBitSet> getKeys() {
            return List.of();
        }

        @Override
        public List<RelReferentialConstraint> getReferentialConstraints() {
            return List.of();
        }

        @Override
        public List<RelCollation> getCollationList() {
            return List.of();
        }

        @Override
        public RelDistribution getDistribution() {
            return RelDistributions.ANY;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public org.apache.calcite.linq4j.tree.Expression getExpression(Class clazz) {
            return null;
        }

        @Override
        public RelOptTable extend(List<RelDataTypeField> extendedFields) {
            return this;
        }
    }
}
