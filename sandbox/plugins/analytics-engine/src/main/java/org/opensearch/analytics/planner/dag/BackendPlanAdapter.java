/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Walks a resolved plan and applies per-function {@link ScalarFunctionAdapter}s
 * provided by the driving backend. Runs between plan forking and fragment conversion.
 *
 * <p>Each backend declares adapters keyed by {@link ScalarFunction} via
 * {@link org.opensearch.analytics.spi.BackendCapabilityProvider#scalarFunctionAdapters()}.
 * This component looks up the adapter for each scalar function RexCall in the plan
 * and applies it if present.
 *
 * @opensearch.internal
 */
public class BackendPlanAdapter {

    private static final Logger LOGGER = LogManager.getLogger(BackendPlanAdapter.class);

    private BackendPlanAdapter() {}

    /**
     * Adapt all plan alternatives in the DAG using each alternative's driving backend's adapters.
     */
    public static void adaptAll(QueryDAG dag, CapabilityRegistry registry) {
        adaptStage(dag.rootStage(), registry);
    }

    private static void adaptStage(Stage stage, CapabilityRegistry registry) {
        for (Stage child : stage.getChildStages()) {
            adaptStage(child, registry);
        }
        List<StagePlan> adapted = new ArrayList<>(stage.getPlanAlternatives().size());
        for (StagePlan plan : stage.getPlanAlternatives()) {
            BackendCapabilityProvider caps = registry.getBackend(plan.backendId()).getCapabilityProvider();
            Map<ScalarFunction, ScalarFunctionAdapter> scalarAdapters = caps.scalarFunctionAdapters();
            Map<AggregateFunction, UnaryOperator<AggregateCall>> aggAdapters = caps.aggregateCallAdapters();
            if (scalarAdapters.isEmpty() && aggAdapters.isEmpty()) {
                adapted.add(plan);
            } else {
                LOGGER.debug("Before adaptation [{}]:\n{}", plan.backendId(), RelOptUtil.toString(plan.resolvedFragment()));
                RelNode adaptedFragment = adaptNode(plan.resolvedFragment(), scalarAdapters, aggAdapters);
                LOGGER.debug("After adaptation [{}]:\n{}", plan.backendId(), RelOptUtil.toString(adaptedFragment));
                adapted.add(new StagePlan(adaptedFragment, plan.backendId()));
            }
        }
        stage.setPlanAlternatives(adapted);
    }

    private static RelNode adaptNode(
        RelNode node,
        Map<ScalarFunction, ScalarFunctionAdapter> scalarAdapters,
        Map<AggregateFunction, UnaryOperator<AggregateCall>> aggAdapters
    ) {
        List<RelNode> adaptedChildren = new ArrayList<>(node.getInputs().size());
        boolean childrenChanged = false;
        for (RelNode child : node.getInputs()) {
            RelNode adaptedChild = adaptNode(child, scalarAdapters, aggAdapters);
            adaptedChildren.add(adaptedChild);
            if (adaptedChild != child) childrenChanged = true;
        }

        if (node instanceof OpenSearchFilter filter) {
            return adaptFilter(filter, scalarAdapters, adaptedChildren, childrenChanged);
        }
        if (node instanceof OpenSearchProject project) {
            return adaptProject(project, scalarAdapters, adaptedChildren, childrenChanged);
        }
        if (node instanceof OpenSearchAggregate aggregate) {
            return adaptAggregate(aggregate, aggAdapters, adaptedChildren, childrenChanged);
        }

        return childrenChanged ? node.copy(node.getTraitSet(), adaptedChildren) : node;
    }

    /**
     * Applies per-function adapters to each call in an {@link OpenSearchAggregate}. Resolution
     * uses {@link AggregateFunction#fromAggregateCall} so backends can key adapters by the logical
     * enum rather than SqlKind or operator identity.
     */
    private static RelNode adaptAggregate(
        OpenSearchAggregate aggregate,
        Map<AggregateFunction, UnaryOperator<AggregateCall>> adapters,
        List<RelNode> adaptedChildren,
        boolean childrenChanged
    ) {
        RelNode input = childrenChanged ? adaptedChildren.getFirst() : aggregate.getInput();
        List<AggregateCall> adaptedCalls = new ArrayList<>(aggregate.getAggCallList().size());
        boolean callsChanged = false;
        for (AggregateCall call : aggregate.getAggCallList()) {
            AggregateCall adapted = adaptAggCall(call, adapters);
            adaptedCalls.add(adapted);
            if (adapted != call) callsChanged = true;
        }
        if (!callsChanged && !childrenChanged) {
            return aggregate;
        }
        return aggregate.copy(aggregate.getTraitSet(), input, aggregate.getGroupSet(), aggregate.getGroupSets(), adaptedCalls);
    }

    private static AggregateCall adaptAggCall(AggregateCall call, Map<AggregateFunction, UnaryOperator<AggregateCall>> adapters) {
        if (adapters.isEmpty()) {
            return call;
        }
        AggregateFunction function = AggregateFunction.fromAggregateCall(call);
        if (function == null) {
            return call;
        }
        UnaryOperator<AggregateCall> adapter = adapters.get(function);
        if (adapter == null) {
            return call;
        }
        return adapter.apply(call);
    }

    private static RelNode adaptFilter(
        OpenSearchFilter filter,
        Map<ScalarFunction, ScalarFunctionAdapter> scalarAdapters,
        List<RelNode> adaptedChildren,
        boolean childrenChanged
    ) {
        List<FieldStorageInfo> fieldStorage = filter.getOutputFieldStorage();
        RexNode adaptedCondition = adaptRex(filter.getCondition(), scalarAdapters, fieldStorage, filter.getCluster());
        if (adaptedCondition != filter.getCondition() || childrenChanged) {
            return new OpenSearchFilter(
                filter.getCluster(),
                filter.getTraitSet(),
                childrenChanged ? adaptedChildren.getFirst() : filter.getInput(),
                adaptedCondition,
                filter.getViableBackends()
            );
        }
        return filter;
    }

    private static RelNode adaptProject(
        OpenSearchProject project,
        Map<ScalarFunction, ScalarFunctionAdapter> scalarAdapters,
        List<RelNode> adaptedChildren,
        boolean childrenChanged
    ) {
        // RexInputRef in project expressions references the input's row type
        OpenSearchRelNode inputNode = (OpenSearchRelNode) RelNodeUtils.unwrapHep(project.getInput());
        List<FieldStorageInfo> fieldStorage = inputNode.getOutputFieldStorage();
        List<RexNode> adaptedProjects = new ArrayList<>(project.getProjects().size());
        boolean projectsChanged = false;
        for (RexNode projectExpr : project.getProjects()) {
            RexNode adapted = adaptRex(projectExpr, scalarAdapters, fieldStorage, project.getCluster());
            adaptedProjects.add(adapted);
            if (adapted != projectExpr) projectsChanged = true;
        }
        if (projectsChanged || childrenChanged) {
            return new OpenSearchProject(
                project.getCluster(),
                project.getTraitSet(),
                childrenChanged ? adaptedChildren.getFirst() : project.getInput(),
                adaptedProjects,
                project.getRowType(),
                project.getViableBackends()
            );
        }
        return project;
    }

    /**
     * Adapts RexNodes bottom-up: operands are adapted before the call itself.
     *
     * <p>This means a parent adapter receives already-adapted operands. This is safe
     * because adapters only inspect their <b>direct</b> operands via
     * {@code operand instanceof RexInputRef} to resolve field storage. If a child
     * adapter wraps an operand in CAST, the parent sees a {@code RexCall} (not
     * {@code RexInputRef}) and skips adaptation — no double-CAST occurs.
     *
     * <p>This ordering is validated by {@code testNestedAdaptedFunctionsProduceSingleCast}
     * which confirms {@code SIN(ABS($0))} with both adapted produces one CAST at the leaf.
     */
    private static RexNode adaptRex(
        RexNode node,
        Map<ScalarFunction, ScalarFunctionAdapter> scalarAdapters,
        List<FieldStorageInfo> fieldStorage,
        RelOptCluster cluster
    ) {
        if (!(node instanceof RexCall call)) {
            return node;
        }

        // Annotation wrappers: adapt the inner expression and re-wrap with same metadata.
        // Plain RexCall.clone() would drop the annotation subclass, breaking later stripping.
        if (node instanceof OperatorAnnotation annotation && annotation.unwrap() != null) {
            RexNode adaptedInner = adaptRex(annotation.unwrap(), scalarAdapters, fieldStorage, cluster);
            return adaptedInner == annotation.unwrap() ? node : annotation.withAdaptedOriginal(adaptedInner);
        }

        // Recurse into operands first
        List<RexNode> adaptedOperands = new ArrayList<>(call.getOperands().size());
        boolean operandsChanged = false;
        for (RexNode operand : call.getOperands()) {
            RexNode adapted = adaptRex(operand, scalarAdapters, fieldStorage, cluster);
            adaptedOperands.add(adapted);
            if (adapted != operand) operandsChanged = true;
        }

        RexCall current = operandsChanged ? call.clone(call.getType(), adaptedOperands) : call;

        // Look up adapter for this function
        ScalarFunction function = resolveFunction(current);
        if (function != null) {
            ScalarFunctionAdapter adapter = scalarAdapters.get(function);
            if (adapter != null) {
                return adapter.adapt(current, fieldStorage, cluster);
            }
        }

        return current;
    }

    private static ScalarFunction resolveFunction(RexCall call) {
        if (call.getOperator() instanceof SqlFunction sqlFunction) {
            try {
                return ScalarFunction.fromSqlFunction(sqlFunction);
            } catch (IllegalArgumentException ignored) {
                // Not in our enum — fall through to SqlKind resolution
            }
        }
        return ScalarFunction.fromSqlKind(call.getKind());
    }
}
