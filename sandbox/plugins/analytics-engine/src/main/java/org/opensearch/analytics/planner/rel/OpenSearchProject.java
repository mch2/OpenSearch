/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * OpenSearch custom Project carrying viable backend list and per-expression annotations.
 *
 * @opensearch.internal
 */
public class OpenSearchProject extends Project implements OpenSearchRelNode {

    private final List<String> viableBackends;

    public OpenSearchProject(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<? extends RexNode> projects,
        RelDataType rowType,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, List.of(), input, projects, rowType);
        this.viableBackends = viableBackends;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        if (!(input instanceof OpenSearchRelNode openSearchChild)) {
            throw new IllegalStateException("Project child is not OpenSearchRelNode: " + input.getClass().getSimpleName());
        }
        List<FieldStorageInfo> inputStorage = openSearchChild.getOutputFieldStorage();

        List<FieldStorageInfo> result = new ArrayList<>(getProjects().size());
        for (int i = 0; i < getProjects().size(); i++) {
            RexNode expr = getProjects().get(i);
            if (expr instanceof RexInputRef ref && ref.getIndex() < inputStorage.size()) {
                result.add(inputStorage.get(ref.getIndex()));
            } else {
                String fieldName = getRowType().getFieldList().get(i).getName();
                result.add(FieldStorageInfo.derivedColumn(fieldName, getRowType().getFieldList().get(i).getType().getSqlTypeName()));
            }
        }
        return result;
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new OpenSearchProject(getCluster(), traitSet, input, projects, rowType, viableBackends);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // A windowed Project (any expression containing a RexOver) computes a window aggregate
        // that depends on a totally-ordered global stream — running sums, ranks, frame-bounded
        // aggregates, etc. Executing it over partitioned input is incorrect: each shard would
        // produce its own per-shard window state and the coordinator gather would concatenate
        // mismatched results. Mirror the OpenSearchAggregate.computeSelfCost pattern: return
        // infinite cost when the input distribution is non-SINGLETON-and-non-ANY so Volcano
        // explores inserting an OpenSearchExchangeReducer below this Project, forcing the
        // windowed compute to run on the coordinator over the gathered stream.
        //
        // TODO (FB-3 hash shuffle): once HASH_DISTRIBUTED exchanges are implemented, refine this
        // to allow HASH_DISTRIBUTED input when the RexOver carries a PARTITION BY clause whose
        // keys match the input's hash keys. SINGLETON remains required for unpartitioned RexOver.
        if (containsWindowFunction()) {
            for (int i = 0; i < getInput().getTraitSet().size(); i++) {
                RelTrait trait = getInput().getTraitSet().getTrait(i);
                if (trait instanceof OpenSearchDistribution distribution
                    && distribution.getType() != RelDistribution.Type.SINGLETON
                    && distribution.getType() != RelDistribution.Type.ANY) {
                    return planner.getCostFactory().makeInfiniteCost();
                }
            }
            return planner.getCostFactory().makeTinyCost();
        }
        // Non-windowed Project: prefer running on data-node side (RANDOM) so column
        // pruning shrinks the row width before gather. A SINGLETON variant of the Project
        // (produced by OpenSearchProjectDistributionDeriveRule for the windowed-stack
        // propagation case) is still valid — it just costs more so Volcano picks the
        // RANDOM-Project + ER-above plan when both are viable. When the only viable
        // SINGLETON path is via this Project (e.g. above a windowed-gathered Project
        // whose output is already SINGLETON), the cost penalty doesn't matter — there's
        // no RANDOM alternative because the input doesn't deliver RANDOM.
        for (int i = 0; i < getTraitSet().size(); i++) {
            RelTrait trait = getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution distribution
                && distribution.getType() == RelDistribution.Type.SINGLETON) {
                return planner.getCostFactory().makeCost(10, 10, 0);
            }
        }
        return planner.getCostFactory().makeTinyCost();
    }

    private boolean containsWindowFunction() {
        WindowFunctionDetector detector = new WindowFunctionDetector();
        for (RexNode expr : getProjects()) {
            expr.accept(detector);
            if (detector.found) {
                return true;
            }
        }
        return false;
    }

    /**
     * Recursively scans a RexNode tree for a {@link RexOver}. The project rule wraps top-level
     * window expressions in {@link AnnotatedProjectExpression}, so the search must descend into
     * operand trees rather than only checking top-level project expressions.
     */
    private static final class WindowFunctionDetector extends RexVisitorImpl<Void> {
        boolean found = false;

        WindowFunctionDetector() {
            super(true);
        }

        @Override
        public Void visitOver(RexOver over) {
            found = true;
            return null;
        }
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public List<OperatorAnnotation> getAnnotations() {
        List<OperatorAnnotation> annotations = new ArrayList<>();
        for (RexNode expr : getProjects()) {
            if (expr instanceof AnnotatedProjectExpression annotation) {
                annotations.add(annotation);
            }
        }
        return annotations;
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        int annotationIndex = 0;
        List<RexNode> resolvedExprs = new ArrayList<>();
        for (RexNode expr : getProjects()) {
            if (expr instanceof AnnotatedProjectExpression) {
                resolvedExprs.add((RexNode) resolvedAnnotations.get(annotationIndex++));
            } else {
                // Plain expressions (field refs, literals, scalar calls) have no annotation — pass through.
                resolvedExprs.add(expr);
            }
        }
        return new OpenSearchProject(getCluster(), getTraitSet(), children.getFirst(), resolvedExprs, getRowType(), List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return stripAnnotations(strippedChildren, OperatorAnnotation::unwrap);
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren, Function<OperatorAnnotation, RexNode> annotationResolver) {
        // OpenSearchProjectRule.annotateExpr recurses into operands when validating viable
        // backends, so a top-level call like COALESCE(num0, CEIL(num1)) ends up with the inner
        // CEIL also wrapped. The supplied annotationResolver controls how each top-level
        // wrapper is unwrapped (defaults to OperatorAnnotation::unwrap, returning the original
        // RexNode); a RexShuttle then sweeps the resolver's result to strip any remaining
        // nested wrappers. Substrait conversion only recognizes the underlying RexCall shape,
        // so every wrapper at every depth must be removed before the plan is handed to a
        // backend's FragmentConvertor.
        RexShuttle nestedAnnotationStripper = new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                if (call instanceof AnnotatedProjectExpression nested) {
                    return nested.getOriginal().accept(this);
                }
                return super.visitCall(call);
            }
        };
        // Re-stamp RexInputRef types from the new input column types.
        RelNode strippedInput = strippedChildren.getFirst();
        RexShuttle inputRefRetyper = new RexShuttle() {
            @Override
            public RexNode visitInputRef(org.apache.calcite.rex.RexInputRef ref) {
                org.apache.calcite.rel.type.RelDataType newType = strippedInput.getRowType().getFieldList().get(ref.getIndex()).getType();
                return newType.equals(ref.getType()) ? ref : new org.apache.calcite.rex.RexInputRef(ref.getIndex(), newType);
            }
        };
        List<RexNode> strippedExprs = new ArrayList<>();
        for (RexNode expr : getProjects()) {
            RexNode unwrapped;
            if (expr instanceof AnnotatedProjectExpression annotated) {
                unwrapped = annotationResolver.apply(annotated).accept(nestedAnnotationStripper);
            } else {
                // Plain expressions have no annotation to strip — pass through.
                unwrapped = expr;
            }
            strippedExprs.add(unwrapped.accept(inputRefRetyper));
        }
        // null fieldNames lets Calcite re-derive the row type from the new exprs.
        return LogicalProject.create(strippedInput, List.of(), strippedExprs, (List<String>) null);
    }
}
