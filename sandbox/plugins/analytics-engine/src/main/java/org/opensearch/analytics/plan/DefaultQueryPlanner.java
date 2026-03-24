/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.opensearch.analytics.plan.operators.BackendTagged;
import org.opensearch.analytics.plan.operators.OpenSearchFilter;
import org.opensearch.analytics.plan.operators.OpenSearchHybridFilter;
import org.opensearch.analytics.plan.registry.BackendCapabilityRegistry;
import org.opensearch.analytics.plan.rules.OperatorWrapperVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link QueryPlanner}.
 * // TODO: this is poc - need to reimplement properly using calcite CBO
 *
 * <p>Two-phase pipeline:
 * <ol>
 *   <li>Wrap — convert Logical* to OpenSearch* operators</li>
 *   <li>Resolve — assign backend tags bottom-up, create hybrid filters for delegation</li>
 * </ol>
 */
public final class DefaultQueryPlanner implements QueryPlanner {

    private static final Logger logger = LogManager.getLogger(DefaultQueryPlanner.class);

    private final BackendCapabilityRegistry registry;
    private final RelOptCluster cluster;
    private final FieldCapabilityResolver fieldCapabilityResolver;

    public DefaultQueryPlanner(BackendCapabilityRegistry registry,
                               RelOptCluster cluster,
                               FieldCapabilityResolver fieldCapabilityResolver) {
        this.registry = registry;
        this.cluster = cluster;
        this.fieldCapabilityResolver = fieldCapabilityResolver;
    }

    @Override
    public ResolvedPlan plan(RelNode logicalPlan, int shardCount) {
        logger.info("[QueryPlanner] Input plan:\n{}", logicalPlan.explain());
        RelNode wrapped = wrap(logicalPlan);
        logger.info("[QueryPlanner] After wrap:\n{}", wrapped.explain());
        ResolvedPlan result = resolve(wrapped);
        logger.info("[QueryPlanner] After resolve (backend={}): \n{}",
            result.getPrimaryBackend(), result.getRoot().explain());
        return result;
    }

    // -----------------------------------------------------------------------
    // Phase 1 — Wrap
    // -----------------------------------------------------------------------

    private RelNode wrap(RelNode root) {
        return root.accept(new OperatorWrapperVisitor());
    }

    // -----------------------------------------------------------------------
    // Phase 2 — Resolve
    // -----------------------------------------------------------------------

    private ResolvedPlan resolve(RelNode root) {
        String tableName = extractTableName(root);
        Map<String, RexNode> delegationPredicates = new LinkedHashMap<>();
        RelNode resolvedRoot = resolveNode(root, tableName, delegationPredicates);
        String backendName = ((BackendTagged) resolvedRoot).getBackendTag();
        if ("unresolved".equals(backendName)) {
            throw new QueryPlanningException(List.of(
                "Backend resolution incomplete: root operator still unresolved"));
        }
        return new ResolvedPlan(resolvedRoot, backendName, delegationPredicates);
    }

    private RelNode resolveNode(RelNode node, String tableName, Map<String, RexNode> delegationPredicates) {
        List<RelNode> resolvedInputs = node.getInputs().stream()
            .map(input -> resolveNode(input, tableName, delegationPredicates))
            .collect(Collectors.toList());
        RelNode withResolvedInputs = node.copy(node.getTraitSet(), resolvedInputs);

        if (!(withResolvedInputs instanceof BackendTagged)) {
            throw new QueryPlanningException(List.of(
                "Non-wrapped operator encountered in resolution phase: "
                + withResolvedInputs.getClass().getSimpleName()
                + ". Ensure OperatorWrapperVisitor handles all operator types."));
        }

        if (withResolvedInputs instanceof OpenSearchFilter) {
            withResolvedInputs = resolveFilter((OpenSearchFilter) withResolvedInputs, tableName, delegationPredicates);
        }

        final RelNode resolved = withResolvedInputs;
        List<String> backends = registry.backendsForOperator(resolved.getClass());
        String tag = backends.isEmpty()
            ? ((BackendTagged) resolved).getBackendTag()
            : backends.get(0);

        return ((BackendTagged) resolved).withBackendTag(tag);
    }

    /**
     * Resolves filter predicates by checking field indexing and creating hybrid filters
     * when predicates span indexed and non-indexed fields.
     */
    private RelNode resolveFilter(OpenSearchFilter filter, String tableName,
                                   Map<String, RexNode> delegationPredicates) {
        if (tableName == null || fieldCapabilityResolver == null) {
            return filter;
        }

        String primaryBackend = null;
        if (filter.getInput() instanceof BackendTagged) {
            primaryBackend = ((BackendTagged) filter.getInput()).getBackendTag();
        }
        if (primaryBackend == null || "unresolved".equals(primaryBackend)) {
            return filter;
        }

        List<String> allBackends = registry.getRegisteredBackendNames();
        String secondaryBackend = null;
        for (String name : allBackends) {
            if (!name.equals(primaryBackend)) {
                secondaryBackend = name;
                break;
            }
        }
        if (secondaryBackend == null) {
            return filter;
        }

        RexNode condition = filter.getCondition();
        List<RexNode> conjuncts = new ArrayList<>();
        flattenAnd(condition, conjuncts);
        if (conjuncts.isEmpty()) {
            conjuncts.add(condition);
        }

        RelDataType inputRowType = filter.getInput().getRowType();
        Map<String, List<RexNode>> backendPredicates = new LinkedHashMap<>();
        boolean hasIndexedPredicate = false;

        for (RexNode conjunct : conjuncts) {
            Set<String> fields = extractFieldNames(conjunct, inputRowType);
            boolean allIndexed = !fields.isEmpty()
                && fields.stream().allMatch(f -> fieldCapabilityResolver.isFieldIndexed(tableName, f));

            if (allIndexed) {
                backendPredicates.computeIfAbsent(secondaryBackend, k -> new ArrayList<>()).add(conjunct);
                hasIndexedPredicate = true;
            } else {
                backendPredicates.computeIfAbsent(primaryBackend, k -> new ArrayList<>()).add(conjunct);
            }
        }

        if (!hasIndexedPredicate || backendPredicates.size() <= 1) {
            return filter;
        }

        Map<String, RexNode> splitPredicates = new LinkedHashMap<>();
        for (Map.Entry<String, List<RexNode>> entry : backendPredicates.entrySet()) {
            RexNode combined = RexUtil.composeConjunction(
                filter.getCluster().getRexBuilder(), entry.getValue());
            splitPredicates.put(entry.getKey(), combined);
        }

        for (Map.Entry<String, RexNode> entry : splitPredicates.entrySet()) {
            if (!entry.getKey().equals(primaryBackend)) {
                delegationPredicates.put(entry.getKey(), entry.getValue());
            }
        }

        logger.info("[QueryPlanner] Created hybrid filter: backends={}", splitPredicates.keySet());
        return new OpenSearchHybridFilter(
            filter.getCluster(), filter.getTraitSet(), filter.getInput(),
            condition, primaryBackend, splitPredicates);
    }

    // -----------------------------------------------------------------------
    // Utilities
    // -----------------------------------------------------------------------

    private String extractTableName(RelNode node) {
        if (node instanceof org.apache.calcite.rel.core.TableScan) {
            List<String> names = node.getTable().getQualifiedName();
            return names.get(names.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = extractTableName(input);
            if (name != null) return name;
        }
        return null;
    }

    private static void flattenAnd(RexNode node, List<RexNode> conjuncts) {
        if (node instanceof RexCall call && call.getOperator().getName().equals("AND")) {
            for (RexNode operand : call.getOperands()) {
                flattenAnd(operand, conjuncts);
            }
            return;
        }
        conjuncts.add(node);
    }

    private static Set<String> extractFieldNames(RexNode rex, RelDataType rowType) {
        Set<String> fields = new HashSet<>();
        collectFieldNames(rex, rowType, fields);
        return fields;
    }

    private static void collectFieldNames(RexNode rex, RelDataType rowType, Set<String> fields) {
        if (rex instanceof RexInputRef ref) {
            if (ref.getIndex() < rowType.getFieldCount()) {
                fields.add(rowType.getFieldNames().get(ref.getIndex()));
            }
        } else if (rex instanceof RexCall call) {
            for (RexNode operand : call.getOperands()) {
                collectFieldNames(operand, rowType, fields);
            }
        }
    }
}
