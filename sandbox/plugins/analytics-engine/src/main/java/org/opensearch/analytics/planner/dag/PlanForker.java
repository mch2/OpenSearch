/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;

import java.util.ArrayList;
import java.util.List;

/**
 * Generates plan alternatives for each {@link Stage} in a {@link QueryDAG}.
 *
 * <p>Walks each stage's marked fragment bottom-up. For each operator, generates
 * one {@link StagePlan} per viable backend. Annotations are grouped by target
 * backend to avoid combinatorial explosion — with a single backend (pure DF),
 * this naturally produces one alternative per stage.
 *
 * <p>TODO: gate plan forking based on index stats (size, shard count, doc count).
 * For small indices, generating multiple alternatives adds overhead with minimal benefit.
 *
 * <p>TODO: add pruning via BackendPriority and cost functions when multiple backends
 * are viable for the same stage.
 *
 * @opensearch.internal
 */
public class PlanForker {

    private PlanForker() {}

    public static void forkAll(QueryDAG dag, CapabilityRegistry registry) {
        forkStage(dag.rootStage(), registry);
    }

    /**
     * Resolves a WHOLE marked tree (no DAG cut) to a single driving backend — the distributed-engine
     * analogue of {@link #forkStage}. This is the step the distributed path was missing: after
     * {@code mark}, dual-viable operators (e.g. a filter viable on {@code [lucene, datafusion]}) are
     * still unresolved, so a predicate delegated to a secondary backend is NOT yet turned into a
     * {@code delegated_predicate(id)} marker. Running resolution narrows every operator to
     * {@code preferredBackend} (when viable) and narrows each annotation to its target backend via
     * {@link #resolveAnnotationsToTarget} — so the subsequent {@code convertWholeQuery} strip emits the
     * marker + captures the secondary backend's serialized query, exactly as a per-shard fragment does.
     *
     * <p>Picks the alternative whose top-level chosen backend is {@code preferredBackend}; falls back
     * to the first alternative when the preferred backend is not viable for the whole tree.
     *
     * @param marked the marked whole-query tree ({@code PlannerImpl.createMarkedPlan})
     * @param preferredBackend the driving backend to resolve to (e.g. {@code "datafusion"})
     * @param registry capability registry
     * @return the resolved single-backend tree (delegated predicates left as annotations for the strip)
     */
    public static RelNode resolveWholeQuery(RelNode marked, String preferredBackend, CapabilityRegistry registry) {
        List<Resolved> alternatives = resolve(marked, registry);
        if (alternatives.isEmpty()) {
            throw new IllegalStateException("PlanForker.resolveWholeQuery: no viable backend resolution for the marked plan");
        }
        for (Resolved r : alternatives) {
            if (preferredBackend.equals(r.chosenBackend)) {
                return r.node;
            }
        }
        // Preferred backend not viable at the top — fall back to the first alternative (deterministic
        // order from resolve()); the distributed engine requires the datafusion driver, so the caller
        // may still reject a non-datafusion resolution downstream.
        return alternatives.getFirst().node;
    }

    private static void forkStage(Stage stage, CapabilityRegistry registry) {
        for (Stage child : stage.getChildStages()) {
            forkStage(child, registry);
        }
        if (stage.getFragment() == null) {
            return;
        }
        List<Resolved> alternatives = resolve(stage.getFragment(), registry);
        stage.setPlanAlternatives(alternatives.stream().map(resolved -> new StagePlan(resolved.node, resolved.chosenBackend)).toList());
    }

    /** Resolved node paired with the backend chosen at this operator level. */
    private record Resolved(String chosenBackend, RelNode node) {
    }

    private static List<Resolved> resolve(RelNode node, CapabilityRegistry registry) {
        List<List<Resolved>> childAlternativeSets = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            childAlternativeSets.add(resolve(input, registry));
        }

        if (childAlternativeSets.isEmpty()) {
            return resolveOperator(node, List.of(), null);
        }

        if (childAlternativeSets.size() == 1) {
            List<Resolved> results = new ArrayList<>();
            for (Resolved childAlt : childAlternativeSets.getFirst()) {
                results.addAll(resolveOperator(node, List.of(childAlt.node), childAlt.chosenBackend));
            }
            return results;
        }

        // Multi-input within one exchange-free stage: arms run on the same backend, so emit one
        // alternative per backend EVERY child offers (picking each child's alt on it), not each
        // child's first alt — which could be a backend the parent can't run (e.g. lucene under a
        // DataFusion-only Union), leaving zero alternatives. Cross-backend arms are split by an
        // exchange upstream; TODO: fan out the Cartesian product when multi-backend pipelines land.
        List<String> parentBackends = node instanceof OpenSearchRelNode osNode
            ? osNode.getViableBackends()
            : childAlternativeSets.getFirst().stream().map(Resolved::chosenBackend).distinct().toList();
        List<Resolved> results = new ArrayList<>();
        for (String backend : parentBackends) {
            List<RelNode> picked = new ArrayList<>(childAlternativeSets.size());
            for (List<Resolved> childAlts : childAlternativeSets) {
                Resolved on = altOnBackend(childAlts, backend);
                if (on != null) {
                    picked.add(on.node);
                }
            }
            if (picked.size() == childAlternativeSets.size()) {
                results.addAll(resolveOperator(node, picked, backend));
            }
        }
        return results;
    }

    /** A child alternative runnable on {@code backend} — its own, or a backend-agnostic
     *  (blank-backend) one (e.g. an infrastructure pass-through node); null if neither exists. */
    private static Resolved altOnBackend(List<Resolved> alts, String backend) {
        for (Resolved a : alts) {
            if (backend.equals(a.chosenBackend) || a.chosenBackend == null || a.chosenBackend.isEmpty()) {
                return a;
            }
        }
        return null;
    }

    private static List<Resolved> resolveOperator(RelNode node, List<RelNode> children, String childBackend) {
        if (!(node instanceof OpenSearchRelNode openSearchNode)) {
            // Non-OpenSearch node (e.g. StageInputScan infrastructure) — pass through.
            RelNode result = children.isEmpty() ? node : node.copy(node.getTraitSet(), children);
            return List.of(new Resolved(childBackend != null ? childBackend : "", result));
        }

        List<OperatorAnnotation> annotations = openSearchNode.getAnnotations();

        // Filter viable backends: only consider backends that match the child's chosen backend.
        // TODO: delegation will change this — cross-backend pipelines require revisiting
        // how the child backend propagates upward through the operator chain.
        List<String> backendsToConsider = new ArrayList<>();
        for (String backend : openSearchNode.getViableBackends()) {
            if (childBackend == null || backend.equals(childBackend)) {
                backendsToConsider.add(backend);
            }
        }

        List<Resolved> results = new ArrayList<>();
        for (String backend : backendsToConsider) {
            if (annotations.isEmpty()) {
                results.add(new Resolved(backend, openSearchNode.copyResolved(backend, children, List.of())));
                continue;
            }
            // Group annotations by target backend — one plan per distinct annotation backend group.
            // With a single backend, this produces exactly one alternative naturally.
            results.addAll(resolveWithBranching(openSearchNode, backend, children, annotations));
        }
        return results;
    }

    private static List<Resolved> resolveWithBranching(
        OpenSearchRelNode node,
        String backend,
        List<RelNode> children,
        List<OperatorAnnotation> annotations
    ) {
        // TODO: delegation will change this — when annotations have viable backends that differ
        // from the operator's backend, generate one plan per distinct annotation target backend
        // (e.g. DF operator with Lucene annotation for filter delegation).
        // For PR2 (no delegation), always resolve annotations to the operator's own backend.
        List<OperatorAnnotation> resolved = resolveAnnotationsToTarget(annotations, backend, backend);
        return List.of(new Resolved(backend, node.copyResolved(backend, children, resolved)));
    }

    private static List<OperatorAnnotation> resolveAnnotationsToTarget(
        List<OperatorAnnotation> annotations,
        String targetBackend,
        String operatorBackend
    ) {
        List<OperatorAnnotation> resolved = new ArrayList<>();
        for (OperatorAnnotation annotation : annotations) {
            if (annotation.getViableBackends().contains(targetBackend)) {
                resolved.add(annotation.narrowTo(targetBackend));
            } else if (annotation.getViableBackends().contains(operatorBackend)) {
                resolved.add(annotation.narrowTo(operatorBackend));
            } else {
                // Fallback: narrow to first viable backend.
                resolved.add(annotation.narrowTo(annotation.getViableBackends().getFirst()));
            }
        }
        return resolved;
    }
}
