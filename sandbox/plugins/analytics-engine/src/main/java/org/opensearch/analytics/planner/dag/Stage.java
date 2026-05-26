/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.exec.canmatch.CanMatchFilter;
import org.opensearch.analytics.exec.canmatch.CanMatchFilterExtractor;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.common.Nullable;

import java.util.List;

/**
 * A stage in the query DAG. Each stage holds a marked plan fragment (annotations
 * intact, multiple viableBackends per operator/expression), a {@link TargetResolver}
 * for the Scheduler to resolve execution targets lazily, and references to child stages.
 *
 * <p>Execution shape is surfaced explicitly via {@link #getExecutionType()}, derived
 * at construction in priority order:
 * <ol>
 *   <li>{@link #getTargetResolver()} non-null → {@link StageExecutionType#SHARD_FRAGMENT}
 *       — dispatch fragment per-shard to data nodes.</li>
 *   <li>{@link #getExchangeSinkProvider()} non-null → {@link StageExecutionType#COORDINATOR_REDUCE}
 *       — coordinator-side reduction via backend sink.</li>
 *   <li>Otherwise → {@link StageExecutionType#LOCAL_PASSTHROUGH} — coordinator gather
 *       via {@code RowProducingSink}.</li>
 * </ol>
 *
 * <p>After plan forking, {@code planAlternatives} contains resolved variants
 * where every viableBackends is narrowed to exactly one backend.
 *
 * @opensearch.internal
 */
public class Stage {

    private final int stageId;
    private final RelNode fragment;
    private final List<Stage> childStages;
    private final ExchangeInfo exchangeInfo;
    private final ExchangeSinkProvider exchangeSinkProvider;
    private final TargetResolver targetResolver;
    private final StageExecutionType executionType;
    /**
     * Range-style predicates extracted from the fragment at construction time, suitable
     * for can-match pre-filtering. Captured here because downstream rewrites (e.g.
     * pushing filters into scan operators) may make {@link CanMatchFilterExtractor}
     * unable to recover them by walking the fragment later.
     */
    private final List<CanMatchFilter> canMatchFilters;
    private List<StagePlan> planAlternatives;
    private FragmentInstructionHandlerFactory instructionHandlerFactory;

    public Stage(
        int stageId,
        RelNode fragment,
        List<Stage> childStages,
        ExchangeInfo exchangeInfo,
        ExchangeSinkProvider exchangeSinkProvider,
        TargetResolver targetResolver
    ) {
        this(stageId, fragment, childStages, exchangeInfo, exchangeSinkProvider, targetResolver, null);
    }

    /**
     * Constructor with explicit {@code executionTypeOverride}. Used by the can-match
     * pre-filter stage where the natural inference (targetResolver != null →
     * SHARD_FRAGMENT) would mis-classify a coordinator-side metadata-only stage.
     * When {@code null}, falls back to {@link #setStageExecutionType}.
     */
    public Stage(
        int stageId,
        RelNode fragment,
        List<Stage> childStages,
        ExchangeInfo exchangeInfo,
        ExchangeSinkProvider exchangeSinkProvider,
        TargetResolver targetResolver,
        @Nullable StageExecutionType executionTypeOverride
    ) {
        this.stageId = stageId;
        this.fragment = fragment;
        this.childStages = List.copyOf(childStages);
        this.exchangeInfo = exchangeInfo;
        this.exchangeSinkProvider = exchangeSinkProvider;
        this.targetResolver = targetResolver;
        this.executionType = executionTypeOverride != null
            ? executionTypeOverride
            : setStageExecutionType(exchangeSinkProvider, targetResolver, fragment);
        this.planAlternatives = List.of();
        // Capture predicates now while the plan still carries them in a recognizable
        // (OpenSearchFilter / Filter) shape. Empty list for stages with no extractable
        // predicates — extractor never throws.
        this.canMatchFilters = fragment == null ? List.of() : List.copyOf(CanMatchFilterExtractor.extract(fragment));
    }

    public int getStageId() {
        return stageId;
    }

    /** Marked plan fragment with annotations intact. */
    public RelNode getFragment() {
        return fragment;
    }

    public List<Stage> getChildStages() {
        return childStages;
    }

    /** How this stage connects to its parent. Null for the root stage. */
    @Nullable
    public ExchangeInfo getExchangeInfo() {
        return exchangeInfo;
    }

    /**
     * Non-null for coordinator stages with backend computation (final aggregate, sort).
     * Null for simple gather stages — Scheduler uses a {@code RowProducingSink} instead.
     */
    @Nullable
    public ExchangeSinkProvider getExchangeSinkProvider() {
        return exchangeSinkProvider;
    }

    /**
     * Non-null for DATA_NODE stages. Null for coordinator/gather stages.
     * Scheduler calls {@code targetResolver.resolve(clusterState, childManifest)} lazily
     * just before dispatch.
     */
    @Nullable
    public TargetResolver getTargetResolver() {
        return targetResolver;
    }

    /**
     * Returns where this stage's compute runs. Derived at construction from the
     * target resolver / sink provider pair — see the class-level javadoc.
     */
    public StageExecutionType getExecutionType() {
        return executionType;
    }

    /** Captured at construction; see field doc. Empty when no extractable predicates exist. */
    public List<CanMatchFilter> getCanMatchFilters() {
        return canMatchFilters;
    }

    public List<StagePlan> getPlanAlternatives() {
        return planAlternatives;
    }

    public void setPlanAlternatives(List<StagePlan> planAlternatives) {
        this.planAlternatives = planAlternatives;
    }

    public FragmentInstructionHandlerFactory getInstructionHandlerFactory() {
        return instructionHandlerFactory;
    }

    public void setInstructionHandlerFactory(FragmentInstructionHandlerFactory instructionHandlerFactory) {
        this.instructionHandlerFactory = instructionHandlerFactory;
    }

    private StageExecutionType setStageExecutionType(
        ExchangeSinkProvider exchangeSinkProvider,
        TargetResolver targetResolver,
        RelNode fragment
    ) {
        if (targetResolver != null) {
            return StageExecutionType.SHARD_FRAGMENT;
        } else if (hasComputeLeaf(fragment)) {
            // Coord-only compute leaf (e.g. OpenSearchValues) — run the plan locally
            // via the backend's in-process engine. Takes precedence over the
            // sink-provider check because LOCAL_COMPUTE also requires a sink
            // provider (DAGBuilder attaches one), but the routing differs.
            return StageExecutionType.LOCAL_COMPUTE;
        } else if (exchangeSinkProvider != null) {
            return StageExecutionType.COORDINATOR_REDUCE;
        } else {
            return StageExecutionType.LOCAL_PASSTHROUGH;
        }
    }

    /**
     * True iff {@code fragment} contains a coord-only compute leaf —
     * an {@link org.opensearch.analytics.planner.rel.OpenSearchValues} today;
     * future literal-source rels would extend this list.
     */
    private static boolean hasComputeLeaf(RelNode fragment) {
        return org.opensearch.analytics.planner.RelNodeUtils.findNode(
            fragment,
            org.opensearch.analytics.planner.rel.OpenSearchValues.class
        ) != null;
    }
}
