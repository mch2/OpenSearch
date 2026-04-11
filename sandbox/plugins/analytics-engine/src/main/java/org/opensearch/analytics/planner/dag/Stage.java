/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.exec.ShardFilterPhase;
import org.opensearch.analytics.exec.TerminationDecider;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.common.Nullable;

import java.util.List;

/**
 * A stage in the query DAG. Each stage holds a marked plan fragment (annotations
 * intact, multiple viableBackends per operator/expression) and references to
 * child stages.
 *
 * <p>After plan forking, {@code planAlternatives} contains resolved variants
 * where every viableBackends is narrowed to exactly one backend.
 *
 * <p>Stage properties like {@code tableName}, {@code coordinatorGather}, and
 * {@code shuffleWrite} are computed once during DAG construction and avoid
 * repeated RelNode tree walking at execution time.
 *
 * @opensearch.internal
 */
public class Stage {

    private final int stageId;
    private final RelNode fragment;
    private final List<Stage> childStages;
    private final ExchangeInfo exchangeInfo;
    private final String tableName;
    private List<StagePlan> planAlternatives;
    private ShardFilterPhase shardFilterPhase = ShardFilterPhase.IDENTITY;
    private TerminationDecider terminationDecider = TerminationDecider.DISPATCH_ALL;

    public Stage(int stageId, RelNode fragment, List<Stage> childStages, ExchangeInfo exchangeInfo) {
        this.stageId = stageId;
        this.fragment = fragment;
        this.childStages = List.copyOf(childStages);
        this.exchangeInfo = exchangeInfo;
        this.tableName = findTableName(fragment);
        this.planAlternatives = List.of();
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
    public ExchangeInfo getExchangeInfo() {
        return exchangeInfo;
    }

    /**
     * The table name scanned by this stage, or null if the stage has no TableScan
     * (e.g., coordinator gather with StageInputScan).
     */
    @Nullable
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns true if this stage is a coordinator gather: no exchange and no TableScan.
     * Child results are already in the root sink; nothing to dispatch.
     */
    public boolean isCoordinatorGather() {
        return exchangeInfo == null && tableName == null;
    }

    /**
     * Returns true if this stage writes shuffle output (exchange is HASH/RANGE with shuffle).
     * Responses carry metadata (partition manifests) instead of rows.
     */
    public boolean isShuffleWrite() {
        return exchangeInfo != null && exchangeInfo.isShuffle();
    }

    public List<StagePlan> getPlanAlternatives() {
        return planAlternatives;
    }

    public void setPlanAlternatives(List<StagePlan> planAlternatives) {
        this.planAlternatives = planAlternatives;
    }

    public ShardFilterPhase getShardFilterPhase() {
        return shardFilterPhase;
    }

    public void setShardFilterPhase(ShardFilterPhase shardFilterPhase) {
        this.shardFilterPhase = shardFilterPhase;
    }

    public TerminationDecider getTerminationDecider() {
        return terminationDecider;
    }

    public void setTerminationDecider(TerminationDecider terminationDecider) {
        this.terminationDecider = terminationDecider;
    }

    /** Walks the fragment tree to find OpenSearchTableScan and extract the table name. */
    private static String findTableName(RelNode node) {
        if (node == null) return null;
        if (node instanceof OpenSearchTableScan scan) {
            List<String> qn = scan.getTable().getQualifiedName();
            return qn.get(qn.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = findTableName(input);
            if (name != null) return name;
        }
        return null;
    }
}
