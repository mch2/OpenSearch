/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.shard;

import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.stage.StageExecutionFactory;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardTargetResolver;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.DelegationDescriptor;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShardScanWithDelegationInstructionNode;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Builds a {@link ShardFragmentStageExecution} that fans out shard requests via
 * {@link AnalyticsSearchTransportService}. Takes a pre-resolved {@link ExchangeSink}
 * and doesn't care whether it is a root sink or a parent-provided child sink
 * — {@link StageExecutionBuilder} resolves that distinction before calling.
 *
 * @opensearch.internal
 */
public final class ShardFragmentStageExecutionFactory implements StageExecutionFactory {

    private final ClusterService clusterService;
    private final AnalyticsSearchTransportService transport;

    public ShardFragmentStageExecutionFactory(ClusterService clusterService, AnalyticsSearchTransportService transport) {
        this.clusterService = clusterService;
        this.transport = transport;
    }

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        // Inject the per-query max-shards limit (snapshotted from the dynamic cluster setting by
        // DefaultPlanExecutor into the QueryContext) into the resolver, which enforces it at resolve().
        if (stage.getTargetResolver() instanceof ShardTargetResolver shardResolver) {
            shardResolver.setMaxShardsPerQuery(config.maxShardsPerQuery());
        }
        final String queryId = config.queryId();
        final int stageId = stage.getStageId();
        // df-proto migration D14: when the shard stage was finalized to a DataFusion proto
        // plan (full_proto), ship one DF_PROTO request carrying {planFormatVersion,
        // dataFusionVersion, planBytes} — no PlanAlternative list, no instructions, no
        // delegation descriptor. Otherwise ship the legacy PlanAlternative form.
        byte[] protoPlanBytes = protoPlanBytes(stage);
        Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder;
        if (protoPlanBytes != null) {
            requestBuilder = target -> new FragmentExecutionRequest(
                queryId,
                stageId,
                target.shardId(),
                FragmentExecutionRequest.PLAN_FORMAT_VERSION_CURRENT,
                FragmentExecutionRequest.DATAFUSION_VERSION,
                protoPlanBytes
            );
        } else {
            List<FragmentExecutionRequest.PlanAlternative> planAlternatives = buildPlanAlternatives(stage);
            requestBuilder = target -> new FragmentExecutionRequest(queryId, stageId, target.shardId(), planAlternatives);
        }
        // Execution pulls the resolver off `stage` and calls resolve() lazily at start().
        // This keeps target resolution out of the build phase so cancellation before
        // dispatch doesn't pay for cluster-state routing, and leaves room for shuffle
        // reads whose targets depend on child manifests only available at dispatch time.
        return new ShardFragmentStageExecution(stage, config, sink, clusterService, requestBuilder, transport);
    }

    /**
     * The finalized DataFusion proto plan bytes for this shard stage, or {@code null} when the
     * stage was not finalized to proto (legacy / reduce_proto). Returns the first alternative's
     * {@code planBytes} — shard stages carry exactly one alternative post-selection.
     *
     * <p>GATING (df-proto migration Phase 2b): shipping a DF_PROTO shard request requires the
     * data-node {@code execute_stage_task} route that builds the indexed session from a
     * {@code ShardBindings} TaskContext extension and runs {@code OpenSearchShardScanExec}. Until
     * that lands, we do NOT ship proto to shards even under {@code full_proto} — the stage is still
     * finalized (validating the finalizer end to end) but ships the legacy PlanAlternative form, so
     * {@code full_proto} degrades safely to the working shard path rather than failing the query.
     */
    private static byte[] protoPlanBytes(Stage stage) {
        if (!SHARD_PROTO_EXECUTION_READY) {
            return null;
        }
        if (stage.getPlanAlternatives().isEmpty()) {
            return null;
        }
        byte[] bytes = stage.getPlanAlternatives().getFirst().planBytes();
        return (bytes != null && bytes.length > 0) ? bytes : null;
    }

    /**
     * Flips to {@code true} when the data-node DF_PROTO shard execution route
     * ({@code execute_stage_task} + {@code OpenSearchShardScanExec} session build from
     * {@code ShardBindings}) is implemented. Keeping it {@code false} makes {@code full_proto}
     * ship the working legacy shard request while the reduce stages still go proto.
     */
    private static final boolean SHARD_PROTO_EXECUTION_READY = false;

    private static List<FragmentExecutionRequest.PlanAlternative> buildPlanAlternatives(Stage stage) {
        List<FragmentExecutionRequest.PlanAlternative> alternatives = new ArrayList<>();
        for (StagePlan plan : stage.getPlanAlternatives()) {
            DelegationDescriptor delegationDescriptor = buildDelegationDescriptor(plan);
            alternatives.add(
                new FragmentExecutionRequest.PlanAlternative(
                    plan.backendId(),
                    plan.convertedBytes(),
                    plan.instructions(),
                    delegationDescriptor
                )
            );
        }
        return alternatives;
    }

    private static DelegationDescriptor buildDelegationDescriptor(StagePlan plan) {
        if (plan.delegatedExpressions().isEmpty()) {
            return null;
        }
        // Extract treeShape and count from the ShardScanWithDelegationInstructionNode
        for (InstructionNode node : plan.instructions()) {
            if (node instanceof ShardScanWithDelegationInstructionNode delegationNode) {
                return new DelegationDescriptor(
                    delegationNode.getTreeShape(),
                    delegationNode.getDelegatedPredicateCount(),
                    plan.delegatedExpressions()
                );
            }
        }
        return null;
    }
}
