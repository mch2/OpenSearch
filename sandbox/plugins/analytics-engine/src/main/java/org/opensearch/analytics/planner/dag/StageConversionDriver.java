/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.proto.StageMetaCodec;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rel.OpenSearchValues;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-stage proto conversion driver (df-proto migration §5, Phase 2a).
 *
 * <p>The new path that replaces the legacy {@link FragmentConversionDriver} layered
 * byte-stitching for stages selected by {@code analytics.engine.plan_format}. For each
 * stage it: strips the whole fragment via {@link AnnotationStripper} (one pass, no
 * layering), runs one {@code convertFragment} call to produce the stage's Substrait
 * bytes, builds a {@link StageMetaCodec.StageMeta}, then issues a single batch FFM
 * finalize call (the native finalizer orders stages child-first and returns one
 * serialized DataFusion physical plan per stage). The resulting {@code planBytes} are
 * stored on each {@link StagePlan} alongside the retained {@code debugSubstrait} (D11).
 *
 * <p>Calcite owns the cut, the split rules, and the DAG — this driver only changes how
 * a stage's plan is serialized for the wire. The {@link AnnotationStripper} and the
 * delegation resolver are reused verbatim, so delegation classification is byte-identical
 * to the legacy path.
 *
 * @opensearch.internal
 */
public final class StageConversionDriver {

    private static final Logger LOGGER = LogManager.getLogger(StageConversionDriver.class);

    private StageConversionDriver() {}

    /**
     * Finalize stages in the DAG to a DataFusion proto plan, storing the result on each
     * stage's plan alternatives (df-proto migration D12).
     *
     * <p>Stages skipped:
     * <ul>
     *   <li>Late-materialization (D10 — the LM stage gets no Rust plan).</li>
     *   <li>Shard fragments when {@code shardStagesProto} is false (Phase 2a {@code reduce_proto}:
     *       shard stages stay byte-identical legacy; only reduce / coordinator-local stages go
     *       proto). The shard fragment's leaf is a TableScan over the real index, which can't be
     *       lowered on the coordinator's finalizer session — so it MUST be excluded under
     *       {@code reduce_proto}, otherwise lowering fails with "No table named ...".</li>
     * </ul>
     *
     * @param shardStagesProto when true (full_proto) shard stages are also finalized; when false
     *                         (reduce_proto) only reduce / coordinator-local stages are.
     */
    public static void convertAll(QueryDAG dag, CapabilityRegistry registry, boolean shardStagesProto) {
        // 1. Collect all stages (any order — the native finalizer re-orders child-first).
        List<Stage> stages = new ArrayList<>();
        collectStages(dag.rootStage(), stages);

        // 2. Build one FinalizeStage per (stage, chosen plan alternative). Phase 2a selects
        //    exactly one alternative per stage upstream (PlanAlternativeSelector), so we take
        //    the first.
        List<StageMetaCodec.FinalizeStage> finalizeStages = new ArrayList<>(stages.size());
        Map<Integer, Stage> byId = new HashMap<>();
        // Convertor is backend-specific but identical across DataFusion stages; capture the
        // first one that can finalize and use it for the batch call.
        FragmentConvertor finalizer = null;

        for (Stage stage : stages) {
            byId.put(stage.getStageId(), stage);
            if (stage.getExecutionType() == StageExecutionType.LATE_MATERIALIZATION) {
                continue; // D10
            }
            // Phase 2a (reduce_proto): shard fragments stay legacy — their TableScan leaf
            // references the real index, which the coordinator finalizer session can't lower.
            if (!shardStagesProto && stage.getExecutionType() == StageExecutionType.SHARD_FRAGMENT) {
                continue;
            }
            if (stage.getPlanAlternatives().isEmpty()) {
                continue;
            }
            StagePlan plan = stage.getPlanAlternatives().getFirst();
            AnalyticsSearchBackendPlugin backend = registry.getBackend(plan.backendId());
            FragmentConvertor convertor = backend.getFragmentConvertor();
            if (finalizer == null) {
                finalizer = convertor;
            }

            // Filter tree shape BEFORE stripping (annotations intact).
            OpenSearchFilter filter = RelNodeUtils.findNode(plan.resolvedFragment(), OpenSearchFilter.class);
            FilterTreeShape treeShape = filter != null
                ? FilterTreeShapeDeriver.derive(filter, plan.backendId())
                : FilterTreeShape.NO_DELEGATION;

            // Strip the whole fragment in one pass (verbatim resolver via AnnotationStripper).
            AnnotationStripper.IntraOperatorDelegationBytes delegationBytes =
                new AnnotationStripper.IntraOperatorDelegationBytes(registry);
            RelNode stripped = AnnotationStripper.strip(plan.resolvedFragment(), delegationBytes);

            // One whole-fragment Substrait conversion (no layering).
            byte[] substrait = convertor.convertFragment(stripped);

            StageMetaCodec.StageMeta meta = buildStageMeta(stage, plan, treeShape, delegationBytes, registry);
            finalizeStages.add(new StageMetaCodec.FinalizeStage(substrait, meta));

            // Stash the Substrait on the stage now; planBytes filled after the FFM call.
            stage.setPlanAlternatives(List.of(plan.withProtoPlan(null, substrait)));
        }

        if (finalizer == null || finalizeStages.isEmpty()) {
            return; // nothing to finalize (e.g. pure-LM DAG)
        }

        // 3. Single batch FFM finalize call.
        byte[] request = StageMetaCodec.encodeFinalizeRequest(finalizeStages);
        byte[] response = finalizer.finalizeStages(request);
        List<StageMetaCodec.FinalizedStage> finalized = StageMetaCodec.decodeFinalizeResponse(response);

        // 4. Distribute planBytes back onto each stage's plan alternative.
        for (StageMetaCodec.FinalizedStage fs : finalized) {
            Stage stage = byId.get(fs.stageId());
            if (stage == null || stage.getPlanAlternatives().isEmpty()) {
                LOGGER.warn("finalize response references unknown stage {}", fs.stageId());
                continue;
            }
            StagePlan plan = stage.getPlanAlternatives().getFirst();
            stage.setPlanAlternatives(List.of(plan.withProtoPlan(fs.planBytes(), plan.debugSubstrait())));
        }
        LOGGER.debug("Finalized {} stages to DataFusion proto plans", finalized.size());
    }

    private static void collectStages(Stage stage, List<Stage> out) {
        for (Stage child : stage.getChildStages()) {
            collectStages(child, out);
        }
        out.add(stage);
    }

    /** Build the {@link StageMetaCodec.StageMeta} describing how the finalizer treats this stage. */
    private static StageMetaCodec.StageMeta buildStageMeta(
        Stage stage,
        StagePlan plan,
        FilterTreeShape treeShape,
        AnnotationStripper.IntraOperatorDelegationBytes delegationBytes,
        CapabilityRegistry registry
    ) {
        int[] childIds = stage.getChildStages().stream().mapToInt(Stage::getStageId).toArray();
        int aggMode = aggModeOf(plan.resolvedFragment());
        int leafKind = leafKindOf(plan.resolvedFragment());

        RelNode leaf = findLeaf(plan.resolvedFragment());
        boolean requestsRowIds = leaf instanceof OpenSearchTableScan ts
            && ts.getRowType().getFieldNames().contains(OpenSearchLateMaterialization.ROW_ID_FIELD);

        List<StageMetaCodec.DelegatedExpr> delegated = new ArrayList<>();
        for (DelegatedExpression d : delegationBytes.getResult()) {
            delegated.add(new StageMetaCodec.DelegatedExpr(d.getAnnotationId(), d.getAcceptingBackendId(), d.getExpressionBytes()));
        }

        // D5 (reduce_proto): supply each child's partial-stage Substrait (parallel to childIds)
        // so the finalizer derives the child's ACTUAL physical output schema coordinator-side
        // (derive_schema_from_partial_plan) — the boundary source of truth, instead of Calcite's
        // declared rowType which can drift (e.g. Int32 vs the real Int64 SUM state). The child's
        // partial Substrait is its legacy convertedBytes(). Empty when a child has none.
        List<byte[]> childPartialSubstrait = new ArrayList<>(stage.getChildStages().size());
        for (Stage child : stage.getChildStages()) {
            byte[] childBytes = child.getPlanAlternatives().isEmpty()
                ? null
                : child.getPlanAlternatives().getFirst().convertedBytes();
            childPartialSubstrait.add(childBytes != null ? childBytes : new byte[0]);
        }

        return new StageMetaCodec.StageMeta(
            stage.getStageId(),
            childIds,
            aggMode,
            leafKind,
            treeShape.ordinal(),
            requestsRowIds,
            delegated,
            // declared_input_row_types (D6 assertion targets) — filled when the parent stage
            // declares input rowTypes on its StageInputScan; left empty here for the common
            // agg path where the graft owns the boundary (D6 at graft top, not StageReadExec).
            List.of(),
            // lm_output_row_type (D10) — set by the parent of an LM child; not applicable here.
            null,
            childPartialSubstrait
        );
    }

    /** The aggregate mode Calcite declared for this stage's top aggregate, or NONE. */
    private static int aggModeOf(RelNode fragment) {
        OpenSearchAggregate agg = RelNodeUtils.findNode(fragment, OpenSearchAggregate.class);
        if (agg == null) {
            return StageMetaCodec.AGG_MODE_NONE;
        }
        return switch (agg.getMode()) {
            case PARTIAL -> StageMetaCodec.AGG_MODE_PARTIAL;
            case FINAL -> StageMetaCodec.AGG_MODE_FINAL;
            default -> StageMetaCodec.AGG_MODE_NONE; // SINGLE → no split, treat as NONE
        };
    }

    /** The leaf kind of this stage's fragment. */
    private static int leafKindOf(RelNode fragment) {
        RelNode leaf = findLeaf(fragment);
        if (leaf instanceof OpenSearchTableScan) {
            return StageMetaCodec.LEAF_KIND_SHARD_SCAN;
        }
        if (leaf instanceof OpenSearchStageInputScan) {
            return StageMetaCodec.LEAF_KIND_STAGE_INPUT;
        }
        if (leaf instanceof OpenSearchValues) {
            return StageMetaCodec.LEAF_KIND_VALUES;
        }
        // Default: a stage-input read (the most common reduce-stage leaf).
        return StageMetaCodec.LEAF_KIND_STAGE_INPUT;
    }

    private static RelNode findLeaf(RelNode node) {
        RelNode n = node;
        while (!n.getInputs().isEmpty()) {
            n = n.getInputs().getFirst();
        }
        return n;
    }
}
