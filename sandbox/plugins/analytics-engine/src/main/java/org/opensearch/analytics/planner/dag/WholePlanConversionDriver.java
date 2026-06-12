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
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.WholePlanScan;
import org.opensearch.analytics.spi.WholePlanStageResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Whole-plan conversion driver (whole-plan-lowering-spec.md §5, Phase 2).
 *
 * <p>Replaces the per-stage {@link StageConversionDriver}: instead of finalizing each stage's
 * fragment independently, this converts every stage to its own Substrait (reusing the verbatim
 * {@link AnnotationStripper} + {@code convertFragment}), then hands the whole set to the backend's
 * {@code lowerWholePlan} — which stitches them into ONE whole-query Substrait plan (boundaries are
 * {@code os_stage_boundary} markers), lowers it once on the coordinator, and cuts it into per-stage
 * DataFusion physical plans. The returned {@code planBytes} are distributed back onto each stage by
 * {@code boundaryId == stageId}; the root stage carries {@code boundaryId == -1}.
 *
 * <p>Calcite still owns the cut, the split rules, and the DAG — boundary schemas are correct by
 * construction (read off the one lowered tree at the cut point), so there is no graft, no
 * aggregate-mode forcing, and no schema reconciliation.
 *
 * @opensearch.internal
 */
public final class WholePlanConversionDriver {

    private static final Logger LOGGER = LogManager.getLogger(WholePlanConversionDriver.class);

    /** The native cut tags the un-wrapped coordinator root stage with this boundary id. */
    static final int ROOT_BOUNDARY_ID = -1;

    private WholePlanConversionDriver() {}

    /**
     * Lower the whole DAG to per-stage DataFusion physical plans and store them on each stage.
     *
     * @throws IllegalStateException if a stage other than the root reports no plan, or if the
     *         native cut's stage edges disagree with the DAG's (the D6 cross-check)
     */
    public static void convertAll(QueryDAG dag, CapabilityRegistry registry) {
        Stage root = dag.rootStage();
        List<Stage> stages = new ArrayList<>();
        collectStages(root, stages);

        // 1. Convert each stage to its own Substrait (child stages become input-<childId> reads).
        Map<Integer, byte[]> stageSubstrait = new HashMap<>();
        Map<Integer, Stage> byId = new HashMap<>();
        List<WholePlanScan> scans = new ArrayList<>();
        FragmentConvertor convertor = null;

        for (Stage stage : stages) {
            byId.put(stage.getStageId(), stage);
            if (stage.getExecutionType() == StageExecutionType.LATE_MATERIALIZATION) {
                // LM is Java-only scatter/gather (D10) — it emits no Substrait compute and is not
                // eligible for whole_plan in Phase 2. Routing (D9) must exclude LM queries.
                throw new IllegalStateException(
                    "WholePlanConversionDriver: late-materialization stage " + stage.getStageId()
                        + " is not eligible for whole_plan (D9/D10)"
                );
            }
            if (stage.getPlanAlternatives().isEmpty()) {
                continue;
            }
            StagePlan plan = stage.getPlanAlternatives().getFirst();
            AnalyticsSearchBackendPlugin backend = registry.getBackend(plan.backendId());
            FragmentConvertor c = backend.getFragmentConvertor();
            if (convertor == null) {
                convertor = c;
            }

            // Filter tree shape BEFORE stripping (annotations intact) — one entry per real scan.
            RelNode leaf = findLeaf(plan.resolvedFragment());
            if (leaf instanceof OpenSearchTableScan ts) {
                OpenSearchFilter filter = RelNodeUtils.findNode(plan.resolvedFragment(), OpenSearchFilter.class);
                FilterTreeShape treeShape = filter != null
                    ? FilterTreeShapeDeriver.derive(filter, plan.backendId())
                    : FilterTreeShape.NO_DELEGATION;
                boolean requestsRowIds = ts.getRowType().getFieldNames().contains(OpenSearchLateMaterialization.ROW_ID_FIELD);
                scans.add(new WholePlanScan(tableNameOf(ts), treeShape.ordinal(), requestsRowIds));
            }

            // Strip the whole fragment in one pass (verbatim resolver), then convert.
            AnnotationStripper.IntraOperatorDelegationBytes delegationBytes =
                new AnnotationStripper.IntraOperatorDelegationBytes(registry);
            RelNode stripped = AnnotationStripper.strip(plan.resolvedFragment(), delegationBytes);
            byte[] substrait = c.convertFragment(stripped);
            stageSubstrait.put(stage.getStageId(), substrait);

            // Retain the Substrait for explain/profile (D11); planBytes filled after the cut.
            stage.setPlanAlternatives(List.of(plan.withProtoPlan(null, substrait)));
        }

        if (convertor == null || stageSubstrait.isEmpty()) {
            return; // nothing to lower
        }

        // 2. One whole-plan lowering: stitch -> lower -> cut -> per-stage plans.
        List<WholePlanStageResult> results = convertor.lowerWholePlan(
            root.getStageId(),
            stageSubstrait,
            scans,
            dag.queryId()
        );

        // 3. Distribute planBytes onto stages by boundaryId (== stageId; -1 is the coordinator root).
        Map<Integer, WholePlanStageResult> byBoundary = new HashMap<>();
        for (WholePlanStageResult r : results) {
            int stageId = r.boundaryId() == ROOT_BOUNDARY_ID ? root.getStageId() : r.boundaryId();
            byBoundary.put(stageId, r);
            Stage stage = byId.get(stageId);
            if (stage == null || stage.getPlanAlternatives().isEmpty()) {
                throw new IllegalStateException(
                    "WholePlanConversionDriver: cut returned unknown stage/boundary " + r.boundaryId()
                );
            }
            StagePlan plan = stage.getPlanAlternatives().getFirst();
            stage.setPlanAlternatives(List.of(plan.withProtoPlan(r.planBytes(), plan.debugSubstrait())));
        }

        // 4. D6 DAG cross-check: every converted stage must have a plan, and the native cut's
        //    inbound edges (child boundary ids) must match the DAG's child stage ids.
        crossCheckDag(stages, byBoundary, root.getStageId());

        LOGGER.debug("Whole-plan lowering produced {} stage plans from {} stages", results.size(), stages.size());
    }

    /** D6: the cut's per-stage child edges must equal the DAG's child stage ids. */
    private static void crossCheckDag(List<Stage> stages, Map<Integer, WholePlanStageResult> byBoundary, int rootStageId) {
        for (Stage stage : stages) {
            if (stage.getExecutionType() == StageExecutionType.LATE_MATERIALIZATION || stage.getPlanAlternatives().isEmpty()) {
                continue;
            }
            WholePlanStageResult r = byBoundary.get(stage.getStageId());
            if (r == null) {
                throw new IllegalStateException(
                    "WholePlanConversionDriver: stage " + stage.getStageId() + " got no plan from the cut (D6)"
                );
            }
            Set<Integer> dagChildren = new HashSet<>();
            for (Stage child : stage.getChildStages()) {
                dagChildren.add(child.getStageId());
            }
            Set<Integer> cutChildren = new HashSet<>();
            for (int id : r.childBoundaryIds()) {
                cutChildren.add(id);
            }
            if (!dagChildren.equals(cutChildren)) {
                throw new IllegalStateException(
                    "WholePlanConversionDriver: boundary-id mismatch at stage " + stage.getStageId()
                        + " (D6) — DAG children " + dagChildren + " vs cut children " + cutChildren
                );
            }
        }
    }

    private static void collectStages(Stage stage, List<Stage> out) {
        for (Stage child : stage.getChildStages()) {
            collectStages(child, out);
        }
        out.add(stage);
    }

    private static RelNode findLeaf(RelNode node) {
        RelNode n = node;
        while (!n.getInputs().isEmpty()) {
            n = n.getInputs().getFirst();
        }
        return n;
    }

    /** The Substrait NamedTable name the convertor uses for this scan (its qualified table name). */
    private static String tableNameOf(OpenSearchTableScan scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        return String.join(".", qualifiedName);
    }
}
