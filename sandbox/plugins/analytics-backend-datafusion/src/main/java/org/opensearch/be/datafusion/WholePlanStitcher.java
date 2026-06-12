/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.ImmutablePlan;
import io.substrait.plan.ImmutableRoot;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.util.EmptyVisitationContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Whole-plan Substrait stitcher (whole-plan-lowering-spec.md §5, D2).
 *
 * <p>Each distributed stage is converted to its own Substrait plan exactly as the legacy
 * per-fragment path does — its child stages appear as {@code NamedScan("input-<childId>")} leaves
 * (see {@code DataFusionFragmentConvertor#rewriteStageInputScans}). This stitcher assembles those
 * per-stage plans into ONE whole-query Substrait plan by walking the root stage's rel tree and
 * replacing every {@code input-<childId>} scan with an {@code os_stage_boundary}
 * {@link ExtensionSingle} whose single input is the child stage's own (recursively stitched) tree.
 *
 * <p><b>Why POJO, not raw proto:</b> each stage is converted by its own isthmus
 * {@code ExtensionCollector}, so each plan's {@code function_reference}/{@code type} anchors are
 * numbered independently. Splicing at the raw-proto level (keeping only the root's extension table)
 * dangles every child subtree's anchors → {@code Unsupported function name}. Stitching at the
 * isthmus POJO level (function/type refs held by value) and re-serializing once via
 * {@link PlanProtoConverter} re-collects ONE consistent extension table — the same mechanism the
 * legacy {@code attachFragmentOnTop} relies on.
 *
 * @opensearch.internal
 */
final class WholePlanStitcher {

    /** Extension-relation type URL — exposed for tests; the marker JSON lives in {@link StageBoundaryDetail}. */
    static final String STAGE_BOUNDARY_TYPE_URL = StageBoundaryDetail.TYPE_URL;

    private static final String INPUT_TABLE_PREFIX = "input-";

    private final SimpleExtension.ExtensionCollection extensions;

    WholePlanStitcher(SimpleExtension.ExtensionCollection extensions) {
        this.extensions = extensions;
    }

    /**
     * Stitch the per-stage Substrait plans into one whole-query Substrait plan rooted at
     * {@code rootStageId}.
     *
     * @param rootStageId    the id of the root (coordinator) stage
     * @param stageSubstrait each stage's own converted Substrait plan bytes, keyed by stage id
     * @return the stitched whole-query Substrait plan bytes, ready for {@code planWholeQuery}
     */
    byte[] stitch(int rootStageId, Map<Integer, byte[]> stageSubstrait) {
        Plan rootPlan = decode(stageSubstrait, rootStageId);
        Plan.Root root = singleRoot(rootPlan, rootStageId);

        Rel stitchedRoot = rewriteReads(root.getInput(), stageSubstrait);

        Plan.Root newRoot = ImmutableRoot.builder().input(stitchedRoot).names(root.getNames()).build();
        Plan stitched = ImmutablePlan.builder().addRoots(newRoot).build();
        return new PlanProtoConverter().toProto(stitched).toByteArray();
    }

    /**
     * Walk {@code rel}; replace each {@code input-<childId>} {@link NamedScan} with an
     * {@code os_stage_boundary} marker wrapping the child stage's recursively-stitched tree; copy
     * all other rels unchanged (their child inputs are rewritten by the visitor).
     */
    private Rel rewriteReads(Rel rel, Map<Integer, byte[]> stageSubstrait) {
        RelCopyOnWriteVisitor<RuntimeException> visitor = new RelCopyOnWriteVisitor<>() {
            @Override
            public Optional<Rel> visit(NamedScan scan, EmptyVisitationContext context) {
                Integer childId = stageInputChildId(scan);
                if (childId == null) {
                    return Optional.empty(); // a real base-table scan (shard leaf) — leave it
                }
                Rel childTree = stitchStageTree(childId, stageSubstrait);
                // ExtensionSingle.from wires deriveRecordType from the detail (passthrough = child's).
                return Optional.of(
                    ExtensionSingle.from(new StageBoundaryDetail(childId), childTree).build()
                );
            }
        };
        return rel.accept(visitor, EmptyVisitationContext.INSTANCE).orElse(rel);
    }

    /** The fully-stitched root rel of stage {@code stageId} (its own tree, child scans fenced). */
    private Rel stitchStageTree(int stageId, Map<Integer, byte[]> stageSubstrait) {
        Plan plan = decode(stageSubstrait, stageId);
        Plan.Root root = singleRoot(plan, stageId);
        return rewriteReads(root.getInput(), stageSubstrait);
    }

    /** {@code <childId>} if {@code scan} is an {@code input-<childId>} stage-input scan, else null. */
    private static Integer stageInputChildId(NamedScan scan) {
        List<String> names = scan.getNames();
        if (names.isEmpty()) {
            return null;
        }
        String name = names.get(names.size() - 1);
        if (!name.startsWith(INPUT_TABLE_PREFIX)) {
            return null;
        }
        try {
            return Integer.parseInt(name.substring(INPUT_TABLE_PREFIX.length()));
        } catch (NumberFormatException e) {
            return null; // a real table that merely starts with "input-"
        }
    }

    private Plan decode(Map<Integer, byte[]> stageSubstrait, int stageId) {
        byte[] bytes = stageSubstrait.get(stageId);
        if (bytes == null) {
            throw new IllegalArgumentException("WholePlanStitcher: no Substrait for stage " + stageId);
        }
        try {
            io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(bytes);
            return new ProtoPlanConverter(extensions).from(proto);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("WholePlanStitcher: stage " + stageId + " has invalid Substrait", e);
        }
    }

    private static Plan.Root singleRoot(Plan plan, int stageId) {
        if (plan.getRoots().isEmpty()) {
            throw new IllegalStateException("WholePlanStitcher: stage " + stageId + " Substrait has no root relation");
        }
        return plan.getRoots().get(0);
    }
}
