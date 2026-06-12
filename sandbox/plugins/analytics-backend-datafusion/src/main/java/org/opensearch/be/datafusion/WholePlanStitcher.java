/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.proto.ExtensionSingleRel;
import io.substrait.proto.FetchRel;
import io.substrait.proto.FilterRel;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.SetRel;
import io.substrait.proto.SortRel;

import java.util.Map;

/**
 * Whole-plan Substrait stitcher (whole-plan-lowering-spec.md §5, D2).
 *
 * <p>Each distributed stage is converted to its own Substrait {@link Plan} exactly as the
 * legacy per-fragment path does — its child stages appear as {@code Read("input-<childId>")}
 * named-table leaves (see {@code DataFusionFragmentConvertor#rewriteStageInputScans}). This
 * stitcher assembles those per-stage plans into ONE whole-query Substrait plan by walking the
 * root stage's rel tree and replacing every {@code input-<childId>} read with an
 * {@code os_stage_boundary} {@link ExtensionSingleRel} whose single input is the child stage's
 * own (recursively stitched) rel tree.
 *
 * <p>The marker's {@code detail} is a {@link com.google.protobuf.Any} with
 * {@code type_url == "os_stage_boundary"} and a JSON value
 * {@code {"boundary_id":<childStageId>,"exchange_type":"GATHER"}} — byte-for-byte the contract
 * the Rust {@code StageBoundarySerializerRegistry} consumes ({@code StageBoundaryDetail}). The
 * boundary id is the child stage's id, so the Rust cut produces one stage per boundary id (plus
 * {@code ROOT_BOUNDARY_ID = -1} for the un-wrapped root tree) and the Java DAG can correlate the
 * returned {@code plan_bytes} back onto its stages by id.
 *
 * <p>No isthmus involvement: this is pure protobuf surgery over the already-converted bytes, so
 * it composes with the entire existing convertor and is unit-testable without a cluster.
 *
 * @opensearch.internal
 */
public final class WholePlanStitcher {

    /** Substrait extension-relation type URL for a stage boundary — must match Rust's {@code STAGE_BOUNDARY_TYPE_URL}. */
    static final String STAGE_BOUNDARY_TYPE_URL = "os_stage_boundary";

    /** The {@code input-} prefix that {@code rewriteStageInputScans} stamps onto stage-input named tables. */
    private static final String INPUT_TABLE_PREFIX = "input-";

    private WholePlanStitcher() {}

    /**
     * Stitch the per-stage Substrait plans into one whole-query Substrait plan rooted at
     * {@code rootStageId}.
     *
     * @param rootStageId    the id of the root (coordinator) stage
     * @param stageSubstrait each stage's own converted Substrait {@link Plan} bytes, keyed by stage id
     *                       (the same bytes the legacy path produces; child stages appear as
     *                       {@code input-<childId>} reads inside their parent's plan)
     * @return the stitched whole-query Substrait {@link Plan} bytes, ready for {@code planWholeQuery}
     */
    public static byte[] stitch(int rootStageId, Map<Integer, byte[]> stageSubstrait) {
        Plan rootPlan = decode(stageSubstrait, rootStageId);
        PlanRel rootPlanRel = singleRoot(rootPlan, rootStageId);
        RelRoot root = rootPlanRel.getRoot();

        Rel stitchedRoot = rewriteReads(root.getInput(), stageSubstrait);

        // Preserve the root stage's output field names — they are the query's result column names.
        RelRoot newRoot = RelRoot.newBuilder().setInput(stitchedRoot).addAllNames(root.getNamesList()).build();
        return Plan.newBuilder(rootPlan)
            .clearRelations()
            .addRelations(PlanRel.newBuilder().setRoot(newRoot).build())
            .build()
            .toByteArray();
    }

    /**
     * Walk {@code rel}; replace each {@code input-<childId>} read with an {@code os_stage_boundary}
     * marker wrapping the child stage's recursively-stitched rel; rebuild all other rels with
     * rewritten inputs.
     */
    private static Rel rewriteReads(Rel rel, Map<Integer, byte[]> stageSubstrait) {
        switch (rel.getRelTypeCase()) {
            case READ:
                Integer childId = stageInputChildId(rel.getRead());
                if (childId == null) {
                    return rel; // a real base-table scan (shard leaf) — leave it for scan-leaf swap
                }
                // Recursively stitch the child stage's own tree, then fence it behind a boundary.
                Rel childTree = stitchStageTree(childId, stageSubstrait);
                return Rel.newBuilder().setExtensionSingle(boundaryMarker(childId, childTree)).build();

            case FILTER:
                FilterRel f = rel.getFilter();
                return Rel.newBuilder(rel)
                    .setFilter(FilterRel.newBuilder(f).setInput(rewriteReads(f.getInput(), stageSubstrait)))
                    .build();

            case PROJECT:
                ProjectRel p = rel.getProject();
                return Rel.newBuilder(rel)
                    .setProject(ProjectRel.newBuilder(p).setInput(rewriteReads(p.getInput(), stageSubstrait)))
                    .build();

            case AGGREGATE:
                AggregateRel a = rel.getAggregate();
                return Rel.newBuilder(rel)
                    .setAggregate(AggregateRel.newBuilder(a).setInput(rewriteReads(a.getInput(), stageSubstrait)))
                    .build();

            case SORT:
                SortRel s = rel.getSort();
                return Rel.newBuilder(rel)
                    .setSort(SortRel.newBuilder(s).setInput(rewriteReads(s.getInput(), stageSubstrait)))
                    .build();

            case FETCH:
                FetchRel ft = rel.getFetch();
                return Rel.newBuilder(rel)
                    .setFetch(FetchRel.newBuilder(ft).setInput(rewriteReads(ft.getInput(), stageSubstrait)))
                    .build();

            case SET:
                SetRel set = rel.getSet();
                SetRel.Builder setB = SetRel.newBuilder(set).clearInputs();
                for (Rel in : set.getInputsList()) {
                    setB.addInputs(rewriteReads(in, stageSubstrait));
                }
                return Rel.newBuilder(rel).setSet(setB).build();

            case EXTENSION_SINGLE:
                ExtensionSingleRel ext = rel.getExtensionSingle();
                return Rel.newBuilder(rel)
                    .setExtensionSingle(ExtensionSingleRel.newBuilder(ext).setInput(rewriteReads(ext.getInput(), stageSubstrait)))
                    .build();

            default:
                // Eligible whole-plan queries (D9) produce only the rel kinds above. Anything else
                // (Join/Cross/Window/Expand/...) means routing admitted an ineligible plan — fail
                // loudly rather than silently dropping a boundary.
                throw new IllegalStateException(
                    "WholePlanStitcher: unsupported Substrait rel " + rel.getRelTypeCase()
                        + " — query should not have been routed whole_plan (D9)"
                );
        }
    }

    /** The fully-stitched root rel of stage {@code stageId} (its own tree, child reads fenced). */
    private static Rel stitchStageTree(int stageId, Map<Integer, byte[]> stageSubstrait) {
        Plan plan = decode(stageSubstrait, stageId);
        PlanRel planRel = singleRoot(plan, stageId);
        return rewriteReads(planRel.getRoot().getInput(), stageSubstrait);
    }

    /** Build the {@code os_stage_boundary} marker for {@code boundaryId} over {@code input}. */
    private static ExtensionSingleRel boundaryMarker(int boundaryId, Rel input) {
        // JSON must match Rust's StageBoundaryDetail serde: {boundary_id, exchange_type:"GATHER"}.
        String detailJson = "{\"boundary_id\":" + boundaryId + ",\"exchange_type\":\"GATHER\"}";
        Any detail = Any.newBuilder()
            .setTypeUrl(STAGE_BOUNDARY_TYPE_URL)
            .setValue(ByteString.copyFromUtf8(detailJson))
            .build();
        return ExtensionSingleRel.newBuilder().setDetail(detail).setInput(input).build();
    }

    /** {@code <childId>} if {@code read} is a {@code input-<childId>} stage-input scan, else null. */
    private static Integer stageInputChildId(ReadRel read) {
        if (!read.hasNamedTable() || read.getNamedTable().getNamesCount() == 0) {
            return null;
        }
        String name = read.getNamedTable().getNames(0);
        if (!name.startsWith(INPUT_TABLE_PREFIX)) {
            return null;
        }
        try {
            return Integer.parseInt(name.substring(INPUT_TABLE_PREFIX.length()));
        } catch (NumberFormatException e) {
            return null; // a real table that merely starts with "input-"
        }
    }

    private static Plan decode(Map<Integer, byte[]> stageSubstrait, int stageId) {
        byte[] bytes = stageSubstrait.get(stageId);
        if (bytes == null) {
            throw new IllegalArgumentException("WholePlanStitcher: no Substrait for stage " + stageId);
        }
        try {
            return Plan.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("WholePlanStitcher: stage " + stageId + " has invalid Substrait", e);
        }
    }

    private static PlanRel singleRoot(Plan plan, int stageId) {
        for (PlanRel pr : plan.getRelationsList()) {
            if (pr.hasRoot()) {
                return pr;
            }
        }
        throw new IllegalStateException("WholePlanStitcher: stage " + stageId + " Substrait has no root relation");
    }
}
