/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * One finalized stage from whole-plan lowering (whole-plan-lowering-spec.md D5/D12). The native
 * cut returns one of these per {@code os_stage_boundary} (keyed by {@code boundaryId == stageId})
 * plus one for the un-wrapped coordinator root ({@code boundaryId == -1}).
 *
 * @param boundaryId       the boundary id (== the producing stage's id; {@code -1} for the root)
 * @param childBoundaryIds boundary ids this stage reads from (its inbound stage edges) — the
 *                         {@code StageReadExec} leaves; used for the D6 DAG cross-check
 * @param planBytes        the serialized DataFusion {@code PhysicalPlanNode} for this stage
 * @param outputSchemaIpc  the stage's output schema as Arrow IPC bytes
 *
 * @opensearch.internal
 */
public record WholePlanStageResult(int boundaryId, int[] childBoundaryIds, byte[] planBytes, byte[] outputSchemaIpc) {}
