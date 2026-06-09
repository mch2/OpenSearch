/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Post-CBO rewrite that folds a passthrough {@link OpenSearchProject} (only column references — a
 * strict subset of its scan's columns) into the {@link OpenSearchTableScan} below it, narrowing
 * the scan's row type to exactly the projected columns and dropping the now-redundant Project. The
 * fold sees through an intervening {@link OpenSearchExchangeReducer} ({@code Project → ER → Scan}
 * becomes {@code ER → narrowedScan}); the ER's row type follows its narrowed input.
 *
 * <p>Field trimming ({@code RelFieldTrimmer}, pre-mark) discovers the columns a query uses and
 * materializes them as such a Project on the scan. On its own that only reduces transport when the
 * Project lands in the shard fragment, which depends on the consuming operator: an aggregate splits
 * PARTIAL/FINAL and pushes the exchange above the Project (shard prunes), but a join or plain union
 * gathers each scan input directly and Volcano leaves the narrowing Project on the coordinator side
 * (above the per-input ER), so the shard still ships every column. Folding the selection
 * <em>into the scan</em> removes that dependence on exchange placement — the scan itself declares
 * only the needed columns, so the narrowed schema reaches the shard fragment's Substrait and the
 * native reader prunes the parquet read in every case (join and union included), and even a plain
 * single-table {@code fields} read prunes at the source instead of only on the shard.
 *
 * <p>Runs <em>after</em> CBO (and after the QTF late-materialization rewriter), unlike a pre-CBO
 * fold: Volcano canonicalizes a scan's row type from its {@code RelOptTable} and drops a pre-CBO
 * {@code overrideRowType}, so the narrowing must be applied to the final plan — which is exactly
 * where QTF safely narrows scans via the same mechanism. QTF's own narrowed scans (which carry the
 * {@code ___row_id} helper column) are skipped so the two never fight.
 *
 * <p>A scan whose table is read <em>more than once</em> in the same plan is never folded. DataFusion
 * registers a table provider once per plan keyed by name, so all scans of one index share a single
 * base schema; narrowing one scan's schema would make the other scans' (unfolded) column references
 * invalid. This happens for self-joins, {@code appendcol} (a FULL-OUTER pairing of the pipeline with
 * a subsearch over the same index), and same-index union arms. A pre-fold census counts scans per
 * qualified table name and skips any table seen more than once — those keep the shared full schema
 * and prune via the Project on top, exactly as before this rule existed.
 *
 * @opensearch.internal
 */
public final class OpenSearchProjectIntoScanRewriter {

    private OpenSearchProjectIntoScanRewriter() {}

    public static RelNode rewrite(RelNode root) {
        Map<List<String>, Integer> scanCensus = new HashMap<>();
        censusScans(root, scanCensus);
        RelNode rewritten = rewrite(root, scanCensus);
        // Fold the root itself too — a bare `source=x | fields a` makes the narrowing Project the
        // plan root, which the child-folding recursion never reaches. The narrowed scan keeps the
        // Project's exact row type (names included), so it's a valid drop-in for the result schema.
        if (rewritten instanceof OpenSearchProject rootProject) {
            RelNode folded = tryFold(rootProject, scanCensus);
            if (folded != null) {
                rewritten = folded;
            }
        }
        return rewritten;
    }

    /** Counts OpenSearchTableScans per qualified table name across the whole plan. */
    private static void censusScans(RelNode node, Map<List<String>, Integer> census) {
        if (node instanceof OpenSearchTableScan scan) {
            census.merge(scan.getTable().getQualifiedName(), 1, Integer::sum);
        }
        for (RelNode child : node.getInputs()) {
            censusScans(child, census);
        }
    }

    private static RelNode rewrite(RelNode root, Map<List<String>, Integer> census) {
        List<RelNode> children = root.getInputs();
        if (children.isEmpty()) {
            return root;
        }
        RelNode[] newChildren = new RelNode[children.size()];
        boolean changed = false;
        for (int i = 0; i < children.size(); i++) {
            RelNode child = children.get(i);
            RelNode rewritten = rewrite(child, census);
            if (rewritten instanceof OpenSearchProject project) {
                RelNode folded = tryFold(project, census);
                if (folded != null) {
                    rewritten = folded;
                }
            }
            newChildren[i] = rewritten;
            if (rewritten != child) changed = true;
        }
        return changed ? root.copy(root.getTraitSet(), List.of(newChildren)) : root;
    }

    /**
     * If {@code project} is a foldable strict-subset pure-reference selection over a scan (optionally
     * through one ER), returns the replacement subtree — a narrowed scan, or that scan re-wrapped in
     * the original ER — with the Project dropped. Returns null when not foldable.
     */
    private static RelNode tryFold(OpenSearchProject project, Map<List<String>, Integer> census) {
        RelNode input = project.getInput();
        if (input instanceof OpenSearchTableScan scan) {
            return narrow(project, scan, census);
        }
        if (input instanceof OpenSearchExchangeReducer er && er.getInput() instanceof OpenSearchTableScan scan) {
            OpenSearchTableScan narrowed = narrow(project, scan, census);
            if (narrowed == null) {
                return null;
            }
            return er.copy(er.getTraitSet(), List.of(narrowed));
        }
        return null;
    }

    /** Builds the narrowed scan for a foldable project, or null if the project isn't a pure pruning selection. */
    private static OpenSearchTableScan narrow(OpenSearchProject project, OpenSearchTableScan scan, Map<List<String>, Integer> census) {
        // A table read more than once in this plan shares a single DataFusion provider/schema, so
        // narrowing one of its scans would break the others' column references. Leave it full.
        if (census.getOrDefault(scan.getTable().getQualifiedName(), 0) > 1) {
            return null;
        }
        // Skip QTF's own narrowed scans (they carry the row-id helper) — leave late-materialization alone.
        for (RelDataTypeField f : scan.getRowType().getFieldList()) {
            if (OpenSearchLateMaterialization.ROW_ID_FIELD.equals(f.getName())) {
                return null;
            }
        }
        List<FieldStorageInfo> inputStorage = scan.getOutputFieldStorage();
        List<RexNode> projects = project.getProjects();
        // Only fold when the projection actually prunes columns. A full identity passthrough is a
        // harmless no-op; folding it just churns plan shape with no transport gain.
        if (projects.size() >= inputStorage.size()) {
            return null;
        }
        List<FieldStorageInfo> narrowedStorage = new ArrayList<>(projects.size());
        boolean[] seen = new boolean[inputStorage.size()];
        for (RexNode expr : projects) {
            if (!(expr instanceof RexInputRef ref) || ref.getIndex() >= inputStorage.size()) {
                return null; // computed expression or out-of-range — not a plain column selection
            }
            if (seen[ref.getIndex()]) {
                return null; // duplicated column — a scan row type can't carry the same column twice
            }
            seen[ref.getIndex()] = true;
            narrowedStorage.add(inputStorage.get(ref.getIndex()));
        }
        // The Project's row type is the narrowed (possibly reordered) schema its parent already
        // expects, so reusing it as the scan's overrideRowType is type-safe — replacing the Project
        // (and any ER above it) with this scan leaves parent references valid. rowCount carries over
        // so aggregate-split cost estimates stay intact.
        return new OpenSearchTableScan(
            scan.getCluster(),
            scan.getTraitSet(),
            scan.getTable(),
            scan.getViableBackends(),
            narrowedStorage,
            project.getRowType()
        );
    }
}
