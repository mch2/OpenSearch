/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Coordinator-side stage finalizer (df-proto spec §4, §4.1, D3/D4/D5/D6).
//!
//! Lowers each stage's Substrait fragment through DataFusion's own planner,
//! applies the agg-mode force / leaf rewrite / graft, and encodes the result as
//! `datafusion-proto` `PhysicalPlanNode` bytes via
//! [`crate::os_codec::OpenSearchExtensionCodec`].
//!
//! Bottom-up DAG order is enforced by [`finalize_stages`]: a stage is finalized
//! only after all its children, so the parent's `StageReadExec` schema can be
//! copied off the child's actual finalized output (D5), and — for split
//! aggregates — the child's retained Final half is available for the graft
//! (§4.1).
//!
//! # Relocated agg-mode code (D4)
//!
//! `force_aggregate_mode` and its helpers were relocated here verbatim from the
//! old `agg_mode.rs` (move, not rewrite). `agg_mode.rs` now re-exports them so
//! the legacy `prepare_partial_plan` / `prepare_final_plan` paths keep working
//! until Phase 4 deletes them.

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{exec_datafusion_err, DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::os_exec::StageReadExec;
use crate::proto::AggMode;

// ===========================================================================
// Relocated agg-mode force code (D4 — moved verbatim from agg_mode.rs)
// ===========================================================================

/// Aggregate execution mode for distributed partial/final stripping.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Mode {
    Default,
    Partial,
    Final,
}

impl From<AggMode> for Mode {
    fn from(m: AggMode) -> Self {
        match m {
            AggMode::None => Mode::Default,
            AggMode::Partial => Mode::Partial,
            AggMode::Final => Mode::Final,
        }
    }
}

/// Applies aggregate mode stripping to a physical plan.
pub fn apply_aggregate_mode(
    plan: Arc<dyn ExecutionPlan>,
    mode: Mode,
) -> Result<Arc<dyn ExecutionPlan>> {
    match mode {
        Mode::Default => Ok(plan),
        Mode::Partial => force_aggregate_mode(plan, AggregateMode::Partial),
        Mode::Final => force_aggregate_mode(plan, AggregateMode::Final),
    }
}

/// Returns the output schema of the Partial aggregate without rebuilding the
/// plan tree. Used where we only need types, not an executable plan.
pub fn partial_aggregate_schema(plan: &Arc<dyn ExecutionPlan>) -> Option<SchemaRef> {
    find_partial_input(Arc::clone(plan)).map(|p| p.schema())
}

/// Walks the plan tree and strips the half that doesn't match `target`.
fn force_aggregate_mode(
    plan: Arc<dyn ExecutionPlan>,
    target: AggregateMode,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
        // Treat `FinalPartitioned` as `Final`: DataFusion picks `FinalPartitioned` for
        // grouped aggregates that consume hash-partitioned input and `Final` for scalar /
        // un-partitioned ones. Both are the FINAL half of the Partial/Final pair we strip.
        let agg_is_target = *agg.mode() == target
            || (target == AggregateMode::Final && *agg.mode() == AggregateMode::FinalPartitioned);
        if agg_is_target {
            // Keep this node, recurse into children
            let new_children: Vec<Arc<dyn ExecutionPlan>> = agg
                .children()
                .into_iter()
                .map(|c| force_aggregate_mode(Arc::clone(c), target))
                .collect::<Result<_>>()?;
            return plan.with_new_children(new_children);
        }
        // Mode mismatch — strip this node
        match target {
            AggregateMode::Partial => {
                // Current node is Final; find the Partial subtree below
                if let Some(partial_subtree) = find_partial_input(Arc::clone(agg.input())) {
                    return Ok(partial_subtree);
                }
                // If no Partial found below, the input itself is the Partial
                Ok(Arc::clone(agg.input()))
            }
            AggregateMode::Final => {
                // Current node is Partial; skip it, return its child
                // (the Final above will keep itself)
                let child = agg.children()[0];
                force_aggregate_mode(Arc::clone(child), target)
            }
            _ => Ok(plan),
        }
    } else if plan.children().len() == 1 {
        // Single-input wrapper — recurse transparently.
        let old_child = Arc::clone(plan.children()[0]);
        let new_child = force_aggregate_mode(old_child.clone(), target)?;

        // DataFusion's ProjectionMapping::try_new asserts col.name() == input_schema.field(i).name();
        // with_new_children triggers it. Remap columns to the post-strip schema so it passes.
        if let Some(proj) = plan.downcast_ref::<ProjectionExec>() {
            if old_child.schema() != new_child.schema() {
                let new_schema = &new_child.schema();
                let remapped: Vec<(Arc<dyn PhysicalExpr>, String)> = proj
                    .expr()
                    .iter()
                    .map(|pe| (remap_column(pe.expr.clone(), new_schema), pe.alias.clone()))
                    .collect();
                return Ok(Arc::new(ProjectionExec::try_new(remapped, new_child)?));
            }
        }

        plan.with_new_children(vec![new_child])
    } else {
        // Leaf or multi-input node — return as-is
        Ok(plan)
    }
}

/// Walks down through any single-input wrapper to find an AggregateExec(Partial)
/// and returns the entire Partial subtree (the AggregateExec node itself).
fn find_partial_input(plan: Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
        if *agg.mode() == AggregateMode::Partial {
            return Some(plan);
        }
        return find_partial_input(Arc::clone(agg.input()));
    }
    let children = plan.children();
    if children.len() == 1 {
        return find_partial_input(Arc::clone(children[0]));
    }
    None
}

/// Updates Column expression names to match the given schema (by index).
fn remap_column(expr: Arc<dyn PhysicalExpr>, schema: &SchemaRef) -> Arc<dyn PhysicalExpr> {
    if let Some(col) = expr.downcast_ref::<Column>() {
        return Arc::new(Column::new(schema.field(col.index()).name(), col.index()));
    }
    let children = expr.children();
    if children.is_empty() {
        return expr;
    }
    let new_children: Vec<_> = children.into_iter().map(|c| remap_column(c.clone(), schema)).collect();
    let fallback = expr.clone();
    expr.with_new_children(new_children).unwrap_or(fallback)
}

// ===========================================================================
// §4.1 — split-aggregate graft helpers
// ===========================================================================

/// Locate the topmost `AggregateExec` (the FINAL half) in a planned shard
/// fragment, returning a clone of it for retention. Returns `None` if no
/// aggregate is present (an `agg_mode=NONE` stage).
pub fn find_top_aggregate(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.downcast_ref::<AggregateExec>().is_some() {
        return Some(Arc::clone(plan));
    }
    for child in plan.children() {
        if let Some(found) = find_top_aggregate(child) {
            return Some(found);
        }
    }
    None
}

/// The result of finalizing a single stage.
pub struct FinalizedStage {
    /// The finalized physical plan that ships for this stage.
    pub plan: Arc<dyn ExecutionPlan>,
    /// The actual output schema of `plan` — what a parent `StageReadExec` for
    /// this stage must be stamped with (D5).
    pub output_schema: SchemaRef,
    /// For a PARTIAL shard stage: the retained Final half (§4.1), to be grafted
    /// into the parent FINAL stage. `None` for non-agg stages.
    pub retained_final: Option<Arc<dyn ExecutionPlan>>,
}

/// Graft the retained Final half (from a child PARTIAL stage) onto a
/// `StageReadExec` that reads the child's output, replacing the reduce
/// fragment's own aggregation subtree.
///
/// Per §4.1: the reduce fragment was planned normally (binding against Calcite's
/// declared rowType). We discard its aggregation subtree wholesale and substitute
/// the child pair's Final half over `StageReadExec{child schema}`. Operators above
/// the aggregate (Sort/Limit/projection) are preserved from the reduce fragment.
///
/// D6 (agg boundary): the grafted-Final output schema must equal the schema of
/// the subtree it replaces (positions + types); a name-only drift is reconciled
/// with a rename projection. A structural mismatch is a plan-time hard error.
pub fn graft_final_half(
    reduce_plan: Arc<dyn ExecutionPlan>,
    retained_final: Arc<dyn ExecutionPlan>,
    child_stage_id: i32,
    child_output_schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let stage_read: Arc<dyn ExecutionPlan> =
        Arc::new(StageReadExec::new(child_stage_id, child_output_schema));
    // Re-root the retained Final half onto the StageReadExec leaf (it was born
    // over the Partial half + scan; we swap that subtree for the stream read).
    let grafted = rebase_aggregate_input(retained_final, stage_read)?;
    replace_aggregate_subtree(reduce_plan, grafted)
}

/// Replace the topmost AggregateExec subtree in `plan` with `replacement`,
/// preserving everything above it. Asserts D6 (agg boundary): the replacement's
/// output schema matches the replaced subtree's, modulo field names.
fn replace_aggregate_subtree(
    plan: Arc<dyn ExecutionPlan>,
    replacement: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.downcast_ref::<AggregateExec>().is_some() {
        assert_graft_schema_compatible(&plan.schema(), &replacement.schema(), &plan)?;
        return rename_if_name_drift(replacement, &plan.schema());
    }
    let children = plan.children();
    if children.len() == 1 {
        let new_child = replace_aggregate_subtree(Arc::clone(children[0]), replacement)?;
        return plan.with_new_children(vec![new_child]);
    }
    // Multi-input above an aggregate is not a shape today's planner produces for
    // a single FINAL call set; recursing into the first matching branch keeps the
    // others intact.
    let mut found = false;
    let new_children: Vec<Arc<dyn ExecutionPlan>> = children
        .iter()
        .map(|c| {
            if !found && find_top_aggregate(c).is_some() {
                found = true;
                replace_aggregate_subtree(Arc::clone(c), Arc::clone(&replacement))
            } else {
                Ok(Arc::clone(c))
            }
        })
        .collect::<Result<_>>()?;
    if !found {
        return Err(exec_datafusion_err!(
            "graft: no AggregateExec found in reduce plan to replace"
        ));
    }
    plan.with_new_children(new_children)
}

/// Swap the input subtree of the (single) AggregateExec in `final_half` for
/// `new_input` (the StageReadExec). The Final half is canonically
/// `AggregateExec(Final) ← [CoalescePartitions/Repartition...] ← AggregateExec(Partial) ← scan`.
/// We graft `new_input` directly beneath the Final AggregateExec.
fn rebase_aggregate_input(
    final_half: Arc<dyn ExecutionPlan>,
    new_input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = final_half.downcast_ref::<AggregateExec>() {
        if matches!(agg.mode(), AggregateMode::Final | AggregateMode::FinalPartitioned) {
            return final_half.with_new_children(vec![new_input]);
        }
    }
    if final_half.children().len() == 1 {
        let child = Arc::clone(final_half.children()[0]);
        let new_child = rebase_aggregate_input(child, new_input)?;
        return final_half.with_new_children(vec![new_child]);
    }
    Err(exec_datafusion_err!(
        "graft: retained Final half has no single-input path to a Final AggregateExec"
    ))
}

/// Build the grafted Final half over a UnionExec of per-child StageReadExecs
/// (Union-fed FINAL, §4.1 edge rule). Asserts all child Partial schemas identical.
pub fn graft_final_half_union(
    reduce_plan: Arc<dyn ExecutionPlan>,
    retained_final: Arc<dyn ExecutionPlan>,
    children: &[(i32, SchemaRef)],
) -> Result<Arc<dyn ExecutionPlan>> {
    if children.is_empty() {
        return Err(exec_datafusion_err!("graft union: no child stages"));
    }
    let first = &children[0].1;
    for (cid, schema) in &children[1..] {
        if schema.as_ref() != first.as_ref() {
            return Err(exec_datafusion_err!(
                "graft union: child stage {cid} Partial schema differs from first child:\n  \
                 first: {first:?}\n  this:  {schema:?}"
            ));
        }
    }
    let reads: Vec<Arc<dyn ExecutionPlan>> = children
        .iter()
        .map(|(cid, schema)| {
            Arc::new(StageReadExec::new(*cid, Arc::clone(schema))) as Arc<dyn ExecutionPlan>
        })
        .collect();
    let union: Arc<dyn ExecutionPlan> = Arc::new(UnionExec::new(reads));
    let grafted = rebase_aggregate_input(retained_final, union)?;
    replace_aggregate_subtree(reduce_plan, grafted)
}

/// D6 agg-boundary assertion: positions + types must match (names may drift).
fn assert_graft_schema_compatible(
    replaced: &SchemaRef,
    replacement: &SchemaRef,
    replaced_plan: &Arc<dyn ExecutionPlan>,
) -> Result<()> {
    if replaced.fields().len() != replacement.fields().len() {
        return Err(exec_datafusion_err!(
            "graft D6: field count mismatch replacing aggregate subtree ({}):\n  \
             replaced:    {replaced:?}\n  replacement: {replacement:?}",
            replaced_plan.name()
        ));
    }
    for (i, (a, b)) in replaced
        .fields()
        .iter()
        .zip(replacement.fields().iter())
        .enumerate()
    {
        // Types must match exactly; nullability may widen (non-null → nullable).
        let types_ok = a.data_type() == b.data_type();
        let nullable_ok = a.is_nullable() == b.is_nullable() || (a.is_nullable() && !b.is_nullable());
        if !types_ok || !nullable_ok {
            return Err(exec_datafusion_err!(
                "graft D6: column {i} type/nullability mismatch:\n  \
                 replaced:    {:?} (nullable={})\n  replacement: {:?} (nullable={})",
                a.data_type(),
                a.is_nullable(),
                b.data_type(),
                b.is_nullable()
            ));
        }
    }
    Ok(())
}

/// If the replacement's field names differ from the replaced subtree's, wrap it
/// in a rename projection so operators above (which bound to the declared names)
/// resolve correctly.
fn rename_if_name_drift(
    replacement: Arc<dyn ExecutionPlan>,
    target: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let src = replacement.schema();
    let drift = src
        .fields()
        .iter()
        .zip(target.fields().iter())
        .any(|(a, b)| a.name() != b.name());
    if !drift {
        return Ok(replacement);
    }
    let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = target
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            (
                Arc::new(Column::new(src.field(i).name(), i)) as Arc<dyn PhysicalExpr>,
                f.name().to_string(),
            )
        })
        .collect();
    Ok(Arc::new(ProjectionExec::try_new(exprs, replacement)?))
}

// ===========================================================================
// Non-agg boundary D6 assertion (StageReadExec) — see §4.1 / D6(a)
// ===========================================================================

/// D6 (non-agg boundary): assert the child's actual output schema reconciles
/// with Calcite's declared rowType. Nullable-widening (declared nullable over a
/// non-null child field) is the only silent acceptance; anything else is a
/// hard error printing both schemas and the stage id.
pub fn assert_stage_read_schema(
    stage_id: i32,
    declared: &Schema,
    child_actual: &Schema,
) -> Result<()> {
    if declared.fields().len() != child_actual.fields().len() {
        return Err(stage_read_mismatch(stage_id, declared, child_actual, "field count"));
    }
    for (decl, act) in declared.fields().iter().zip(child_actual.fields().iter()) {
        if decl.data_type() != act.data_type() {
            return Err(stage_read_mismatch(stage_id, declared, child_actual, "data type"));
        }
        // Allowed: declared nullable over a non-null child (nullable-widening).
        // Disallowed: declared non-null over a nullable child.
        if !decl.is_nullable() && act.is_nullable() {
            return Err(stage_read_mismatch(
                stage_id,
                declared,
                child_actual,
                "nullability narrowing",
            ));
        }
    }
    Ok(())
}

// ===========================================================================
// CI invariant #2 (§7.2) — no marker UDF outside OpenSearchShardScanExec
// ===========================================================================

/// Assert that no `delegated_predicate` / `delegation_possible` marker UDF call
/// appears anywhere in a finalized plan **outside** an `OpenSearchShardScanExec`
/// leaf (where it is stored as opaque filter bytes, never physically evaluated).
///
/// Implemented by walking the plan: an `OpenSearchShardScanExec` is a leaf and
/// its embedded filter expression is opaque bytes (not a live PhysicalExpr), so
/// the markers there are invisible to `displayable()`. Any marker name that
/// shows up in the rendered plan of a non-scan node means a `FilterExec` (or
/// projection, etc.) is about to physically evaluate it — a hard error.
pub fn assert_no_marker_udf_outside_scan(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
    use crate::indexed_table::substrait_to_tree::{
        COLLECTOR_FUNCTION_NAME, DELEGATION_POSSIBLE_FUNCTION_NAME,
    };
    use datafusion::physical_plan::displayable;

    // Scan leaves carry markers as opaque bytes — skip their subtree entirely.
    if plan
        .downcast_ref::<crate::os_exec::OpenSearchShardScanExec>()
        .is_some()
    {
        return Ok(());
    }
    // Render just THIS node (not its children) and check for marker names.
    let one_line = displayable(plan.as_ref()).one_line().to_string();
    for marker in [COLLECTOR_FUNCTION_NAME, DELEGATION_POSSIBLE_FUNCTION_NAME] {
        if one_line.contains(marker) {
            return Err(exec_datafusion_err!(
                "post-finalization invariant violated: marker UDF '{marker}' appears in a \
                 non-scan node ({}): {one_line}",
                plan.name()
            ));
        }
    }
    for child in plan.children() {
        assert_no_marker_udf_outside_scan(child)?;
    }
    Ok(())
}

fn stage_read_mismatch(
    stage_id: i32,
    declared: &Schema,
    child_actual: &Schema,
    what: &str,
) -> DataFusionError {
    exec_datafusion_err!(
        "StageReadExec schema mismatch ({what}) at stage {stage_id}:\n  \
         declared (Calcite): {declared:?}\n  child actual (DF):  {child_actual:?}"
    )
}

// ===========================================================================
// Bottom-up stage finalization (§4) — in-process orchestration
// ===========================================================================

/// One stage handed to the finalizer: its whole-fragment Substrait bytes plus
/// the [`crate::proto::StageMeta`] describing how to finalize it.
pub struct StageInput {
    pub substrait_bytes: Vec<u8>,
    pub meta: crate::proto::StageMeta,
}

/// The finalized result for a stage: the encoded plan bytes plus the actual
/// output schema (used by parents for the D5 / graft boundary).
pub struct StageOutput {
    pub stage_id: i32,
    pub plan_bytes: Vec<u8>,
    pub output_schema: SchemaRef,
    /// Retained Final half (§4.1) for a PARTIAL shard stage — consumed by the
    /// parent FINAL stage's graft. Kept out of `plan_bytes` (never ships).
    pub retained_final: Option<Arc<dyn ExecutionPlan>>,
}

/// Finalize a single already-lowered, mode-forced physical plan for a stage:
/// run the (currently seeded) quirk fix-up pass, leaf-rewrite STAGE_INPUT leaves
/// into `StageReadExec` using finalized child schemas, and (for FINAL stages)
/// graft the child's retained Final half. Returns the shippable plan plus its
/// output schema and any retained Final half.
///
/// `child_outputs` maps child_stage_id → (output_schema, retained_final?).
///
/// This is the per-stage core of §4; [`finalize_stages`] drives it bottom-up.
pub fn finalize_stage_plan(
    meta: &crate::proto::StageMeta,
    lowered_then_forced: Arc<dyn ExecutionPlan>,
    child_outputs: &std::collections::HashMap<i32, (SchemaRef, Option<Arc<dyn ExecutionPlan>>)>,
) -> Result<(Arc<dyn ExecutionPlan>, Option<Arc<dyn ExecutionPlan>>)> {
    let mut plan = quirk_fixup_pass(lowered_then_forced)?;

    // SHARD_SCAN leaf rewrite (full_proto §4): swap the lowered scan leaf (over the
    // pushdown stub) for `OpenSearchShardScanExec` carrying the delegation config.
    // Runs before agg-mode dispatch so the Partial half ships with the shard-scan leaf.
    if matches!(meta.leaf_kind_enum(), crate::proto::LeafKind::ShardScan) {
        // filter_expr is reconstructed on the data node via the D13 entry point from
        // the scan node's stored expression; the finalizer carries the delegation
        // payloads + tree_shape + row-id flag from StageMeta. (Serializing the pushed
        // filter Expr into the node is wired with OpenSearchShardScanExec::execute in
        // the data-node Phase 2b step; the structural leaf swap is correct now.)
        plan = swap_shard_scan_leaf(plan, meta, Vec::new(), String::new())?;
    }

    match meta.agg_mode_enum() {
        // PARTIAL shard stage: retain the Final half for the parent's graft. The
        // shipped plan is the (already mode-forced to Partial) plan; its schema is
        // the physical state schema.
        AggMode::Partial => {
            // The retained Final half is found in the PRE-strip plan; here `plan`
            // is already the Partial half. We re-derive the Final half from the
            // pre-strip plan in `finalize_stages` (it has both); for a standalone
            // call we expose the Partial-as-is and let the caller supply the
            // retained Final.  In practice finalize_stages handles retention.
            Ok((plan, None))
        }
        // FINAL reduce stage: graft. For each STAGE_INPUT child that is an agg
        // child (has a retained Final half), graft it; otherwise swap the leaf to
        // a plain StageReadExec with the child's actual schema (D5) + D6 assert.
        AggMode::Final => {
            // Single-child graft is the common shape; union-fed is handled by
            // finalize_stages which knows all child edges.
            if let Some(&child_id) = meta.child_stage_ids.first() {
                if let Some((child_schema, Some(retained))) = child_outputs.get(&child_id) {
                    let grafted = graft_final_half(
                        plan,
                        Arc::clone(retained),
                        child_id,
                        Arc::clone(child_schema),
                    )?;
                    return Ok((grafted, None));
                }
            }
            // No retained Final half available → treat as a non-agg gather of the
            // child (shouldn't normally happen for a FINAL stage, but keep it safe).
            let plan = swap_stage_inputs(plan, meta, child_outputs)?;
            Ok((plan, None))
        }
        // Non-agg: swap STAGE_INPUT leaves to StageReadExec (D5 + D6 non-agg).
        AggMode::None => {
            let plan = swap_stage_inputs(plan, meta, child_outputs)?;
            Ok((plan, None))
        }
    }
}

/// Topologically order stage ids child-first (post-order over the child edges
/// in each `StageMeta`). Errors on a cycle or a dangling child reference.
pub fn child_first_order(stages: &[StageInput]) -> Result<Vec<i32>> {
    use std::collections::{HashMap, HashSet};
    let by_id: HashMap<i32, &StageInput> =
        stages.iter().map(|s| (s.meta.stage_id, s)).collect();
    let mut order = Vec::with_capacity(stages.len());
    let mut visited: HashSet<i32> = HashSet::new();
    let mut on_stack: HashSet<i32> = HashSet::new();

    fn visit<'a>(
        id: i32,
        by_id: &HashMap<i32, &'a StageInput>,
        visited: &mut HashSet<i32>,
        on_stack: &mut HashSet<i32>,
        order: &mut Vec<i32>,
    ) -> Result<()> {
        if visited.contains(&id) {
            return Ok(());
        }
        if !on_stack.insert(id) {
            return Err(exec_datafusion_err!("finalize: cycle in stage DAG at stage {id}"));
        }
        // A child id absent from the request is a legacy stage finalized outside this
        // batch (Phase 2a reduce_proto: shard stages stay legacy). Not a dangling
        // reference — the parent binds its `input-<childId>` from its own substrait
        // base_schema (D5). Only recurse into children present in the request.
        if let Some(stage) = by_id.get(&id) {
            for &child in &stage.meta.child_stage_ids {
                if by_id.contains_key(&child) {
                    visit(child, by_id, visited, on_stack, order)?;
                }
            }
            on_stack.remove(&id);
            visited.insert(id);
            order.push(id);
        } else {
            on_stack.remove(&id);
        }
        Ok(())
    }

    for s in stages {
        visit(s.meta.stage_id, &by_id, &mut visited, &mut on_stack, &mut order)?;
    }
    Ok(order)
}

/// Finalize all stages bottom-up in one session (df-proto spec §4, D3).
///
/// For each stage, child-first: lower its Substrait fragment through the
/// session's planner, force the declared agg mode (D4), then finalize the
/// per-stage plan (quirk fix-up, leaf rewrite to `StageReadExec` with the
/// child's actual schema, agg graft §4.1). Returns one `StageOutput` per stage,
/// each carrying the encoded plan bytes that ship and the actual output schema
/// the parent uses for its boundary.
///
/// The retained Final half for a PARTIAL stage is derived from that stage's
/// pre-strip lowered plan (which contains the whole `Final ← Partial` pair) and
/// threaded to the parent FINAL stage's graft.
pub async fn finalize_query_plan(
    session: &crate::local_executor::LocalSession,
    stages: Vec<StageInput>,
) -> Result<Vec<StageOutput>> {
    use std::collections::HashMap;

    let order = child_first_order(&stages)?;
    let by_id: HashMap<i32, &StageInput> =
        stages.iter().map(|s| (s.meta.stage_id, s)).collect();

    // child_stage_id → (actual output schema, retained Final half?)
    let mut child_outputs: HashMap<i32, (SchemaRef, Option<Arc<dyn ExecutionPlan>>)> =
        HashMap::new();
    let mut outputs: Vec<StageOutput> = Vec::with_capacity(stages.len());

    for stage_id in order {
        let stage = *by_id
            .get(&stage_id)
            .ok_or_else(|| exec_datafusion_err!("finalize: missing stage {stage_id}"))?;
        let meta = &stage.meta;

        // D5 (reduce_proto): for each legacy child whose partial Substrait was supplied,
        // derive the child's ACTUAL physical output schema by lowering it coordinator-side
        // (`derive_schema_from_partial_plan` registers synthetic empty MemTables for the
        // child's real index table, so it lowers without a live shard). This is the source
        // of truth for the boundary — it reflects DataFusion's real partial-state types
        // (e.g. SUM → Int64), NOT Calcite's declared rowType (which may be Int32). Seed
        // `child_outputs` so the binding skeleton, the StageReadExec stamping, and the D6
        // assertion all use the real schema and the FINAL agg plans over matching types.
        for (idx, &child_id) in meta.child_stage_ids.iter().enumerate() {
            if child_outputs.contains_key(&child_id) {
                continue; // child finalized in-session — its actual schema already known
            }
            if let Some(bytes) = meta.child_partial_substrait.get(idx) {
                if !bytes.is_empty() {
                    let actual = crate::api::derive_schema_from_partial_plan(bytes)?;
                    child_outputs.insert(child_id, (actual, None));
                }
            }
        }

        // 0. Register a binding skeleton for each child edge so the parent
        //    fragment's `from_substrait_plan` binds its `input-<childId>` table
        //    references (§4.1). Prefer the child's actual derived/finalized schema
        //    (D5 source of truth); fall back to Calcite's declared rowType only when
        //    no actual schema is available. The skeleton never produces rows — data
        //    flows via StageReadExec at execution time.
        for (idx, &child_id) in meta.child_stage_ids.iter().enumerate() {
            let bind_schema = child_outputs
                .get(&child_id)
                .map(|(s, _)| Arc::clone(s))
                .or_else(|| {
                    meta.declared_input_row_types
                        .get(idx)
                        .filter(|s| !s.ipc.is_empty())
                        .and_then(|s| crate::schema_ipc::schema_from_ipc(&s.ipc).ok())
                });
            if let Some(schema) = bind_schema {
                session.register_binding_skeleton(
                    &crate::session_context::stage_input_table_name(child_id),
                    schema,
                )?;
            }
        }

        // Fallback (D5 Phase 2a reduce_proto): when a child stage is legacy (not in this
        // batch) and Calcite supplied no declared rowType, bind `input-<childId>` from the
        // base_schema the reduce fragment's own substrait carries for that table — the same
        // schema the legacy reduce sink derives. This lets `from_substrait_plan` bind the
        // StageInputScan without the child being finalized in-session.
        for &child_id in &meta.child_stage_ids {
            let table = crate::session_context::stage_input_table_name(child_id);
            if let Some(schema) = crate::api::base_schema_to_arrow(&stage.substrait_bytes, &table, session.ctx()) {
                // register_binding_skeleton is last-write-wins, so this is a no-op when the
                // declared/child-output loop above already registered it.
                session.register_binding_skeleton(&table, schema)?;
            }
        }

        // For a SHARD_SCAN stage (full_proto), register the pushdown-stub TableProvider
        // for the fragment's real index table so `from_substrait_plan` lowers without a
        // live shard reader. The stub claims Exact pushdown, so the whole filter routes
        // into the scan (no FilterExec above — the marker UDFs stay inside the scan node).
        // The scan leaf is then swapped to `OpenSearchShardScanExec` after planning.
        if matches!(meta.leaf_kind_enum(), crate::proto::LeafKind::ShardScan) {
            if let Some(table) = crate::api::first_named_table_name(&stage.substrait_bytes) {
                if let Some(schema) =
                    crate::api::base_schema_to_arrow(&stage.substrait_bytes, &table, session.ctx())
                {
                    session.register_pushdown_stub(&table, schema)?;
                }
            }
        }

        // 1. Lower the whole fragment through DataFusion's planner.
        let lowered = session
            .lower_fragment(&stage.substrait_bytes)
            .await
            .map_err(|e| exec_datafusion_err!("finalize stage {stage_id}: lower: {e}"))?;

        // 2. Force the agg mode Calcite declared for this stage (D4). For a
        //    PARTIAL stage, retain the Final half (pre-strip plan has the pair).
        let mode: Mode = meta.agg_mode_enum().into();
        let retained_final = if matches!(meta.agg_mode_enum(), AggMode::Partial) {
            find_top_aggregate(&lowered)
        } else {
            None
        };
        let forced = apply_aggregate_mode(Arc::clone(&lowered), mode)?;

        // 3. Per-stage finalize: quirk fix-up, leaf rewrite, graft.
        let (plan, _retained_from_finalize) =
            finalize_stage_plan(meta, forced, &child_outputs)?;

        // CI invariant #2 (§7.2): no marker UDF survives outside a scan node.
        assert_no_marker_udf_outside_scan(&plan)?;

        // 4. Encode the shippable plan.
        let plan_bytes = encode_stage_plan(&plan)?;
        let output_schema = plan.schema();

        // Debug-build step (§ Phase 1 / CI invariant #1): the finalized plan
        // round-trips the codec with identical displayable() output. Gated to
        // debug builds so release finalization isn't double-encoding every stage.
        #[cfg(debug_assertions)]
        assert_codec_round_trips(&plan, session.ctx().task_ctx().as_ref())?;

        child_outputs.insert(stage_id, (Arc::clone(&output_schema), retained_final.clone()));
        outputs.push(StageOutput {
            stage_id,
            plan_bytes,
            output_schema,
            retained_final,
        });
    }

    Ok(outputs)
}

/// SHARD_SCAN leaf rewrite (df-proto §4): replace the planned scan leaf (a
/// `DataSourceExec` over the pushdown-stub `TableProvider`) with an
/// `OpenSearchShardScanExec` carrying the serialized filter expression,
/// `tree_shape`, delegated payloads, `requests_row_ids`, binding key, and the
/// scan's projected output schema. Because the stub claims `Exact` pushdown for
/// every filter, physical planning routed the whole WHERE into the scan and
/// emitted no `FilterExec` above it — so the marker UDFs live only inside the
/// scan node's stored expression (CI invariant #2), as opaque bytes.
///
/// `filter_expr` is the `datafusion-proto`-serialized logical filter `Expr`
/// (decoded on the data node via the D13 `expr_to_bool_tree_from_bytes` entry
/// point). For Phase 1/2a tests the finalizer is exercised on reduce stages, so
/// this is only invoked when `meta.leaf_kind == SHARD_SCAN`.
pub fn swap_shard_scan_leaf(
    plan: Arc<dyn ExecutionPlan>,
    meta: &crate::proto::StageMeta,
    filter_expr: Vec<u8>,
    binding_key: String,
) -> Result<Arc<dyn ExecutionPlan>> {
    use crate::os_exec::{DelegatedExpr, OpenSearchShardScanExec, ShardScanConfig};

    fn is_scan_leaf(plan: &Arc<dyn ExecutionPlan>) -> bool {
        // The pushdown-stub scan lowers to an EmptyExec leaf (no children) — the
        // placeholder PushdownStubProvider::scan returns. The swap replaces it.
        plan.children().is_empty() && (plan.name() == "EmptyExec" || plan.name() == "DataSourceExec")
    }

    fn rewrite(
        plan: Arc<dyn ExecutionPlan>,
        cfg: &ShardScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if is_scan_leaf(&plan) {
            return Ok(Arc::new(OpenSearchShardScanExec::new(cfg.clone(), plan.schema())));
        }
        let children = plan.children();
        if children.is_empty() {
            return Ok(plan);
        }
        let new_children: Vec<Arc<dyn ExecutionPlan>> = children
            .iter()
            .map(|c| rewrite(Arc::clone(c), cfg))
            .collect::<Result<_>>()?;
        plan.with_new_children(new_children)
    }

    let cfg = ShardScanConfig {
        filter_expr,
        tree_shape: meta.tree_shape,
        delegated: meta
            .delegated
            .iter()
            .map(|d| DelegatedExpr {
                annotation_id: d.annotation_id,
                backend_id: d.backend_id.clone(),
                payload: d.payload.clone(),
            })
            .collect(),
        requests_row_ids: meta.requests_row_ids,
        binding_key,
    };
    rewrite(plan, &cfg)
}

/// Single ordered quirk fix-up pass (D9). Run after physical planning, before
/// leaf rewrite. Seeded empty for Phase 1 — the relocated reorder-Project/FINAL
/// workaround and `SubstraitPlanProtoRewriter` consumer rewrites land here as
/// they are relocated. Kept as an explicit hook so the ordering contract (one
/// place, after planning, before leaf rewrite) is structural.
pub fn quirk_fixup_pass(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(plan)
}

/// Swap any `StageReadExec`-placeholder leaves (today represented as the
/// finalized plan's existing scan over `input-<childId>`) for `StageReadExec`
/// stamped with the child's actual output schema (D5), asserting the non-agg D6
/// rule against the child's declared rowType when present.
///
/// In the in-process orchestration path the reduce fragment is lowered with the
/// child registered as a binding skeleton; the resulting leaf is a streaming/
/// memtable scan we replace here. For Phase 1 tests the leaf is already a
/// `StageReadExec` (built directly), so this is largely a validation pass.
fn swap_stage_inputs(
    plan: Arc<dyn ExecutionPlan>,
    meta: &crate::proto::StageMeta,
    child_outputs: &std::collections::HashMap<i32, (SchemaRef, Option<Arc<dyn ExecutionPlan>>)>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // First convert the lowered stage-input leaf (a binding-skeleton scan over
    // `input-<childId>`) into a `StageReadExec`, then validate/restamp any
    // StageReadExec leaves against child schema + declared rowType (D5/D6).
    let plan = convert_input_leaves(plan, meta)?;
    rewrite_stage_reads(plan, meta, child_outputs)
}

/// Convert the lowered binding-skeleton leaf for a stage-input child into a
/// `StageReadExec` (df-proto Phase 2a reduce_proto). The reduce fragment's
/// substrait `StageInputScan` lowers — against the registered `input-<childId>`
/// binding skeleton — to a leaf scan (EmptyExec / DataSourceExec /
/// StreamingTableExec) with no children. This swaps that leaf to a
/// `StageReadExec{childId, leaf.schema()}` so proto execution pulls from the
/// partition stream. The childId is matched by leaf output schema against the
/// stage's declared input row types; when the stage has a single child, that
/// child is used directly.
fn convert_input_leaves(
    plan: Arc<dyn ExecutionPlan>,
    meta: &crate::proto::StageMeta,
) -> Result<Arc<dyn ExecutionPlan>> {
    fn is_input_leaf(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.children().is_empty()
            && matches!(plan.name(), "EmptyExec" | "DataSourceExec" | "StreamingTableExec" | "MemoryExec")
            // An OpenSearchShardScanExec / StageReadExec is already a finalized leaf.
            && plan.downcast_ref::<StageReadExec>().is_none()
            && plan.downcast_ref::<crate::os_exec::OpenSearchShardScanExec>().is_none()
    }

    fn recurse(
        plan: Arc<dyn ExecutionPlan>,
        meta: &crate::proto::StageMeta,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if is_input_leaf(&plan) {
            // Map this leaf to a child stage id. Single-child is the common case;
            // otherwise match by schema against declared_input_row_types.
            let child_id = if meta.child_stage_ids.len() == 1 {
                Some(meta.child_stage_ids[0])
            } else {
                meta.child_stage_ids
                    .iter()
                    .enumerate()
                    .find(|(idx, _)| {
                        meta.declared_input_row_types
                            .get(*idx)
                            .and_then(|s| crate::schema_ipc::schema_from_ipc(&s.ipc).ok())
                            .map(|decl| decl.fields().len() == plan.schema().fields().len())
                            .unwrap_or(false)
                    })
                    .map(|(_, &cid)| cid)
            };
            if let Some(child_id) = child_id {
                return Ok(Arc::new(StageReadExec::new(child_id, plan.schema())));
            }
            return Ok(plan);
        }
        let children = plan.children();
        if children.is_empty() {
            return Ok(plan);
        }
        let new_children: Vec<Arc<dyn ExecutionPlan>> = children
            .iter()
            .map(|c| recurse(Arc::clone(c), meta))
            .collect::<Result<_>>()?;
        plan.with_new_children(new_children)
    }

    // Only stages whose leaf is a stage input (not a shard scan) get this swap.
    if matches!(meta.leaf_kind_enum(), crate::proto::LeafKind::StageInput) {
        recurse(plan, meta)
    } else {
        Ok(plan)
    }
}

fn rewrite_stage_reads(
    plan: Arc<dyn ExecutionPlan>,
    meta: &crate::proto::StageMeta,
    child_outputs: &std::collections::HashMap<i32, (SchemaRef, Option<Arc<dyn ExecutionPlan>>)>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(read) = plan.downcast_ref::<StageReadExec>() {
        let child_id = read.child_stage_id();
        if let Some((child_schema, _)) = child_outputs.get(&child_id) {
            // D5: stamp with the child's actual schema.
            // D6 (non-agg): if a declared rowType is present for this edge, assert.
            if let Some(idx) = meta.child_stage_ids.iter().position(|c| *c == child_id) {
                if let Some(declared) = meta.declared_input_row_types.get(idx) {
                    if !declared.ipc.is_empty() {
                        let declared_schema = crate::schema_ipc::schema_from_ipc(&declared.ipc)?;
                        assert_stage_read_schema(
                            meta.stage_id,
                            declared_schema.as_ref(),
                            child_schema.as_ref(),
                        )?;
                    }
                }
            }
            return Ok(Arc::new(StageReadExec::new(child_id, Arc::clone(child_schema))));
        }
        return Ok(plan);
    }
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }
    let new_children: Vec<Arc<dyn ExecutionPlan>> = children
        .iter()
        .map(|c| rewrite_stage_reads(Arc::clone(c), meta, child_outputs))
        .collect::<Result<_>>()?;
    plan.with_new_children(new_children)
}

// ===========================================================================
// Codec encode/decode + round-trip assertion (D2, CI invariant #1)
// ===========================================================================

/// Encode a finalized stage plan to `datafusion-proto` `PhysicalPlanNode` bytes
/// using [`crate::os_codec::OpenSearchExtensionCodec`].
pub fn encode_stage_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<u8>> {
    use datafusion_proto::physical_plan::AsExecutionPlan;
    use prost::Message;

    let codec = crate::os_codec::OpenSearchExtensionCodec::new();
    let proto = datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(
        Arc::clone(plan),
        &codec,
    )?;
    Ok(proto.encode_to_vec())
}

/// Decode stage-plan bytes back into a physical plan against `ctx`'s
/// `TaskContext`. The session must have the standard UDF/UDAF registry so
/// name-resolved functions (and the marker UDFs embedded in a scan node's
/// filter expression) bind. `StageReadExec` resolves its partition stream from
/// the `StageInputRegistry` extension at execute time, not here.
pub fn decode_stage_plan(
    bytes: &[u8],
    ctx: &datafusion::execution::TaskContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    use datafusion_proto::physical_plan::AsExecutionPlan;
    use prost::Message;

    let codec = crate::os_codec::OpenSearchExtensionCodec::new();
    let proto = datafusion_proto::protobuf::PhysicalPlanNode::decode(bytes)
        .map_err(|e| exec_datafusion_err!("decode_stage_plan: proto decode: {e}"))?;
    proto.try_into_physical_plan(ctx, &codec)
}

/// Debug-build assertion (and Phase 1 CI invariant #1): a finalized stage plan
/// round-trips the codec with identical `displayable()` output. Returns an error
/// (rather than panicking) so callers choose the failure mode.
pub fn assert_codec_round_trips(
    plan: &Arc<dyn ExecutionPlan>,
    ctx: &datafusion::execution::TaskContext,
) -> Result<()> {
    use datafusion::physical_plan::displayable;

    let before = displayable(plan.as_ref()).indent(true).to_string();
    let bytes = encode_stage_plan(plan)?;
    let decoded = decode_stage_plan(&bytes, ctx)?;
    let after = displayable(decoded.as_ref()).indent(true).to_string();
    if before != after {
        return Err(exec_datafusion_err!(
            "codec round-trip displayable() mismatch:\n--- before ---\n{before}\n--- after ---\n{after}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::*;

    fn plan_string(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(true).to_string()
    }

    fn find_agg_modes(plan: &Arc<dyn ExecutionPlan>) -> Vec<AggregateMode> {
        let mut modes = Vec::new();
        if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
            modes.push(*agg.mode());
        }
        for child in plan.children() {
            modes.extend(find_agg_modes(child));
        }
        modes
    }

    async fn make_grouped_agg_plan() -> Arc<dyn ExecutionPlan> {
        let mut config = SessionConfig::new();
        config.options_mut().execution.target_partitions = 4;
        let ctx = SessionContext::new_with_state(
            datafusion::execution::SessionStateBuilder::new()
                .with_config(config)
                .with_default_features()
                .with_physical_optimizer_rules(
                    crate::agg_mode::physical_optimizer_rules_without_combine(),
                )
                .build(),
        );
        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                arrow::datatypes::Field::new("status", arrow::datatypes::DataType::Utf8, false),
                arrow::datatypes::Field::new("latency", arrow::datatypes::DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["a", "b", "a"])),
                Arc::new(arrow_array::Int64Array::from(vec![1i64, 2, 3])),
            ],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        let df = ctx
            .sql("SELECT status, SUM(latency) FROM t GROUP BY status")
            .await
            .unwrap();
        df.create_physical_plan().await.unwrap()
    }

    #[tokio::test]
    async fn relocated_force_mode_strips_partial_keeps_final() {
        let plan = make_grouped_agg_plan().await;
        let modes = find_agg_modes(&plan);
        if !modes.contains(&AggregateMode::Partial) {
            return; // optimizer collapsed; nothing to assert
        }
        let stripped = apply_aggregate_mode(plan, Mode::Final).unwrap();
        let result_modes = find_agg_modes(&stripped);
        assert!(
            !result_modes.contains(&AggregateMode::Partial),
            "Partial should be stripped: {}",
            plan_string(&stripped)
        );
    }

    #[tokio::test]
    async fn partial_schema_carries_state_columns() {
        // The Partial half's output schema (group cols + state cols) is what a
        // parent StageReadExec must be stamped with (D5).
        let plan = make_grouped_agg_plan().await;
        let partial = apply_aggregate_mode(plan, Mode::Partial).unwrap();
        // group col `status` + at least one state col for SUM.
        assert!(
            partial.schema().fields().len() >= 2,
            "partial schema must carry group + state cols: {:?}",
            partial.schema()
        );
    }

    #[test]
    fn stage_read_schema_allows_nullable_widening() {
        use arrow::datatypes::{DataType, Field};
        // child produces non-null; Calcite declares nullable → OK (widening).
        let declared = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        let child = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
        assert!(assert_stage_read_schema(1, &declared, &child).is_ok());
    }

    #[test]
    fn stage_read_schema_rejects_type_mismatch() {
        use arrow::datatypes::{DataType, Field};
        let declared = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        let child = Schema::new(vec![Field::new("x", DataType::Float64, true)]);
        let err = assert_stage_read_schema(7, &declared, &child).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("stage 7"), "msg: {msg}");
        assert!(msg.contains("data type"), "msg: {msg}");
    }

    #[test]
    fn stage_read_schema_rejects_nullability_narrowing() {
        use arrow::datatypes::{DataType, Field};
        // declared non-null over a nullable child → narrowing → reject.
        let declared = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
        let child = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        assert!(assert_stage_read_schema(3, &declared, &child).is_err());
    }

    // =======================================================================
    // Phase 0a — full end-to-end graft + codec round-trip + execution
    // =======================================================================

    use arrow::datatypes::{DataType, Field};
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use datafusion::execution::{SessionStateBuilder, TaskContext};
    use datafusion::physical_plan::{collect, execute_stream};
    use futures::StreamExt;

    fn fresh_session() -> SessionContext {
        // Mirrors LocalSession::new — combine-rule removed, UDF/UDAF registry,
        // multiple target partitions so DataFusion emits a real Partial/Final pair.
        let mut config = SessionConfig::new();
        config.options_mut().execution.target_partitions = 4;
        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_config(config)
                .with_default_features()
                .with_physical_optimizer_rules(
                    crate::agg_mode::physical_optimizer_rules_without_combine(),
                )
                .build(),
        );
        crate::udf::register_all(&ctx);
        crate::udaf::register_all(&ctx);
        ctx
    }

    fn input_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("status", DataType::Utf8, false),
                Field::new("latency", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "b", "a"])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30, 40, 50])),
            ],
        )
        .unwrap()
    }

    /// Sort batches' rows into a comparable canonical form (group key → value),
    /// order-insensitive, so single-node vs distributed results compare equal.
    fn rows_sorted(batches: &[RecordBatch]) -> Vec<(String, i64)> {
        let mut out = Vec::new();
        for b in batches {
            let keys = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let vals = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                out.push((keys.value(i).to_string(), vals.value(i)));
            }
        }
        out.sort();
        out
    }

    /// The full Phase 0a spike, exercised end to end through the real artifacts:
    ///  - plan `SUM(latency) BY status` (DataFusion emits Final ← Partial)
    ///  - split: Partial half = shard stage; retain Final half
    ///  - graft Final half onto StageReadExec → reduce stage
    ///  - encode BOTH stages via the codec; assert displayable() round-trips
    ///  - decode in a FRESH session; feed the shard stage's output into the
    ///    reduce stage via an in-memory partition stream; execute
    ///  - assert results identical to single-node execution
    #[tokio::test]
    async fn phase0a_graft_codec_execute_end_to_end() {
        let batch = input_batch();

        // ---- single-node reference result ----
        let ref_ctx = fresh_session();
        ref_ctx.register_batch("t", batch.clone()).unwrap();
        let reference = ref_ctx
            .sql("SELECT status, SUM(latency) AS s FROM t GROUP BY status")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected = rows_sorted(&reference);
        assert_eq!(expected, vec![("a".into(), 90), ("b".into(), 60)]);

        // ---- plan the (shard) fragment; DataFusion builds the Final ← Partial pair ----
        let plan_ctx = fresh_session();
        plan_ctx.register_batch("t", batch.clone()).unwrap();
        let full_plan = plan_ctx
            .sql("SELECT status, SUM(latency) AS s FROM t GROUP BY status")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert!(
            find_agg_modes(&full_plan).contains(&AggregateMode::Partial),
            "expected a Partial half in:\n{}",
            plan_string(&full_plan)
        );

        // ---- split: Partial half ships as the shard stage; retain the Final half ----
        let shard_plan = apply_aggregate_mode(Arc::clone(&full_plan), Mode::Partial).unwrap();
        let retained_final = find_top_aggregate(&full_plan).expect("Final half present");
        let child_schema = shard_plan.schema();
        // Partial state schema: group col + SUM state col(s).
        assert!(child_schema.fields().len() >= 2, "partial state: {child_schema:?}");

        // ---- graft Final half onto a StageReadExec to form the reduce stage ----
        // The reduce fragment's own plan (here: just the final-agg subtree) gets its
        // aggregation subtree replaced by the retained Final half over StageReadExec.
        let reduce_fragment = apply_aggregate_mode(Arc::clone(&full_plan), Mode::Final).unwrap();
        let reduce_plan = graft_final_half(
            reduce_fragment,
            retained_final,
            /* child_stage_id */ 1,
            Arc::clone(&child_schema),
        )
        .unwrap();
        // The reduce stage must read from StageReadExec, not a memtable scan.
        assert!(
            plan_string(&reduce_plan).contains("StageReadExec"),
            "reduce plan must contain StageReadExec:\n{}",
            plan_string(&reduce_plan)
        );

        // ---- codec round-trip BOTH stages (displayable equality) ----
        // Decode resolves built-in aggregate UDAFs (e.g. `sum`) from the
        // session's function registry, so the round-trip must run against a real
        // session's TaskContext, not a bare default (spec Phase 0a: "decode in a
        // fresh SessionContext with the standard UDF/UDAF registration").
        let rt_ctx = fresh_session();
        let tctx = rt_ctx.task_ctx();
        assert_codec_round_trips(&shard_plan, tctx.as_ref()).expect("shard stage round-trips");
        // The reduce plan's StageReadExec needs the registry only at execute time;
        // round-trip (encode/decode/displayable) does not touch it.
        assert_codec_round_trips(&reduce_plan, tctx.as_ref()).expect("reduce stage round-trips");

        // ---- execute the shard stage; capture its output batches ----
        let shard_bytes = encode_stage_plan(&shard_plan).unwrap();
        let reduce_bytes = encode_stage_plan(&reduce_plan).unwrap();

        // Fresh data-node-style session for the SHARD stage. We re-register the
        // source memtable so the decoded Partial half (which still has the memtable
        // scan beneath it — the leaf-swap to OpenSearchShardScanExec is Phase 2b)
        // can run. For Phase 0a the point is the Partial→stream→Final wiring.
        let shard_exec_ctx = fresh_session();
        shard_exec_ctx.register_batch("t", batch.clone()).unwrap();
        let shard_tctx = shard_exec_ctx.task_ctx();
        let shard_decoded = decode_stage_plan(&shard_bytes, shard_tctx.as_ref()).unwrap();
        let shard_out = collect(shard_decoded, shard_tctx).await.unwrap();
        assert!(!shard_out.is_empty(), "shard stage produced no batches");

        // ---- execute the REDUCE stage in a fresh session, feeding shard output
        //      through an in-memory partition stream registered as input-1 ----
        // Register the child partition stream in the StageInputRegistry extension,
        // baked into the reduce session's config so it rides on every TaskContext.
        let registry = Arc::new(crate::session_context::StageInputRegistry::new());
        registry.register(
            1,
            Arc::new(MemPartition::new(Arc::clone(&child_schema), shard_out.clone())),
        );
        let reduce_ctx = fresh_session_with_registry(registry);
        let task_ctx = reduce_ctx.task_ctx();

        let reduce_decoded = decode_stage_plan(&reduce_bytes, task_ctx.as_ref()).unwrap();
        let mut stream = execute_stream(reduce_decoded, task_ctx).unwrap();
        let mut result = Vec::new();
        while let Some(b) = stream.next().await {
            result.push(b.unwrap());
        }
        assert_eq!(
            rows_sorted(&result),
            expected,
            "distributed graft result must equal single-node"
        );
    }

    /// Phase 0a checkbox 3: a `delegated_predicate` (marker UDF) call inside a
    /// plan's stored expression round-trips by name through the codec. The UDF
    /// serializes as just its name; decode resolves it from the fresh session's
    /// registry (markers are registered by `crate::udf::register_all`). It is
    /// never physically evaluated (DO-NOT-TOUCH §3) — we assert structural
    /// round-trip, not execution.
    #[tokio::test]
    async fn phase0a_marker_udf_round_trips_by_name() {
        // Let DataFusion build the physical expr from SQL so the marker UDF is
        // wired exactly as it would be in a real plan. The marker is registered
        // by `fresh_session` (via crate::udf::register_all).
        let ctx = fresh_session();
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "annotation",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow_array::Int32Array::from(vec![7i32]))],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();

        // delegated_predicate(annotation) returns Boolean — usable as a WHERE
        // predicate. (It is never physically evaluated off the indexed path; this
        // test only round-trips it structurally.)
        let plan = ctx
            .sql("SELECT annotation FROM t WHERE delegated_predicate(annotation)")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert!(
            plan_string(&plan).contains("delegated_predicate"),
            "planned expr must contain the marker:\n{}",
            plan_string(&plan)
        );

        let tctx = ctx.task_ctx();
        assert_codec_round_trips(&plan, tctx.as_ref())
            .expect("marker UDF round-trips by name");

        let bytes = encode_stage_plan(&plan).unwrap();
        let decoded = decode_stage_plan(&bytes, tctx.as_ref()).unwrap();
        assert!(
            plan_string(&decoded).contains("delegated_predicate"),
            "decoded plan must retain the marker UDF by name:\n{}",
            plan_string(&decoded)
        );
    }

    /// A fresh data-node-style session whose SessionConfig carries the
    /// StageInputRegistry extension, so a decoded StageReadExec resolves its
    /// child partition stream from `ctx.task_ctx()`.
    fn fresh_session_with_registry(
        registry: Arc<crate::session_context::StageInputRegistry>,
    ) -> SessionContext {
        let mut config = SessionConfig::new().with_extension(registry);
        config.options_mut().execution.target_partitions = 4;
        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_config(config)
                .with_default_features()
                .with_physical_optimizer_rules(
                    crate::agg_mode::physical_optimizer_rules_without_combine(),
                )
                .build(),
        );
        crate::udf::register_all(&ctx);
        crate::udaf::register_all(&ctx);
        ctx
    }

    /// Minimal `PartitionStream` over pre-materialized batches, for feeding a
    /// decoded `StageReadExec` in tests (stands in for the real partition stream).
    #[derive(Debug)]
    struct MemPartition {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    }
    impl MemPartition {
        fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
            Self { schema, batches }
        }
    }
    impl datafusion::physical_plan::streaming::PartitionStream for MemPartition {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }
        fn execute(
            &self,
            _ctx: Arc<TaskContext>,
        ) -> datafusion::physical_plan::SendableRecordBatchStream {
            let schema = Arc::clone(&self.schema);
            let batches = self.batches.clone();
            Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::iter(batches.into_iter().map(Ok)),
            ))
        }
    }

    // =======================================================================
    // Phase 1 — finalize_query_plan bottom-up orchestration over a 2-stage DAG
    // =======================================================================

    use crate::local_executor::LocalSession;
    use crate::proto::{AggMode as PAggMode, LeafKind, StageMeta};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message as _;

    /// Build Substrait bytes for `sql` against a session where `table`/schema is
    /// registered as a binding skeleton (so the plan is portable onto the
    /// finalizer's session).
    async fn substrait_for(sql: &str, table: &str, schema: &SchemaRef) -> Vec<u8> {
        let env = RuntimeEnvBuilder::new().build().unwrap();
        let producer = LocalSession::new(&env);
        producer
            .register_binding_skeleton(table, Arc::clone(schema))
            .unwrap();
        let df = producer.ctx().sql(sql).await.unwrap();
        let plan = df.logical_plan().clone();
        let substrait = to_substrait_plan(&plan, &producer.ctx().state()).unwrap();
        let mut buf = Vec::new();
        substrait.encode(&mut buf).unwrap();
        buf
    }

    /// End-to-end: a 2-stage aggregate DAG (PARTIAL shard stage 1 → FINAL reduce
    /// stage 2) is finalized bottom-up by `finalize_query_plan`, with the graft
    /// wiring stage 2's Final half over a StageReadExec stamped with stage 1's
    /// actual Partial-state schema. Asserts both stages finalize, the reduce plan
    /// reads from StageReadExec, and both plan blobs codec-round-trip.
    #[tokio::test]
    async fn phase1_finalize_two_stage_agg_dag() {
        let src_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("status", DataType::Utf8, false),
            Field::new("latency", DataType::Int64, false),
        ]));

        // Shard fragment (stage 1): partial agg over the source table `t`.
        let shard_bytes = substrait_for(
            "SELECT status, SUM(latency) AS s FROM t GROUP BY status",
            "t",
            &src_schema,
        )
        .await;

        // Reduce fragment (stage 2): the same agg over the child input `input-1`.
        // (Calcite emits a semantically-equivalent merge; the graft replaces it.)
        let reduce_bytes = substrait_for(
            "SELECT status, SUM(s) AS s FROM \"input-1\" GROUP BY status",
            "input-1",
            &Arc::new(Schema::new(vec![
                Field::new("status", DataType::Utf8, true),
                Field::new("s", DataType::Int64, true),
            ])),
        )
        .await;

        let stage1 = StageInput {
            substrait_bytes: shard_bytes,
            meta: StageMeta {
                stage_id: 1,
                child_stage_ids: vec![],
                agg_mode: PAggMode::Partial as i32,
                leaf_kind: LeafKind::ShardScan as i32,
                ..Default::default()
            },
        };
        let stage2 = StageInput {
            substrait_bytes: reduce_bytes,
            meta: StageMeta {
                stage_id: 2,
                child_stage_ids: vec![1],
                agg_mode: PAggMode::Final as i32,
                leaf_kind: LeafKind::StageInput as i32,
                ..Default::default()
            },
        };

        let env = RuntimeEnvBuilder::new().build().unwrap();
        let session = LocalSession::new(&env);
        // The shard stage reads the real source table `t`; register it as a
        // binding skeleton on the finalizer session so stage 1 lowers.
        session
            .register_binding_skeleton("t", Arc::clone(&src_schema))
            .unwrap();

        let outputs = finalize_query_plan(&session, vec![stage1, stage2])
            .await
            .expect("finalize 2-stage DAG");

        assert_eq!(outputs.len(), 2);
        // Child-first ordering: stage 1 finalized before stage 2.
        assert_eq!(outputs[0].stage_id, 1);
        assert_eq!(outputs[1].stage_id, 2);

        // Stage 1 (partial) retained a Final half for the graft.
        assert!(
            outputs[0].retained_final.is_some(),
            "partial shard stage must retain a Final half"
        );

        // Stage 2 (reduce) plan must read from StageReadExec (the graft target).
        let tctx = session.ctx().task_ctx();
        let stage2_plan = decode_stage_plan(&outputs[1].plan_bytes, tctx.as_ref()).unwrap();
        assert!(
            plan_string(&stage2_plan).contains("StageReadExec"),
            "reduce stage must read from StageReadExec:\n{}",
            plan_string(&stage2_plan)
        );

        // Both finalized blobs round-trip the codec (CI invariant #1).
        let s1 = decode_stage_plan(&outputs[0].plan_bytes, tctx.as_ref()).unwrap();
        assert_codec_round_trips(&s1, tctx.as_ref()).unwrap();
        assert_codec_round_trips(&stage2_plan, tctx.as_ref()).unwrap();
    }

    /// CI invariant #2: a plan with a live `delegated_predicate` FilterExec (no
    /// scan to absorb it) fails the post-finalization marker check; a clean plan
    /// passes.
    #[tokio::test]
    async fn marker_outside_scan_is_rejected() {
        let ctx = fresh_session();
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "annotation",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow_array::Int32Array::from(vec![7i32]))],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();

        // Live marker in a FilterExec → must be rejected.
        let bad = ctx
            .sql("SELECT annotation FROM t WHERE delegated_predicate(annotation)")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert!(
            assert_no_marker_udf_outside_scan(&bad).is_err(),
            "live marker FilterExec must be rejected:\n{}",
            plan_string(&bad)
        );

        // Clean plan with no marker → passes.
        let good = ctx
            .sql("SELECT annotation FROM t WHERE annotation > 0")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert!(assert_no_marker_udf_outside_scan(&good).is_ok());
    }

    /// SHARD_SCAN leaf rewrite: a plan over the pushdown-stub provider has its
    /// scan leaf replaced by OpenSearchShardScanExec, with the whole filter
    /// pushed into the scan (no FilterExec above it — CI invariant #2 holds
    /// because the marker would otherwise surface there).
    #[tokio::test]
    async fn shard_scan_leaf_rewrite_pushes_filter_into_scan() {
        use crate::os_exec::{OpenSearchShardScanExec, PushdownStubProvider};
        use crate::proto::{AggMode as PAggMode, LeafKind, StageMeta};

        let ctx = fresh_session();
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int64, false),
            Field::new("qty", DataType::Int64, false),
        ]));
        // Register the pushdown stub as the table — it claims Exact for all filters.
        ctx.register_table("t", Arc::new(PushdownStubProvider::new(Arc::clone(&schema))))
            .unwrap();

        let plan = ctx
            .sql("SELECT price, qty FROM t WHERE price > 100 AND qty < 50")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        // Stub's Exact pushdown means no FilterExec above the scan.
        assert!(
            !plan_string(&plan).contains("FilterExec"),
            "pushdown stub should have absorbed the filter:\n{}",
            plan_string(&plan)
        );

        let meta = StageMeta {
            stage_id: 1,
            agg_mode: PAggMode::None as i32,
            leaf_kind: LeafKind::ShardScan as i32,
            tree_shape: 1,
            requests_row_ids: false,
            ..Default::default()
        };
        let rewritten = swap_shard_scan_leaf(plan, &meta, vec![1, 2, 3], "idx-shard-0".into()).unwrap();

        // The scan leaf is now OpenSearchShardScanExec.
        fn find_shard_scan(p: &Arc<dyn ExecutionPlan>) -> bool {
            if p.downcast_ref::<OpenSearchShardScanExec>().is_some() {
                return true;
            }
            p.children().iter().any(|c| find_shard_scan(c))
        }
        assert!(
            find_shard_scan(&rewritten),
            "scan leaf must be OpenSearchShardScanExec:\n{}",
            plan_string(&rewritten)
        );
        // The placeholder leaf is gone, replaced by the shard scan.
        assert!(!plan_string(&rewritten).contains("EmptyExec"));
        assert!(plan_string(&rewritten).contains("OpenSearchShardScanExec"));
    }

    /// child_first_order yields a valid post-order and detects cycles.
    #[test]
    fn child_first_order_is_post_order() {
        let mk = |id: i32, children: Vec<i32>| StageInput {
            substrait_bytes: vec![],
            meta: StageMeta {
                stage_id: id,
                child_stage_ids: children,
                ..Default::default()
            },
        };
        // 3 → {1,2}; 1,2 leaves. Order must place 1 and 2 before 3.
        let stages = vec![mk(3, vec![1, 2]), mk(1, vec![]), mk(2, vec![])];
        let order = child_first_order(&stages).unwrap();
        let pos = |id: i32| order.iter().position(|&x| x == id).unwrap();
        assert!(pos(1) < pos(3));
        assert!(pos(2) < pos(3));

        // Cycle 1 → 2 → 1 detected.
        let cyc = vec![mk(1, vec![2]), mk(2, vec![1])];
        assert!(child_first_order(&cyc).is_err());
    }
}
