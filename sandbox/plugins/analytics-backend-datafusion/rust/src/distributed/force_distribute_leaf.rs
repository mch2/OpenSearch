/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Coordinator physical-optimizer rule: guarantee every [`ShardScanExec`] is DISTRIBUTED.
//!
//! # The bug this fixes
//!
//! `datafusion-distributed`'s planner only fans a leaf out to per-shard tasks when it inserts a
//! network boundary in some ancestor stage, and it inserts one ONLY where the physical plan already
//! has a distribution seam it recognises:
//!   * a hash `RepartitionExec` (→ `NetworkShuffleExec`), or
//!   * a `CoalescePartitionsExec` / `SortPreservingMergeExec` parent (→ `NetworkCoalesceExec`).
//!
//! For a plan whose head stage stays single-partition with NO such seam anywhere above the leaf, no
//! boundary is injected, `scale_up_leaf_node` never runs, and the `ShardScanExec` reaches `execute()`
//! still UNASSIGNED — on the coordinator, which hosts no shards. That is a hard failure
//! ("ShardScanExec executed while still unassigned"); even if it didn't error the coordinator can't
//! read remote nodes' shards, so the answer would be wrong.
//!
//! This bites the "pull everything to the coordinator" shapes the distributed engine most needs to
//! handle correctly:
//!   * a GLOBAL window — `sum(x) OVER ()` — `WindowAggExec` sits directly on the single-partition leaf;
//!   * a bare `SELECT *` / projection / filter-only query with no aggregate or sort;
//!   * any single-partition head stage the built-in optimizers didn't parallelise.
//!
//! (A GLOBAL aggregate like `count(*)` escapes the bug only incidentally: DataFusion parallelises the
//! partial aggregate with a round-robin `RepartitionExec` and then a `CoalescePartitionsExec` before
//! the final — that coalesce is the seam the library turns into a `NetworkCoalesceExec`.)
//!
//! # The fix
//!
//! Run LAST among physical-optimizer rules (appended after DataFusion's built-ins, before the
//! distributed query planner's boundary injection). Walk top-down tracking whether an ANCESTOR is
//! already a distribution seam. For a `ShardScanExec` with NO seam above it, wrap it in
//!
//! ```text
//!   CoalescePartitionsExec
//!     RepartitionExec(RoundRobinBatch(target_partitions))
//!       ShardScanExec
//! ```
//!
//! — the EXACT shape the working global-aggregate plan produces. The `CoalescePartitionsExec` is the
//! seam the library converts to a `NetworkCoalesceExec`, so the leaf distributes per shard and the
//! head stage gathers already-scanned rows over the network (never a bare remote leaf on the
//! coordinator). The intervening `RepartitionExec(RoundRobin)` is REQUIRED: the library refuses to
//! put a network boundary directly above a leaf ("wasteful"), so the node beneath the coalesce must
//! be a non-leaf — matching how `count(*)` is planned (coalesce over the partial aggregate, not over
//! the bare scan).
//!
//! Whole-path awareness keeps it shape-preserving where it matters: a leaf that ALREADY has a seam
//! above it (a group-by's hash repartition, a sort's SPM, the global aggregate's coalesce) is left
//! untouched, so those plans — and their distributed partial aggregates — are unchanged. Wrapping
//! them would be actively wrong: it would gather raw rows to the coordinator before the partial
//! aggregate.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::Result;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::distributed::shard_scan_exec::ShardScanExec;

/// Ensures every `ShardScanExec` sits beneath a distribution seam the distributed planner recognises.
/// `target_partitions` sizes the round-robin repartition inserted above an otherwise-bare leaf.
#[derive(Debug)]
pub struct ForceDistributeLeaf {
    target_partitions: usize,
}

impl ForceDistributeLeaf {
    pub fn new(target_partitions: usize) -> Self {
        Self {
            target_partitions: target_partitions.max(1),
        }
    }
}

/// True if `plan` is a seam that makes the distributed planner inject a network boundary in the
/// stage below it: a hash `RepartitionExec` (→ shuffle), a `CoalescePartitionsExec` or a
/// `SortPreservingMergeExec` (→ coalesce). A round-robin `RepartitionExec` is NOT a seam on its own
/// (the library ignores it) — but the enforce-distribution pass always pairs a round-robin that
/// gathers back to one partition with a `CoalescePartitionsExec` above it, and that coalesce is the
/// seam we detect, so a genuinely-distributing plan is always recognised via its coalesce/SPM/hash.
fn is_distribution_seam(plan: &dyn ExecutionPlan) -> bool {
    if plan.downcast_ref::<CoalescePartitionsExec>().is_some()
        || plan.downcast_ref::<SortPreservingMergeExec>().is_some()
    {
        return true;
    }
    if let Some(r) = plan.downcast_ref::<RepartitionExec>() {
        return matches!(r.partitioning(), Partitioning::Hash(_, _));
    }
    false
}

/// Recursively rebuild `plan`. `seam_above` is true once any ancestor was a distribution seam.
/// A `ShardScanExec` reached with `seam_above == false` is wrapped so it distributes.
fn rewrite(
    plan: Arc<dyn ExecutionPlan>,
    seam_above: bool,
    target_partitions: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.downcast_ref::<ShardScanExec>().is_some() {
        return Ok(if seam_above {
            plan
        } else {
            wrap_leaf(plan, target_partitions)
        });
    }
    let child_seam = seam_above || is_distribution_seam(plan.as_ref());
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }
    let new_children = children
        .iter()
        .map(|c| rewrite(Arc::clone(c), child_seam, target_partitions))
        .collect::<Result<Vec<_>>>()?;
    plan.with_new_children(new_children)
}

impl PhysicalOptimizerRule for ForceDistributeLeaf {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        rewrite(plan, false, self.target_partitions)
    }

    fn name(&self) -> &str {
        "ForceDistributeLeaf"
    }

    fn schema_check(&self) -> bool {
        // We only insert repartition/coalesce (both schema-preserving) — the plan schema is unchanged.
        true
    }
}

/// Builds `CoalescePartitionsExec(ProjectionExec(identity, RepartitionExec(RoundRobin(n), leaf)))` —
/// the seam shape the library turns into a `NetworkCoalesceExec` so the leaf distributes per shard.
///
/// Node order matters. The library's boundary-injection matches a `RepartitionExec` node FIRST and,
/// if it isn't a *hash* repartition, falls through WITHOUT considering the coalesce-parent rule (they
/// are `if`/`else if`). So the DIRECT child of the `CoalescePartitionsExec` must NOT be a
/// `RepartitionExec`, or the coalesce boundary never fires. The working global-aggregate plan has a
/// NON-repartition node (`AggregateExec(Partial)`) directly beneath its coalesce, with the RoundRobin
/// repartition BELOW that. We mirror that exactly with an IDENTITY `ProjectionExec` (a pure
/// passthrough standing in for the Partial aggregate — it just re-emits every input column unchanged)
/// as the coalesce's direct child, and the RoundRobin repartition — which gives the leaf stage
/// `target_partitions` output partitions — right above the leaf.
fn wrap_leaf(leaf: Arc<dyn ExecutionPlan>, target_partitions: usize) -> Arc<dyn ExecutionPlan> {
    let repartitioned = Arc::new(
        RepartitionExec::try_new(
            Arc::clone(&leaf),
            Partitioning::RoundRobinBatch(target_partitions),
        )
        .expect("RoundRobinBatch repartition is always valid"),
    );
    // Identity projection: (col_i -> field_name_i) for every field, re-emitting the input verbatim.
    let identity: Vec<(Arc<dyn PhysicalExpr>, String)> = leaf
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            (
                Arc::new(Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>,
                f.name().to_string(),
            )
        })
        .collect();
    let projected = Arc::new(
        ProjectionExec::try_new(identity, repartitioned)
            .expect("identity projection is always valid"),
    );
    Arc::new(CoalescePartitionsExec::new(projected))
}
