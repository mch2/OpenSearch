/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `TaskEstimator` for the shard scan: fans a [`ShardScanExec`] placeholder out to one task per
//! shard and pins each shard-task to a worker URL.
//!
//! - `task_estimation` → desired task count = number of shards.
//! - `scale_up_leaf_node` → one `ShardScanExec` variant per shard id, wrapped in a
//!   `DistributedLeafExec`.
//! - `route_tasks` → shard-task i → worker URL. Phase 1 uses deterministic round-robin over the
//!   available workers (direct gRPC). Phase 2 swaps this for the real shard→node map supplied by
//!   Java (the `ShardTargetResolver` analogue), which is the only change needed for shard affinity.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{
    DistributedLeafExec, TaskEstimation, TaskEstimator, TaskRoutingContext,
    get_distributed_worker_resolver,
};
use url::Url;

use crate::distributed::shard_scan_exec::ShardScanExec;

/// Per-table shard fan-out + routing. `shard_ids` are the dense ordinals to fan out to (in task
/// order); `task_to_worker[i]` maps task i → an index into the resolver's worker-URL list (shard
/// affinity: the task for `shard_ids[i]` lands on the node hosting it). Empty `task_to_worker` →
/// round-robin fallback (tests / no shard→node map).
#[derive(Debug, Clone, Default)]
pub struct TableRouting {
    pub shard_ids: Vec<i32>,
    pub task_to_worker: Vec<usize>,
}

/// `TaskEstimator` for the shard scan, keyed BY TABLE so a multi-table join fans each leaf out to its
/// OWN shard list (join legs on indices with different shard counts/placement route independently).
/// A `ShardScanExec` leaf carries its `table_name`; the estimator looks up that table's `TableRouting`.
/// For back-compat, a single-table plan may use `new`/`with_routing` (registers under the empty key,
/// which is also the fallback when a table isn't found in the map).
#[derive(Debug, Clone)]
pub struct ShardScanTaskEstimator {
    by_table: HashMap<String, TableRouting>,
}

impl ShardScanTaskEstimator {
    pub fn new(shard_ids: Vec<i32>) -> Self {
        Self::with_routing(shard_ids, Vec::new())
    }

    /// Single-table construction (back-compat): registers the routing under the empty key, used as
    /// the fallback for any leaf table not explicitly mapped.
    pub fn with_routing(shard_ids: Vec<i32>, task_to_worker: Vec<usize>) -> Self {
        let mut by_table = HashMap::new();
        by_table.insert(String::new(), TableRouting { shard_ids, task_to_worker });
        Self { by_table }
    }

    /// Multi-table construction: one `TableRouting` per leaf table name.
    pub fn per_table(by_table: HashMap<String, TableRouting>) -> Self {
        Self { by_table }
    }

    /// The routing for `table` — its own entry, else the empty-key (single-table / fallback) entry,
    /// else an empty routing.
    fn routing_for(&self, table: &str) -> &TableRouting {
        self.by_table
            .get(table)
            .or_else(|| self.by_table.get(""))
            .unwrap_or_else(|| {
                // No mapping at all — return a shared empty routing. Static so we can borrow it.
                static EMPTY: std::sync::OnceLock<TableRouting> = std::sync::OnceLock::new();
                EMPTY.get_or_init(TableRouting::default)
            })
    }
}

impl TaskEstimator for ShardScanTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        let scan = plan.downcast_ref::<ShardScanExec>()?;
        let routing = self.routing_for(&scan.table_name);
        // One task per shard OF THIS TABLE. If there are no shards the leaf can't be distributed.
        if routing.shard_ids.is_empty() {
            Some(TaskEstimation::maximum(1))
        } else {
            Some(TaskEstimation::desired(routing.shard_ids.len()))
        }
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _cfg: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(scan) = plan.downcast_ref::<ShardScanExec>() else {
            return Ok(None);
        };
        let routing = self.routing_for(&scan.table_name);
        // Pack ALL shards of THIS table into `task_count` groups round-robin: shard at position j →
        // task j % task_count. When shards <= task_count each task gets one shard (the common case);
        // when shards > task_count (shards > workers) the surplus shards are packed onto tasks so NO
        // shard is dropped. route_tasks uses the same task indexing to pin each group to a hosting node.
        let mut groups: Vec<Vec<i32>> = vec![Vec::new(); task_count.max(1)];
        for (j, &shard_id) in routing.shard_ids.iter().enumerate() {
            groups[j % task_count.max(1)].push(shard_id);
        }
        let variants: Vec<Arc<dyn ExecutionPlan>> = groups
            .into_iter()
            .map(|group| Arc::new(scan.with_shards(group)) as Arc<dyn ExecutionPlan>)
            .collect();
        // `DistributedLeafExec::execute` runs its `original` (not a variant) on the single-task fast
        // path (`task_count == 1`). Our `original` is the UNASSIGNED placeholder (`shard_ids=[]`), which
        // errors at execute time. For a single-task stage (one shard, or one worker packing all shards)
        // there is exactly one variant, so use THAT assigned variant as the original — otherwise a
        // single-shard leaf executes unassigned. With >1 task the original is never executed (the
        // per-task variant is), so the placeholder is fine there.
        let original = if variants.len() == 1 {
            Arc::clone(&variants[0])
        } else {
            Arc::clone(plan)
        };
        Ok(Some(Arc::new(DistributedLeafExec::try_new(original, variants)?)))
    }

    fn route_tasks(&self, ctx: &TaskRoutingContext<'_>) -> Result<Option<Vec<Url>>> {
        // Only route stages whose leaf is our ShardScanExec; capture which TABLE the leaf scans so we
        // use that table's shard→worker affinity map (join legs on different indices route apart).
        let mut leaf_table: Option<String> = None;
        ctx.plan.apply(|node| {
            if let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() {
                if let Some(scan) = leaf.original().downcast_ref::<ShardScanExec>() {
                    leaf_table = Some(scan.table_name.clone());
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let Some(table) = leaf_table else {
            return Ok(None);
        };

        let urls = get_distributed_worker_resolver(ctx.task_ctx.session_config())?
            .get_urls()?;
        if urls.is_empty() {
            return Ok(None);
        }
        let routing = self.routing_for(&table);
        // Shard-affine placement when Java supplied a routing map: task i -> urls[task_to_worker[i]],
        // so the task for shard_ids[i] lands on the node hosting that shard. Falls back to
        // deterministic round-robin when no map is present (tests / direct-gRPC Phase 1).
        if !routing.task_to_worker.is_empty() {
            let routed = (0..ctx.task_count)
                .map(|i| {
                    let w = routing.task_to_worker.get(i).copied().unwrap_or(i % urls.len());
                    urls[w.min(urls.len() - 1)].clone()
                })
                .collect();
            return Ok(Some(routed));
        }
        Ok(Some(
            (0..ctx.task_count).map(|i| urls[i % urls.len()].clone()).collect(),
        ))
    }
}
