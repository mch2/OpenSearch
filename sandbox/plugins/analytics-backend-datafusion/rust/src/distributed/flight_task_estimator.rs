/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Model A `TaskEstimator`: fans a `FlightShardScanExec` placeholder out to one variant per shard,
//! each bound to its data-node Flight URL + shard id. The variants are then partitioned across MPP
//! tasks by datafusion-distributed.
//!
//! Unlike Model B's `ShardScanTaskEstimator`, there is NO `route_tasks` shard-affinity here: the
//! Flight leaf runs on the coordinator / MPP workers and FETCHES from the data node over Arrow
//! Flight, so the leaf stage's tasks are not pinned to the shard-hosting nodes — the data locality
//! lives in each variant's `node_url`, not in task placement. We still fan to `num_shards` tasks so
//! the fetches parallelize across the cluster.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{DistributedLeafExec, TaskEstimation, TaskEstimator};

use crate::distributed::flight_shard_scan_exec::FlightShardScanExec;

/// Per-shard targets in task order: `(shard_id, node_flight_url)`. Built from `DfShardRouting`
/// (shard→node) + the node→Flight-URL advertisement, handed across FFM.
#[derive(Debug, Clone)]
pub struct FlightShardScanTaskEstimator {
    pub targets: Vec<(i32, String)>,
}

impl FlightShardScanTaskEstimator {
    pub fn new(targets: Vec<(i32, String)>) -> Self {
        Self { targets }
    }
}

impl TaskEstimator for FlightShardScanTaskEstimator {
    fn task_estimation(&self, plan: &Arc<dyn ExecutionPlan>, _cfg: &ConfigOptions) -> Option<TaskEstimation> {
        plan.downcast_ref::<FlightShardScanExec>()?;
        if self.targets.is_empty() {
            Some(TaskEstimation::maximum(1))
        } else {
            Some(TaskEstimation::desired(self.targets.len()))
        }
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _cfg: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(leaf) = plan.downcast_ref::<FlightShardScanExec>() else {
            return Ok(None);
        };
        let variants: Vec<Arc<dyn ExecutionPlan>> = (0..task_count)
            .map(|i| {
                let (shard_id, url) = self
                    .targets
                    .get(i)
                    .cloned()
                    .unwrap_or((leaf.shard_id, leaf.node_url.clone()));
                Arc::new(leaf.with_target(shard_id, url)) as Arc<dyn ExecutionPlan>
            })
            .collect();
        Ok(Some(Arc::new(DistributedLeafExec::try_new(Arc::clone(plan), variants)?)))
    }

    // No route_tasks: the Flight leaf runs coordinator/MPP-side and fetches remotely; data locality
    // is encoded in each variant's node_url, not task placement.
}
