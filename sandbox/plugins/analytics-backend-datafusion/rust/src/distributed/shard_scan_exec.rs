/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `ShardScanExec` — the leaf placeholder for a shard scan on the distributed path.
//!
//! It carries SHARD IDENTITY ONLY: `(table_name, index_uuid, shard_id)` plus the output schema
//! (derivable at coordinator planning time from the Substrait `base_schema` — no data-node
//! contact). It never carries file names. At `execute()` time, the worker resolves the shard's
//! real files from the [`crate::distributed::shard_catalog::ShardCatalog`] session extension and
//! delegates the actual scan to [`crate::shard_table_provider::ShardTableProvider`], reusing all
//! existing parquet + row_base + (future) delegation machinery.
//!
//! Pre-distribution the node is a placeholder with `shard_id = -1`; the TaskEstimator clones it
//! into one variant per shard with the real id. Which variant a worker runs is decided by
//! `DistributedTaskContext.task_index`, exactly as proven in the Spike-E prototype.

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{exec_datafusion_err, exec_err, Result};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::table_schema::TableSchema;
use datafusion_datasource::PartitionedFile;
use datafusion_distributed::DistributedTaskContext;

use crate::distributed::shard_catalog::ShardCatalog;

/// Placeholder shard id used before the TaskEstimator fans the leaf out per shard group.
pub const UNASSIGNED_SHARD: i32 = -1;

/// Predicate delegation carried to the data node: the serialized Lucene filter tree plus the shape
/// metadata the indexed path needs. When present, `ShardScanExec::execute` takes the indexed/
/// delegation branch (Phase 3) — acquiring the shard reader + registering the filter handle via an
/// FFM upcall into the JVM, then scanning through the IndexedTableProvider + Lucene callbacks.
/// Mirrors the existing `IndexedExecutionConfig` (tree_shape / delegated_predicate_count /
/// requests_row_ids) plus the filter-tree bytes that today travel in the Java `DelegationDescriptor`.
#[derive(Debug, Clone, PartialEq)]
pub struct DelegationDescriptor {
    /// The shard-local leaf fragment Substrait (`Filter(delegated_predicate markers) -> Read`) the
    /// worker's indexed executor decodes into the Lucene BoolNode tree. Built by the coordinator from
    /// the same stripped marked plan (the WHERE-filter subtree).
    pub filter_tree: Vec<u8>,
    pub tree_shape: i32,
    pub delegated_predicate_count: i32,
    pub requests_row_ids: bool,
    /// The Java-serialized `DelegationDescriptor` (the per-annotation Lucene queries) the leaf hands
    /// back to the co-located JVM so it can build the `FilterDelegationHandle` for this query. Opaque
    /// to Rust — passed through the open upcall verbatim.
    pub descriptor_bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ShardScanExec {
    /// Logical table name (the Substrait NamedTable leaf name). Carried for diagnostics + multi-table
    /// join routing (the estimator keys per-table shard lists by this name).
    pub table_name: String,
    /// Index UUID — identifies which index these shards belong to; the leaf upcall resolves the shard
    /// reader by `(index_uuid, shard_id)`.
    pub index_uuid: String,
    /// The shard GROUP this variant (task) scans. Empty (`UNASSIGNED_SHARD` placeholder) until
    /// `scale_up_leaf_node` packs shards into per-task groups. A task scans MORE THAN ONE shard when
    /// the index has more shards than the stage has tasks (shards > workers) — the surplus is packed
    /// onto tasks round-robin, so no shard is ever dropped. `execute` scans each shard in the group
    /// and concatenates the streams.
    pub shard_ids: Vec<i32>,
    /// Optional predicate delegation. `None` → plain parquet scan (Phase 1/2); `Some` → indexed
    /// delegation branch (Phase 3).
    pub delegation: Option<DelegationDescriptor>,
    /// Output schema, derived from the Substrait base_schema at planning time (Spike F).
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl ShardScanExec {
    /// Construct an UNASSIGNED placeholder leaf (no shard group yet). `scale_up_leaf_node` clones it
    /// into per-task variants via [`with_shards`](Self::with_shards).
    pub fn new(table_name: String, index_uuid: String, _shard_id: i32, schema: SchemaRef) -> Self {
        Self::with_group(table_name, index_uuid, Vec::new(), schema)
    }

    fn with_group(table_name: String, index_uuid: String, shard_ids: Vec<i32>, schema: SchemaRef) -> Self {
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self { table_name, index_uuid, shard_ids, delegation: None, schema, props }
    }

    /// Attach a predicate-delegation descriptor (Phase 3).
    pub fn with_delegation(mut self, delegation: Option<DelegationDescriptor>) -> Self {
        self.delegation = delegation;
        self
    }

    /// Clone this placeholder into a variant bound to a concrete shard GROUP, preserving delegation.
    pub fn with_shards(&self, shard_ids: Vec<i32>) -> Self {
        Self::with_group(self.table_name.clone(), self.index_uuid.clone(), shard_ids, Arc::clone(&self.schema))
            .with_delegation(self.delegation.clone())
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl DisplayAs for ShardScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ShardScanExec: table={}, shard_ids={:?}", self.table_name, self.shard_ids)
    }
}

impl ExecutionPlan for ShardScanExec {
    fn name(&self) -> &str {
        "ShardScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self, partition: usize, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let dtc = DistributedTaskContext::from_ctx(&ctx);
        log::debug!(
            "ShardScanExec::execute table={} shard_ids={:?} task_index={} partition={}",
            self.table_name, self.shard_ids, dtc.task_index, partition
        );

        if self.shard_ids.is_empty() {
            return exec_err!(
                "ShardScanExec executed while still unassigned (no shard group); \
                 TaskEstimator::scale_up_leaf_node must pack shards into per-task groups first"
            );
        }

        // Scan every shard in this task's group and concatenate the streams. One shard → single scan
        // (the common case, shards<=workers). Multiple shards → this task owns >1 shard (shards>workers)
        // so we chain their streams; no shard is dropped. Each shard scan reuses the exact per-shard
        // logic below (eager catalog path for tests, or the JVM open-upcall for production).
        if self.shard_ids.len() == 1 {
            return self.execute_shard(self.shard_ids[0], partition, ctx);
        }
        let mut streams: Vec<SendableRecordBatchStream> = Vec::with_capacity(self.shard_ids.len());
        for &shard_id in &self.shard_ids {
            streams.push(self.execute_shard(shard_id, partition, Arc::clone(&ctx))?);
        }
        // Chain the per-shard streams into one, preserving the advertised schema.
        use futures::StreamExt;
        let schema = Arc::clone(&self.schema);
        let chained = futures::stream::iter(streams).flatten();
        Ok(Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, chained)))
    }
}

impl ShardScanExec {
    /// Scans a SINGLE shard, returning its stream. Shared by the single-shard fast path and the
    /// multi-shard group loop in `execute`.
    fn execute_shard(&self, shard_id: i32, partition: usize, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        // 2. EAGER/TEST path: a ShardCatalog injected directly into the session (in-process tests,
        //    or a pre-publish hook) means files are already known — scan parquet directly. This is
        //    the path the in-process FFM/e2e tests exercise.
        if let Some(entry) = ctx
            .session_config()
            .get_extension::<ShardCatalog>()
            .and_then(|c| c.get(shard_id).cloned())
        {
            if self.delegation.is_some() {
                return exec_err!("eager ShardCatalog path does not support delegation; use the open-upcall path");
            }
            let partitioned_files: Vec<PartitionedFile> =
                entry.files.iter().map(|fi| PartitionedFile::from(fi.object_meta.clone())).collect();
            let table_schema = TableSchema::new(Arc::clone(&self.schema), vec![]);
            let parquet_source = ParquetSource::new(table_schema);
            let file_scan_config = FileScanConfigBuilder::new(entry.store_url.clone(), Arc::new(parquet_source))
                .with_file_groups(vec![FileGroup::new(partitioned_files)])
                .build();
            return DataSourceExec::from_data_source(file_scan_config).execute(partition, ctx);
        }

        // 3. PRODUCTION path: open the fragment via ONE upcall into the co-located JVM, which runs the
        //    UNCHANGED AnalyticsSearchService reader-acquisition + delegation setup and returns a
        //    discriminated handle. The leaf is pull-based either way (DataFusion is pull).
        let query_id = ctx
            .session_config()
            .get_extension::<crate::distributed::worker_server::WorkerQueryId>()
            .map(|q| q.0)
            .ok_or_else(|| exec_datafusion_err!("WorkerQueryId missing from session config; cannot open leaf fragment"))?;
        // The shard-local plan: the delegated leaf fragment (Filter(markers)->Read) if present, else
        // empty (plain full scan). The descriptor bytes + shape/count drive the JVM's indexed setup.
        let substrait: Vec<u8> = self.delegation.as_ref().map(|d| d.filter_tree.clone()).unwrap_or_default();
        let descriptor: Vec<u8> = self.delegation.as_ref().map(|d| d.descriptor_bytes.clone()).unwrap_or_default();
        let tree_shape: i32 = self.delegation.as_ref().map(|d| d.tree_shape).unwrap_or(0);
        let predicate_count: i32 = self.delegation.as_ref().map(|d| d.delegated_predicate_count).unwrap_or(0);
        let index_uuid = self.index_uuid.clone();
        let schema = Arc::clone(&self.schema);

        // CRITICAL: `execute()` runs on a worker thread that is ALREADY inside the IO tokio runtime
        // (the Worker gRPC server drives plan execution there). The Java open upcall synchronously
        // downcalls `createSessionContext`, which `io_runtime.block_on(...)`s parquet schema inference —
        // and the native scan below also block_on's. A nested `block_on` on the runtime we're already
        // on panics ("Cannot start a runtime from within a runtime"). So run the WHOLE open+scan-setup
        // on a dedicated OS thread that is NOT a runtime worker; there `block_on` is legal. The
        // resulting stream is then returned and polled by the worker runtime as normal (poll != block_on).
        // `block_in_place` lets the IO runtime spin a replacement worker while we park on the join.
        let opened_stream: Result<SendableRecordBatchStream> = tokio::task::block_in_place(|| {
            std::thread::scope(|s| {
                s.spawn(|| -> Result<SendableRecordBatchStream> {
                    let opened = crate::distributed::leaf_bridge::open_fragment(
                        query_id, &index_uuid, shard_id, &substrait, &descriptor, tree_shape, predicate_count,
                    )
                    .map_err(|e| exec_datafusion_err!("openFragment(shard={shard_id}) failed: {e}"))?;
                    match opened {
                        // Case 3: Java/Lucene produces; pull batches from the Java cursor.
                        crate::distributed::leaf_bridge::LeafOpen::JavaCursor { cursor } => {
                            let stream = crate::distributed::leaf_stream::JavaCursorStream::new(cursor, Arc::clone(&schema));
                            Ok(Box::pin(stream) as SendableRecordBatchStream)
                        }
                        // Cases 1&2: DF executes natively from the Java-built SessionContextHandle
                        // (existing createSessionContext / indexed path — reader + delegation already set
                        // up). The Rust leaf ADOPTS that native execution's bare stream — no
                        // native->Java->native round-trip.
                        crate::distributed::leaf_bridge::LeafOpen::Native { session_handle } => {
                            if session_handle == 0 {
                                return exec_err!("openFragment returned NATIVE mode with a null SessionContextHandle for shard {shard_id}");
                            }
                            // Take ownership of the handle the Java upcall built (it boxed a SessionContextHandle).
                            let handle = unsafe { *Box::from_raw(session_handle as *mut crate::session_context::SessionContextHandle) };
                            let mgr = crate::ffm::try_get_rt_manager()
                                .ok_or_else(|| exec_datafusion_err!("runtime manager not initialized"))?;
                            // Pass the coordinator-advertised schema so the full-shard scan is projected
                            // + reordered to exactly those columns (the parent operators bind by position).
                            let target = Arc::clone(&schema);
                            let inner = mgr.io_runtime.block_on(async move {
                                crate::query_executor::scan_stream_from_handle_projected(handle, &substrait, Some(target)).await
                            })?;
                            // Wrap so the JVM-side reader lease (keyed by session_handle) is released via
                            // leaf_close when this stream drops — otherwise the reader gate leaks per scan.
                            Ok(Box::pin(crate::distributed::leaf_stream::NativeLeafStream::new(inner, session_handle))
                                as SendableRecordBatchStream)
                        }
                    }
                })
                .join()
                .map_err(|_| exec_datafusion_err!("leaf open thread panicked for shard {shard_id}"))?
            })
        });
        opened_stream
    }
}
