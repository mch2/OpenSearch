//! SPIKE (branch spike/datafusion-distributed): prove the planning half of the
//! datafusion-distributed integration WITHOUT any transport.
//!
//! Spike B: a whole-query plan expressed as **Substrait** (simulating what Java emits) is
//! round-tripped through `to_substrait_plan` -> `from_substrait_plan` and handed to the
//! distributed planner. We assert it gets cut into stages with a `NetworkShuffleExec`
//! between the Partial and Final aggregates, and that a native `count(DISTINCT)` survives
//! the Substrait round-trip as a distinct aggregate (Spike C1, planning half).
//!
//! No workers / no channel resolver are needed: `create_physical_plan` only needs a
//! `WorkerResolver` to size stages. Execution-correctness across the shuffle is a separate
//! spike (C/E) that needs the in-memory transport.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_distributed::{DistributedExt, SessionStateBuilderExt, WorkerResolver, display_plan_ascii};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use url::Url;

/// Minimal synchronous WorkerResolver: advertises N fake worker URLs so the distributed
/// planner can size stages. Never dialed (we only plan, not execute).
#[derive(Debug)]
struct StubWorkerResolver(usize);

impl WorkerResolver for StubWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        (0..self.0)
            .map(|i| Url::parse(&format!("http://stub-worker-{i}")).map_err(|e| DataFusionError::External(Box::new(e))))
            .collect()
    }
}

fn events_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, true),
        Field::new("user", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]))
}

/// Writes 3 parquet files into a temp dir so the scan has multiple file groups. With
/// `bytes_per_partition=1` the built-in FileScanConfigTaskEstimator then fans the leaf out
/// to multiple tasks, which is what makes the distributed planner insert a network boundary
/// (mirrors the library's own `distributed_aggregation` test). Returns the dir (kept alive).
fn write_parquet_table() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let schema = events_schema();
    let rows = [
        (vec!["a", "b", "a"], vec!["u1", "u2", "u1"], vec![10i64, 20, 30]),
        (vec!["b", "a"], vec!["u3", "u4"], vec![40i64, 50]),
        (vec!["a", "b", "b"], vec!["u4", "u2", "u5"], vec![60i64, 70, 80]),
    ];
    for (i, (cat, usr, amt)) in rows.iter().enumerate() {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(cat.clone())),
                Arc::new(StringArray::from(usr.clone())),
                Arc::new(Int64Array::from(amt.clone())),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.path().join(format!("part-{i}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(f, Arc::clone(&schema), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    dir
}

#[tokio::test]
async fn spike_b_our_substrait_gets_distributed() -> Result<(), Box<dyn std::error::Error>> {
    let dir = write_parquet_table();
    let dir_path = dir.path().to_str().unwrap();

    // ── 1. Plain context (simulates the Java side producing Substrait for the whole query).
    let producer_ctx = SessionContext::new();
    producer_ctx.register_parquet("events", dir_path, ParquetReadOptions::default()).await?;

    let query = "SELECT category, count(*) AS n, count(DISTINCT \"user\") AS distinct_users \
                 FROM events GROUP BY category ORDER BY category";

    let logical = producer_ctx.sql(query).await?.into_optimized_plan()?;
    let substrait = to_substrait_plan(&logical, &producer_ctx.state())?;
    println!("[spike B] produced Substrait for whole query OK");

    // ── 2. Distributed coordinator context: distributed planner + a WorkerResolver stub.
    //    bytes_per_partition=1 forces the built-in estimator to fan the leaf scan out to
    //    multiple tasks so a network boundary is warranted.
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(4))
        .with_default_features()
        .with_distributed_worker_resolver(StubWorkerResolver(3))
        .with_distributed_planner()
        .with_distributed_file_scan_config_bytes_per_partition(1)?
        .build();
    let coord_ctx = SessionContext::from(state);
    coord_ctx.register_parquet("events", dir_path, ParquetReadOptions::default()).await?;

    // ── 3. Consume our Substrait -> LogicalPlan -> distributed physical plan.
    let logical_in = from_substrait_plan(&coord_ctx.state(), &substrait).await?;
    let physical = coord_ctx.state().create_physical_plan(&logical_in).await?;

    let plan_str = display_plan_ascii(physical.as_ref(), false);
    println!("[spike B] distributed plan:\n{plan_str}");

    // ── 4. Assertions: the plan distributed, and count(DISTINCT) survived as a distinct agg.
    assert!(
        plan_str.contains("DistributedExec"),
        "expected a DistributedExec wrapper, got:\n{plan_str}"
    );
    assert!(
        plan_str.contains("NetworkShuffleExec") || plan_str.contains("NetworkCoalesceExec"),
        "expected a network boundary (shuffle/coalesce) between agg stages, got:\n{plan_str}"
    );
    // count(DISTINCT) lowers to a distinct-marked aggregate; confirm it survived the round-trip.
    assert!(
        plan_str.to_lowercase().contains("distinct"),
        "expected count(DISTINCT) to survive Substrait round-trip, got:\n{plan_str}"
    );
    Ok(())
}

/// Spike C: EXECUTION correctness across a REAL shuffle, using an in-memory Worker.
///
/// Runs a distributed aggregation with native `count(DISTINCT)` and `approx_distinct` (HLL)
/// over multi-file parquet and asserts the distributed result equals a single-node result.
/// This proves DataFusion's native two-phase `state()`/`merge_batch()` works end-to-end across
/// `NetworkShuffleExec` — i.e. we can drop the custom `os_count_distinct`/`approx_distinct_safe`
/// UDAFs on this path (Finding 2 / open question C1, execution half).
///
/// Uses the library's `start_configured_in_memory_context` (gated behind df-distributed's
/// `integration` feature, dev-dep only). Our UDFs/UDAFs are registered on BOTH the coordinator
/// and the in-process worker (via the session-builder closure) so name resolution succeeds on
/// both sides of the boundary.
#[tokio::test]
async fn spike_c_native_aggregates_correct_across_shuffle() -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::SessionState;
    use datafusion::physical_plan::execute_stream;
    use datafusion_distributed::WorkerQueryContext;
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
    use futures::TryStreamExt;

    let dir = write_parquet_table();
    let dir_path = dir.path().to_str().unwrap().to_string();

    // Worker session builder: register OUR udfs/udafs on every worker session so the decoded
    // plan resolves count(DISTINCT)/approx_distinct (and our custom UDAFs) by name.
    let worker_builder = move |ctx: WorkerQueryContext| async move {
        let state = ctx.builder.build();
        let sc = SessionContext::from(state);
        opensearch_datafusion::udf::register_all(&sc);
        opensearch_datafusion::udaf::register_all(&sc);
        opensearch_datafusion::udwf::register_all(&sc);
        Ok::<SessionState, DataFusionError>(sc.state())
    };

    // 3 in-memory workers; bytes_per_partition=1 is set inside the helper to force fan-out.
    let dctx = start_configured_in_memory_context(3, worker_builder, |w| w).await;
    opensearch_datafusion::udf::register_all(&dctx);
    opensearch_datafusion::udaf::register_all(&dctx);
    opensearch_datafusion::udwf::register_all(&dctx);
    dctx.register_parquet("events", &dir_path, ParquetReadOptions::default()).await?;

    // Single-node reference context (no distribution).
    let sctx = SessionContext::new();
    sctx.register_parquet("events", &dir_path, ParquetReadOptions::default()).await?;

    let query = "SELECT category, \
                        count(*) AS n, \
                        count(DISTINCT \"user\") AS distinct_users, \
                        approx_distinct(\"user\") AS approx_users, \
                        sum(amount) AS total \
                 FROM events GROUP BY category ORDER BY category";

    // Distributed run.
    let dplan = dctx.sql(query).await?.create_physical_plan().await?;
    assert!(
        display_plan_ascii(dplan.as_ref(), false).contains("NetworkShuffleExec"),
        "distributed plan should contain a real shuffle"
    );
    let dbatches = execute_stream(dplan, dctx.task_ctx())?.try_collect::<Vec<_>>().await?;
    let dout = pretty_format_batches(&dbatches)?.to_string();
    println!("[spike C] distributed result:\n{dout}");

    // Single-node reference run.
    let splan = sctx.sql(query).await?.create_physical_plan().await?;
    let sbatches = execute_stream(splan, sctx.task_ctx())?.try_collect::<Vec<_>>().await?;
    let sout = pretty_format_batches(&sbatches)?.to_string();
    println!("[spike C] single-node result:\n{sout}");

    assert_eq!(dout, sout, "distributed result must equal single-node result");
    Ok(())
}

/// Spike C2: a genuinely-custom UDAF with NO native equivalent — `take(col, n)` — must merge
/// correctly across a real shuffle via its own `state()`/`merge_batch()`. We compare the
/// distributed result to single-node. (`take`'s cross-shard element ORDER is documented as
/// non-deterministic, same as the legacy path, so we compare the count of groups and that each
/// group's list is bounded by n and is a subset of the single-node result — i.e. merge_batch is
/// wired and bounded — rather than exact list equality.)
#[tokio::test]
async fn spike_c2_custom_take_udaf_merges_across_shuffle() -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::arrow::array::{Array, ListArray};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::execution::SessionState;
    use datafusion::physical_plan::execute_stream;
    use datafusion_distributed::WorkerQueryContext;
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
    use futures::TryStreamExt;
    use std::collections::HashSet;

    let dir = write_parquet_table();
    let dir_path = dir.path().to_str().unwrap().to_string();

    let worker_builder = move |ctx: WorkerQueryContext| async move {
        let sc = SessionContext::from(ctx.builder.build());
        opensearch_datafusion::udaf::register_all(&sc);
        Ok::<SessionState, DataFusionError>(sc.state())
    };
    let dctx = start_configured_in_memory_context(3, worker_builder, |w| w).await;
    opensearch_datafusion::udaf::register_all(&dctx);
    dctx.register_parquet("events", &dir_path, ParquetReadOptions::default()).await?;

    let sctx = SessionContext::new();
    opensearch_datafusion::udaf::register_all(&sctx);
    sctx.register_parquet("events", &dir_path, ParquetReadOptions::default()).await?;

    // take(user, 2) per category — bounded buffer of <=2 users per group.
    let query = "SELECT category, take(\"user\", 2) AS sample FROM events GROUP BY category ORDER BY category";

    let dplan = dctx.sql(query).await?.create_physical_plan().await?;
    let dplan_str = display_plan_ascii(dplan.as_ref(), false);
    println!("[spike C2] distributed plan:\n{dplan_str}");
    assert!(dplan_str.contains("NetworkShuffleExec"), "take() query must distribute with a real shuffle");

    let dbatches = execute_stream(dplan, dctx.task_ctx())?.try_collect::<Vec<_>>().await?;
    let splan = sctx.sql(query).await?.create_physical_plan().await?;
    let sbatches = execute_stream(splan, sctx.task_ctx())?.try_collect::<Vec<_>>().await?;

    // Build {category -> set(users)} for both; assert same groups and distributed ⊆ single-node,
    // each bounded by n=2. This proves merge_batch ran across the shuffle and stayed bounded.
    // Strings may arrive as Utf8 or Utf8View (force-view); read either via the cast kernel.
    fn strings_of(arr: &dyn Array) -> Vec<Option<String>> {
        use datafusion::arrow::compute::cast;
        let utf8 = cast(arr, &DataType::Utf8).unwrap();
        let sv = utf8.as_any().downcast_ref::<StringArray>().unwrap();
        (0..sv.len()).map(|i| if sv.is_null(i) { None } else { Some(sv.value(i).to_string()) }).collect()
    }
    fn extract(batches: &[RecordBatch]) -> std::collections::BTreeMap<String, (usize, HashSet<String>)> {
        let mut out = std::collections::BTreeMap::new();
        for b in batches {
            let cats = strings_of(b.column(0).as_ref());
            let lists = b.column(1).as_any().downcast_ref::<ListArray>().unwrap();
            for r in 0..b.num_rows() {
                let cat = cats[r].clone().unwrap_or_default();
                let vals = lists.value(r);
                let elems = strings_of(vals.as_ref());
                let n = elems.len();
                let set: HashSet<String> = elems.into_iter().flatten().collect();
                out.insert(cat, (n, set));
            }
        }
        out
    }
    let dmap = extract(&dbatches);
    let smap = extract(&sbatches);
    println!("[spike C2] distributed={dmap:?}\n[spike C2] single-node={smap:?}");

    // The full per-category population (ground truth for membership). take()'s cross-shard element
    // CHOICE is non-deterministic by design (audit: "keeps first-n in arrival order"; arrival order
    // differs distributed vs single-node), so the correct invariants are: (1) same groups, (2) each
    // result bounded by n=2, (3) every element is a real member of that category. NOT subset-equality
    // against the single-node sample — both are valid n-bounded samples of the same population.
    let population: std::collections::BTreeMap<&str, HashSet<String>> = std::collections::BTreeMap::from([
        ("a", HashSet::from(["u1".into(), "u4".into()])),          // category a: u1,u1,u4
        ("b", HashSet::from(["u2".into(), "u3".into(), "u5".into()])), // category b: u2,u3,u2,u5
    ]);

    assert_eq!(dmap.keys().collect::<Vec<_>>(), smap.keys().collect::<Vec<_>>(), "same group keys");
    for (cat, (dlen, dset)) in &dmap {
        assert!(*dlen <= 2, "take(_, 2) group '{cat}' must be bounded by 2, got {dlen}");
        let pop = &population[cat.as_str()];
        assert!(dset.is_subset(pop), "distributed take group '{cat}' = {dset:?} must be members of population {pop:?}");
    }
    // Single-node must satisfy the same invariants (sanity on the harness/data).
    for (cat, (slen, sset)) in &smap {
        assert!(*slen <= 2 && sset.is_subset(&population[cat.as_str()]));
    }
    Ok(())
}

// ───────────────────────── Spike E: custom leaf + codec + per-task slice ─────────────────────
//
// Prototypes the Phase-1 `ShardScanExec` trio in miniature:
//   * `SpikeShardScan` — a custom leaf ExecutionPlan carrying a `shard_id` (no files), whose
//     `execute()` reads `DistributedTaskContext::from_ctx` to know which task it is.
//   * `SpikeShardCodec` — a `PhysicalExtensionCodec` that round-trips `SpikeShardScan` across the
//     stage boundary (this is the thing the library's built-in codec can't do for our node).
//   * `SpikeShardEstimator` — fans the leaf out to N per-shard variants under `DistributedLeafExec`
//     and pins each shard-task to a worker via `route_tasks`.
// The leaf emits one row holding its shard_id; distributing over 3 shards must yield rows {0,1,2}.
// That proves: (a) our custom node survives encode->wire->decode on the worker, and (b) each task
// executed the correct per-shard variant.

mod spike_e {
    use super::*;
    use std::any::Any;
    use std::fmt::Formatter;

    use async_trait::async_trait;
    use datafusion::arrow::array::Int32Array;
    use datafusion::catalog::{Session, TableProvider};
    use datafusion::datasource::TableType;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::logical_expr::Expr;
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    };
    use datafusion_distributed::{
        DistributedExt, DistributedLeafExec, DistributedTaskContext, SessionStateBuilderExt,
        TaskEstimation, TaskEstimator, TaskRoutingContext, WorkerQueryContext, display_plan_ascii,
    };
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_in_memory_context;
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;
    use datafusion::execution::SessionState;
    use datafusion::physical_plan::execute_stream;
    use futures::TryStreamExt;

    fn shard_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("shard_id", DataType::Int32, false)]))
    }

    /// TableProvider whose scan returns the custom `SpikeShardScan` leaf (placeholder shard_id).
    /// Mirrors how the real `ShardScanExec` will be produced from a TableProvider::scan.
    #[derive(Debug)]
    struct SpikeShardTable;

    #[async_trait]
    impl TableProvider for SpikeShardTable {
        fn schema(&self) -> SchemaRef { shard_schema() }
        fn table_type(&self) -> TableType { TableType::Base }
        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(SpikeShardScan::new(-1)))
        }
    }

    #[derive(Debug)]
    struct SpikeShardScan {
        shard_id: i32,
        props: Arc<PlanProperties>,
    }

    impl SpikeShardScan {
        fn new(shard_id: i32) -> Self {
            let props = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(shard_schema()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self { shard_id, props }
        }
    }

    impl DisplayAs for SpikeShardScan {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "SpikeShardScan: shard_id={}", self.shard_id)
        }
    }

    impl ExecutionPlan for SpikeShardScan {
        fn name(&self) -> &str { "SpikeShardScan" }
        fn properties(&self) -> &Arc<PlanProperties> { &self.props }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
        fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(&self, _partition: usize, ctx: Arc<TaskContext>) -> datafusion::common::Result<SendableRecordBatchStream> {
            // Prove we can see which task we are. (In real ShardScanExec this selects the shard
            // slice; here we just assert the per-task variant carried the right shard_id.)
            let dtc = DistributedTaskContext::from_ctx(&ctx);
            println!("[spike E] executing shard_id={} as task_index={}", self.shard_id, dtc.task_index);
            let batch = RecordBatch::try_new(
                shard_schema(),
                vec![Arc::new(Int32Array::from(vec![self.shard_id]))],
            )?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                shard_schema(),
                futures::stream::iter(vec![Ok(batch)]),
            )))
        }
    }

    #[derive(Debug)]
    struct SpikeShardCodec;
    impl PhysicalExtensionCodec for SpikeShardCodec {
        fn try_decode(&self, buf: &[u8], _inputs: &[Arc<dyn ExecutionPlan>], _ctx: &TaskContext) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            // Minimal wire format: 4 bytes little-endian shard_id.
            if buf.len() != 4 {
                return datafusion::common::exec_err!("SpikeShardScan expects 4 bytes, got {}", buf.len());
            }
            let shard_id = i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
            Ok(Arc::new(SpikeShardScan::new(shard_id)))
        }
        fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> datafusion::common::Result<()> {
            let scan = node.downcast_ref::<SpikeShardScan>()
                .ok_or_else(|| datafusion::common::exec_datafusion_err!("expected SpikeShardScan"))?;
            buf.extend_from_slice(&scan.shard_id.to_le_bytes());
            Ok(())
        }
    }

    #[derive(Debug)]
    struct SpikeShardEstimator { num_shards: usize }

    impl TaskEstimator for SpikeShardEstimator {
        fn task_estimation(&self, plan: &Arc<dyn ExecutionPlan>, _: &datafusion::config::ConfigOptions) -> Option<TaskEstimation> {
            plan.downcast_ref::<SpikeShardScan>()?;
            Some(TaskEstimation::desired(self.num_shards))
        }
        fn scale_up_leaf_node(&self, plan: &Arc<dyn ExecutionPlan>, task_count: usize, _: &datafusion::config::ConfigOptions) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
            if plan.downcast_ref::<SpikeShardScan>().is_none() {
                return Ok(None);
            }
            // One variant per shard, each carrying its own shard_id.
            let variants: Vec<Arc<dyn ExecutionPlan>> = (0..task_count)
                .map(|i| Arc::new(SpikeShardScan::new(i as i32)) as Arc<dyn ExecutionPlan>)
                .collect();
            Ok(Some(Arc::new(DistributedLeafExec::try_new(Arc::clone(plan), variants)?)))
        }
        fn route_tasks(&self, ctx: &TaskRoutingContext<'_>) -> datafusion::common::Result<Option<Vec<Url>>> {
            // Pin shard-task i -> worker[i % n] (the ShardTargetResolver analogue).
            use datafusion_distributed::DistributedConfig;
            let urls = DistributedConfig::from_task_context(&ctx.task_ctx)?.worker_resolver().get_urls()?;
            if urls.is_empty() { return Ok(None); }
            Ok(Some((0..ctx.task_count).map(|i| urls[i % urls.len()].clone()).collect()))
        }
    }

    #[tokio::test]
    async fn spike_e_custom_leaf_codec_and_task_slice() -> Result<(), Box<dyn std::error::Error>> {
        const N: usize = 3;

        // Worker registers our codec so it can decode SpikeShardScan from the wire.
        let worker_builder = move |ctx: WorkerQueryContext| async move {
            Ok::<SessionState, DataFusionError>(
                ctx.builder.with_distributed_user_codec(SpikeShardCodec).build(),
            )
        };
        let dctx = start_in_memory_context(N, worker_builder).await;

        // Re-build coordinator state with our codec + estimator (start_in_memory_context doesn't
        // expose those knobs), reusing its channel/worker resolvers via copied_config.
        let state = SessionStateBuilder::new()
            .with_config(dctx.copied_config())
            .with_default_features()
            .with_distributed_user_codec(SpikeShardCodec)
            .with_distributed_task_estimator(SpikeShardEstimator { num_shards: N })
            .with_distributed_planner()
            .build();
        let coord = SessionContext::from(state);
        coord.register_table("shards", Arc::new(SpikeShardTable))?;

        // A GROUP BY forces a Partial/Repartition/Final aggregate, which makes the distributed
        // planner insert a network boundary and fan the leaf scan out to N per-shard tasks. Each
        // task's SpikeShardScan variant emits its own shard_id; we group by it so the head stage
        // sees one row per shard. (A bare SELECT has no boundary, so nothing distributes.)
        let physical = coord
            .sql("SELECT shard_id, count(*) AS n FROM shards GROUP BY shard_id ORDER BY shard_id")
            .await?
            .create_physical_plan()
            .await?;

        let plan_str = display_plan_ascii(physical.as_ref(), false);
        println!("[spike E] distributed plan:\n{plan_str}");
        assert!(
            plan_str.contains("DistributedExec") && plan_str.contains("SpikeShardScan"),
            "expected a distributed plan containing our custom leaf, got:\n{plan_str}"
        );

        let batches = execute_stream(physical, coord.task_ctx())?.try_collect::<Vec<_>>().await?;
        let mut shard_ids: Vec<i32> = batches.iter()
            .flat_map(|b| {
                b.column(0).as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec()
            })
            .collect();
        shard_ids.sort();
        println!("[spike E] collected shard_ids={shard_ids:?}");
        assert_eq!(shard_ids, vec![0, 1, 2], "each shard task must have executed its own variant (codec round-trip + DistributedTaskContext slice)");
        Ok(())
    }
}
