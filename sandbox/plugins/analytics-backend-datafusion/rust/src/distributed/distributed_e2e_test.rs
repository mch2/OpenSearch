/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Phase-1 end-to-end test: a distributed aggregation over multiple shards, executed across REAL
//! in-memory workers (direct gRPC, no Java tunnel, no delegation). Proves the six Phase-1 pieces
//! work together — ShardScanExec leaf, ShardScanCodec, ShardScanTaskEstimator, ShardCatalog
//! resolution, native two-phase aggregation across NetworkShuffleExec — and that the result equals
//! a single-node scan over the same files.

#![cfg(all(test, feature = "spike_integration"))]

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::test_utils::in_memory_channel_resolver::InMemoryChannelResolver;
use datafusion_distributed::{
    DistributedExt, SessionStateBuilderExt, WorkerQueryContext, display_plan_ascii,
};
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;

use crate::api::ShardFileInfo;
use crate::distributed::codec::ShardScanCodec;
use crate::distributed::shard_catalog::{ShardCatalog, ShardEntry};
use crate::distributed::shard_task_estimator::ShardScanTaskEstimator;

const NUM_SHARDS: usize = 3;

fn events_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]))
}

/// Builds a fully-configured distributed coordinator `SessionContext`: distributed planner +
/// in-memory worker resolver/channel (workers inject the ShardCatalog + read shard files locally)
/// + our codec + a ShardScanTaskEstimator. Shared by both Phase-1 tests.
fn build_coordinator(
    shards: &[(i32, ShardFileInfo)],
    store_url: &ObjectStoreUrl,
    dir: &std::path::Path,
    shard_ids: Vec<i32>,
) -> SessionContext {
    let shards_for_worker = shards.to_vec();
    let store_url_for_worker = store_url.clone();
    let dir_for_worker = dir.to_path_buf();
    let worker_builder = move |ctx: WorkerQueryContext| {
        let shards = shards_for_worker.clone();
        let store_url = store_url_for_worker.clone();
        let dir = dir_for_worker.clone();
        async move {
            // Build the worker session with our codec, then inject the ShardCatalog into its config
            // (ShardScanExec::execute reads it from ctx.session_config()). In production the Java
            // data node injects this after acquiring the shard reader.
            let state = ctx.builder.with_distributed_user_codec(ShardScanCodec).build();
            let sc = SessionContext::from(state);
            sc.state_ref()
                .write()
                .config_mut()
                .set_extension(Arc::new(build_catalog(&shards, &store_url)));
            let store = Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
            sc.runtime_env().register_object_store(store_url.as_ref(), store);
            Ok::<SessionState, DataFusionError>(sc.state())
        }
    };

    let channel_resolver = InMemoryChannelResolver::from_session_builder(worker_builder);
    let coord_state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(4))
        .with_default_features()
        .with_distributed_worker_resolver(
            datafusion_distributed::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver::new(NUM_SHARDS),
        )
        .with_distributed_channel_resolver(channel_resolver)
        .with_distributed_user_codec(ShardScanCodec)
        .with_distributed_task_estimator(ShardScanTaskEstimator::new(shard_ids))
        .with_distributed_planner()
        .build();
    SessionContext::from(coord_state)
}

/// Writes one parquet file per shard into `dir`, returns (shard_id, ShardFileInfo) pairs and the
/// LocalFileSystem object-store URL rooted at `dir`.
fn write_shards(dir: &std::path::Path) -> (Vec<(i32, ShardFileInfo)>, ObjectStoreUrl) {
    let schema = events_schema();
    // shard 0: a/10, b/20 ; shard 1: a/30, c/40 ; shard 2: b/50, a/60
    let data = [
        (vec!["a", "b"], vec![10i64, 20]),
        (vec!["a", "c"], vec![30i64, 40]),
        (vec!["b", "a"], vec![50i64, 60]),
    ];
    let store = object_store::local::LocalFileSystem::new_with_prefix(dir).unwrap();
    let _ = &store; // store only needed to compute ObjectMeta; we re-create per worker below.
    let mut out = Vec::new();
    for (i, (cats, amts)) in data.iter().enumerate() {
        let fname = format!("shard-{i}.parquet");
        let path = dir.join(&fname);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(cats.clone())),
                Arc::new(Int64Array::from(amts.clone())),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(f, Arc::clone(&schema), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let size = std::fs::metadata(&path).unwrap().len();
        let object_meta = object_store::ObjectMeta {
            location: object_store::path::Path::from(fname),
            last_modified: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            size: size as u64,
            e_tag: None,
            version: None,
        };
        out.push((
            i as i32,
            ShardFileInfo {
                object_meta,
                row_base: 0,
                num_rows: 2,
                row_group_row_counts: vec![2],
                access_plan: None,
            },
        ));
    }
    // ObjectStoreUrl is scheme+authority only (no path). The path is carried by the prefixed
    // LocalFileSystem store registered under this URL; ObjectMeta.location is relative to it.
    let store_url = ObjectStoreUrl::local_filesystem();
    (out, store_url)
}

fn build_catalog(shards: &[(i32, ShardFileInfo)], store_url: &ObjectStoreUrl) -> ShardCatalog {
    let mut cat = ShardCatalog::new();
    for (sid, fi) in shards {
        cat.insert(
            *sid,
            ShardEntry {
                files: Arc::new(vec![fi.clone()]),
                store_url: store_url.clone(),
            },
        );
    }
    cat
}

#[tokio::test]
async fn phase1_distributed_shard_scan_matches_single_node() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let (shards, store_url) = write_shards(dir.path());
    let shard_ids: Vec<i32> = shards.iter().map(|(s, _)| *s).collect();
    let dir_path = dir.path().to_path_buf();

    let coord = build_coordinator(&shards, &store_url, &dir_path, shard_ids.clone());
    // Register the leaf table directly as a ShardScanExec-producing provider (this test drives via
    // SQL; the Substrait-entry path is exercised by phase1_substrait_entry below).
    coord.register_table(
        "events",
        Arc::new(crate::distributed::coordinator::ShardScanTable::new(
            "events".to_string(),
            "idx-uuid".to_string(),
            events_schema(),
        )),
    )?;

    let query = "SELECT category, count(*) AS n, sum(amount) AS total \
                 FROM events GROUP BY category ORDER BY category";

    let dplan = coord.sql(query).await?.create_physical_plan().await?;
    let plan_str = display_plan_ascii(dplan.as_ref(), false);
    println!("[phase1] distributed plan:\n{plan_str}");
    assert!(plan_str.contains("ShardScanExec"), "leaf must be our ShardScanExec");
    assert!(plan_str.contains("NetworkShuffleExec"), "must distribute with a real shuffle");

    let dbatches = execute_stream(dplan, coord.task_ctx())?.try_collect::<Vec<_>>().await?;
    let dout = pretty_format_batches(&dbatches)?.to_string();
    println!("[phase1] distributed result:\n{dout}");

    // Single-node reference: plain parquet scan over the same 3 files.
    let sctx = SessionContext::new();
    sctx.register_parquet(
        "events",
        dir_path.to_str().unwrap(),
        datafusion::prelude::ParquetReadOptions::default(),
    )
    .await?;
    let sbatches = execute_stream(sctx.sql(query).await?.create_physical_plan().await?, sctx.task_ctx())?
        .try_collect::<Vec<_>>()
        .await?;
    let sout = pretty_format_batches(&sbatches)?.to_string();
    println!("[phase1] single-node result:\n{sout}");

    assert_eq!(dout, sout, "distributed shard scan must equal single-node");

    // Sanity: 3 categories, totals correct (a:10+30+60=100, b:20+50=70, c:40).
    let total: i64 = dbatches
        .iter()
        .flat_map(|b| {
            b.column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .sum();
    assert_eq!(total, 210, "sum of all amounts");
    Ok(())
}

/// Phase-1, the REAL production entry: drive the coordinator from whole-query **Substrait** (what
/// Java emits) through `coordinator::register_shard_tables` + `plan_distributed`, not via SQL. Also
/// exercises native `count(DISTINCT)` through a `ShardScanExec` leaf across the shuffle (closing the
/// gap that Spike C proved only via a parquet table). Result must equal single-node.
#[tokio::test]
async fn phase1_substrait_entry_with_distinct_count() -> Result<(), Box<dyn std::error::Error>> {
    use crate::distributed::coordinator::{plan_distributed, register_shard_tables};
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;

    let dir = tempfile::tempdir()?;
    let (shards, store_url) = write_shards(dir.path());
    let shard_ids: Vec<i32> = shards.iter().map(|(s, _)| *s).collect();
    let dir_path = dir.path().to_path_buf();

    let query = "SELECT category, count(*) AS n, count(DISTINCT amount) AS distinct_amounts \
                 FROM events GROUP BY category ORDER BY category";

    // 1. Simulate Java: produce whole-query Substrait. (Java uses substrait-java/isthmus; we use the
    //    DataFusion producer over a plain parquet registration to get equivalent bytes.)
    let producer = SessionContext::new();
    producer
        .register_parquet("events", dir_path.to_str().unwrap(), datafusion::prelude::ParquetReadOptions::default())
        .await?;
    let logical = producer.sql(query).await?.into_optimized_plan()?;
    let substrait = to_substrait_plan(&logical, &producer.state())?;
    let mut plan_bytes = Vec::new();
    substrait.encode(&mut plan_bytes)?;

    // 2. Coordinator: register ShardScanTables from the Substrait base_schema (no data-node
    //    contact), then plan_distributed consumes the Substrait into a DistributedExec.
    let coord = build_coordinator(&shards, &store_url, &dir_path, shard_ids.clone());
    let registered = register_shard_tables(&coord, &plan_bytes, |_name| "idx-uuid".to_string())?;
    assert_eq!(registered, vec!["events".to_string()], "should register the events leaf");

    let dplan = plan_distributed(&coord, &plan_bytes).await?;
    let plan_str = display_plan_ascii(dplan.as_ref(), false);
    println!("[phase1-substrait] distributed plan:\n{plan_str}");
    assert!(plan_str.contains("ShardScanExec"), "leaf must be ShardScanExec");
    assert!(plan_str.contains("NetworkShuffleExec"), "must distribute with a real shuffle");
    assert!(plan_str.to_lowercase().contains("distinct"), "count(DISTINCT) must survive into the plan");

    let dbatches = execute_stream(dplan, coord.task_ctx())?.try_collect::<Vec<_>>().await?;
    let dout = pretty_format_batches(&dbatches)?.to_string();
    println!("[phase1-substrait] distributed result:\n{dout}");

    // Single-node reference.
    let sctx = SessionContext::new();
    sctx.register_parquet("events", dir_path.to_str().unwrap(), datafusion::prelude::ParquetReadOptions::default())
        .await?;
    let sbatches = execute_stream(sctx.sql(query).await?.create_physical_plan().await?, sctx.task_ctx())?
        .try_collect::<Vec<_>>()
        .await?;
    let sout = pretty_format_batches(&sbatches)?.to_string();
    println!("[phase1-substrait] single-node result:\n{sout}");

    assert_eq!(dout, sout, "Substrait-entry distributed result (incl. count DISTINCT) must equal single-node");
    Ok(())
}
