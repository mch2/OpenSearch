/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Phase-2 test: the data plane runs over REAL TCP gRPC (not the in-memory duplex), with our custom
//! ShardScanExec leaf + codec + per-worker ShardCatalog and the real `OsWorkerResolver`. Each worker
//! is a `Worker::into_worker_server()` bound to a loopback port — exactly the multi-process topology,
//! just colocated in one test process. Proves coordinator→worker leaf fetch and worker↔worker shuffle
//! are direct rust↔rust over the network (no Java tunnel), and that results equal single-node.

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
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_distributed::test_utils::localhost::spawn_worker_service;
use datafusion_distributed::{
    DistributedExt, SessionStateBuilderExt, WorkerQueryContext, display_plan_ascii,
};
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use tokio::net::TcpListener;
use url::Url;

use crate::api::ShardFileInfo;
use crate::distributed::codec::ShardScanCodec;
use crate::distributed::coordinator::{plan_distributed, register_shard_tables};
use crate::distributed::shard_catalog::{ShardCatalog, ShardEntry};
use crate::distributed::shard_task_estimator::ShardScanTaskEstimator;
use crate::distributed::worker_resolver::OsWorkerResolver;

const NUM_SHARDS: usize = 3;

fn events_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]))
}

fn write_shards(dir: &std::path::Path) -> (Vec<(i32, ShardFileInfo)>, ObjectStoreUrl) {
    let schema = events_schema();
    let data = [
        (vec!["a", "b"], vec![10i64, 20]),
        (vec!["a", "c"], vec![30i64, 40]),
        (vec!["b", "a"], vec![50i64, 60]),
    ];
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
        out.push((
            i as i32,
            ShardFileInfo {
                object_meta: object_store::ObjectMeta {
                    location: object_store::path::Path::from(fname),
                    last_modified: chrono::DateTime::from_timestamp(0, 0).unwrap(),
                    size: size as u64,
                    e_tag: None,
                    version: None,
                },
                row_base: 0,
                num_rows: 2,
                row_group_row_counts: vec![2],
                access_plan: None,
            },
        ));
    }
    (out, ObjectStoreUrl::local_filesystem())
}

fn build_catalog(shards: &[(i32, ShardFileInfo)], store_url: &ObjectStoreUrl) -> ShardCatalog {
    let mut cat = ShardCatalog::new();
    for (sid, fi) in shards {
        cat.insert(
            *sid,
            ShardEntry { files: Arc::new(vec![fi.clone()]), store_url: store_url.clone() },
        );
    }
    cat
}

#[tokio::test]
async fn phase2_real_tcp_distributed_shard_scan() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let (shards, store_url) = write_shards(dir.path());
    let shard_ids: Vec<i32> = shards.iter().map(|(s, _)| *s).collect();
    let dir_path = dir.path().to_path_buf();

    // ── Stand up NUM_SHARDS Worker gRPC servers on loopback TCP. Each worker session registers our
    //    codec + injects the ShardCatalog + the local object store (what the Java data node does in
    //    prod, here colocated). This is the real multi-process topology over real sockets.
    let listeners = futures::future::try_join_all(
        (0..NUM_SHARDS).map(|_| TcpListener::bind("127.0.0.1:0")),
    )
    .await?;
    let ports: Vec<u16> = listeners.iter().map(|l| l.local_addr().unwrap().port()).collect();

    for listener in listeners {
        let shards = shards.clone();
        let store_url = store_url.clone();
        let dir = dir_path.clone();
        let session_builder = move |ctx: WorkerQueryContext| {
            let shards = shards.clone();
            let store_url = store_url.clone();
            let dir = dir.clone();
            async move {
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
        tokio::spawn(async move {
            spawn_worker_service(session_builder, listener).await.unwrap();
        });
    }
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // ── Coordinator: real OsWorkerResolver with the loopback URLs + DefaultChannelResolver (native
    //    gRPC, NOT the in-memory duplex). No channel resolver override == library default == real TCP.
    let urls: Vec<Url> = ports
        .iter()
        .map(|p| Url::parse(&format!("http://localhost:{p}")).unwrap())
        .collect();
    let resolver = OsWorkerResolver::with_urls(urls);

    let mut coord_state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(4))
        .with_default_features()
        .with_distributed_worker_resolver(resolver)
        .with_distributed_user_codec(ShardScanCodec)
        .with_distributed_task_estimator(ShardScanTaskEstimator::new(shard_ids.clone()))
        .with_distributed_planner()
        .build();
    // Enable partial_reduce so the plan gains an intermediate PartialReduce above the hash repartition
    // (the high-cardinality intermediate-reduce). Asserted on the plan shape below.
    if let Ok(dcfg) =
        datafusion_distributed::DistributedConfig::from_config_options_mut(coord_state.config_mut().options_mut())
    {
        dcfg.partial_reduce = true;
    }
    let coord = SessionContext::from(coord_state);

    // Drive the real Substrait entry, just like Phase 1's production path.
    let query = "SELECT category, count(*) AS n, sum(amount) AS total \
                 FROM events GROUP BY category ORDER BY category";
    let producer = SessionContext::new();
    producer
        .register_parquet("events", dir_path.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    let logical = producer.sql(query).await?.into_optimized_plan()?;
    let substrait = datafusion_substrait::logical_plan::producer::to_substrait_plan(&logical, &producer.state())?;
    let mut plan_bytes = Vec::new();
    prost::Message::encode(&substrait, &mut plan_bytes)?;

    register_shard_tables(&coord, &plan_bytes, |_| "idx-uuid".to_string())?;
    let dplan = plan_distributed(&coord, &plan_bytes).await?;
    let plan_str = display_plan_ascii(dplan.as_ref(), false);
    println!("[phase2-tcp] distributed plan:\n{plan_str}");
    assert!(plan_str.contains("ShardScanExec") && plan_str.contains("NetworkShuffleExec"));
    // partial_reduce ON → an intermediate PartialReduce aggregate above the hash repartition, before
    // the shuffle (proves the high-cardinality intermediate-reduce is wired, not just configured).
    assert!(
        plan_str.contains("PartialReduce"),
        "partial_reduce should insert a PartialReduce aggregate before the shuffle; plan:\n{plan_str}"
    );

    let dbatches = execute_stream(dplan, coord.task_ctx())?.try_collect::<Vec<_>>().await?;
    let dout = pretty_format_batches(&dbatches)?.to_string();
    println!("[phase2-tcp] distributed result (over real TCP):\n{dout}");

    // Single-node reference.
    let sctx = SessionContext::new();
    sctx.register_parquet("events", dir_path.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    let sout = pretty_format_batches(
        &execute_stream(sctx.sql(query).await?.create_physical_plan().await?, sctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?,
    )?
    .to_string();

    assert_eq!(dout, sout, "real-TCP distributed result must equal single-node");
    Ok(())
}

/// Regression: a GLOBAL window (`sum(amount) OVER ()`) followed by `ORDER BY amount`, executed over
/// real TCP workers, must return rows in the SAME order as single-node. The distributed plan gathers
/// per-shard leaves via `NetworkCoalesceExec` (interleaving task outputs), so the final top-level
/// `SortExec` on the coordinator is what establishes the global order — this proves it actually runs
/// (not just that the plan shape places it correctly). Exercises the `ForceDistributeLeaf` rule: a
/// global window has no natural shuffle/coalesce seam, so without the rule the leaf would execute
/// unassigned on the coordinator (which hosts no shards). Mirrors the `eventstats | sort` IT.
#[tokio::test]
async fn phase2_global_window_then_sort_orders_on_coordinator() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let (shards, store_url) = write_shards(dir.path());
    let shard_ids: Vec<i32> = shards.iter().map(|(s, _)| *s).collect();
    let dir_path = dir.path().to_path_buf();

    let listeners = futures::future::try_join_all((0..NUM_SHARDS).map(|_| TcpListener::bind("127.0.0.1:0"))).await?;
    let ports: Vec<u16> = listeners.iter().map(|l| l.local_addr().unwrap().port()).collect();
    for listener in listeners {
        let shards = shards.clone();
        let store_url = store_url.clone();
        let dir = dir_path.clone();
        let session_builder = move |ctx: WorkerQueryContext| {
            let shards = shards.clone();
            let store_url = store_url.clone();
            let dir = dir.clone();
            async move {
                let state = ctx.builder.with_distributed_user_codec(ShardScanCodec).build();
                let sc = SessionContext::from(state);
                sc.state_ref().write().config_mut().set_extension(Arc::new(build_catalog(&shards, &store_url)));
                let store = Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());
                sc.runtime_env().register_object_store(store_url.as_ref(), store);
                Ok::<SessionState, DataFusionError>(sc.state())
            }
        };
        tokio::spawn(async move {
            spawn_worker_service(session_builder, listener).await.unwrap();
        });
    }
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let urls: Vec<Url> = ports.iter().map(|p| Url::parse(&format!("http://localhost:{p}")).unwrap()).collect();
    let resolver = OsWorkerResolver::with_urls(urls);

    // Build the coordinator with the SAME recipe production uses, incl. ForceDistributeLeaf (so the
    // global-window leaf distributes and gathers via NetworkCoalesce).
    let coord_state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(4))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(
            crate::distributed::force_distribute_leaf::ForceDistributeLeaf::new(NUM_SHARDS),
        ))
        .with_distributed_worker_resolver(resolver)
        .with_distributed_user_codec(ShardScanCodec)
        .with_distributed_task_estimator(ShardScanTaskEstimator::new(shard_ids.clone()))
        .with_distributed_planner()
        .build();
    let coord = SessionContext::from(coord_state);

    let query = "SELECT category, amount, sum(amount) OVER () AS grand_total FROM events ORDER BY amount";
    let producer = SessionContext::new();
    producer.register_parquet("events", dir_path.to_str().unwrap(), ParquetReadOptions::default()).await?;
    let logical = producer.sql(query).await?.into_optimized_plan()?;
    let substrait = datafusion_substrait::logical_plan::producer::to_substrait_plan(&logical, &producer.state())?;
    let mut plan_bytes = Vec::new();
    prost::Message::encode(&substrait, &mut plan_bytes)?;

    register_shard_tables(&coord, &plan_bytes, |_| "idx-uuid".to_string())?;
    let dplan = plan_distributed(&coord, &plan_bytes).await?;
    println!("[phase2-window-sort] distributed plan:\n{}", display_plan_ascii(dplan.as_ref(), false));

    let dbatches = execute_stream(dplan, coord.task_ctx())?.try_collect::<Vec<_>>().await?;
    let dout = pretty_format_batches(&dbatches)?.to_string();
    println!("[phase2-window-sort] distributed result (over real TCP):\n{dout}");

    let sctx = SessionContext::new();
    sctx.register_parquet("events", dir_path.to_str().unwrap(), ParquetReadOptions::default()).await?;
    let sout = pretty_format_batches(
        &execute_stream(sctx.sql(query).await?.create_physical_plan().await?, sctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?,
    )?
    .to_string();

    assert_eq!(dout, sout, "global-window+sort distributed result must equal single-node (row order incl.)");
    Ok(())
}
