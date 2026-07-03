/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Production-path integration test: drives the EXACT functions the Java NativeBridge will call —
//! `create_global_runtime`, `worker_server::start_worker` (df_create_worker body),
//! `shard_registry().put_query` (df_worker_put_shards body), and `coordinator::distributed_execute`
//! (df_distributed_execute body) — then drains the returned stream handle via `api::stream_next`
//! exactly like `DatafusionResultStream` does. Workers run as real gRPC servers over loopback TCP.
//!
//! This is the Rust half of the Java integration, proven before the Java is written. If this passes,
//! the remaining work is purely Java (NativeBridge bindings + DefaultPlanExecutor branch + plugin
//! lifecycle + shard-map feed), all of which call into exactly these functions.

#![cfg(all(test, feature = "spike_integration"))]

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use prost::Message;

use crate::api::{DataFusionRuntime, ShardFileInfo};
use crate::distributed::coordinator::distributed_execute;
use crate::distributed::worker_server::{shard_registry, start_worker, stop_worker};
use crate::runtime_manager::RuntimeManager;

const NUM_SHARDS: usize = 3;
const QUERY_ID: i64 = 4242;

fn events_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]))
}

fn write_shards(dir: &std::path::Path) -> Vec<(i32, ShardFileInfo)> {
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
                    size,
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
    out
}

/// Build whole-query Substrait the way Java will (single logical aggregate, no pre-split).
fn build_substrait(query: &str, dir: &std::path::Path) -> Vec<u8> {
    // A blocking tokio runtime just for substrait production (register_parquet is async).
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let ctx = SessionContext::new();
        ctx.register_parquet("events", dir.to_str().unwrap(), datafusion::prelude::ParquetReadOptions::default())
            .await
            .unwrap();
        let logical = ctx.sql(query).await.unwrap().into_optimized_plan().unwrap();
        let substrait = to_substrait_plan(&logical, &ctx.state()).unwrap();
        let mut buf = Vec::new();
        substrait.encode(&mut buf).unwrap();
        buf
    })
}

#[test]
fn ffm_path_distributed_execute_end_to_end() {
    // ── Node bootstrap: RuntimeManager (the df_init_runtime_manager body) + a DataFusionRuntime
    //    (the df_create_global_runtime body). Both are what Java sets up once per node.
    super::super::ffm::df_init_runtime_manager(2, 1.5, 1.5);
    // 256MB pool (0 = zero-byte pool -> the aggregate would exhaust memory with spilling disabled).
    let runtime_ptr = crate::api::create_global_runtime(256 * 1024 * 1024, 0, "", 0).expect("create_global_runtime");
    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };
    let mgr = crate::ffm::try_get_rt_manager().expect("runtime manager");

    let dir = tempfile::tempdir().unwrap();
    let shards = write_shards(dir.path());
    let dir_path = dir.path().to_path_buf();

    // ── Start NUM_SHARDS Worker gRPC servers on loopback TCP (df_create_worker body). Each shares
    //    the node DataFusionRuntime. Collect their bound ports.
    let mut worker_handles = Vec::new();
    let mut worker_urls = Vec::new();
    for _ in 0..NUM_SHARDS {
        let mut port: i32 = 0;
        let h = unsafe { start_worker(runtime, 0, &mut port as *mut i32, &mgr) }.expect("start_worker");
        worker_handles.push(h);
        worker_urls.push(format!("http://127.0.0.1:{port}"));
    }
    // Give the servers a moment to begin accepting.
    std::thread::sleep(std::time::Duration::from_millis(200));

    // ── Publish this query's shards into the worker-side registry (df_worker_put_shards body). The
    //    in-process workers share the same process-global registry, so all see QUERY_ID's shards.
    let store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&dir_path).unwrap());
    shard_registry().put_query(
        QUERY_ID,
        shards.iter().map(|(sid, fi)| (*sid, vec![fi.clone()])).collect(),
        datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
        store,
    );

    // ── Build whole-query Substrait (Java's emission) + the shard→worker affinity map.
    let query = "SELECT category, count(*) AS n, sum(amount) AS total FROM events GROUP BY category ORDER BY category";
    let substrait = build_substrait(query, &dir_path);
    let shard_ids: Vec<i32> = shards.iter().map(|(s, _)| *s).collect();
    let task_to_worker: Vec<usize> = (0..NUM_SHARDS).collect(); // shard i -> worker i
    // Single-table routing → empty-key entry (the fallback the coordinator uses for every leaf).
    let mut by_table = std::collections::HashMap::new();
    by_table.insert(
        String::new(),
        crate::distributed::shard_task_estimator::TableRouting { shard_ids, task_to_worker },
    );
    let mut index_uuid_by_table = std::collections::HashMap::new();
    index_uuid_by_table.insert(String::new(), "idx-uuid".to_string());

    // ── Run distributed_execute (df_distributed_execute body) on the IO runtime, exactly as the FFM
    //    entry does. NOTE: the worker session needs QUERY_ID; the in-process registry is keyed by it
    //    and the test workers default to query_id 0 unless the coordinator propagates the header. For
    //    this in-process test we also register under 0 so the header-less worker path resolves.
    shard_registry().put_query(
        0,
        shards.iter().map(|(sid, fi)| (*sid, vec![fi.clone()])).collect(),
        datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&dir_path).unwrap()),
    );

    let stream_ptr = mgr
        .io_runtime
        .block_on(async {
            distributed_execute(
                runtime,
                &substrait,
                worker_urls.clone(),
                by_table,
                index_uuid_by_table,
                &mgr,
                QUERY_ID,
                None,       // no delegation (plain scan)
                Vec::new(), // no leaf fragment
                0,          // tree_shape
                0,          // predicate_count
                true,       // partial_reduce
                0.0,        // cardinality_task_count_factor (0 = library default)
                0,          // max_tasks_per_stage (0 = inherit worker count)
            )
            .await
        })
        .expect("distributed_execute");
    assert!(stream_ptr > 0, "distributed_execute must return a valid stream handle");

    // ── Drain the stream handle exactly like DatafusionResultStream: fetch the schema once, then
    //    stream_next (async, run on the IO runtime) until it returns 0 (EOF). Import each batch via
    //    Arrow C-Data and tally — mirrors the Java-side import.
    use arrow::array::StructArray;
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};

    let schema_ptr = unsafe { crate::api::stream_get_schema(stream_ptr) }.expect("stream_get_schema");
    let ffi_schema = unsafe { Box::from_raw(schema_ptr as *mut FFI_ArrowSchema) };
    let arrow_schema = ArrowSchema::try_from(ffi_schema.as_ref()).expect("schema import");
    let total_col = arrow_schema.index_of("total").ok();

    let mut total_rows = 0usize;
    let mut total_amount = 0i64;
    loop {
        let batch_ptr = mgr
            .io_runtime
            .block_on(async { unsafe { crate::api::stream_next(stream_ptr).await } })
            .expect("stream_next");
        if batch_ptr == 0 {
            break;
        }
        let ffi_array = unsafe { Box::from_raw(batch_ptr as *mut FFI_ArrowArray) };
        // Re-derive a schema FFI for each import (from_ffi consumes a schema). Build from arrow_schema.
        let schema_for_import = FFI_ArrowSchema::try_from(&arrow_schema).expect("schema ffi");
        let data = unsafe { from_ffi(*ffi_array, &schema_for_import) }.expect("from_ffi");
        let batch = RecordBatch::from(StructArray::from(data));
        total_rows += batch.num_rows();
        if let Some(idx) = total_col {
            if let Some(a) = batch.column(idx).as_any().downcast_ref::<Int64Array>() {
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        total_amount += a.value(i);
                    }
                }
            }
        }
    }
    unsafe { crate::api::stream_close(stream_ptr) };

    // a:10+30+60=100, b:20+50=70, c:40 -> 3 rows, total 210.
    assert_eq!(total_rows, 3, "3 category groups");
    assert_eq!(total_amount, 210, "sum of all amounts across shards");

    // ── Teardown.
    for h in worker_handles {
        unsafe { stop_worker(h) };
    }
    shard_registry().clear_query(QUERY_ID);
    shard_registry().clear_query(0);
    crate::ffm::df_shutdown_runtime_manager();
}
