/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Plan-shape verification for the distributed engine. `plan_distributed` builds the full
//! `DistributedExec` physical plan entirely in-memory — no workers are contacted at planning time —
//! so this test is deterministic and needs no cluster or sockets. It exercises the EXACT production
//! coordinator recipe (`build_coordinator_context`, driven by the same tuning knobs the
//! `analytics.query.distributed.*` cluster settings feed) and asserts, per query shape, that:
//!   * the reduce is DISTRIBUTED — a `NetworkShuffleExec` sits between the partial and final halves,
//!     so the final aggregate/window/sort runs across workers, and
//!   * there is NO premature gather-to-coordinator — the head `NetworkCoalesceExec` sits ABOVE a
//!     partitioned final stage (it coalesces an already-reduced result), never directly above the
//!     per-shard leaves.
//!
//! This is the Rust half of task "verify the execution flow / mode" — the analogue of the Java
//! plan-shape golden ITs, but for the distributed physical plan the library actually runs. It covers
//! the gather-prone shapes the user called out: high-cardinality GROUP BY, window functions,
//! global (ungrouped) aggregates, and ORDER BY.

#![cfg(all(test, feature = "spike_integration"))]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_distributed::display_plan_ascii;

use crate::api::DataFusionRuntime;
use crate::distributed::coordinator::{
    build_coordinator_context, plan_distributed, register_shard_tables,
};
use crate::distributed::shard_task_estimator::TableRouting;

const NUM_SHARDS: usize = 3;

fn events_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]))
}

/// Write NUM_SHARDS single-batch parquet files, returning the dir. Only needed so the Substrait
/// producer can register a real `events` table; the distributed planner never reads the files.
fn write_shards(dir: &std::path::Path) {
    let schema = events_schema();
    let data = [
        (vec!["a", "b"], vec![10i64, 20]),
        (vec!["a", "c"], vec![30i64, 40]),
        (vec!["b", "a"], vec![50i64, 60]),
    ];
    for (i, (cats, amts)) in data.iter().enumerate() {
        let path = dir.join(format!("shard-{i}.parquet"));
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
    }
}

/// Turn a SQL query into whole-query Substrait bytes, exactly as the Java coordinator emits.
async fn substrait_for(query: &str, dir: &std::path::Path) -> Vec<u8> {
    let producer = SessionContext::new();
    producer
        .register_parquet(
            "events",
            dir.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
    let logical = producer
        .sql(query)
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    let substrait = datafusion_substrait::logical_plan::producer::to_substrait_plan(
        &logical,
        &producer.state(),
    )
    .unwrap();
    let mut buf = Vec::new();
    prost::Message::encode(&substrait, &mut buf).unwrap();
    buf
}

/// Build the distributed physical plan for a query via the PRODUCTION coordinator recipe
/// (`build_coordinator_context` + `register_shard_tables` + `plan_distributed`), then render it.
/// `partial_reduce` mirrors the `analytics.query.distributed.partial_reduce` setting.
async fn plan_string_for(query: &str, partial_reduce: bool) -> String {
    plan_string_for_shards(query, partial_reduce, NUM_SHARDS).await
}

/// As [`plan_string_for`], but with `num_shards` shards routed round-robin onto `num_shards` workers.
/// `num_shards == 1` reproduces the single-shard cluster shape (the common IT dataset layout), where
/// the distributed planner would otherwise elide a 1-task network boundary and run the leaf on the
/// coordinator.
async fn plan_string_for_shards(query: &str, partial_reduce: bool, num_shards: usize) -> String {
    let dir = tempfile::tempdir().unwrap();
    write_shards(dir.path());
    let plan_bytes = substrait_for(query, dir.path()).await;

    // A minimal node runtime (bench constructor: no jemalloc pool wiring needed for planning).
    let runtime = DataFusionRuntime::new_for_bench(RuntimeEnvBuilder::new().build().unwrap());

    // `num_shards` synthetic worker URLs + single-table routing over `num_shards` shards, round-robin
    // onto the workers — the same shape DfShardRouting hands the FFM entry in prod.
    let worker_urls: Vec<String> = (0..num_shards)
        .map(|i| format!("http://127.0.0.1:{}", 9000 + i))
        .collect();
    let mut by_table = HashMap::new();
    by_table.insert(
        String::new(),
        TableRouting {
            shard_ids: (0..num_shards as i32).collect(),
            task_to_worker: (0..num_shards).collect(),
        },
    );

    let ctx = build_coordinator_context(
        &runtime,
        worker_urls,
        by_table,
        num_shards.max(1), // target_partitions
        4242,              // query_id
        partial_reduce,    // analytics.query.distributed.partial_reduce
        0.0,               // cardinality_task_count_factor (library default)
        0,                 // max_tasks_per_stage (inherit worker count)
        true,              // force_partitioned_joins
    )
    .unwrap();
    register_shard_tables(&ctx, &plan_bytes, |_| "idx-uuid".to_string()).unwrap();
    let dplan = plan_distributed(&ctx, &plan_bytes).await.unwrap();
    display_plan_ascii(dplan.as_ref(), false)
}

/// A distributed reduce must place a NetworkShuffleExec between the per-shard leaves and the final
/// aggregate/sort — so the final stage runs across workers, not gathered onto the coordinator.
fn assert_distributed_reduce(plan: &str, shape: &str) {
    assert!(
        plan.contains("ShardScanExec"),
        "[{shape}] expected per-shard ShardScanExec leaves; plan:\n{plan}"
    );
    assert!(
        plan.contains("NetworkShuffleExec"),
        "[{shape}] expected a NetworkShuffleExec (distributed reduce, no coordinator gather); plan:\n{plan}"
    );
    // The head NetworkCoalesceExec must appear ABOVE the shuffle, i.e. the shuffle line index precedes
    // the LAST coalesce — a coalesce directly over the leaves (gather-then-reduce) would invert this.
    let first_shuffle = plan.find("NetworkShuffleExec");
    let first_leaf = plan.find("ShardScanExec");
    if let (Some(sh), Some(leaf)) = (first_shuffle, first_leaf) {
        assert!(
            sh < leaf,
            "[{shape}] NetworkShuffleExec must sit ABOVE ShardScanExec (reduce distributed before the \
             leaves stream up), not below it; plan:\n{plan}"
        );
    }
}

#[tokio::test]
async fn group_by_distributes_the_final_aggregate() {
    let plan = plan_string_for(
        "SELECT category, count(*) AS n, sum(amount) AS total FROM events GROUP BY category",
        true,
    )
    .await;
    assert_distributed_reduce(&plan, "group-by");
    // partial_reduce ON → an intermediate PartialReduce aggregate above the hash repartition.
    assert!(
        plan.contains("PartialReduce"),
        "[group-by] partial_reduce should insert a PartialReduce before the shuffle; plan:\n{plan}"
    );
}

#[tokio::test]
async fn group_by_without_partial_reduce_still_distributes() {
    let plan = plan_string_for(
        "SELECT category, count(*) AS n FROM events GROUP BY category",
        false,
    )
    .await;
    // The reduce is still distributed across the shuffle; only the intermediate PartialReduce is gone.
    assert_distributed_reduce(&plan, "group-by-no-partial-reduce");
    assert!(
        plan.contains("PartialReduce") == false,
        "[group-by-no-partial-reduce] PartialReduce must be absent when the knob is off; plan:\n{plan}"
    );
}

#[tokio::test]
async fn window_function_distributes_over_a_shuffle() {
    // A partitioned window: the partition-by key drives a hash shuffle so each worker owns whole
    // partitions and the window runs distributed rather than gathered to the coordinator.
    let plan = plan_string_for(
        "SELECT category, amount, \
         sum(amount) OVER (PARTITION BY category) AS cat_total \
         FROM events",
        true,
    )
    .await;
    assert!(
        plan.contains("WindowAggExec") || plan.contains("BoundedWindowAggExec"),
        "[window] expected a WindowAggExec in the distributed plan; plan:\n{plan}"
    );
    assert!(
        plan.contains("NetworkShuffleExec"),
        "[window] a PARTITION BY window must shuffle on the partition key (no coordinator gather); plan:\n{plan}"
    );
    assert!(
        plan.contains("ShardScanExec"),
        "[window] expected per-shard leaves; plan:\n{plan}"
    );
}

#[tokio::test]
async fn order_by_over_group_by_still_distributes_the_reduce() {
    // ORDER BY forces a top-level sort on the coordinator, but the aggregate reduce underneath must
    // still be distributed across the shuffle — the sort coalesces an already-reduced result.
    let plan = plan_string_for(
        "SELECT category, sum(amount) AS total FROM events GROUP BY category ORDER BY category",
        true,
    )
    .await;
    assert_distributed_reduce(&plan, "order-by");
    assert!(
        plan.contains("SortExec"),
        "[order-by] expected a SortExec for ORDER BY; plan:\n{plan}"
    );
}

#[tokio::test]
async fn global_window_no_partition_key_still_distributes_leaves() {
    // A GLOBAL window (no PARTITION BY) — sum(amount) OVER () annotated onto every row. The window's
    // frame spans the whole table, so it collapses to a single partition, but the per-shard leaves must
    // STILL be fanned out (scale_up_leaf_node must pack shards into a DistributedLeafExec) — otherwise a
    // ShardScanExec reaches execute() unassigned. This reproduces the global-eventstats IT failure.
    let plan = plan_string_for(
        "SELECT category, amount, sum(amount) OVER () AS grand_total FROM events",
        true,
    )
    .await;
    assert!(
        plan.contains("ShardScanExec"),
        "[global-window] expected per-shard leaves; plan:\n{plan}"
    );
    assert!(
        plan.contains("DistributedLeafExec"),
        "[global-window] leaves must be wrapped in DistributedLeafExec (packed per task), else \
         ShardScanExec executes unassigned; plan:\n{plan}"
    );
    // ForceDistributeLeaf inserts the coalesce seam → the library turns it into a NetworkCoalesceExec,
    // so the leaf distributes and the window runs above a network stage (not on a bare coordinator leaf).
    assert!(
        plan.contains("NetworkCoalesceExec"),
        "[global-window] a NetworkCoalesceExec must gather the distributed leaves to the head; plan:\n{plan}"
    );
    let net = plan.find("NetworkCoalesceExec");
    let leaf = plan.find("ShardScanExec");
    if let (Some(n), Some(l)) = (net, leaf) {
        assert!(
            n < l,
            "[global-window] network stage must sit ABOVE the leaves; plan:\n{plan}"
        );
    }
}

#[tokio::test]
async fn global_window_then_order_by_sorts_on_coordinator() {
    // eventstats-then-sort shape: `sum(amount) OVER ()` annotated onto every row, THEN ORDER BY amount.
    // The global window collapses to one partition; the final sort must run ABOVE the network gather on
    // the coordinator so the output is globally ordered — if the optimizer pushes the sort into the
    // per-shard leaf stage, NetworkCoalesce concatenates task outputs and global order is lost.
    let plan = plan_string_for(
        "SELECT category, amount, sum(amount) OVER () AS grand_total FROM events ORDER BY amount",
        true,
    )
    .await;
    let sort = plan.find("SortExec");
    let net = plan
        .find("NetworkCoalesceExec")
        .or_else(|| plan.find("NetworkShuffleExec"));
    if let (Some(s), Some(n)) = (sort, net) {
        assert!(
            s < n,
            "[window+sort] the final SortExec must sit ABOVE the network gather (coordinator), else \
             the gather interleaves per-shard sorted runs and global order is lost; plan:\n{plan}"
        );
    } else {
        panic!("[window+sort] expected both a SortExec and a network stage; plan:\n{plan}");
    }
}

#[tokio::test]
async fn bare_projection_still_distributes_leaves() {
    // The simplest gather shape: a plain projection with no aggregate/window/sort. Nothing in the plan
    // naturally introduces a repartition or coalesce, so without ForceDistributeLeaf the ShardScanExec
    // would execute unassigned on the coordinator. The rule must still distribute + gather the leaves.
    let plan = plan_string_for("SELECT category, amount FROM events", true).await;
    assert!(
        plan.contains("ShardScanExec"),
        "[bare-projection] expected per-shard leaves; plan:\n{plan}"
    );
    assert!(
        plan.contains("DistributedLeafExec") && plan.contains("NetworkCoalesceExec"),
        "[bare-projection] leaves must distribute + gather via a network stage; plan:\n{plan}"
    );
}

#[tokio::test]
async fn single_shard_filter_still_distributes_leaf() {
    // SINGLE-SHARD index (the common IT dataset layout): `where amount > 0 | fields amount`. With one
    // shard the leaf stage is one task, and the library elides a 1-producer/1-consumer network boundary
    // — so ForceDistributeLeaf's coalesce evaporates and ShardScanExec runs UNASSIGNED on the
    // coordinator (which hosts no shards). This is the whole-suite sweep's #1 failure (70 tests).
    let plan = plan_string_for_shards("SELECT amount FROM events WHERE amount > 0", true, 1).await;
    assert!(plan.contains("ShardScanExec"), "[single-shard] expected the leaf; plan:\n{plan}");
    assert!(
        plan.contains("shard_ids=[0]"),
        "[single-shard] the leaf must be ASSIGNED to shard 0 (not shard_ids=[] running on the \
         coordinator); plan:\n{plan}"
    );
    assert!(
        plan.contains("NetworkCoalesceExec") || plan.contains("NetworkShuffleExec"),
        "[single-shard] a single-shard leaf must still cross a network stage to its worker, not run on \
         the coordinator; plan:\n{plan}"
    );
}

#[tokio::test]
async fn filter_then_projection_distributes_leaves() {
    // The WhereCommandIT shape: `where amount > 0 | fields amount` — a FilterExec + projection over a
    // bare scan, no aggregate/window/sort. Single-partition head with no natural seam; ForceDistributeLeaf
    // must still distribute + gather. This is the shape the whole-suite distributed sweep found failing.
    let plan = plan_string_for("SELECT amount FROM events WHERE amount > 0", true).await;
    eprintln!("[FILTER+PROJECTION PLAN]\n{plan}");
    assert!(plan.contains("ShardScanExec"), "[filter] expected per-shard leaves; plan:\n{plan}");
    assert!(
        plan.contains("DistributedLeafExec") && plan.contains("NetworkCoalesceExec"),
        "[filter] leaves must distribute + gather via a network stage; plan:\n{plan}"
    );
}

#[tokio::test]
async fn global_aggregate_gathers_only_partial_results() {
    // A global (ungrouped) aggregate legitimately ends in a single-partition final on the
    // coordinator, but the partials must still be computed per-shard and shuffled up — the
    // coordinator merges compact partial states, not raw rows. Assert the shuffle is present so the
    // per-shard partial work happens on the workers, not after a gather.
    let plan = plan_string_for(
        "SELECT count(*) AS n, sum(amount) AS total FROM events",
        true,
    )
    .await;
    assert!(
        plan.contains("ShardScanExec"),
        "[global-agg] expected per-shard leaves; plan:\n{plan}"
    );
    assert!(
        plan.contains("NetworkShuffleExec") || plan.contains("NetworkCoalesceExec"),
        "[global-agg] partial aggregates must travel a network stage to the coordinator, not raw rows; plan:\n{plan}"
    );
    // The final single-partition merge must sit ABOVE the network stage (partials computed on workers).
    let agg_final = plan.find("AggregateExec: mode=Final");
    let leaf = plan.find("ShardScanExec");
    if let (Some(f), Some(l)) = (agg_final, leaf) {
        assert!(
            f < l,
            "[global-agg] Final aggregate must be above the leaves; plan:\n{plan}"
        );
    }
}
