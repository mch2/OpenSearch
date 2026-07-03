/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Distributed execution path built on the `datafusion-distributed` library.
//!
//! This module replaces the Java-side distributed planner/scheduler: the coordinator builds a
//! single physical plan from whole-query Substrait, and `datafusion-distributed` cuts it into
//! MPP stages, dispatching per-shard leaf tasks to workers. The pieces here are the integration
//! seams the library asks us to implement:
//!
//! - [`shard_scan_exec::ShardScanExec`] — leaf placeholder carrying SHARD IDENTITY only (index +
//!   shard id), never file names. Files are resolved worker-side from the [`shard_catalog`].
//! - [`codec::ShardScanCodec`] — serializes `ShardScanExec` across the stage boundary.
//! - [`shard_task_estimator::ShardScanTaskEstimator`] — fans the leaf out to one task per shard
//!   and (via `route_tasks`) pins each shard-task to the node hosting it.
//! - [`shard_catalog::ShardCatalog`] — a `SessionConfig` extension mapping shard id → files +
//!   object store. In production this is injected by the Java data node after it acquires the
//!   shard reader; in tests it is populated directly.
//!
//! Phase 1 (current): exercised over direct rust↔rust gRPC with the library's localhost workers,
//! reading real parquet via [`crate::shard_table_provider::ShardTableProvider`]. No Java tunnel
//! and no Lucene delegation yet — those are Phases 2 and 3.

pub mod codec;
pub mod coordinator;
pub mod flight_shard_scan_exec;
pub mod flight_task_estimator;
pub mod leaf_bridge;
pub mod leaf_stream;
pub mod shard_catalog;
pub mod shard_scan_exec;
pub mod shard_task_estimator;
pub mod worker_resolver;
pub mod worker_server;

// The e2e test needs datafusion-distributed's test_utils (InMemoryChannelResolver), which only
// exist under that crate's `integration` feature — surfaced here via our `spike_integration`
// feature. Gated so the normal build/test of this crate doesn't require the test harness.
#[cfg(all(test, feature = "spike_integration"))]
mod distributed_e2e_test;

#[cfg(all(test, feature = "spike_integration"))]
mod distributed_tcp_test;

#[cfg(all(test, feature = "spike_integration"))]
mod distributed_ffm_test;

#[cfg(all(test, feature = "spike_integration"))]
mod flight_leaf_test;
