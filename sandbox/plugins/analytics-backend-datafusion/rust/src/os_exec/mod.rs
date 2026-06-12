/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Custom physical leaves that travel in serialized stage plans
//! (df-proto-migration-implementation-spec.md §5).
//!
//! - [`shard_scan_exec::OpenSearchShardScanExec`] — replaces the legacy
//!   `ShardScanWithDelegation` instruction + `DelegationDescriptor` at a shard
//!   stage's leaf.
//! - [`stage_read_exec::StageReadExec`] — replaces the legacy
//!   `OpenSearchStageInputScan` → `input-<childStageId>` `StreamingTable` read
//!   at a reduce stage's leaf.
//!
//! Both serialize through [`crate::os_codec::OpenSearchExtensionCodec`].

pub mod pushdown_stub;
pub mod shard_scan_exec;
pub mod stage_read_exec;

pub use pushdown_stub::PushdownStubProvider;
pub use shard_scan_exec::{DelegatedExpr, OpenSearchShardScanExec, ShardScanConfig};
pub use stage_read_exec::StageReadExec;
