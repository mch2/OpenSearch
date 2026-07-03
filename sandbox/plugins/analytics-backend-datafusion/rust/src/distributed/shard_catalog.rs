/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Worker-side resolution of a shard id to its concrete files + object store.
//!
//! `ShardScanExec` carries only `(table, index_uuid, shard_id)` over the wire — never file names.
//! At execution time the worker resolves those names locally. In production the Java data node
//! injects this mapping into the per-query `SessionConfig` after acquiring the shard reader (the
//! "Java terminates the leaf" contract); in Phase-1 tests it is populated directly. Either way the
//! coordinator never needs to know file names, matching today's shard-location-only model.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreUrl;

use crate::api::ShardFileInfo;

/// Everything needed to scan one shard locally: its file list (with row_base offsets and optional
/// QTF access plans) and the object-store URL the files live under.
#[derive(Clone)]
pub struct ShardEntry {
    pub files: Arc<Vec<ShardFileInfo>>,
    pub store_url: ObjectStoreUrl,
}

impl std::fmt::Debug for ShardEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardEntry")
            .field("files", &self.files.len())
            .field("store_url", &self.store_url.as_str())
            .finish()
    }
}

/// `SessionConfig` extension: maps a shard id to its [`ShardEntry`]. Keyed by the same integer
/// shard id that `ShardScanExec` carries and that `route_tasks` uses for placement.
#[derive(Debug, Default)]
pub struct ShardCatalog {
    shards: HashMap<i32, ShardEntry>,
}

impl ShardCatalog {
    pub fn new() -> Self {
        Self { shards: HashMap::new() }
    }

    pub fn insert(&mut self, shard_id: i32, entry: ShardEntry) {
        self.shards.insert(shard_id, entry);
    }

    pub fn get(&self, shard_id: i32) -> Option<&ShardEntry> {
        self.shards.get(&shard_id)
    }

    /// Number of registered shards — used by the TaskEstimator to size the leaf stage.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Shard ids in ascending order — deterministic task ordering.
    pub fn shard_ids(&self) -> Vec<i32> {
        let mut ids: Vec<i32> = self.shards.keys().copied().collect();
        ids.sort_unstable();
        ids
    }
}
