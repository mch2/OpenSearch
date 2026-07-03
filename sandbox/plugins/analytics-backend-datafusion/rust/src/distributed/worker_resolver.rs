/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `WorkerResolver` backed by the set of data-node gRPC URLs. In production Java pushes the
//! current data-node addresses (from `ShardTargetResolver`'s `searchShards` / cluster state) into
//! this resolver per query before planning; `get_urls` is synchronous (the library calls it during
//! planning and right before execution), so the URL set is snapshotted, never blocking on cluster
//! state from inside the resolver.

use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion_distributed::WorkerResolver;
use parking_lot::RwLock;
use url::Url;

/// Holds the current data-node worker URLs. Cheap to clone (shares the inner `RwLock`), so the
/// same handle can be registered on the coordinator `SessionState` and updated as the cluster
/// membership changes between queries.
#[derive(Clone, Default)]
pub struct OsWorkerResolver {
    urls: Arc<RwLock<Vec<Url>>>,
}

impl std::fmt::Debug for OsWorkerResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OsWorkerResolver")
            .field("urls", &self.urls.read().len())
            .finish()
    }
}

impl OsWorkerResolver {
    pub fn new() -> Self {
        Self { urls: Arc::new(RwLock::new(Vec::new())) }
    }

    /// Construct from an initial set of URLs (tests / static clusters).
    pub fn with_urls(urls: Vec<Url>) -> Self {
        Self { urls: Arc::new(RwLock::new(urls)) }
    }

    /// Replace the worker URL set. Called from Java (via FFM) per query when cluster membership
    /// may have changed. Parsing is done here so a bad URL surfaces at set time, not plan time.
    pub fn set_urls_from_strs(&self, urls: &[String]) -> Result<(), DataFusionError> {
        let parsed = urls
            .iter()
            .map(|s| Url::parse(s).map_err(|e| DataFusionError::External(Box::new(e))))
            .collect::<Result<Vec<_>, _>>()?;
        *self.urls.write() = parsed;
        Ok(())
    }

    pub fn set_urls(&self, urls: Vec<Url>) {
        *self.urls.write() = urls;
    }
}

impl WorkerResolver for OsWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self.urls.read().clone())
    }
}
