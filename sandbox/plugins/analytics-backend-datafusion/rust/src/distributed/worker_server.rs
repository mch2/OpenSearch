/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Data-node Worker gRPC server lifecycle.
//!
//! A long-lived `datafusion-distributed` `Worker` is started once per data node at plugin boot and
//! listens on its own gRPC port (separate from the OpenSearch transport port). The coordinator's
//! `DistributedExec` dials it directly (rust↔rust) to run leaf scans + shuffle stages.
//!
//! Shard→files resolution happens worker-side (the worker terminates the request). The files for a
//! given query+shard are published into a process-global [`ShardRegistry`] by Java before the query
//! executes (in production via the leaf FFM upcall after acquiring the Lucene reader; the registry
//! is the seam that decouples "where the worker runs" from "how it learns a shard's files"). The
//! per-query worker `SessionState` reads the registry and exposes the matching [`ShardCatalog`].

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_distributed::{DistributedExt, Worker, WorkerQueryContext};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::api::{DataFusionRuntime, ShardFileInfo};
use crate::distributed::codec::ShardScanCodec;
use crate::distributed::shard_catalog::{ShardCatalog, ShardEntry};
use crate::runtime_manager::RuntimeManager;

/// Process-global mapping of `query_id -> (shard_id -> ShardEntry)`. Java publishes a query's shard
/// files here before executing, and clears them when the query finishes. This is the worker-side
/// counterpart to the coordinator's per-query worker-URL feed.
pub struct ShardRegistry {
    by_query: RwLock<HashMap<i64, Arc<ShardCatalog>>>,
    /// Object store registered per query (file:// LocalFileSystem in tests; the shard's tiered store
    /// in prod). Keyed by query id; applied to the per-query worker runtime env.
    stores: RwLock<HashMap<i64, (ObjectStoreUrl, Arc<dyn object_store::ObjectStore>)>>,
}

static SHARD_REGISTRY: Lazy<ShardRegistry> = Lazy::new(|| ShardRegistry {
    by_query: RwLock::new(HashMap::new()),
    stores: RwLock::new(HashMap::new()),
});

pub fn shard_registry() -> &'static ShardRegistry {
    &SHARD_REGISTRY
}

impl ShardRegistry {
    /// Publish a query's full shard catalog (called by Java before execute).
    pub fn put_query(
        &self,
        query_id: i64,
        shards: Vec<(i32, Vec<ShardFileInfo>)>,
        store_url: ObjectStoreUrl,
        store: Arc<dyn object_store::ObjectStore>,
    ) {
        let mut cat = ShardCatalog::new();
        for (sid, files) in shards {
            cat.insert(sid, ShardEntry { files: Arc::new(files), store_url: store_url.clone() });
        }
        self.by_query.write().insert(query_id, Arc::new(cat));
        self.stores.write().insert(query_id, (store_url, store));
    }

    pub fn catalog(&self, query_id: i64) -> Option<Arc<ShardCatalog>> {
        self.by_query.read().get(&query_id).cloned()
    }

    pub fn store(&self, query_id: i64) -> Option<(ObjectStoreUrl, Arc<dyn object_store::ObjectStore>)> {
        self.stores.read().get(&query_id).cloned()
    }

    pub fn clear_query(&self, query_id: i64) {
        self.by_query.write().remove(&query_id);
        self.stores.write().remove(&query_id);
    }
}

/// Handle to a running Worker gRPC server. Dropping/`stop` fires graceful shutdown.
pub struct WorkerServerHandle {
    shutdown: Option<oneshot::Sender<()>>,
    pub port: u16,
}

impl WorkerServerHandle {
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for WorkerServerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Builds the per-query worker `SessionState`: registers our codec, then reads the process-global
/// [`ShardRegistry`] for this query's catalog + object store. The query id is carried in a gRPC
/// header / config extension by the coordinator; for the registry lookup we use the
/// `DistributedTaskContext`-independent query id threaded through the session config extension
/// `WorkerQueryId` (set from the coordinator's propagated headers).
async fn build_worker_session(
    ctx: WorkerQueryContext,
    runtime_env: Arc<RuntimeEnv>,
) -> Result<datafusion::execution::SessionState, DataFusionError> {
    // The coordinator propagates the query id via a passthrough header; datafusion-distributed
    // surfaces headers on WorkerQueryContext. Fall back to 0 (tests inject the catalog directly).
    let query_id: i64 = ctx
        .headers
        .get("x-opensearch-query-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let state = ctx
        .builder
        .with_runtime_env(Arc::clone(&runtime_env))
        .with_distributed_user_codec(ShardScanCodec)
        .build();
    let sc = SessionContext::from(state);

    // Carry the query id so ShardScanExec::execute can lazily upcall the JVM to resolve a shard's
    // files on a catalog miss (the registry may be empty at session-build time when resolution is
    // driven lazily per shard).
    sc.state_ref().write().config_mut().set_extension(Arc::new(WorkerQueryId(query_id)));

    // Eager path: if Java already published this query's shards (e.g. tests, or a pre-publish hook),
    // install the catalog + object store now. Otherwise ShardScanExec resolves lazily via upcall.
    if let Some(catalog) = shard_registry().catalog(query_id) {
        sc.state_ref().write().config_mut().set_extension(catalog);
    }
    if let Some((url, store)) = shard_registry().store(query_id) {
        sc.runtime_env().register_object_store(url.as_ref(), store);
    }
    Ok(sc.state())
}

/// Session-config extension carrying the query id to `ShardScanExec::execute` for lazy upcall.
#[derive(Debug, Clone, Copy)]
pub struct WorkerQueryId(pub i64);

/// Starts the Worker gRPC server on the node IO runtime, binding `bind_port` (0 = ephemeral).
/// Writes the bound port to `out_port`. Returns a boxed [`WorkerServerHandle`] pointer.
///
/// # Safety
/// `out_port` must be a valid writable `*mut i32`.
pub unsafe fn start_worker(
    runtime: &DataFusionRuntime,
    bind_port: i32,
    out_port: *mut i32,
    manager: &RuntimeManager,
) -> Result<i64, DataFusionError> {
    let runtime_env = Arc::new(runtime.runtime_env.clone());

    // Bind on the IO runtime so the listener + server share that reactor.
    let listener = manager
        .io_runtime
        .block_on(async move { TcpListener::bind(("0.0.0.0", bind_port as u16)).await })
        .map_err(|e| DataFusionError::Execution(format!("worker bind failed: {e}")))?;
    let port = listener
        .local_addr()
        .map_err(|e| DataFusionError::Execution(format!("worker local_addr failed: {e}")))?
        .port();
    if !out_port.is_null() {
        *out_port = port as i32;
    }

    let session_runtime = Arc::clone(&runtime_env);
    let worker = Worker::from_session_builder(move |ctx: WorkerQueryContext| {
        let rt = Arc::clone(&session_runtime);
        async move { build_worker_session(ctx, rt).await }
    })
    .with_runtime_env(runtime_env);

    let (tx, rx) = oneshot::channel::<()>();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    manager.io_runtime.spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(worker.into_worker_server())
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = rx.await;
            })
            .await
        {
            log::error!("worker gRPC server exited with error: {e}");
        }
    });

    let handle = WorkerServerHandle { shutdown: Some(tx), port };
    Ok(Box::into_raw(Box::new(handle)) as i64)
}

/// Stops + frees a worker server handle.
///
/// # Safety
/// `ptr` must be 0 or a pointer returned by [`start_worker`].
pub unsafe fn stop_worker(ptr: i64) {
    if ptr != 0 {
        let mut handle = Box::from_raw(ptr as *mut WorkerServerHandle);
        handle.stop();
    }
}
