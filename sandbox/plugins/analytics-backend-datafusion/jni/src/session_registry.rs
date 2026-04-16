/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Global session and sender registry for coordinator-local execution.
//!
//! Each coordinator-local query creates a `SessionState` that holds:
//! - Registered `FfiPartitionStream`s (keyed by stage input ID)
//! - Sender handles (keyed by monotonic i64 handle)
//!
//! Handles are allocated from a global `AtomicI64` counter starting at 1.
//! Handle 0 is reserved as the "failure" sentinel returned to Java.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use once_cell::sync::Lazy;
use tokio::sync::mpsc;

use crate::ffi_partition_stream::FfiPartitionStream;

/// Monotonic handle counter. Starts at 1; 0 is the failure sentinel.
static NEXT_HANDLE: AtomicI64 = AtomicI64::new(1);

/// Allocate a fresh handle (always > 0).
fn next_handle() -> i64 {
    NEXT_HANDLE.fetch_add(1, Ordering::Relaxed)
}

/// Global registry: session handle → SessionState.
static SESSIONS: Lazy<Mutex<HashMap<i64, SessionState>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Per-session state for a coordinator-local execution.
pub struct SessionState {
    /// Registered partition streams, keyed by stage input ID (e.g. `__stage_0_input__`).
    pub partition_streams: HashMap<String, Arc<FfiPartitionStream>>,
    /// Sender handles → bounded mpsc senders. Java pushes batches through these.
    /// Uses `blocking_send` to provide backpressure when the channel is full.
    pub senders: HashMap<i64, mpsc::Sender<Result<RecordBatch, DataFusionError>>>,
    /// Output stream handle → SendableRecordBatchStream (set after execute).
    pub output_streams: HashMap<i64, SendableRecordBatchStream>,
}

impl SessionState {
    fn new() -> Self {
        Self {
            partition_streams: HashMap::new(),
            senders: HashMap::new(),
            output_streams: HashMap::new(),
        }
    }
}

/// Create a new session. Returns the session handle (> 0).
pub fn create_session() -> i64 {
    let handle = next_handle();
    let mut sessions = SESSIONS.lock().unwrap();
    sessions.insert(handle, SessionState::new());
    handle
}

/// Register a partition stream under a session. Returns the sender handle (> 0).
///
/// The `FfiPartitionStream` is stored keyed by `stage_input_id` for later
/// lookup during plan substitution. The sender is stored keyed by a fresh
/// handle that Java uses for `push_batch` / `close_partition_stream`.
pub fn create_partition_stream(
    session_handle: i64,
    stage_input_id: &str,
    schema: SchemaRef,
) -> Result<i64, DataFusionError> {
    let (stream, tx) = FfiPartitionStream::new(schema);
    let sender_handle = next_handle();

    let mut sessions = SESSIONS.lock().unwrap();
    let session = sessions.get_mut(&session_handle).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Session {} not found",
            session_handle
        ))
    })?;

    session
        .partition_streams
        .insert(stage_input_id.to_string(), Arc::new(stream));
    session.senders.insert(sender_handle, tx);

    Ok(sender_handle)
}

/// Push a record batch through a sender handle.
///
/// Uses `blocking_send` on the bounded channel. If the channel is full (all
/// slots occupied), the calling thread blocks until DataFusion's poll loop
/// consumes a batch. This provides backpressure to the Java transport thread.
///
/// **Must not be called from a tokio async context** — `blocking_send` panics
/// inside a tokio runtime. The caller (Java FFM downcall) runs on a plain
/// OS thread, so this is safe.
pub fn push_batch(
    sender_handle: i64,
    batch: RecordBatch,
) -> Result<(), DataFusionError> {
    // Clone the sender under the lock, then send outside the lock.
    // This avoids holding the global sessions lock while blocking on
    // a full channel (which would deadlock if the consumer also needs
    // the lock).
    let tx = {
        let sessions = SESSIONS.lock().unwrap();
        let mut found = None;
        for session in sessions.values() {
            if let Some(tx) = session.senders.get(&sender_handle) {
                found = Some(tx.clone());
                break;
            }
        }
        found.ok_or_else(|| {
            DataFusionError::Execution(format!("Sender handle {} not found", sender_handle))
        })?
    };
    // Send outside the lock — may block if channel is full.
    tx.blocking_send(Ok(batch)).map_err(|_| {
        DataFusionError::Execution("Channel closed; receiver dropped".to_string())
    })
}

/// Close (drop) a sender, signaling EOF to the receiver. Idempotent.
pub fn close_sender(sender_handle: i64) {
    let mut sessions = SESSIONS.lock().unwrap();
    for session in sessions.values_mut() {
        if session.senders.remove(&sender_handle).is_some() {
            return;
        }
    }
    // Already closed or never existed — idempotent.
}

/// Register an output stream under a session. Returns the output handle.
pub fn register_output_stream(
    session_handle: i64,
    stream: SendableRecordBatchStream,
) -> Result<i64, DataFusionError> {
    let output_handle = next_handle();
    let mut sessions = SESSIONS.lock().unwrap();
    let session = sessions.get_mut(&session_handle).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Session {} not found",
            session_handle
        ))
    })?;
    session.output_streams.insert(output_handle, stream);
    Ok(output_handle)
}

/// Get a reference to the session's partition streams (for plan substitution).
/// The caller must hold the lock for the duration of use.
pub fn with_session<F, R>(session_handle: i64, f: F) -> Result<R, DataFusionError>
where
    F: FnOnce(&SessionState) -> Result<R, DataFusionError>,
{
    let sessions = SESSIONS.lock().unwrap();
    let session = sessions.get(&session_handle).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Session {} not found",
            session_handle
        ))
    })?;
    f(session)
}

/// Drop a session and all its resources. Idempotent.
pub fn drop_session(session_handle: i64) {
    let mut sessions = SESSIONS.lock().unwrap();
    sessions.remove(&session_handle);
    // Dropping SessionState drops all senders (EOF), receivers (via Arc),
    // and output streams.
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
    }

    #[test]
    fn test_create_session_returns_positive_handle() {
        let h = create_session();
        assert!(h > 0);
    }

    #[test]
    fn test_create_partition_stream_returns_positive_handle() {
        let session = create_session();
        let sender = create_partition_stream(session, "__stage_0_input__", test_schema()).unwrap();
        assert!(sender > 0);
        drop_session(session);
    }

    #[test]
    fn test_close_sender_is_idempotent() {
        let session = create_session();
        let sender = create_partition_stream(session, "__stage_0_input__", test_schema()).unwrap();
        close_sender(sender);
        close_sender(sender); // second call is a no-op
        drop_session(session);
    }

    #[test]
    fn test_drop_session_is_idempotent() {
        let session = create_session();
        drop_session(session);
        drop_session(session); // second call is a no-op
    }

    #[test]
    fn test_push_batch_to_unknown_handle_errors() {
        let result = push_batch(999999, RecordBatch::new_empty(test_schema()));
        assert!(result.is_err());
    }
}
