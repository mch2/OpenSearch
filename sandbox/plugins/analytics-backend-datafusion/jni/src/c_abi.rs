/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Plain `extern "C"` exports for coordinator-local execution.
//!
//! Consumed by Java via JDK 25 FFM (`java.lang.foreign.Linker`).
//! **No JNI**: no `JNIEnv`, no `JClass`, no `Java_` prefixed names, no `jni::*` imports.
//!
//! All functions use `#[no_mangle] pub extern "C"` with the `analytics_` prefix.
//! Error handling: return 0 on failure, log via `log::error!`.
//! All function bodies are wrapped in `std::panic::catch_unwind` to prevent
//! unwinding across the FFI boundary (which is UB).

use std::os::raw::c_int;
use std::panic;
use std::slice;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use log::error;

use crate::runtime_manager::RuntimeManager;
use crate::session_registry;

/// Helper: get the tokio runtime for blocking operations.
fn get_runtime() -> Option<&'static Arc<RuntimeManager>> {
    crate::TOKIO_RUNTIME_MANAGER.get()
}

/// Create a local session context. Returns a session handle (> 0) or 0 on failure.
#[no_mangle]
pub extern "C" fn analytics_create_local_session() -> i64 {
    match panic::catch_unwind(|| session_registry::create_session()) {
        Ok(handle) => handle,
        Err(e) => {
            error!("analytics_create_local_session panicked: {:?}", e);
            0
        }
    }
}

/// Create an FfiPartitionStream under a session. Registers it under the given
/// stageInputId (used later as the table name in the Substrait plan).
/// Returns a sender handle (> 0) or 0 on failure.
///
/// # Safety
/// `stage_input_id_ptr` must point to `stage_input_id_len` valid UTF-8 bytes.
/// `schema_ipc_ptr` must point to `schema_ipc_len` valid Arrow Schema IPC bytes.
#[no_mangle]
pub unsafe extern "C" fn analytics_create_partition_stream(
    session_handle: i64,
    stage_input_id_ptr: *const u8,
    stage_input_id_len: c_int,
    schema_ipc_ptr: *const u8,
    schema_ipc_len: c_int,
) -> i64 {
    let result = panic::catch_unwind(|| {
        // Parse stage input ID
        let id_bytes = unsafe {
            slice::from_raw_parts(stage_input_id_ptr, stage_input_id_len as usize)
        };
        let stage_input_id = std::str::from_utf8(id_bytes).map_err(|e| {
            DataFusionError::Execution(format!("Invalid UTF-8 in stage_input_id: {}", e))
        })?;

        // Parse Arrow schema from IPC bytes
        let schema_bytes = unsafe {
            slice::from_raw_parts(schema_ipc_ptr, schema_ipc_len as usize)
        };
        let schema = decode_schema_from_ipc(schema_bytes)?;

        session_registry::create_partition_stream(
            session_handle,
            stage_input_id,
            Arc::new(schema),
        )
    });

    match result {
        Ok(Ok(handle)) => handle,
        Ok(Err(e)) => {
            error!("analytics_create_partition_stream failed: {}", e);
            0
        }
        Err(e) => {
            error!("analytics_create_partition_stream panicked: {:?}", e);
            0
        }
    }
}

/// Push a record batch via Arrow C Data Interface.
///
/// # Safety
/// `array_ptr` and `schema_ptr` must be valid Arrow C Data Interface pointers
/// (produced by Arrow Java's `ArrowArray.allocateNew` + `Data.exportVectorSchemaRoot`).
#[no_mangle]
pub unsafe extern "C" fn analytics_push_batch(
    sender_handle: i64,
    array_ptr: i64,
    schema_ptr: i64,
) {
    let result = panic::catch_unwind(|| {
        // Import via Arrow C Data Interface — same pattern as parquet-data-format/writer.rs
        let arrow_schema = unsafe { FFI_ArrowSchema::from_raw(schema_ptr as *mut _) };
        let arrow_array = unsafe { FFI_ArrowArray::from_raw(array_ptr as *mut _) };
        let array_data = arrow::ffi::from_ffi(arrow_array, &arrow_schema)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let array = arrow::array::make_array(array_data);

        // Convert StructArray → RecordBatch
        let struct_array = array
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "Expected StructArray from Arrow C Data Interface".to_string(),
                )
            })?;
        let schema = Arc::new(Schema::new(struct_array.fields().clone()));
        let batch = RecordBatch::try_new(schema, struct_array.columns().to_vec())
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        session_registry::push_batch(sender_handle, batch)
    });

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            error!("analytics_push_batch failed: {}", e);
        }
        Err(e) => {
            error!("analytics_push_batch panicked: {:?}", e);
        }
    }
}

/// Close a partition stream's sender (EOF). Idempotent.
#[no_mangle]
pub extern "C" fn analytics_close_partition_stream(sender_handle: i64) {
    let result = panic::catch_unwind(|| {
        session_registry::close_sender(sender_handle);
    });
    if let Err(e) = result {
        error!("analytics_close_partition_stream panicked: {:?}", e);
    }
}

/// Execute the Substrait plan under the session, substituting registered
/// FfiPartitionStreams for any table reference whose name matches a registered
/// stage_input_id. Returns an output stream handle (> 0) or 0 on failure.
///
/// # Safety
/// `substrait_ptr` must point to `substrait_len` valid Substrait plan bytes.
#[no_mangle]
pub unsafe extern "C" fn analytics_execute_local_plan(
    session_handle: i64,
    substrait_ptr: *const u8,
    substrait_len: c_int,
) -> i64 {
    let result = panic::catch_unwind(|| {
        let bytes = unsafe {
            slice::from_raw_parts(substrait_ptr, substrait_len as usize)
        };

        // We need a tokio runtime to drive the async plan execution.
        // Reuse the existing RuntimeManager's IO runtime.
        let manager = get_runtime().ok_or_else(|| {
            DataFusionError::Execution(
                "Tokio runtime not initialized; call initTokioRuntimeManager first".to_string(),
            )
        })?;

        manager
            .io_runtime
            .block_on(crate::local_executor::execute_local_plan(
                session_handle,
                bytes,
            ))
    });

    match result {
        Ok(Ok(handle)) => handle,
        Ok(Err(e)) => {
            error!("analytics_execute_local_plan failed: {}", e);
            0
        }
        Err(e) => {
            error!("analytics_execute_local_plan panicked: {:?}", e);
            0
        }
    }
}

/// Drop a session and all its resources. Idempotent.
#[no_mangle]
pub extern "C" fn analytics_drop_local_session(session_handle: i64) {
    let result = panic::catch_unwind(|| {
        session_registry::drop_session(session_handle);
    });
    if let Err(e) = result {
        error!("analytics_drop_local_session panicked: {:?}", e);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Decode an Arrow Schema from IPC bytes.
///
/// The Java side serializes the schema using Arrow's IPC format.
/// We decode it using `arrow::ipc::convert::try_schema_from_ipc_buffer`.
fn decode_schema_from_ipc(bytes: &[u8]) -> Result<Schema, DataFusionError> {
    arrow::ipc::convert::try_schema_from_ipc_buffer(bytes)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}
