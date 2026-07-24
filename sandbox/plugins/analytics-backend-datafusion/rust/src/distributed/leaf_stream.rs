/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Case-3 pull stream: a DataFusion `RecordBatchStream` that PULLS Arrow batches from a Java-side
//! cursor on demand (`leaf_bridge::leaf_next`), importing each via the Arrow C-Data interface using
//! the leaf's known output schema. Closes the Java cursor on drop. This is the "Lucene executes /
//! Java produces" leaf input — DataFusion drives the pace via `poll_next` (no push into DF).

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::array::StructArray;
use arrow::datatypes::SchemaRef;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::RecordBatchStream;
use futures::Stream;

use crate::distributed::leaf_bridge;

/// Pull stream over a Java cursor (case 3). Each `poll_next` downcalls `leaf_next(cursor)`:
/// `Some(array_ptr)` → import one batch; `None` → EOS. The cursor is released on drop via
/// `leaf_close`.
pub struct JavaCursorStream {
    cursor: i64,
    schema: SchemaRef,
    done: bool,
}

impl JavaCursorStream {
    pub fn new(cursor: i64, schema: SchemaRef) -> Self {
        Self { cursor, schema, done: false }
    }

    /// Import one Java-produced batch (FFI_ArrowArray ptr) using the leaf's output schema, or —
    /// when `schema_ptr != 0` — the per-batch schema Java exported alongside it (dictionary-encoded
    /// keyword batches, dv.keyword_encoding=dictionary). A dictionary batch is CAST to the leaf's
    /// advertised schema after import so parent operators see the planned types; the dictionary
    /// still saves decode + transfer (the A/B instrument), while dictionary-native compute is the
    /// roadmap item this flag informs.
    ///
    /// # Safety
    /// `array_ptr` (and `schema_ptr` when non-zero) must be valid C-Data structs produced by the
    /// Java cursor's export; ownership of both transfers here.
    unsafe fn import(&self, array_ptr: i64, schema_ptr: i64) -> Result<RecordBatch, DataFusionError> {
        let ffi_array = FFI_ArrowArray::from_raw(array_ptr as *mut FFI_ArrowArray);
        let batch = if schema_ptr != 0 {
            let ffi_schema = FFI_ArrowSchema::from_raw(schema_ptr as *mut FFI_ArrowSchema);
            let mut array_data = arrow_array::ffi::from_ffi(ffi_array, &ffi_schema)
                .map_err(|e| DataFusionError::Execution(format!("leaf C-Data import (per-batch schema) failed: {e}")))?;
            array_data.align_buffers();
            RecordBatch::from(StructArray::from(array_data))
        } else {
            // The leaf schema is authoritative for the cursor's batches; build the FFI schema from
            // it rather than requiring Java to also ship a schema pointer per batch.
            let ffi_schema = FFI_ArrowSchema::try_from(self.schema.as_ref())
                .map_err(|e| DataFusionError::Execution(format!("leaf schema -> FFI failed: {e}")))?;
            let mut array_data = arrow_array::ffi::from_ffi(ffi_array, &ffi_schema)
                .map_err(|e| DataFusionError::Execution(format!("leaf C-Data import failed: {e}")))?;
            array_data.align_buffers();
            RecordBatch::from(StructArray::from(array_data))
        };
        if batch.schema() == self.schema {
            return Ok(batch);
        }
        // Column-wise cast to the advertised schema (Dictionary(Int32,Utf8) -> Utf8/Utf8View etc).
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (i, field) in self.schema.fields().iter().enumerate() {
            let col = batch.column(i);
            if col.data_type() == field.data_type() {
                columns.push(col.clone());
            } else {
                columns.push(
                    arrow::compute::cast(col, field.data_type())
                        .map_err(|e| DataFusionError::Execution(format!("leaf batch cast to advertised schema failed: {e}")))?,
                );
            }
        }
        RecordBatch::try_new(std::sync::Arc::clone(&self.schema), columns)
            .map_err(|e| DataFusionError::Execution(format!("leaf batch rebuild failed: {e}")))
    }
}

impl Stream for JavaCursorStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        // leaf_next is a blocking JVM downcall that produces one batch. It runs on the CPU executor
        // thread driving this stream (the leaf is wrapped in CrossRtStream like every other native
        // stream), so blocking here is consistent with the existing native-execution model.
        match leaf_bridge::leaf_next(self.cursor) {
            Ok(Some((array_ptr, schema_ptr))) => {
                let res = unsafe { self.import(array_ptr, schema_ptr) };
                if res.is_err() {
                    self.done = true;
                }
                Poll::Ready(Some(res))
            }
            Ok(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Err(e) => {
                self.done = true;
                Poll::Ready(Some(Err(DataFusionError::Execution(e))))
            }
        }
    }
}

impl RecordBatchStream for JavaCursorStream {
    fn schema(&self) -> SchemaRef {
        std::sync::Arc::clone(&self.schema)
    }
}

impl Drop for JavaCursorStream {
    fn drop(&mut self) {
        leaf_bridge::leaf_close(self.cursor);
    }
}

/// Wraps the NATIVE-mode adopted stream (cases 1&2) so the Java-side reader lease is released when
/// the stream is dropped. In NATIVE mode the Rust leaf adopts the `SessionContextHandle` the JVM built
/// and takes ownership of the native session, but the JVM ALSO holds a `GatedCloseable<Reader>` (and
/// any per-query FilterDelegationHandle) keyed by the returned handle pointer. Without a close upcall
/// that reader gate leaks on every distributed native scan — so this wrapper fires `leaf_close(handle)`
/// on drop (the same upcall the case-3 cursor uses), which routes to `DistributedLeafBridge.close` and
/// releases the gate + delegation binding.
pub struct NativeLeafStream {
    inner: datafusion::execution::SendableRecordBatchStream,
    /// The SessionContextHandle pointer the open upcall returned; also the key the JVM stored the
    /// reader lease under. Released via `leaf_close` on drop.
    handle_ptr: i64,
}

impl NativeLeafStream {
    pub fn new(inner: datafusion::execution::SendableRecordBatchStream, handle_ptr: i64) -> Self {
        Self { inner, handle_ptr }
    }
}

impl Stream for NativeLeafStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for NativeLeafStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Drop for NativeLeafStream {
    fn drop(&mut self) {
        // Release the JVM-side reader gate + delegation binding for this leaf (no-op if never
        // registered / already released; DistributedLeafBridge.close tolerates an unknown handle).
        leaf_bridge::leaf_close(self.handle_ptr);
    }
}
