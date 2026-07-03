/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Leaf open/pull bridge for the distributed path (Model B, PULL-based — DataFusion is pull, we do
//! NOT push into it).
//!
//! The data-node Worker terminates a distributed leaf task. To run the EXISTING, unchanged
//! `AnalyticsSearchService` reader-acquisition + delegation setup, `ShardScanExec::execute` makes one
//! UPCALL into the co-located JVM to OPEN the fragment, then PULLS batches on demand. The open upcall
//! returns a discriminated handle (a tagged i64) so all three execution modes are covered with a
//! single leaf abstraction:
//!
//!   - NATIVE (cases 1 & 2 — parquet DF-driven, or indexed + delegation DF-driven): Java builds the
//!     `SessionContextHandle` (existing `createSessionContext` / `createSessionContextForIndexedExecution`,
//!     which acquires the reader + sets up delegation) and the tag says "execute natively". The Rust
//!     leaf runs the existing native execution and adopts its `SendableRecordBatchStream` — no
//!     native→Java→native round-trip.
//!   - JAVA_CURSOR (case 3 — Lucene executes and hands back rows / Arrow doc-values): Java produces the
//!     batches; the tag carries a Java-side cursor handle. The Rust leaf pulls each batch via
//!     [`leaf_next`] downcalls (Arrow C-Data), closing via [`leaf_close`] on drop.
//!
//! The two upcalls (open) + two downcalls (next/close) mirror the proven reduce-sink Arrow-C-Data
//! plumbing and the existing `stream_next` drain, just in the leaf direction. `AnalyticsSearchService`
//! is untouched: the open upcall calls into its existing resolution.

use std::sync::atomic::{AtomicPtr, Ordering};

/// Discriminator for what [`open_fragment`] returned.
pub const LEAF_MODE_NATIVE: i32 = 1;
pub const LEAF_MODE_JAVA_CURSOR: i32 = 2;

/// `openFragment(query_id, index_uuid*, len, shard_id, substrait*, len, descriptor*, len, tree_shape,
///  predicate_count, out_mode*, out_handle*) -> 0|neg`.
/// Java runs the unchanged AnalyticsSearchService setup and writes:
///   *out_mode   = LEAF_MODE_NATIVE | LEAF_MODE_JAVA_CURSOR
///   *out_handle = a SessionContextHandle ptr (NATIVE) or an opaque Java cursor id (JAVA_CURSOR)
/// `substrait` is the shard-local leaf fragment (empty = plain full scan); `descriptor` is the
/// Java-serialized DelegationDescriptor (empty = no delegation) used to build the FilterDelegationHandle.
type OpenFragmentFn = unsafe extern "C" fn(
    i64,
    *const u8,
    i64,
    i32,
    *const u8,
    i64,
    *const u8,
    i64,
    i32,
    i32,
    *mut i32,
    *mut i64,
) -> i32;
/// `leafNext(cursor) -> FFI_ArrowArray ptr | 0 (EOS) | negative (error)`. Pull one batch (case 3).
type LeafNextFn = unsafe extern "C" fn(i64, *mut i64) -> i32;
/// `leafClose(cursor)`. Release the Java cursor + its reader/context (case 3).
type LeafCloseFn = unsafe extern "C" fn(i64);

static OPEN_FRAGMENT: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());
static LEAF_NEXT: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());
static LEAF_CLOSE: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Registered by Java at node start (mirrors `df_register_filter_tree_callbacks`).
///
/// # Safety
/// All three must be valid Java upcall stubs for the process lifetime.
#[no_mangle]
pub unsafe extern "C" fn df_register_leaf_bridge(
    open_fragment: OpenFragmentFn,
    leaf_next: LeafNextFn,
    leaf_close: LeafCloseFn,
) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        OPEN_FRAGMENT.store(open_fragment as *mut (), Ordering::Release);
        LEAF_NEXT.store(leaf_next as *mut (), Ordering::Release);
        LEAF_CLOSE.store(leaf_close as *mut (), Ordering::Release);
    }));
}

fn load<T>(slot: &AtomicPtr<()>) -> Option<T> {
    let p = slot.load(Ordering::Acquire);
    if p.is_null() {
        None
    } else {
        Some(unsafe { std::mem::transmute_copy::<*mut (), T>(&p) })
    }
}

/// What the open upcall resolved to.
pub enum LeafOpen {
    /// Cases 1&2: Rust executes natively from this `SessionContextHandle` pointer.
    Native { session_handle: i64 },
    /// Case 3: Java produces; pull batches from this cursor.
    JavaCursor { cursor: i64 },
}

/// Open the leaf fragment via the JVM upcall (unchanged AnalyticsSearchService setup).
///
/// `substrait` is the shard-local leaf fragment (empty = plain full scan). `descriptor` is the
/// Java-serialized DelegationDescriptor (empty = no delegation); `tree_shape`/`predicate_count`
/// classify the delegated filter for the indexed executor.
#[allow(clippy::too_many_arguments)]
pub fn open_fragment(
    query_id: i64,
    index_uuid: &str,
    shard_id: i32,
    substrait: &[u8],
    descriptor: &[u8],
    tree_shape: i32,
    predicate_count: i32,
) -> Result<LeafOpen, String> {
    let cb: OpenFragmentFn = load(&OPEN_FRAGMENT)
        .ok_or_else(|| "leaf bridge not registered (Java did not call df_register_leaf_bridge)".to_string())?;
    let mut mode: i32 = 0;
    let mut handle: i64 = 0;
    let rc = unsafe {
        cb(
            query_id,
            index_uuid.as_ptr(),
            index_uuid.len() as i64,
            shard_id,
            substrait.as_ptr(),
            substrait.len() as i64,
            descriptor.as_ptr(),
            descriptor.len() as i64,
            tree_shape,
            predicate_count,
            &mut mode as *mut i32,
            &mut handle as *mut i64,
        )
    };
    if rc != 0 {
        return Err(format!("Java openFragment(query_id={query_id}, shard_id={shard_id}) failed with code {rc}"));
    }
    match mode {
        LEAF_MODE_NATIVE => Ok(LeafOpen::Native { session_handle: handle }),
        LEAF_MODE_JAVA_CURSOR => Ok(LeafOpen::JavaCursor { cursor: handle }),
        other => Err(format!("openFragment returned unknown mode {other}")),
    }
}

/// Pull one batch from a Java cursor (case 3). Returns `Some(FFI_ArrowArray ptr)` or `None` at EOS.
pub fn leaf_next(cursor: i64) -> Result<Option<i64>, String> {
    let cb: LeafNextFn = load(&LEAF_NEXT).ok_or_else(|| "leaf bridge not registered".to_string())?;
    let mut array_ptr: i64 = 0;
    let rc = unsafe { cb(cursor, &mut array_ptr as *mut i64) };
    if rc < 0 {
        return Err(format!("leafNext(cursor={cursor}) failed with code {rc}"));
    }
    if array_ptr == 0 {
        Ok(None) // EOS
    } else {
        Ok(Some(array_ptr))
    }
}

/// Close a Java cursor (case 3).
pub fn leaf_close(cursor: i64) {
    if let Some(cb) = load::<LeafCloseFn>(&LEAF_CLOSE) {
        unsafe { cb(cursor) };
    }
}
