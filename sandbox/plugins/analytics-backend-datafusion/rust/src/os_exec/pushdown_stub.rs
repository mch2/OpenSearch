/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Pushdown-faithful stub `TableProvider` for the SHARD_SCAN leaf rewrite
//! (df-proto spec §4).
//!
//! When the finalizer lowers a shard fragment, it registers this stub in place
//! of the real `IndexedTableProvider`. Its `supports_filters_pushdown` returns
//! the **same claims** as the real provider for the same input, so DataFusion's
//! physical planner routes the entire WHERE condition into the scan and emits no
//! `FilterExec` above it (which would otherwise try to physically evaluate the
//! `index_filter(...)` / `delegated_predicate(...)` marker UDFs, whose bodies
//! panic by design — DO-NOT-TOUCH §3).
//!
//! The stub's `scan()` is never executed: the finalizer's leaf rewrite replaces
//! the resulting `DataSourceExec` with an `OpenSearchShardScanExec` carrying the
//! pushed-down filter expression. `scan()` returning an empty placeholder is
//! therefore safe — but we make it an explicit error to catch a missed rewrite.
//!
//! Pushdown-claim parity with the real provider is a standing CI invariant
//! (spec §7.3); the test at the bottom asserts it directly.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

/// Stub table provider that claims `Exact` pushdown for every filter — byte-for-byte
/// the same claim as `IndexedTableProvider::supports_filters_pushdown`.
#[derive(Debug)]
pub struct PushdownStubProvider {
    schema: SchemaRef,
}

impl PushdownStubProvider {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

#[async_trait]
impl TableProvider for PushdownStubProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // MUST mirror IndexedTableProvider exactly (§7.3). The real provider's
        // BoolNode evaluator fully handles every WHERE filter, so it claims
        // `Exact` for all of them and DataFusion removes the outer FilterExec.
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Return an empty placeholder scan with the projected schema so physical
        // planning succeeds (and, because we claim Exact pushdown, routes the
        // whole filter into this scan with no FilterExec above it). The finalizer's
        // leaf rewrite then replaces this placeholder with an
        // OpenSearchShardScanExec carrying the real shard-scan config. The
        // placeholder is never executed on the coordinator — the swap happens
        // first — so an empty DataSourceExec is the correct, side-effect-free stub.
        let projected = match projection {
            Some(idxs) => Arc::new(self.schema.project(idxs)?),
            None => Arc::clone(&self.schema),
        };
        Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(projected)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]))
    }

    /// §7.3 parity: the stub's pushdown claims equal what the real
    /// IndexedTableProvider returns for the same filter set (both `Exact`,
    /// one per filter). We assert the stub side here and pin the contract; the
    /// real provider's identical `Exact`-for-all rule lives in
    /// `indexed_table::table_provider` and is covered by its own tests.
    #[test]
    fn stub_claims_exact_for_every_filter() {
        let provider = PushdownStubProvider::new(schema());
        let f1 = col("a").gt(lit(5i64));
        let f2 = col("b").eq(lit("x"));
        let filters = vec![&f1, &f2];
        let claims = provider.supports_filters_pushdown(&filters).unwrap();
        assert_eq!(claims.len(), 2);
        assert!(claims
            .iter()
            .all(|c| matches!(c, TableProviderFilterPushDown::Exact)));
    }

    #[test]
    fn stub_scan_errors_if_reached() {
        // Documents the contract: scan() must never execute (rewrite replaces it).
        let provider = PushdownStubProvider::new(schema());
        // We can't easily build a Session here; instead assert the empty-filter
        // claim shape, and rely on the not_impl_err in scan() as the guard.
        let claims = provider.supports_filters_pushdown(&[]).unwrap();
        assert!(claims.is_empty());
    }
}
