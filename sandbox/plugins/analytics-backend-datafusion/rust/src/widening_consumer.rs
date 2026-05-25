/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Custom Substrait consumer that handles schema widening for multi-index queries.
//!
//! When a shard's ListingTable is registered with its inferred parquet schema (which may be
//! a subset of the plan's base_schema for alias/pattern queries), this consumer detects the
//! mismatch and re-registers the ListingTable with a widened schema before building the scan.
//! The widened ListingTable tells DataFusion's ParquetExec about the extra columns, and its
//! DefaultPhysicalExprAdapterFactory null-fills them at read time.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::common::{DFSchema, TableReference};
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::provider_as_source;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::logical_plan::consumer::{
    DefaultSubstraitConsumer, SubstraitConsumer, from_substrait_named_struct,
};
use substrait::proto::ReadRel;
use substrait::proto::read_rel::ReadType;

use arrow::datatypes::{Field, Schema, SchemaRef};

/// A Substrait consumer that widens table schemas by re-registering the ListingTable
/// with missing columns before building the scan.
pub struct WideningSubstraitConsumer<'a> {
    inner: DefaultSubstraitConsumer<'a>,
    state: &'a SessionState,
}

impl<'a> WideningSubstraitConsumer<'a> {
    pub fn new(extensions: &'a Extensions, state: &'a SessionState) -> Self {
        Self {
            inner: DefaultSubstraitConsumer::new(extensions, state),
            state,
        }
    }
}

#[async_trait]
impl SubstraitConsumer for WideningSubstraitConsumer<'_> {
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        self.inner.resolve_table_ref(table_ref).await
    }

    fn get_extensions(&self) -> &Extensions {
        self.inner.get_extensions()
    }

    fn get_function_registry(&self) -> &impl FunctionRegistry {
        self.inner.get_function_registry()
    }

    async fn consume_read(
        &self,
        rel: &ReadRel,
    ) -> datafusion::common::Result<LogicalPlan> {
        use datafusion::logical_expr::utils::split_conjunction_owned;

        let named_struct = rel.base_schema.as_ref().ok_or_else(|| {
            datafusion::common::DataFusionError::Substrait(
                "No base schema provided for Read Relation".to_string(),
            )
        })?;
        let substrait_schema = from_substrait_named_struct(self, named_struct)?;

        let Some(ReadType::NamedTable(nt)) = &rel.read_type else {
            return datafusion_substrait::logical_plan::consumer::from_read_rel(self, rel).await;
        };

        let table_ref = match nt.names.len() {
            0 => return Err(datafusion::common::DataFusionError::Plan(
                "No table name found in NamedTable".to_string(),
            )),
            1 => TableReference::bare(nt.names[0].clone()),
            2 => TableReference::partial(nt.names[0].clone(), nt.names[1].clone()),
            _ => TableReference::full(
                nt.names[0].clone(),
                nt.names[1].clone(),
                nt.names[2].clone(),
            ),
        };

        let provider = match self.resolve_table_ref(&table_ref).await? {
            Some(p) => p,
            None => return Err(datafusion::common::DataFusionError::Plan(
                format!("No table named '{table_ref}'"),
            )),
        };

        // Check if schema needs widening: are any base_schema columns missing from the table?
        let table_schema = provider.schema();
        let provider = self.widen_if_needed(provider, &table_ref, &substrait_schema, &table_schema)?;

        let filters = if let Some(f) = &rel.filter {
            let qualified_schema = DFSchema::try_from(provider.schema().as_ref().clone())?
                .replace_qualifier(table_ref.clone());
            let filter_expr = self.consume_expression(f, &qualified_schema).await?;
            split_conjunction_owned(filter_expr)
        } else {
            vec![]
        };

        let plan = LogicalPlanBuilder::scan_with_filters(
            table_ref,
            provider_as_source(provider),
            None,
            filters,
        )?
        .build()?;

        Ok(plan)
    }
}

impl WideningSubstraitConsumer<'_> {
    /// If the table is missing columns from the substrait schema, re-create the ListingTable
    /// with a widened schema so ParquetExec knows to null-fill those columns.
    fn widen_if_needed(
        &self,
        provider: Arc<dyn TableProvider>,
        table_ref: &TableReference,
        substrait_schema: &DFSchema,
        table_schema: &SchemaRef,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let have: std::collections::HashSet<&str> = table_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        let mut missing: Vec<Field> = Vec::new();
        for field in substrait_schema.fields() {
            if !have.contains(field.name().as_str()) {
                missing.push(Field::new(field.name(), field.data_type().clone(), true));
            }
        }

        if missing.is_empty() {
            return Ok(provider);
        }

        // Apply force_view + coerce to the missing columns' types so they match real parquet output.
        let missing_schema = Schema::new(missing);
        let force_view = self.state.config().options().execution.parquet.schema_force_view_types;
        let missing_schema = if force_view {
            datafusion::datasource::file_format::parquet::transform_schema_to_view(&missing_schema)
        } else {
            missing_schema
        };
        let missing_schema = crate::schema_coerce::coerce_inferred_schema(Arc::new(missing_schema));

        // Build widened schema: existing + missing appended.
        let mut fields: Vec<Field> = table_schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        for f in missing_schema.fields() {
            fields.push(f.as_ref().clone());
        }
        let widened_schema = Arc::new(Schema::new_with_metadata(fields, table_schema.metadata().clone()));

        // Downcast to ListingTable to get its path, then recreate with widened schema.
        let listing_table = provider.as_any().downcast_ref::<ListingTable>().ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(
                "WideningConsumer: expected ListingTable provider for schema widening".to_string(),
            )
        })?;

        let table_paths = listing_table.table_paths().to_vec();
        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_collect_stat(true);

        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(listing_options)
            .with_schema(widened_schema);

        let new_table = Arc::new(ListingTable::try_new(config)?);

        // Re-register the table under the same name so subsequent resolves see the widened version.
        let ctx = datafusion::prelude::SessionContext::new_with_state(self.state.clone());
        ctx.deregister_table(table_ref.clone())?;
        ctx.register_table(table_ref.clone(), Arc::clone(&new_table) as Arc<dyn TableProvider>)?;

        Ok(new_table)
    }
}
