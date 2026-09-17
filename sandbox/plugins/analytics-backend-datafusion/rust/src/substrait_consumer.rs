/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! The default Substrait consumer, plus the two things OpenSearch plans express that it declines.
//!
//! One is a reference to a field *inside* a struct: an OpenSearch `object` is stored as a Parquet
//! struct, so a query naming a sub-field is reaching into one. Calcite expresses that as
//! `RexFieldAccess` and isthmus serializes it the way Substrait means it — a `StructField` whose
//! `child` names the field one level down — which the default consumer rejects outright with
//! `"Direct reference StructField with child is not supported"`. `consume_field_reference` walks the
//! chain, resolving each index to its field name against the type it sits in, and builds the
//! `get_field` calls DataFusion represents struct access with.
//!
//! The other is the multi-value expand, which crosses the wire as an `ExtensionSingleRel`.
//!
//! Everything else delegates to [`DefaultSubstraitConsumer`].

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableProvider;
use datafusion::common::{not_impl_err, substrait_err, Column, DFSchema, Result, TableReference};
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::functions::core::expr_fn::get_field;
use datafusion::functions_nested::expr_fn::{array_distinct, array_slice};
use datafusion::logical_expr::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::logical_plan::consumer::{
    from_substrait_plan_with_consumer, DefaultSubstraitConsumer, SubstraitConsumer,
};
use substrait::proto::expression::field_reference::{ReferenceType, RootType};
use substrait::proto::expression::reference_segment;
use substrait::proto::expression::FieldReference;
use substrait::proto::{ExtensionSingleRel, Plan};

pub const MULTI_VALUE_EXPAND_TYPE_URL: &str = "opensearch://analytics/multi_value_expand/v1";
const PAYLOAD_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExpandSpec {
    field_index: usize,
    limit: Option<usize>,
    append: bool,
    distinct: bool,
}

impl ExpandSpec {
    fn decode(bytes: &[u8]) -> datafusion::common::Result<Self> {
        if bytes.len() != PAYLOAD_LEN {
            return substrait_err!(
                "multi-value expand payload must contain {PAYLOAD_LEN} bytes, got {}",
                bytes.len()
            );
        }
        let read_i32 = |offset: usize| -> datafusion::common::Result<i32> {
            Ok(i32::from_be_bytes(
                bytes[offset..offset + 4].try_into().map_err(|_| {
                    datafusion::common::DataFusionError::Substrait(format!(
                        "multi-value expand: failed to read i32 at offset {offset}"
                    ))
                })?,
            ))
        };
        let field_index = read_i32(0)?;
        let limit = read_i32(4)?;
        let append = read_i32(8)?;
        let distinct = read_i32(12)?;
        if field_index < 0 || !matches!(append, 0 | 1) || !matches!(distinct, 0 | 1) || limit < -1 {
            return substrait_err!("invalid multi-value expand payload");
        }
        Ok(Self {
            field_index: field_index as usize,
            limit: (limit >= 0).then_some(limit as usize),
            append: append == 1,
            distinct: distinct == 1,
        })
    }
}

struct OpenSearchSubstraitConsumer<'a> {
    default: DefaultSubstraitConsumer<'a>,
}

impl<'a> OpenSearchSubstraitConsumer<'a> {
    fn new(extensions: &'a Extensions, state: &'a SessionState) -> Self {
        Self {
            default: DefaultSubstraitConsumer::new(extensions, state),
        }
    }
}

#[async_trait]
impl SubstraitConsumer for OpenSearchSubstraitConsumer<'_> {
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        self.default.resolve_table_ref(table_ref).await
    }

    fn get_extensions(&self) -> &Extensions {
        self.default.get_extensions()
    }

    fn get_function_registry(&self) -> &impl FunctionRegistry {
        self.default.get_function_registry()
    }

    fn push_outer_schema(&self, schema: Arc<DFSchema>) {
        self.default.push_outer_schema(schema);
    }

    fn pop_outer_schema(&self) {
        self.default.pop_outer_schema();
    }

    fn get_outer_schema(&self, steps_out: usize) -> Option<Arc<DFSchema>> {
        self.default.get_outer_schema(steps_out)
    }

    /// A reference to a field of the row, or to a field inside a struct of that row.
    ///
    /// Substrait spells the latter as a chain: the outer `StructField` picks the struct column, its
    /// `child` picks the field within, and so on to any depth. Each index is resolved to a name
    /// against the type it indexes, because DataFusion addresses struct fields by name.
    async fn consume_field_reference(
        &self,
        expr: &FieldReference,
        input_schema: &DFSchema,
    ) -> Result<Expr> {
        let Some(ReferenceType::DirectReference(direct)) = &expr.reference_type else {
            return substrait_err!("unsupported field reference type");
        };
        let Some(reference_segment::ReferenceType::StructField(root)) = &direct.reference_type
        else {
            return substrait_err!("field reference must be a struct field");
        };

        // Only a root reference names a column of this input; an outer reference indexes an
        // enclosing query's schema instead.
        let is_root = matches!(&expr.root_type, Some(RootType::RootReference(_)) | None);

        // Bounds-check ahead of anything that indexes the schema, the delegation below included: the
        // plan is wire input and `qualified_field` indexes without checking, so a malformed
        // reference would panic across the FFM boundary rather than fail the query.
        let root_index = root.field as usize;
        if is_root && root_index >= input_schema.fields().len() {
            return substrait_err!(
                "field reference {} is out of range for an input of {} columns",
                root_index,
                input_schema.fields().len()
            );
        }

        // A reference with no child is a plain column, which the default consumer already handles —
        // including outer references, whose schema this does not have.
        if root.child.is_none() {
            return self.default.consume_field_reference(expr, input_schema).await;
        }

        // Dereferencing only works against a column of this input.
        if is_root == false {
            return substrait_err!(
                "nested struct field references are only supported against the current input"
            );
        }

        let (qualifier, field) = input_schema.qualified_field(root_index);
        let mut value = Expr::Column(Column::from((qualifier, field)));
        let mut data_type = field.data_type().clone();
        let mut segment = root.child.as_deref();

        while let Some(next) = segment {
            let Some(reference_segment::ReferenceType::StructField(step)) = &next.reference_type
            else {
                return substrait_err!("only struct fields can be dereferenced");
            };
            let DataType::Struct(fields) = &data_type else {
                return substrait_err!(
                    "cannot read field {} of non-struct type {}",
                    step.field,
                    data_type
                );
            };
            let Some(child) = fields.get(step.field as usize) else {
                return substrait_err!(
                    "struct has no field at index {} (it has {})",
                    step.field,
                    fields.len()
                );
            };
            value = get_field(value, child.name().clone());
            data_type = child.data_type().clone();
            segment = step.child.as_deref();
        }

        Ok(value)
    }

    async fn consume_extension_single(
        &self,
        rel: &ExtensionSingleRel,
    ) -> datafusion::common::Result<LogicalPlan> {
        let detail = rel.detail.as_ref().ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "ExtensionSingleRel missing detail".to_string(),
            )
        })?;
        if detail.type_url != MULTI_VALUE_EXPAND_TYPE_URL {
            return self.default.consume_extension_single(rel).await;
        }
        let input = self
            .consume_rel(rel.input.as_ref().ok_or_else(|| {
                datafusion::common::DataFusionError::Plan(
                    "multi-value expand missing input".to_string(),
                )
            })?)
            .await?;
        expand_multivalue(input, ExpandSpec::decode(&detail.value)?)
    }
}

/// Converts a Substrait [`Plan`] into a DataFusion [`LogicalPlan`], routing any
/// OpenSearch extension relations through [`OpenSearchSubstraitConsumer`] while
/// delegating all standard relations to DataFusion's built-in consumer.
///
/// The overall structure mirrors the upstream DataFusion entry-point; see
/// <https://github.com/apache/datafusion/blob/branch-55/datafusion/substrait/src/logical_plan/consumer/plan.rs#L28-L40>
/// for context on the `Extensions` / consumer wiring pattern.
pub async fn from_substrait_plan(
    state: &SessionState,
    plan: &Plan,
) -> datafusion::common::Result<LogicalPlan> {
    let extensions = Extensions::try_from(&plan.extensions)?;
    if !extensions.type_variations.is_empty() {
        return not_impl_err!("Type variation extensions are not supported");
    }
    let consumer = OpenSearchSubstraitConsumer::new(&extensions, state);
    from_substrait_plan_with_consumer(&consumer, plan).await
}

/// Lowers a multi-value expand spec to a DataFusion `Unnest` plan.
///
/// Two modes are supported, controlled by [`ExpandSpec::append`]:
///
/// * **Replace** (`append = false`): the source LIST column at `field_index` is
///   replaced in-place with its scalar element.  Used for implicit GROUP BY
///   expansion where the downstream aggregate already references the same column
///   position.
///
/// * **Append** (`append = true`): the original source LIST column is **kept** and
///   a new scalar column is appended under a collision-free internal name
///   (`___mvexpand_<N>`), which is then unnested.  This is required for explicit
///   `mvexpand` when the source field also appears as an aggregate argument in the
///   same query (e.g. `stats list(tags) by tags`): the aggregate must receive the
///   original LIST values while the GROUP BY key expands to individual elements.
///   Keeping both columns allows the planner to route each reference to the right
///   physical column.
fn expand_multivalue(
    input: LogicalPlan,
    spec: ExpandSpec,
) -> datafusion::common::Result<LogicalPlan> {
    let columns = input.schema().columns();
    let source = columns.get(spec.field_index).cloned().ok_or_else(|| {
        datafusion::common::DataFusionError::Plan(format!(
            "multi-value expand field index {} is outside {} columns",
            spec.field_index,
            columns.len()
        ))
    })?;
    let mut expanded = Expr::Column(source.clone());
    if spec.distinct {
        expanded = array_distinct(expanded);
    }
    if let Some(limit) = spec.limit {
        expanded = array_slice(expanded, lit(1_i64), lit(limit as i64), None);
    }

    let expand_name = if spec.append {
        let mut candidate = format!("___mvexpand_{}", spec.field_index);
        while input
            .schema()
            .field_with_unqualified_name(&candidate)
            .is_ok()
        {
            candidate.push('_');
        }
        candidate
    } else {
        source.name.clone()
    };

    let mut projection = columns
        .iter()
        .cloned()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    if spec.append {
        projection.push(expanded.alias(&expand_name));
    } else {
        projection[spec.field_index] = expanded.alias(&expand_name);
    }

    LogicalPlanBuilder::from(input)
        .project(projection)?
        .unnest_column(expand_name)?
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int32Array, ListBuilder};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    fn string_lists(rows: &[&[&str]]) -> datafusion::arrow::array::ListArray {
        let mut builder = ListBuilder::new(datafusion::arrow::array::StringViewBuilder::new());
        for values in rows {
            for value in *values {
                builder.values().append_value(*value);
            }
            builder.append(true);
        }
        builder.finish()
    }

    fn input_batch() -> RecordBatch {
        let tags = string_lists(&[&["b", "a", "a"], &["c", "d"]]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("tags", tags.data_type().clone(), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2])), Arc::new(tags)],
        )
        .unwrap()
    }

    async fn run(spec: ExpandSpec) -> Vec<(i32, String)> {
        let ctx = SessionContext::new();
        let batch = input_batch();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        let input = ctx.table("t").await.unwrap().into_unoptimized_plan();
        let plan = expand_multivalue(input, spec).unwrap();
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let values = batch
                .column(if spec.append { 2 } else { 1 })
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringViewArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((ids.value(row), values.value(row).to_string()));
            }
        }
        rows
    }

    #[tokio::test]
    async fn explicit_expand_appends_elements_and_honors_per_document_limit() {
        assert_eq!(
            run(ExpandSpec {
                field_index: 1,
                limit: Some(2),
                append: true,
                distinct: false,
            })
            .await,
            vec![
                (1, "b".into()),
                (1, "a".into()),
                (2, "c".into()),
                (2, "d".into())
            ]
        );
    }

    #[tokio::test]
    async fn group_expand_replaces_list_and_deduplicates_within_document() {
        assert_eq!(
            run(ExpandSpec {
                field_index: 1,
                limit: None,
                append: false,
                distinct: true,
            })
            .await,
            vec![
                (1, "b".into()),
                (1, "a".into()),
                (2, "c".into()),
                (2, "d".into())
            ]
        );
    }

    #[tokio::test]
    async fn sequential_group_expansion_forms_cartesian_product() {
        let first = string_lists(&[&["a", "a", "b"]]);
        let second = string_lists(&[&["x", "y"]]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first", first.data_type().clone(), true),
            Field::new("second", second.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(first),
                Arc::new(second),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        let input = ctx.table("t").await.unwrap().into_unoptimized_plan();
        let first_expanded = expand_multivalue(
            input,
            ExpandSpec {
                field_index: 1,
                limit: None,
                append: false,
                distinct: true,
            },
        )
        .unwrap();
        let plan = expand_multivalue(
            first_expanded,
            ExpandSpec {
                field_index: 2,
                limit: None,
                append: false,
                distinct: true,
            },
        )
        .unwrap();
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let first = batch
                .column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringViewArray>()
                .unwrap();
            let second = batch
                .column(2)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringViewArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((first.value(row).to_string(), second.value(row).to_string()));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![
                ("a".into(), "x".into()),
                ("a".into(), "y".into()),
                ("b".into(), "x".into()),
                ("b".into(), "y".into()),
            ]
        );
    }

    #[test]
    fn payload_validation_rejects_invalid_flags() {
        let mut payload = Vec::new();
        for value in [1_i32, -1, 2, 0] {
            payload.extend_from_slice(&value.to_be_bytes());
        }
        assert!(ExpandSpec::decode(&payload).is_err());
    }
}
#[cfg(test)]
mod field_reference_tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Fields, Schema};
    use datafusion::prelude::SessionContext;
    use substrait::proto::expression::field_reference::RootReference;
    use substrait::proto::expression::reference_segment::StructField;
    use substrait::proto::expression::ReferenceSegment;

    /// `id BIGINT, city STRUCT<name UTF8, props STRUCT<zone UTF8>>`
    fn schema() -> DFSchema {
        let props = DataType::Struct(Fields::from(vec![Field::new("zone", DataType::Utf8, true)]));
        let city = DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("props", props, true),
        ]));
        DFSchema::try_from(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("city", city, true),
        ]))
        .unwrap()
    }

    /// A reference to column `field`, optionally dereferencing the given child indexes below it.
    fn reference(field: i32, children: &[i32]) -> FieldReference {
        let mut segment: Option<Box<ReferenceSegment>> = None;
        for index in children.iter().rev() {
            segment = Some(Box::new(ReferenceSegment {
                reference_type: Some(reference_segment::ReferenceType::StructField(Box::new(
                    StructField {
                        field: *index,
                        child: segment,
                    },
                ))),
            }));
        }
        FieldReference {
            reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                reference_type: Some(reference_segment::ReferenceType::StructField(Box::new(
                    StructField {
                        field,
                        child: segment,
                    },
                ))),
            })),
            root_type: Some(RootType::RootReference(RootReference {})),
        }
    }

    async fn consume(reference: &FieldReference) -> Result<Expr> {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let extensions = Extensions::default();
        let consumer = OpenSearchSubstraitConsumer::new(&extensions, &state);
        consumer.consume_field_reference(reference, &schema()).await
    }

    /// No child: an ordinary column, same as the default consumer produces.
    #[tokio::test]
    async fn a_plain_column_reference_is_a_column() {
        let expr = consume(&reference(0, &[])).await.unwrap();
        assert_eq!(format!("{expr}"), "id");
    }

    /// One child: the case the default consumer declines.
    #[tokio::test]
    async fn a_struct_field_reference_reads_the_field() {
        let expr = consume(&reference(1, &[0])).await.unwrap();
        assert_eq!(format!("{expr}"), "get_field(city, Utf8(\"name\"))");
    }

    /// A chain reads through the sub-object, resolving each index against the type it indexes.
    #[tokio::test]
    async fn a_chained_reference_reads_through_a_sub_object() {
        let expr = consume(&reference(1, &[1, 0])).await.unwrap();
        assert_eq!(
            format!("{expr}"),
            "get_field(get_field(city, Utf8(\"props\")), Utf8(\"zone\"))"
        );
    }

    /// A child index past the end of the struct is a malformed plan, not a panic.
    #[tokio::test]
    async fn an_out_of_range_child_is_an_error() {
        let error = consume(&reference(1, &[7])).await.unwrap_err().to_string();
        assert!(error.contains("no field at index 7"), "got: {error}");
    }

    /// So is a root index past the end of the input — the plan arrives over the wire, and
    /// `qualified_field` would otherwise panic across the FFM boundary.
    #[tokio::test]
    async fn an_out_of_range_root_is_an_error() {
        let error = consume(&reference(9, &[])).await.unwrap_err().to_string();
        assert!(error.contains("out of range"), "got: {error}");
    }

    /// Dereferencing something that is not a struct is an error, not a wrong answer.
    #[tokio::test]
    async fn dereferencing_a_scalar_is_an_error() {
        let error = consume(&reference(0, &[0])).await.unwrap_err().to_string();
        assert!(error.contains("non-struct"), "got: {error}");
    }
}
