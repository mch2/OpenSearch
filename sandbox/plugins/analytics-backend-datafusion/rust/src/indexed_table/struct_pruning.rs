/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Reading only the sub-fields of an `object` that a query actually named.
//!
//! An OpenSearch `object` is stored as a Parquet struct, so a scan that asks for the struct column
//! reads every leaf under it. A query naming one sub-field of a 55-field OTel `attributes` therefore
//! pays roughly 18x what it needs, and up to 1300x when the sub-field it wants is a small one — see
//! `tests/struct_projection_cost_tests.rs`.
//!
//! The plan says which sub-fields it wants: the projection above the scan reads each one with
//! `get_field(attributes, 'k07')`, and so does any predicate on a leaf. This module collects those
//! accesses, reads the struct as one column per referenced leaf — which parquet satisfies with a
//! leaf-level mask — and then rebuilds the struct before anything downstream sees the batch.
//!
//! ## Why read flat and rebuild, rather than read a narrower struct
//!
//! A narrowed struct is not expressible through `FileScanConfig`, whose only projection lever is a
//! list of expressions: pruning leaves there means supplying `get_field`, whose output is one column
//! per child. Rebuilding the struct afterwards is what keeps the change confined to the read. The
//! scan's output schema, its ordering and row-id handling, the evaluator, and the projection above
//! it all continue to see exactly the batch they saw before.
//!
//! Rebuilding costs a `StructArray` per batch over arrays that are already decoded — pointer work,
//! no data movement — and null arrays for the children nobody asked for.
//!
//! ## What makes it correct
//!
//! Unreferenced children are rebuilt as null. That is sound only because the leaf set is collected
//! from *everything* downstream that touches the column: the projection and every predicate the
//! evaluator will apply. A leaf left out reads as absent rather than failing, so an incomplete
//! collection is a correctness bug and not a missed optimisation — which is why a struct referenced
//! anywhere as a whole value disables pruning for that column entirely.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{new_null_array, Array, ArrayRef, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::ScalarFunctionExpr;

/// One column of the read.
#[derive(Debug, Clone)]
enum ReadColumn {
    /// Read whole, exactly as before: not a struct, or a struct some expression uses as a value.
    Whole { table_index: usize, field: FieldRef },
    /// A struct read as the leaves that were referenced, each an ordered field path within it.
    Leaves {
        table_index: usize,
        field: FieldRef,
        paths: Vec<Vec<String>>,
    },
}

impl ReadColumn {
    fn field(&self) -> &FieldRef {
        match self {
            ReadColumn::Whole { field, .. } | ReadColumn::Leaves { field, .. } => field,
        }
    }
}

/// A read that pulls an object's referenced leaves instead of the whole object.
#[derive(Debug, Clone)]
pub(super) struct FlatStructRead {
    columns: Vec<ReadColumn>,
    /// Schema of the batch parquet delivers: one column per leaf where a struct was flattened.
    flat_schema: SchemaRef,
    /// Schema the batch is restored to before anything downstream sees it — the read's own schema,
    /// unchanged from what it would have been without pruning.
    read_schema: SchemaRef,
}

impl FlatStructRead {
    /// The projection to hand the parquet source. Expressed against the table schema, since that is
    /// what `ParquetSource` was constructed with.
    pub fn projection_exprs(&self, table_schema: &Schema) -> Result<ProjectionExprs> {
        let get_field = Arc::new(ScalarUDF::from(GetFieldFunc::new()));
        let config = Arc::new(datafusion::common::config::ConfigOptions::default());
        let mut exprs: Vec<ProjectionExpr> = Vec::with_capacity(self.flat_schema.fields().len());

        for column in &self.columns {
            match column {
                ReadColumn::Whole { table_index, field } => exprs.push(ProjectionExpr {
                    expr: Arc::new(Column::new(field.name(), *table_index)),
                    alias: field.name().clone(),
                }),
                ReadColumn::Leaves {
                    table_index,
                    field,
                    paths,
                } => {
                    for path in paths {
                        let mut expr: Arc<dyn PhysicalExpr> =
                            Arc::new(Column::new(field.name(), *table_index));
                        for name in path {
                            expr = Arc::new(ScalarFunctionExpr::try_new(
                                Arc::clone(&get_field),
                                vec![
                                    expr,
                                    datafusion::physical_expr::expressions::lit(
                                        datafusion::common::ScalarValue::Utf8(Some(name.clone())),
                                    ),
                                ],
                                table_schema,
                                Arc::clone(&config),
                            )?);
                        }
                        exprs.push(ProjectionExpr {
                            expr,
                            alias: dotted_name(field.name(), path),
                        });
                    }
                }
            }
        }
        Ok(ProjectionExprs::new(exprs))
    }

    /// True when every struct field `exprs` reaches for is already in this read.
    ///
    /// Asked of a dynamic filter, which a parent operator hands down after the leaf set was fixed. A
    /// sub-field it names that the read would not fetch reads null, and pruning on null drops row
    /// groups that should have survived — so an uncovered filter means falling back to reading the
    /// struct whole, not narrowing further.
    pub fn covers(&self, exprs: &[Arc<dyn PhysicalExpr>]) -> bool {
        let wanted = collect_accesses(exprs, self.read_schema.as_ref());
        for (column, paths) in &wanted {
            let Some(entry) = self.columns.iter().find(|c| c.field().name() == column) else {
                continue;
            };
            let ReadColumn::Leaves { paths: read, .. } = entry else {
                // Read whole, so anything under it is there.
                continue;
            };
            match paths {
                // The struct is used as a value, which a narrowed read cannot serve.
                None => return false,
                Some(paths) => {
                    if paths.iter().all(|path| read.contains(path)) == false {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Rebuilds the flattened structs, returning a batch with [`Self::read_schema`].
    ///
    /// A no-op when the batch already carries that schema, which is what a file whose columns were
    /// all read whole delivers.
    pub fn reassemble(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if batch.schema().as_ref() == self.read_schema.as_ref() {
            return Ok(batch.clone());
        }
        let rows = batch.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());

        for column in &self.columns {
            match column {
                ReadColumn::Whole { field, .. } => {
                    columns.push(take_by_name(batch, field.name(), field.data_type(), rows));
                }
                ReadColumn::Leaves { field, paths, .. } => {
                    let leaves: Vec<(&[String], ArrayRef)> = paths
                        .iter()
                        .map(|path| {
                            let dotted = dotted_name(field.name(), path);
                            let leaf_type = leaf_type(field.data_type(), path)
                                .unwrap_or_else(|| DataType::Null);
                            (
                                path.as_slice(),
                                take_by_name(batch, &dotted, &leaf_type, rows),
                            )
                        })
                        .collect();
                    columns.push(rebuild_struct(field.data_type(), &leaves, rows)?);
                }
            }
        }

        RecordBatch::try_new_with_options(
            Arc::clone(&self.read_schema),
            columns,
            &datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

/// The batch's column of that name, or an all-null column of the expected type when the file did not
/// have it — the same treatment a column absent under schema drift already gets.
fn take_by_name(batch: &RecordBatch, name: &str, expected: &DataType, rows: usize) -> ArrayRef {
    match batch.schema().index_of(name) {
        Ok(index) => Arc::clone(batch.column(index)),
        Err(_) => new_null_array(expected, rows),
    }
}

/// Rebuilds a struct of `struct_type` from the leaves that were read, nulling the rest.
///
/// Recurses so a sub-object is rebuilt from the leaves beneath it: the paths are grouped by their
/// first component, and a child with no leaves under it is null in full.
fn rebuild_struct(
    struct_type: &DataType,
    leaves: &[(&[String], ArrayRef)],
    rows: usize,
) -> Result<ArrayRef> {
    let DataType::Struct(fields) = struct_type else {
        // Not a struct: the only leaf can be the value itself.
        return Ok(leaves
            .first()
            .map(|(_, array)| Arc::clone(array))
            .unwrap_or_else(|| new_null_array(struct_type, rows)));
    };

    let mut children: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for field in fields.iter() {
        // Leaves that live under this child, with the child's own name stripped off.
        let below: Vec<(&[String], ArrayRef)> = leaves
            .iter()
            .filter(|(path, _)| path.first().map(|p| p == field.name()).unwrap_or(false))
            .map(|(path, array)| (&path[1..], Arc::clone(array)))
            .collect();

        if below.is_empty() {
            // Nobody asked for anything under this child.
            children.push(new_null_array(field.data_type(), rows));
        } else if below.len() == 1 && below[0].0.is_empty() {
            children.push(Arc::clone(&below[0].1));
        } else {
            children.push(rebuild_struct(field.data_type(), &below, rows)?);
        }
    }

    // Presence is carried by the leaves, not tracked separately: a struct whose every child is null
    // for a row is how an absent object already reads back through this path.
    StructArray::try_new(Fields::clone(fields), children, None)
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// One `get_field(...)` chain: the struct column at its root and the field names below it.
struct FieldAccessPath {
    column: String,
    fields: Vec<String>,
}

/// Recognises `get_field(Column, 'name')`, including chains like
/// `get_field(get_field(Column, 'props'), 'zone')`. Returns `None` for anything else — including a
/// `get_field` whose field name is not a literal, which is a runtime Map lookup rather than a struct
/// field and cannot be resolved to a column.
fn field_access_path(expr: &Arc<dyn PhysicalExpr>) -> Option<FieldAccessPath> {
    let func = ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref())?;
    let args = func.args();
    if args.len() != 2 {
        return None;
    }
    let name = args[1]
        .downcast_ref::<Literal>()
        .and_then(|lit| match lit.value() {
            datafusion::common::ScalarValue::Utf8(Some(s))
            | datafusion::common::ScalarValue::LargeUtf8(Some(s))
            | datafusion::common::ScalarValue::Utf8View(Some(s)) => Some(s.clone()),
            _ => None,
        })?;

    if let Some(col) = args[0].downcast_ref::<Column>() {
        return Some(FieldAccessPath {
            column: col.name().to_string(),
            fields: vec![name],
        });
    }
    let mut inner = field_access_path(&args[0])?;
    inner.fields.push(name);
    Some(inner)
}

fn dotted_name(column: &str, fields: &[String]) -> String {
    let mut out = String::from(column);
    for field in fields {
        out.push('.');
        out.push_str(field);
    }
    out
}

/// Resolves a field path inside a struct to its leaf type, or `None` when the path does not resolve.
fn leaf_type(struct_type: &DataType, path: &[String]) -> Option<DataType> {
    let mut current = struct_type.clone();
    for name in path {
        let DataType::Struct(fields) = current else {
            return None;
        };
        current = fields
            .iter()
            .find(|f| f.name() == name)?
            .data_type()
            .clone();
    }
    Some(current)
}

/// Which sub-fields of each struct column a set of expressions references.
///
/// `None` for a column means the struct is used as a value somewhere, so nothing can be pruned.
/// Absent means the column was not referenced at all.
fn collect_accesses(
    exprs: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
) -> HashMap<String, Option<BTreeSet<Vec<String>>>> {
    let mut accesses: HashMap<String, Option<BTreeSet<Vec<String>>>> = HashMap::new();
    for expr in exprs {
        let _ = Arc::clone(expr).transform_down(|node| {
            if let Some(path) = field_access_path(&node) {
                if is_struct_column(schema, &path.column) {
                    let entry = accesses
                        .entry(path.column.clone())
                        .or_insert_with(|| Some(BTreeSet::new()));
                    if let Some(paths) = entry {
                        paths.insert(path.fields);
                    }
                    // Do not descend: the Column at the root of this chain is part of the access,
                    // not a bare reference to the whole struct.
                    return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
                }
            }
            if let Some(col) = node.downcast_ref::<Column>() {
                if is_struct_column(schema, col.name()) {
                    accesses.insert(col.name().to_string(), None);
                }
            }
            Ok(Transformed::no(node))
        });
    }
    accesses
}

fn is_struct_column(schema: &Schema, name: &str) -> bool {
    schema
        .index_of(name)
        .ok()
        .map(|idx| matches!(schema.field(idx).data_type(), DataType::Struct(_)))
        .unwrap_or(false)
}

/// Plans the read implied by `exprs`, or `None` when no struct column can be pruned — because there
/// are none, none are referenced, or every referenced one is used as a value.
///
/// `read_indices` are the table-schema columns the scan reads (output columns plus the predicate
/// columns the evaluator needs); `exprs` must cover everything downstream that touches them.
pub(super) fn plan_flat_struct_read(
    exprs: &[Arc<dyn PhysicalExpr>],
    table_schema: &SchemaRef,
    read_indices: &[usize],
) -> Option<FlatStructRead> {
    let accesses = collect_accesses(exprs, table_schema);
    if accesses.is_empty() {
        return None;
    }

    let mut columns: Vec<ReadColumn> = Vec::with_capacity(read_indices.len());
    let mut flat_fields: Vec<Field> = Vec::with_capacity(read_indices.len());
    let mut read_fields: Vec<Field> = Vec::with_capacity(read_indices.len());
    let mut pruned_any = false;

    for &table_index in read_indices {
        let field = table_schema.fields().get(table_index)?.clone();
        read_fields.push(field.as_ref().clone());

        let paths: Option<Vec<Vec<String>>> = match accesses.get(field.name()) {
            Some(Some(paths)) if paths.is_empty() == false => {
                // Every path must resolve, or this column is read whole: a path that does not
                // resolve would otherwise become an invented column.
                paths
                    .iter()
                    .map(|path| leaf_type(field.data_type(), path).map(|_| path.clone()))
                    .collect()
            }
            _ => None,
        };

        match paths {
            Some(paths) => {
                for path in &paths {
                    let dtype = leaf_type(field.data_type(), path).expect("resolved above");
                    // Always nullable: a leaf of an absent object is null.
                    flat_fields.push(Field::new(dotted_name(field.name(), path), dtype, true));
                }
                pruned_any = true;
                columns.push(ReadColumn::Leaves {
                    table_index,
                    field,
                    paths,
                });
            }
            None => {
                flat_fields.push(field.as_ref().clone());
                columns.push(ReadColumn::Whole { table_index, field });
            }
        }
    }

    if pruned_any == false {
        return None;
    }
    Some(FlatStructRead {
        columns,
        flat_schema: Arc::new(Schema::new(flat_fields)),
        read_schema: Arc::new(Schema::new(read_fields)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Fields;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::physical_expr::expressions::{binary, lit};
    use datafusion::physical_expr::ScalarFunctionExpr;
    use std::sync::Arc;

    /// `id BIGINT, attributes STRUCT<k00 UTF8, k01 UTF8, props STRUCT<zone UTF8>>`
    fn schema() -> SchemaRef {
        let props = DataType::Struct(Fields::from(vec![Field::new("zone", DataType::Utf8, true)]));
        let attributes = DataType::Struct(Fields::from(vec![
            Field::new("k00", DataType::Utf8, true),
            Field::new("k01", DataType::Utf8, true),
            Field::new("props", props, true),
        ]));
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("attributes", attributes, true),
        ]))
    }

    fn get_field(
        base: Arc<dyn PhysicalExpr>,
        name: &str,
        schema: &Schema,
    ) -> Arc<dyn PhysicalExpr> {
        let udf = Arc::new(ScalarUDF::from(GetFieldFunc::new()));
        Arc::new(
            ScalarFunctionExpr::try_new(
                udf,
                vec![base, lit(ScalarValue::Utf8(Some(name.to_string())))],
                schema,
                Arc::new(datafusion::common::config::ConfigOptions::default()),
            )
            .unwrap(),
        )
    }

    fn attributes_col(schema: &Schema) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(
            "attributes",
            schema.index_of("attributes").unwrap(),
        ))
    }

    #[test]
    fn one_referenced_leaf_becomes_one_flat_column() {
        let schema = schema();
        let expr = get_field(attributes_col(&schema), "k00", &schema);

        let plan = plan_flat_struct_read(&[expr], &schema, &[0, 1]).expect("should prune");

        let names: Vec<&str> = plan
            .flat_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            names,
            vec!["id", "attributes.k00"],
            "only the named leaf is read"
        );
        assert_eq!(plan.flat_schema.field(1).data_type(), &DataType::Utf8);
        assert!(
            plan.flat_schema.field(1).is_nullable(),
            "a leaf of an absent object is null"
        );
    }

    #[test]
    fn a_nested_leaf_resolves_through_the_sub_object() {
        let schema = schema();
        let props = get_field(attributes_col(&schema), "props", &schema);
        let expr = get_field(props, "zone", &schema);

        let plan = plan_flat_struct_read(&[expr], &schema, &[0, 1]).expect("should prune");

        let names: Vec<&str> = plan
            .flat_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["id", "attributes.props.zone"]);
    }

    #[test]
    fn several_leaves_are_unioned_across_expressions() {
        let schema = schema();
        let a = get_field(attributes_col(&schema), "k00", &schema);
        let b = get_field(attributes_col(&schema), "k01", &schema);

        let plan = plan_flat_struct_read(&[a, b], &schema, &[0, 1]).expect("should prune");

        let names: Vec<&str> = plan
            .flat_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["id", "attributes.k00", "attributes.k01"]);
    }

    #[test]
    fn a_struct_used_whole_is_not_pruned() {
        // `fields attributes` reads the object itself. There is nothing to save, and flattening it
        // would change what the query asked for.
        let schema = schema();
        assert!(plan_flat_struct_read(&[attributes_col(&schema)], &schema, &[0, 1]).is_none());
    }

    #[test]
    fn a_struct_used_whole_anywhere_is_not_pruned() {
        // One expression reads a leaf, another reads the object. The object still has to arrive
        // intact, so the leaf access alone must not narrow the read.
        let schema = schema();
        let leaf = get_field(attributes_col(&schema), "k00", &schema);

        assert!(
            plan_flat_struct_read(&[leaf, attributes_col(&schema)], &schema, &[0, 1]).is_none()
        );
    }

    #[test]
    fn a_schema_without_structs_is_not_pruned() {
        let flat = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
        assert!(plan_flat_struct_read(&[expr], &flat, &[0]).is_none());
    }

    #[test]
    fn an_unresolvable_path_falls_back_to_the_whole_struct() {
        // Schema drift. The expression is well typed against the table schema — DataFusion rejects
        // a get_field for a field that is not there — but the schema being read may be narrower,
        // because another shard's mapping had the sub-field and this one's did not. Reading the
        // whole struct then is harmless; inventing a column would not be.
        let table = schema();
        let expr = get_field(attributes_col(&table), "k01", &table);

        let drifted: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "attributes",
                DataType::Struct(Fields::from(vec![Field::new("k00", DataType::Utf8, true)])),
                true,
            ),
        ]));

        assert!(
            plan_flat_struct_read(&[expr], &drifted, &[0, 1]).is_none(),
            "an unresolvable path must not produce a pruned read"
        );
    }

    /// The read is flat, but what comes back out is the struct the plan expects — same column, same
    /// type — with the sub-fields nobody asked for null.
    #[test]
    fn reassembly_restores_the_struct_the_plan_expects() {
        let schema = schema();
        let expr = get_field(attributes_col(&schema), "k01", &schema);
        let plan = plan_flat_struct_read(&[expr], &schema, &[0, 1]).expect("should prune");

        // What parquet delivers for the flat read: `id`, `attributes.k01`.
        let flat = RecordBatch::try_new(
            Arc::clone(&plan.flat_schema),
            vec![
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(datafusion::arrow::array::StringArray::from(vec!["a", "b"])) as ArrayRef,
            ],
        )
        .unwrap();

        let restored = plan.reassemble(&flat).unwrap();

        assert_eq!(
            restored.schema().as_ref(),
            plan.read_schema.as_ref(),
            "the batch must carry the schema the rest of the scan was built against"
        );
        let attributes = restored
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let k01 = attributes
            .column_by_name("k01")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(
            (k01.value(0), k01.value(1)),
            ("a", "b"),
            "the leaf that was read keeps its values"
        );
        assert!(
            attributes.column_by_name("k00").unwrap().is_null(0),
            "a sub-field nobody asked for is null, not invented"
        );
        assert!(
            attributes.column_by_name("props").unwrap().is_null(0),
            "and so is a sub-object nobody asked for"
        );
    }

    /// A sub-object is rebuilt from the leaves beneath it, not just at the top level.
    #[test]
    fn reassembly_rebuilds_a_sub_object() {
        let schema = schema();
        let props = get_field(attributes_col(&schema), "props", &schema);
        let expr = get_field(props, "zone", &schema);
        let plan = plan_flat_struct_read(&[expr], &schema, &[1]).expect("should prune");

        let flat = RecordBatch::try_new(
            Arc::clone(&plan.flat_schema),
            vec![Arc::new(datafusion::arrow::array::StringArray::from(vec!["west"])) as ArrayRef],
        )
        .unwrap();

        let restored = plan.reassemble(&flat).unwrap();
        let attributes = restored
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let inner = attributes
            .column_by_name("props")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let zone = inner
            .column_by_name("zone")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(zone.value(0), "west");
    }

    /// A dynamic filter naming a sub-field the read already fetches is covered.
    #[test]
    fn a_dynamic_filter_on_a_read_leaf_is_covered() {
        let schema = schema();
        let leaf = get_field(attributes_col(&schema), "k01", &schema);
        let plan =
            plan_flat_struct_read(&[Arc::clone(&leaf)], &schema, &[0, 1]).expect("should prune");

        assert!(plan.covers(&[leaf]));
    }

    /// One naming a sub-field the read skipped is not — it would read null, and pruning on null drops
    /// row groups that should have survived.
    #[test]
    fn a_dynamic_filter_on_a_skipped_leaf_is_not_covered() {
        let schema = schema();
        let read = get_field(attributes_col(&schema), "k01", &schema);
        let plan = plan_flat_struct_read(&[read], &schema, &[0, 1]).expect("should prune");

        let other = get_field(attributes_col(&schema), "k00", &schema);
        assert!(plan.covers(&[other]) == false);
    }

    /// One that reads the object as a value is not covered either: a narrowed read cannot serve it.
    #[test]
    fn a_dynamic_filter_on_the_whole_object_is_not_covered() {
        let schema = schema();
        let read = get_field(attributes_col(&schema), "k01", &schema);
        let plan = plan_flat_struct_read(&[read], &schema, &[0, 1]).expect("should prune");

        assert!(plan.covers(&[attributes_col(&schema)]) == false);
    }

    /// A filter that touches no object at all is trivially covered.
    #[test]
    fn a_dynamic_filter_on_a_plain_column_is_covered() {
        let schema = schema();
        let read = get_field(attributes_col(&schema), "k01", &schema);
        let plan = plan_flat_struct_read(&[read], &schema, &[0, 1]).expect("should prune");

        let id: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
        assert!(plan.covers(&[id]));
    }

    /// The projection handed to parquet must name the leaves, since that is what makes the reader
    /// mask at leaf level rather than reading the whole group.
    #[test]
    fn the_parquet_projection_names_the_leaves() {
        let schema = schema();
        let expr = get_field(attributes_col(&schema), "k01", &schema);
        let plan = plan_flat_struct_read(&[expr], &schema, &[0, 1]).expect("should prune");

        let exprs = plan.projection_exprs(&schema).unwrap();
        let aliases: Vec<&str> = exprs.iter().map(|e| e.alias.as_str()).collect();
        assert_eq!(aliases, vec!["id", "attributes.k01"]);
        assert!(
            format!("{}", exprs.iter().last().unwrap().expr).contains("get_field"),
            "the leaf must be projected with get_field, which is what parquet prunes on"
        );
    }
}
