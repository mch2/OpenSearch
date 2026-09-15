/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field as ArrowField, Fields, Schema as ArrowSchema};
use parquet::arrow::ArrowSchemaConverter;
use parquet::schema::types::Type;

use super::error::MergeResult;

/// Reserved column name for the synthetic row identifier added during merge.
pub const ROW_ID_COLUMN_NAME: &str = "__row_id__";

/// Derives the output Parquet schema from the merged Arrow output schema.
///
/// The Arrow union is computed by `Schema::try_merge` (see `MergeContext::new`), which merges
/// struct children recursively, so it already describes the widest shape across all inputs — a
/// struct that gains a sub-field in a later segment included. Converting that union is what keeps
/// the Parquet root and the Arrow schema the row-group writer encodes against in agreement: they
/// are derived from one source rather than computed twice.
///
/// A prior version unioned the input Parquet descriptors by TOP-LEVEL name with first-writer-wins,
/// which took a nested group's whole subtree from whichever segment was read first. A struct that
/// gained a sub-field later kept the narrower type, so the two schemas disagreed and the merge
/// failed writing the batch. Dynamic mapping makes a struct gaining a field routine, so that was
/// the blocker for storing OpenSearch `object` fields as native Parquet structs.
///
/// `coerce_types` must match the value on the `WriterProperties` the row-group writer is built
/// with; `ArrowWriter` derives its own schema the same way, and a mismatch would encode arrays
/// against a Parquet type they were not converted for.
pub fn build_parquet_root_schema(
    output_schema: &ArrowSchema,
    coerce_types: bool,
) -> MergeResult<Arc<Type>> {
    let descriptor = ArrowSchemaConverter::new()
        .with_coerce_types(coerce_types)
        .convert(output_schema)?;
    Ok(descriptor.root_schema_ptr())
}

/// Returns column indices that exclude `__row_id__`, for use as a projection mask.
pub fn projection_indices_excluding_row_id(schema: &ArrowSchema) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name() != ROW_ID_COLUMN_NAME)
        .map(|(i, _)| i)
        .collect()
}

/// Appends a `__row_id__` column with sequential values `[start_id, start_id + N)`
/// to the given batch, producing a new batch with the output schema.
pub fn append_row_id(
    batch: &RecordBatch,
    start_id: i64,
    output_schema: &Arc<ArrowSchema>,
) -> MergeResult<RecordBatch> {
    let n = batch.num_rows() as i64;
    let row_ids = Int64Array::from_iter_values(start_id..start_id + n);
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(row_ids));
    let result = RecordBatch::try_new(output_schema.clone(), columns)?;
    Ok(result)
}

// =============================================================================
// ColumnMapping — precomputed source→target index mapping
// =============================================================================

/// Precomputed mapping from target schema field positions to source batch
/// column indices. Built once per cursor, reused for every batch from that cursor.
///
/// Replaces per-batch `schema.index_of(field.name())` name lookups with O(1)
/// indexed access.
pub struct ColumnMapping {
    mapping: Vec<Option<usize>>,
    target_schema: Arc<ArrowSchema>,
    is_identity: bool,
}

impl ColumnMapping {
    /// Build a mapping from `source_schema` → `target_schema`.
    pub fn new(source_schema: &ArrowSchema, target_schema: &Arc<ArrowSchema>) -> Self {
        let mut mapping = Vec::with_capacity(target_schema.fields().len());
        let mut is_identity = source_schema.fields().len() == target_schema.fields().len();

        for (target_idx, field) in target_schema.fields().iter().enumerate() {
            match source_schema.index_of(field.name()) {
                Ok(src_idx) => {
                    // A name match is not enough to skip the remap: a struct column carries the
                    // same name in both schemas while the target holds the union of its children,
                    // so the arrays still need widening. Comparing the data types catches that —
                    // Arrow's `DataType` equality is structural, down to child field nullability.
                    if is_identity
                        && (src_idx != target_idx
                            || source_schema.field(src_idx).data_type() != field.data_type())
                    {
                        is_identity = false;
                    }
                    mapping.push(Some(src_idx));
                }
                Err(_) => {
                    is_identity = false;
                    mapping.push(None);
                }
            }
        }

        Self {
            mapping,
            target_schema: target_schema.clone(),
            is_identity,
        }
    }

    /// Remap a batch using the precomputed mapping. Zero-copy when schemas match.
    #[inline]
    pub fn pad_batch(&self, batch: &RecordBatch) -> MergeResult<RecordBatch> {
        if self.is_identity {
            return Ok(batch.clone());
        }
        let num_rows = batch.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.mapping.len());
        for (i, entry) in self.mapping.iter().enumerate() {
            let field = &self.target_schema.fields()[i];
            match entry {
                Some(src_idx) => columns.push(widen_to(batch.column(*src_idx), field)?),
                None => {
                    columns.push(arrow::array::new_null_array(field.data_type(), num_rows));
                }
            }
        }
        Ok(RecordBatch::try_new(self.target_schema.clone(), columns)?)
    }
}

/// Rewrites `array` so its type is exactly `target`'s, null-filling struct children the source
/// segment does not have.
///
/// Only struct widening is handled. A missing whole column is null-filled by the caller, but a
/// struct that gained a sub-field in a later segment needs the *child* filled: the source array's
/// type is the narrower struct, and `RecordBatch::try_new` rejects it against the union type. Every
/// other type is returned untouched — merge inputs are segments of one index, so a leaf's physical
/// type is the same in all of them.
///
/// The struct's own validity is carried over unchanged, so a row whose object was absent stays
/// null rather than becoming a struct of nulls (which reads back as a present, empty object).
fn widen_to(array: &ArrayRef, target: &ArrowField) -> MergeResult<ArrayRef> {
    if array.data_type() == target.data_type() {
        return Ok(array.clone());
    }
    let (DataType::Struct(target_children), DataType::Struct(_)) =
        (target.data_type(), array.data_type())
    else {
        return Ok(array.clone());
    };
    let source = array
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("data type reports Struct");
    let len = source.len();
    if target_children.is_empty() {
        // A struct with no children carries no data, only presence. `try_new` cannot express that
        // (it infers the length from the children), so build it from the length directly.
        return Ok(Arc::new(StructArray::new_empty_fields(
            len,
            source.nulls().cloned(),
        )));
    }
    let mut children: Vec<ArrayRef> = Vec::with_capacity(target_children.len());
    for child in target_children {
        match source.column_by_name(child.name()) {
            Some(source_child) => children.push(widen_to(source_child, child)?),
            None => children.push(arrow::array::new_null_array(child.data_type(), len)),
        }
    }
    let widened = StructArray::try_new(
        Fields::from(target_children.to_vec()),
        children,
        source.nulls().cloned(),
    )?;
    Ok(Arc::new(widened))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::buffer::NullBuffer;

    fn struct_field(name: &str, children: Vec<ArrowField>) -> ArrowField {
        ArrowField::new(name, DataType::Struct(Fields::from(children)), true)
    }

    fn int_child(name: &str) -> ArrowField {
        ArrowField::new(name, DataType::Int32, true)
    }

    fn utf8_child(name: &str) -> ArrowField {
        ArrowField::new(name, DataType::Utf8, true)
    }

    /// Builds a struct array over `children`, with `nulls` marking which rows have the object at
    /// all — the shape our writer produces for an OpenSearch `object`.
    fn struct_array(
        fields: Vec<ArrowField>,
        children: Vec<ArrayRef>,
        nulls: Vec<bool>,
    ) -> ArrayRef {
        Arc::new(
            StructArray::try_new(
                Fields::from(fields),
                children,
                Some(NullBuffer::from(nulls)),
            )
            .unwrap(),
        )
    }

    #[test]
    fn parquet_root_carries_every_struct_leaf() {
        // The whole point of deriving from the Arrow union: a struct's leaves must each get a
        // column in the Parquet root, at their dotted paths, or the row-group writer has nowhere
        // to put them.
        let schema = ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, false),
            struct_field("meta", vec![int_child("top"), utf8_child("name")]),
            ArrowField::new(ROW_ID_COLUMN_NAME, DataType::Int64, false),
        ]);

        let root = build_parquet_root_schema(&schema, false).unwrap();
        let descriptor = parquet::schema::types::SchemaDescriptor::new(root);
        let paths: Vec<String> = (0..descriptor.num_columns())
            .map(|i| descriptor.column(i).path().string())
            .collect();

        assert_eq!(
            paths,
            vec!["id", "meta.top", "meta.name", ROW_ID_COLUMN_NAME],
            "struct leaves should appear at their dotted paths"
        );
    }

    #[test]
    fn nested_struct_root_carries_the_full_path() {
        let schema = ArrowSchema::new(vec![struct_field(
            "meta",
            vec![struct_field("props", vec![int_child("depth")])],
        )]);

        let root = build_parquet_root_schema(&schema, false).unwrap();
        let descriptor = parquet::schema::types::SchemaDescriptor::new(root);
        assert_eq!(descriptor.num_columns(), 1);
        assert_eq!(descriptor.column(0).path().string(), "meta.props.depth");
    }

    #[test]
    fn struct_gaining_a_child_is_null_filled() {
        // The blocker this replaces: segment 1 wrote `meta{top}`, dynamic mapping then added
        // `meta.name`, so segment 2 wrote `meta{top,name}` and the merged schema is the union.
        // Batches from segment 1 must widen, with the new child null for every one of their rows.
        let narrow = ArrowSchema::new(vec![struct_field("meta", vec![int_child("top")])]);
        let wide = Arc::new(ArrowSchema::new(vec![struct_field(
            "meta",
            vec![int_child("top"), utf8_child("name")],
        )]));

        let batch = RecordBatch::try_new(
            Arc::new(narrow.clone()),
            vec![struct_array(
                vec![int_child("top")],
                vec![Arc::new(Int32Array::from(vec![1, 2]))],
                vec![true, true],
            )],
        )
        .unwrap();

        let padded = ColumnMapping::new(&narrow, &wide)
            .pad_batch(&batch)
            .unwrap();

        assert_eq!(padded.schema(), wide, "batch must carry the union schema");
        let meta = padded
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let name = meta
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(name.is_null(0) && name.is_null(1), "new child is all null");
        let top = meta
            .column_by_name("top")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            (top.value(0), top.value(1)),
            (1, 2),
            "the child that was written keeps its values"
        );
    }

    #[test]
    fn widening_keeps_an_absent_object_null() {
        // An absent object is a null struct, not a struct of nulls: the two read back differently
        // (`isnull(meta)` is true only for the former), so widening must carry the validity over.
        let narrow = ArrowSchema::new(vec![struct_field("meta", vec![int_child("top")])]);
        let wide = Arc::new(ArrowSchema::new(vec![struct_field(
            "meta",
            vec![int_child("top"), utf8_child("name")],
        )]));

        let batch = RecordBatch::try_new(
            Arc::new(narrow.clone()),
            vec![struct_array(
                vec![int_child("top")],
                vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
                vec![true, false],
            )],
        )
        .unwrap();

        let padded = ColumnMapping::new(&narrow, &wide)
            .pad_batch(&batch)
            .unwrap();
        let meta = padded.column(0);
        assert!(!meta.is_null(0), "row 0 has the object");
        assert!(meta.is_null(1), "row 1 never had the object");
    }

    #[test]
    fn nested_struct_gaining_a_grandchild_is_null_filled() {
        let narrow = ArrowSchema::new(vec![struct_field(
            "meta",
            vec![struct_field("props", vec![int_child("depth")])],
        )]);
        let wide = Arc::new(ArrowSchema::new(vec![struct_field(
            "meta",
            vec![struct_field(
                "props",
                vec![int_child("depth"), utf8_child("label")],
            )],
        )]));

        let inner = struct_array(
            vec![int_child("depth")],
            vec![Arc::new(Int32Array::from(vec![7]))],
            vec![true],
        );
        let batch = RecordBatch::try_new(
            Arc::new(narrow.clone()),
            vec![struct_array(
                vec![struct_field("props", vec![int_child("depth")])],
                vec![inner],
                vec![true],
            )],
        )
        .unwrap();

        let padded = ColumnMapping::new(&narrow, &wide)
            .pad_batch(&batch)
            .unwrap();
        assert_eq!(padded.schema(), wide, "widening must recurse to any depth");
    }

    #[test]
    fn a_same_shaped_struct_batch_is_passed_through() {
        // Identity is the hot path — every batch of a merge where no segment drifted. It must stay
        // zero-copy even though the column is a struct.
        let schema = ArrowSchema::new(vec![struct_field("meta", vec![int_child("top")])]);
        let target = Arc::new(schema.clone());
        let batch = RecordBatch::try_new(
            target.clone(),
            vec![struct_array(
                vec![int_child("top")],
                vec![Arc::new(Int32Array::from(vec![1]))],
                vec![true],
            )],
        )
        .unwrap();

        let mapping = ColumnMapping::new(&schema, &target);
        let padded = mapping.pad_batch(&batch).unwrap();
        assert!(
            Arc::ptr_eq(padded.column(0), batch.column(0)),
            "unchanged struct column should not be rebuilt"
        );
    }
}
