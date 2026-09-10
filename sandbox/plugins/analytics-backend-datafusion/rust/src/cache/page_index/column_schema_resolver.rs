/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Predicate-column name → parquet leaf-index resolution.
//!
//! Resolution is done against the file's OWN schema (derived from the footer)
//! rather than the shared table schema to ensure correct leaf indices under
//! schema evolution (see [`resolve_predicate_parquet_columns`] for details).

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use datafusion::parquet::file::metadata::ParquetMetaData;

/// Map the query's predicate-column names to **this file's** parquet leaf
/// indices, resolving against the file's OWN schema so the indices are correct
/// even when the file is missing columns (schema evolution).
///
/// # Why the file's own schema, not the shared table schema
///
/// `StatisticsConverter`/`parquet_column` map a column by finding its position in
/// the supplied arrow schema and then matching that position to a parquet leaf
/// (`get_column_root_idx`). The table schema is the **union** of all
/// files' columns [N]; a given file may physically contain fewer[M] (e.g.
/// the merged file has M leaves — the absent columns are all-null and not
/// written). Resolving against the N-field union therefore maps a column to the
/// WRONG leaf in a M-leaf file. We would then build
/// the scoped ColumnIndex/OffsetIndex at the wrong leaf and leave the real one an
/// empty placeholder — and DataFusion's pruner, which resolves against the file's
/// physical schema, reads the real leaf and panics on the empty `page_locations`
/// (`statistics.rs` `page_locations.last().unwrap()`).
///
/// Deriving the arrow schema from the file footer (`parquet_to_arrow_schema`)
/// gives a 1:1 field↔leaf correspondence for that file, so the resolved index
/// matches what DataFusion dereferences. Columns absent from the file are skipped.
pub fn resolve_predicate_parquet_columns(
    _arrow_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    predicate_column_names: &[String],
    file_schema: &SchemaRef,
) -> Vec<usize> {
    let parquet_schema = metadata.file_metadata().schema_descr();
    resolve_with_schema(file_schema, metadata, predicate_column_names)
}

/// Resolve TWO name-sets (e.g. predicate columns and projection columns) against
/// the same file in one pass. Deriving the per-file arrow schema
/// (`parquet_to_arrow_schema`) is the dominant cost of name→leaf resolution on
/// wide schemas (it rebuilds the whole file's Schema); the two callers in the
/// indexed setup loop previously each rebuilt it, so doing it once here removes a
/// full schema reconstruction per file per query. Pure refactor — each returned
/// Vec is identical to calling `resolve_predicate_parquet_columns` separately.
pub fn resolve_predicate_parquet_columns_pair(
    _union_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    predicate_col_names: &[String],
    projection_col_names: &[String],
    file_schema: &SchemaRef,
) -> (Vec<usize>, Vec<usize>) {
    (
        resolve_with_schema(file_schema, metadata, predicate_col_names),
        resolve_with_schema(file_schema, metadata, projection_col_names),
    )
}

/// Resolve predicate column names → parquet leaf indices against a specific arrow
/// schema, via the same `StatisticsConverter` mapping DataFusion's pruner uses.
///
/// # Nested columns
///
/// `parquet_column_index` returns `None` for a nested Arrow root. That is deliberate in
/// parquet-rs: parquet's physical schema is flat, so a `List`/`Map`/`Struct` root can span
/// several leaves and it will not pick one. An array column named `tags` is therefore stored
/// at `tags.list.element` and nothing in the file is named `tags`.
///
/// Left unhandled, the column is silently omitted from the scoped page index, the reader is
/// handed a placeholder byte range for it, and the decompressor fails with `Src size is
/// incorrect` or `the offset to copy is not contained in the decompressed buffer`. So a nested
/// root is expanded to every physical leaf beneath it. `arrow_schema` is derived from this
/// file's footer, giving a 1:1 root↔`get_column_root_idx` correspondence.
pub(super) fn resolve_with_schema(
    arrow_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    predicate_column_names: &[String],
) -> Vec<usize> {
    let parquet_schema = metadata.file_metadata().schema_descr();
    let mut set = HashSet::new();
    for name in predicate_column_names {
        let resolved = StatisticsConverter::try_new(name, arrow_schema, parquet_schema)
            .ok()
            .and_then(|conv| conv.parquet_column_index());
        if let Some(idx) = resolved {
            set.insert(idx);
            continue;
        }
        if let Some((root_idx, _)) = arrow_schema.fields().find(name) {
            for leaf_idx in 0..parquet_schema.num_columns() {
                if parquet_schema.get_column_root_idx(leaf_idx) == root_idx {
                    set.insert(leaf_idx);
                }
            }
        }
    }
    set.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::builder::ListBuilder;
    use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::WriterProperties;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    /// One scalar `id` column and one `tags` LIST<Utf8> column, so the file has two physical
    /// leaves: `id` and `tags.list.element`. Nothing in the file is named `tags`.
    fn list_column_parquet() -> (Bytes, SchemaRef) {
        // Name the child `element`, matching what ParquetField.toArrowField writes, rather than
        // arrow's default `item` — the leaf path is part of what this test pins.
        let mut tags = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(Field::new(
            "element",
            DataType::Utf8,
            true,
        )));
        for row in 0..8 {
            tags.values().append_value(format!("t{row}"));
            tags.values().append_value("shared");
            tags.append(true);
        }
        let tags: ArrayRef = Arc::new(tags.finish());
        let ids: ArrayRef = Arc::new(Int32Array::from((0..8).collect::<Vec<i32>>()));

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("tags", tags.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![ids, tags]).unwrap();

        let mut buf: Vec<u8> = Vec::new();
        let mut w = ArrowWriter::try_new(
            &mut buf,
            Arc::clone(&schema),
            Some(WriterProperties::builder().build()),
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        (Bytes::from(buf), schema)
    }

    fn file_metadata_and_schema() -> (ParquetMetaData, SchemaRef) {
        let (bytes, _) = list_column_parquet();
        let reader = SerializedFileReader::new(bytes).unwrap();
        let metadata = reader.metadata().clone();
        let file_schema: SchemaRef = Arc::new(
            parquet::arrow::parquet_to_arrow_schema(
                metadata.file_metadata().schema_descr(),
                metadata.file_metadata().key_value_metadata(),
            )
            .unwrap(),
        );
        (metadata, file_schema)
    }

    #[test]
    fn list_root_resolves_to_its_physical_leaf() {
        // Without the nested-root expansion this returns empty: parquet-rs refuses to map the
        // `tags` root to a leaf, the column is scoped out, and the reader is later handed a
        // placeholder byte range for it.
        let (metadata, file_schema) = file_metadata_and_schema();
        let cols = resolve_with_schema(&file_schema, &metadata, &["tags".to_string()]);
        assert_eq!(
            cols.len(),
            1,
            "expected exactly the tags.list.element leaf, got {cols:?}"
        );

        let leaf = metadata.file_metadata().schema_descr().column(cols[0]);
        assert_eq!(
            leaf.path().string(),
            "tags.list.element",
            "resolved leaf should be the list's element path"
        );
    }

    #[test]
    fn scalar_column_still_resolves_directly() {
        let (metadata, file_schema) = file_metadata_and_schema();
        let cols = resolve_with_schema(&file_schema, &metadata, &["id".to_string()]);
        assert_eq!(cols.len(), 1);
        assert_eq!(
            metadata
                .file_metadata()
                .schema_descr()
                .column(cols[0])
                .path()
                .string(),
            "id"
        );
    }

    #[test]
    fn mixed_scalar_and_list_names_resolve_together() {
        let (metadata, file_schema) = file_metadata_and_schema();
        let mut cols = resolve_with_schema(
            &file_schema,
            &metadata,
            &["id".to_string(), "tags".to_string()],
        );
        cols.sort_unstable();
        assert_eq!(cols, vec![0, 1], "both leaves in scope");
    }

    #[test]
    fn absent_column_is_skipped_rather_than_expanded() {
        // Schema evolution: a column in the query but not in this file must contribute nothing,
        // and must not accidentally match a root index.
        let (metadata, file_schema) = file_metadata_and_schema();
        let cols = resolve_with_schema(&file_schema, &metadata, &["not_in_this_file".to_string()]);
        assert!(
            cols.is_empty(),
            "absent column should resolve to nothing, got {cols:?}"
        );
    }
}
