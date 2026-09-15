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
/// parquet-rs: parquet's physical schema is flat, so a `List`/`Map`/`Struct` root can span several
/// leaves and it will not pick one. Both of OpenSearch's composite shapes hit this — an `object`
/// named `city` is a group whose leaves are `city.name`, `city.pop`, …, and a multi-valued `tags` is
/// stored at `tags.list.element`. Nothing in the file is named `city` or `tags`.
///
/// A predicate on one of the object's leaves reads it with `get_field(city, 'name')`, so the column
/// this sees is the struct root. Left unhandled, it is silently omitted from the scoped page index,
/// the reader is handed a placeholder byte range for it, and the decompressor fails with `Src size
/// is incorrect` or `the offset to copy is not contained in the decompressed buffer`. So a nested
/// root is expanded to every physical leaf beneath it. `arrow_schema` is derived from this file's
/// footer, giving a 1:1 root↔`get_column_root_idx` correspondence.
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
    use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray, StringBuilder, StructArray};
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use bytes::Bytes;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::WriterProperties;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    /// One scalar `id`, one `tags` LIST<Utf8> and one `city` STRUCT<name, pop>, so the file has four
    /// physical leaves: `id`, `tags.list.element`, `city.name` and `city.pop`. Nothing in the file is
    /// named `tags` or `city` — both are group nodes, which is the case this resolver exists for.
    fn nested_columns_parquet() -> Bytes {
        // Name the list child `element`, matching what ParquetField.toArrowField writes, rather than
        // arrow's default `item` — the leaf path is part of what these tests pin.
        let mut tags = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(Field::new(
            "element",
            DataType::Utf8,
            true,
        )));
        for row in 0..2 {
            tags.values().append_value(format!("t{row}"));
            tags.values().append_value("shared");
            tags.append(true);
        }
        let tags: ArrayRef = Arc::new(tags.finish());

        let children = Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("pop", DataType::Int32, true),
        ]);
        let city: ArrayRef = Arc::new(
            StructArray::try_new(
                children.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["seattle", "denver"])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![750, 700])) as ArrayRef,
                ],
                None,
            )
            .unwrap(),
        );
        let ids: ArrayRef = Arc::new(Int32Array::from(vec![0, 1]));

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("tags", tags.data_type().clone(), true),
            Field::new("city", DataType::Struct(children), true),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![ids, tags, city]).unwrap();

        let mut buf: Vec<u8> = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buf, schema, Some(WriterProperties::builder().build()))
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buf)
    }

    fn file_metadata_and_schema() -> (ParquetMetaData, SchemaRef) {
        let reader = SerializedFileReader::new(nested_columns_parquet()).unwrap();
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

    fn leaf_paths(metadata: &ParquetMetaData, cols: &[usize]) -> Vec<String> {
        let mut paths: Vec<String> = cols
            .iter()
            .map(|i| {
                metadata
                    .file_metadata()
                    .schema_descr()
                    .column(*i)
                    .path()
                    .string()
            })
            .collect();
        paths.sort();
        paths
    }

    #[test]
    fn struct_root_resolves_to_every_leaf_beneath_it() {
        // A predicate on one of the object's leaves reads it with get_field(city, 'name'), so the
        // column name that reaches here is the struct root. Without the expansion this returns
        // empty: parquet-rs refuses to map `city` to a leaf, the column is scoped out, and the
        // reader is later handed a placeholder byte range for it.
        let (metadata, file_schema) = file_metadata_and_schema();
        let cols = resolve_with_schema(&file_schema, &metadata, &["city".to_string()]);
        assert_eq!(
            leaf_paths(&metadata, &cols),
            vec!["city.name", "city.pop"],
            "both leaves of the object must be in scope"
        );
    }

    #[test]
    fn list_root_resolves_to_its_physical_leaf() {
        // Same expansion, the other composite shape: a multi-valued field is written one level down
        // at `tags.list.element`, so nothing in the file answers to `tags`.
        let (metadata, file_schema) = file_metadata_and_schema();
        let cols = resolve_with_schema(&file_schema, &metadata, &["tags".to_string()]);
        assert_eq!(
            leaf_paths(&metadata, &cols),
            vec!["tags.list.element"],
            "resolved leaf should be the list's element path"
        );
    }

    #[test]
    fn scalar_column_still_resolves_directly() {
        let (metadata, file_schema) = file_metadata_and_schema();
        let cols = resolve_with_schema(&file_schema, &metadata, &["id".to_string()]);
        assert_eq!(leaf_paths(&metadata, &cols), vec!["id"]);
    }

    #[test]
    fn mixed_scalar_and_nested_names_resolve_together() {
        let (metadata, file_schema) = file_metadata_and_schema();
        let cols = resolve_with_schema(
            &file_schema,
            &metadata,
            &["id".to_string(), "city".to_string(), "tags".to_string()],
        );
        assert_eq!(
            leaf_paths(&metadata, &cols),
            vec!["city.name", "city.pop", "id", "tags.list.element"],
            "a scalar, a struct and a list resolve in one pass"
        );
    }

    #[test]
    fn absent_column_is_skipped_rather_than_expanded() {
        // Schema evolution: a column in the query but not in this file must contribute nothing, and
        // must not accidentally match a root index.
        let (metadata, file_schema) = file_metadata_and_schema();
        let cols = resolve_with_schema(&file_schema, &metadata, &["not_in_this_file".to_string()]);
        assert!(
            cols.is_empty(),
            "absent column should resolve to nothing, got {cols:?}"
        );
    }
}
