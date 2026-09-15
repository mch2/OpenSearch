/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! What a root-level projection costs on an `object` stored as a Parquet struct.
//!
//! Reading a struct column means reading every leaf under it. Our scan addresses columns by root
//! index (`read_projection` is a `Vec<usize>`, handed to `with_projection_indices`), so a query
//! naming one sub-field of an object reads the whole object. parquet-rs can mask at leaf level, so
//! the question these tests answer is what the gap is worth — measured as the compressed bytes of
//! the column chunks each mask selects, which is what has to be fetched and decompressed.
//!
//! Both masks are also read back and compared, to show the difference is bytes and not semantics.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tempfile::NamedTempFile;

const ROWS: usize = 50_000;

/// An `object` with `width` keyword sub-fields.
///
/// Heterogeneous on purpose. A real object's sub-fields are nothing alike — a `url` or `message`
/// dominates while a `status` is a handful of repeated tokens — and uniform columns would make the
/// answer trivially "the ratio is the width".
fn write_object_parquet(width: usize) -> NamedTempFile {
    let children: Vec<Field> = (0..width)
        .map(|k| Field::new(format!("k{k:02}"), DataType::Utf8, true))
        .collect();
    let fields = Fields::from(children);

    let child_arrays: Vec<ArrayRef> = (0..width)
        .map(|k| {
            let cardinality = 2 + (k * 37) % 500;
            let padding = 4 + (k * 13) % 120;
            let values: Vec<String> = (0..ROWS)
                .map(|row| format!("k{k:02}-{:0padding$}", row % cardinality, padding = padding))
                .collect();
            Arc::new(StringArray::from(values)) as ArrayRef
        })
        .collect();
    let object: ArrayRef =
        Arc::new(StructArray::try_new(fields.clone(), child_arrays, None).unwrap());
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..ROWS as i64));

    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("attributes", DataType::Struct(fields), true),
    ]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![ids, object]).unwrap();

    // ZSTD on utf8 is what the writer defaults to for keyword columns, so the ratio is comparable.
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();
    let file = NamedTempFile::new().unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Compressed size of every column chunk whose leaf path starts with `prefix`.
fn compressed_bytes_under(file: &NamedTempFile, prefix: &str) -> i64 {
    let builder = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
    let metadata = builder.metadata().clone();
    let descr = metadata.file_metadata().schema_descr();
    let mut total = 0;
    for rg in metadata.row_groups() {
        for (leaf_idx, chunk) in rg.columns().iter().enumerate() {
            if descr.column(leaf_idx).path().string().starts_with(prefix) {
                total += chunk.compressed_size();
            }
        }
    }
    total
}

/// Pulls one child of the `attributes` struct out of every batch a reader produces.
fn collect_child(reader: ParquetRecordBatchReader, child: &str) -> Vec<String> {
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let object = batch
            .column(batch.schema().index_of("attributes").unwrap())
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone();
        let values = object
            .column_by_name(child)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        for i in 0..values.len() {
            out.push(values.value(i).to_string());
        }
    }
    out
}

/// Reads one leaf two ways — the whole struct (what a root-level projection does) and just that
/// leaf — and reports the compressed bytes each one has to touch.
///
/// Returns (whole object, cheapest leaf, dearest leaf), so the answer is a range rather than one
/// number: what a query saves depends on which sub-field it asked for.
fn measure(width: usize) -> (i64, i64, i64) {
    let file = write_object_parquet(width);
    let whole_object = compressed_bytes_under(&file, "attributes.");

    let mut per_leaf: Vec<(String, i64)> = (0..width)
        .map(|k| {
            let name = format!("attributes.k{k:02}");
            let bytes = compressed_bytes_under(&file, &name);
            (name, bytes)
        })
        .collect();
    per_leaf.sort_by_key(|(_, bytes)| *bytes);
    let cheapest = per_leaf.first().unwrap().1;
    let (dearest_path, dearest) = per_leaf.last().unwrap().clone();

    // Same values either way: the mask changes what is decoded, not what it means. Checked on the
    // dearest leaf, the one a query is most likely to actually want.
    let child = dearest_path.rsplit('.').next().unwrap().to_string();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
    let descr = builder.parquet_schema().clone();
    let leaf_index = (0..descr.num_columns())
        .find(|&i| descr.column(i).path().string() == dearest_path)
        .expect("leaf exists");

    let root_reader = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
        .unwrap()
        .with_projection(ProjectionMask::roots(&descr, [1]))
        .build()
        .unwrap();
    let leaf_reader = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
        .unwrap()
        .with_projection(ProjectionMask::leaves(&descr, [leaf_index]))
        .build()
        .unwrap();

    let from_root = collect_child(root_reader, &child);
    let from_leaf = collect_child(leaf_reader, &child);
    assert_eq!(
        from_root, from_leaf,
        "leaf masking must not change the values"
    );
    assert_eq!(from_root.len(), ROWS);

    (whole_object, cheapest, dearest)
}

fn report(width: usize) {
    let (whole_object, cheapest, dearest) = measure(width);
    println!(
        "width={width:<3} whole object={whole_object:>9} B   dearest leaf={dearest:>8} B ({:>5.1}x)   cheapest leaf={cheapest:>8} B ({:>6.1}x)",
        whole_object as f64 / dearest as f64,
        whole_object as f64 / cheapest as f64
    );
    assert!(whole_object > dearest);
}

/// 55 sub-fields — an OTel span's `attributes`.
#[test]
fn wide_object_root_projection_cost() {
    report(55);
}

/// 3 sub-fields — a `city{name,population,zone}`, the shape the object ITs use.
#[test]
fn narrow_object_root_projection_cost() {
    report(3);
}

/// In between, so the shape of the curve is visible rather than inferred from two points.
#[test]
fn mid_width_object_root_projection_cost() {
    report(12);
}
