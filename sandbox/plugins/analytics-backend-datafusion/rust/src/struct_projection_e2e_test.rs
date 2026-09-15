/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Does the vanilla (non-indexed) path prune an `object` to the sub-fields a query asked for?
//!
//! An OpenSearch `object` is stored as a Parquet struct, so reading the struct column reads every
//! leaf under it. DataFusion can mask at leaf level: its `ProjectionPushdown` folds a
//! `get_field(attributes, 'k07')` projection into `DataSourceExec`, and the parquet opener's
//! `build_projection_read_plan` turns that into `ProjectionMask::leaves`.
//!
//! That only holds for DataFusion's own scan node, which is what the vanilla path uses
//! (`register_listing_table`). The indexed path returns a custom `QueryShardExec` and does not get
//! this for free. These tests pin the vanilla behaviour so the difference between the two paths is
//! a measured fact rather than an assumption.
//!
//! Observed via the parquet source's own `bytes_scanned` metric — the bytes actually fetched.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::{collect, ExecutionPlan};
    use datafusion::prelude::ParquetReadOptions;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    const ROWS: usize = 20_000;
    const WIDTH: usize = 40;

    /// One `id` column and one `attributes` struct of `WIDTH` keyword sub-fields, sized unevenly so
    /// the saving is not just "one over the width".
    fn write_wide_object(dir: &TempDir) -> String {
        let children: Vec<Field> = (0..WIDTH)
            .map(|k| Field::new(format!("k{k:02}"), DataType::Utf8, true))
            .collect();
        let fields = Fields::from(children);

        let child_arrays: Vec<ArrayRef> = (0..WIDTH)
            .map(|k| {
                let cardinality = 2 + (k * 37) % 500;
                let padding = 4 + (k * 13) % 120;
                let values: Vec<String> = (0..ROWS)
                    .map(|row| {
                        format!("k{k:02}-{:0padding$}", row % cardinality, padding = padding)
                    })
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

        let path = dir.path().join("wide.parquet");
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .build();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path.to_string_lossy().to_string()
    }

    /// Sums `bytes_scanned` across every operator in the executed plan.
    fn bytes_scanned(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let here = plan
            .metrics()
            .map(|m| {
                m.iter()
                    .filter(|metric| metric.value().name() == "bytes_scanned")
                    .map(|metric| metric.value().as_usize())
                    .sum::<usize>()
            })
            .unwrap_or(0);
        here + plan
            .children()
            .iter()
            .map(|child| bytes_scanned(&Arc::clone(child)))
            .sum::<usize>()
    }

    /// Runs `sql`, returning (rows, bytes actually fetched from the parquet file).
    async fn run(ctx: &SessionContext, sql: &str) -> (usize, usize) {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
        let rows = batches.iter().map(|b| b.num_rows()).sum();
        (rows, bytes_scanned(&plan))
    }

    /// Selecting one sub-field of a wide object must not read the whole object.
    #[tokio::test]
    async fn vanilla_path_prunes_object_to_the_requested_subfield() {
        let dir = TempDir::new().unwrap();
        let path = write_wide_object(&dir);
        let ctx = SessionContext::new();
        ctx.register_parquet("t", &path, ParquetReadOptions::default())
            .await
            .unwrap();

        let (one_rows, one_bytes) = run(&ctx, "SELECT attributes['k07'] FROM t").await;
        let (all_rows, all_bytes) = run(&ctx, "SELECT attributes FROM t").await;

        println!(
            "vanilla path: one sub-field={one_bytes} B, whole object={all_bytes} B ({:.1}x)",
            all_bytes as f64 / one_bytes.max(1) as f64
        );

        assert_eq!(one_rows, ROWS);
        assert_eq!(all_rows, ROWS);
        assert!(one_bytes > 0, "expected to read something");
        assert!(
            one_bytes * 4 < all_bytes,
            "one sub-field of a {WIDTH}-field object should read far less than the whole object, \
             got {one_bytes} B vs {all_bytes} B — projection is not being pruned to leaves"
        );
    }

    /// Same for a filter: the predicate names one sub-field, so only that leaf has to be read.
    #[tokio::test]
    async fn vanilla_path_prunes_object_for_a_filter() {
        let dir = TempDir::new().unwrap();
        let path = write_wide_object(&dir);
        let ctx = SessionContext::new();
        ctx.register_parquet("t", &path, ParquetReadOptions::default())
            .await
            .unwrap();

        let (_, filter_bytes) = run(
            &ctx,
            "SELECT count(*) FROM t WHERE attributes['k07'] = 'nope'",
        )
        .await;
        let (_, all_bytes) = run(&ctx, "SELECT attributes FROM t").await;

        println!(
            "vanilla path: filter on one sub-field={filter_bytes} B, whole object={all_bytes} B"
        );
        assert!(
            filter_bytes * 4 < all_bytes,
            "a filter on one sub-field should read far less than the whole object, got \
             {filter_bytes} B vs {all_bytes} B"
        );
    }
}
