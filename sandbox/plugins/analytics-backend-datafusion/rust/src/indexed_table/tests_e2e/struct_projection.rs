/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! The indexed scan reads only the sub-fields of an `object` that the query named.
//!
//! An object is stored as a Parquet struct, so asking for the struct column reads every leaf under
//! it — 18x the bytes on a 55-field OTel `attributes`, and far worse when the sub-field wanted is a
//! small one (`tests/struct_projection_cost_tests.rs` measures it). The vanilla path already prunes
//! because DataFusion owns its scan node; this pins the same behaviour for the indexed path, which
//! has its own.
//!
//! Measured through the parquet source's `bytes_scanned` metric — the bytes actually fetched.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::{collect, ExecutionPlan};
use object_store::path::Path as ObjectPath;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tempfile::NamedTempFile;

use crate::datafusion_query_config::DatafusionQueryConfig;
use crate::indexed_table::bool_tree::BoolNode;
use crate::indexed_table::eval::bitmap_tree::{BitmapTreeEvaluator, CollectorLeafBitmaps};
use crate::indexed_table::eval::TreeBitsetSource;
use crate::indexed_table::eval::{CollectorCallStrategy, RowGroupBitsetSource};
use crate::indexed_table::page_pruner::PagePruner;
use crate::indexed_table::stream::RowGroupInfo;
use crate::indexed_table::table_provider::{
    EvaluatorFactory, IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use std::collections::HashMap;

const ROWS: usize = 20_000;
const WIDTH: usize = 40;

/// `id BIGINT` and an `attributes` object of `WIDTH` keyword sub-fields, sized unevenly so the
/// saving is not simply one over the width.
fn write_wide_object() -> NamedTempFile {
    let children: Vec<Field> = (0..WIDTH)
        .map(|k| Field::new(format!("k{k:02}"), DataType::Utf8, true))
        .collect();
    let fields = Fields::from(children);

    let child_arrays: Vec<ArrayRef> = (0..WIDTH)
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

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();
    let file = NamedTempFile::new().unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// An `IndexedTableProvider` over the file, with no index filter — the scan-only shape, which is
/// what isolates the projection.
fn provider_for(
    file: &NamedTempFile,
    predicate: Arc<dyn PhysicalExpr>,
    predicate_columns: Vec<usize>,
) -> Arc<IndexedTableProvider> {
    let path = file.path().to_path_buf();
    let size = std::fs::metadata(&path).unwrap().len();
    let handle = std::fs::File::open(&path).unwrap();
    let meta = ArrowReaderMetadata::load(&handle, ArrowReaderOptions::new().with_page_index(true))
        .unwrap();
    let schema = meta.schema().clone();
    let parquet_meta = meta.metadata().clone();

    let mut row_groups = Vec::new();
    let mut offset = 0i64;
    for i in 0..parquet_meta.num_row_groups() {
        let rows = parquet_meta.row_group(i).num_rows();
        row_groups.push(RowGroupInfo {
            index: i,
            first_row: offset,
            num_rows: rows,
        });
        offset += rows;
    }

    let segment = SegmentFileInfo {
        writer_generation: 0,
        max_doc: ROWS as i64,
        object_path: ObjectPath::from(path.to_string_lossy().as_ref()),
        parquet_size: size,
        row_groups,
        metadata: Arc::clone(&parquet_meta),
        arrow_schema: schema.clone(),
        global_base: 0,
        sort_min: None,
        sort_max: None,
    };

    let schema_for_factory = schema.clone();
    Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema,
        segments: vec![segment],
        store: Arc::new(object_store::local::LocalFileSystem::new()),
        store_url: datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
        evaluator_factory: evaluator_over(schema_for_factory, Arc::clone(&predicate)),
        pushdown_predicate: None,
        query_config: Arc::new(
            DatafusionQueryConfig::builder()
                .target_partitions(1)
                .build(),
        ),
        // `predicate_columns` puts what the evaluator reads into the scan; `predicate_exprs` is
        // what keeps the leaf set aware of how it reaches into an object.
        predicate_columns,
        predicate_exprs: vec![predicate],
        emit_row_ids: false,
        prune_tree_config: None,
        sort_fields: vec![],
        sort_orders: vec![],
        cancellation_token: None,
    }))
}

/// Sums `bytes_scanned` across every operator of the executed plan.
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

/// Runs `sql`, returning (batches, bytes fetched from the parquet file).
async fn run(sql: &str) -> (Vec<RecordBatch>, usize) {
    run_with(sql, |_| (always_true(), vec![])).await
}

/// As [`run`], with the predicate the evaluator will apply chosen from the file's schema.
async fn run_with(
    sql: &str,
    predicate: impl Fn(&Schema) -> (Arc<dyn PhysicalExpr>, Vec<usize>),
) -> (Vec<RecordBatch>, usize) {
    let file = write_wide_object();
    let handle = std::fs::File::open(file.path()).unwrap();
    let schema = ArrowReaderMetadata::load(&handle, ArrowReaderOptions::new())
        .unwrap()
        .schema()
        .clone();
    let (predicate, predicate_columns) = predicate(&schema);
    let ctx = SessionContext::new();
    ctx.register_table("t", provider_for(&file, predicate, predicate_columns))
        .unwrap();
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    let bytes = bytes_scanned(&plan);
    (batches, bytes)
}

/// A predicate that is true for every row and reads nothing, so a measurement is not skewed by a
/// column the evaluator dragged into the read.
fn always_true() -> Arc<dyn PhysicalExpr> {
    Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
}

/// `attributes['k07'] = value` — a predicate on one sub-field of the object.
fn subfield_equals(schema: &Schema, value: &str) -> Arc<dyn PhysicalExpr> {
    let get_field = Arc::new(datafusion::logical_expr::ScalarUDF::from(
        datafusion::functions::core::getfield::GetFieldFunc::new(),
    ));
    let leaf: Arc<dyn PhysicalExpr> = Arc::new(
        datafusion::physical_expr::ScalarFunctionExpr::try_new(
            get_field,
            vec![
                Arc::new(Column::new(
                    "attributes",
                    schema.index_of("attributes").unwrap(),
                )),
                Arc::new(Literal::new(ScalarValue::Utf8(Some("k07".to_string())))),
            ],
            schema,
            Arc::new(datafusion::common::config::ConfigOptions::default()),
        )
        .unwrap(),
    );
    Arc::new(BinaryExpr::new(
        leaf,
        Operator::Eq,
        Arc::new(Literal::new(ScalarValue::Utf8(Some(value.to_string())))),
    ))
}

/// An evaluator applying `predicate`. The indexed path always builds one — it owns filtering, since
/// the provider reports `Exact` pushdown and DataFusion therefore drops its own filter.
fn evaluator_over(schema: SchemaRef, predicate: Arc<dyn PhysicalExpr>) -> EvaluatorFactory {
    let tree = Arc::new(BoolNode::Predicate(predicate));
    Arc::new(move |segment, _chunk, _stream_metrics, _stats_prune_tree| {
        let resolved = tree.resolve(&[])?;
        let pruner = Arc::new(PagePruner::new(
            &schema,
            Arc::clone(&segment.metadata),
            schema.clone(),
        ));
        let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
            tree: Arc::new(resolved),
            evaluator: Arc::new(BitmapTreeEvaluator),
            leaves: Arc::new(CollectorLeafBitmaps::without_metrics()),
            page_pruner: pruner,
            cost_predicate: 1,
            cost_collector: 10,
            max_collector_parallelism: 1,
            pruning_predicates: Arc::new(HashMap::new()),
            page_prune_metrics: None,
            collector_strategy: CollectorCallStrategy::TightenOuterBounds,
            stats_prune_tree: None,
            rg_index_to_pos: HashMap::new(),
        });
        Ok(eval)
    })
}

/// Selecting one sub-field must not read the whole object.
#[tokio::test]
async fn indexed_path_prunes_object_to_the_requested_subfield() {
    let (one_batches, one_bytes) = run("SELECT attributes['k07'] FROM t").await;
    let (_, all_bytes) = run("SELECT attributes FROM t").await;

    println!(
        "indexed path: one sub-field={one_bytes} B, whole object={all_bytes} B ({:.1}x)",
        all_bytes as f64 / one_bytes.max(1) as f64
    );

    let rows: usize = one_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, ROWS);
    assert!(one_bytes > 0, "expected to read something");
    assert!(
        one_bytes * 4 < all_bytes,
        "one sub-field of a {WIDTH}-field object should read far less than the whole object, got \
         {one_bytes} B vs {all_bytes} B"
    );
}

/// And the values must be the ones on disk — pruning changes what is read, not what it means.
#[tokio::test]
async fn indexed_path_returns_the_same_values_when_pruned() {
    let (batches, _) = run("SELECT id, attributes['k07'] FROM t ORDER BY id LIMIT 3").await;

    let batch = batches.first().expect("a batch");
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Written as `k07-<row % cardinality>` zero-padded; row 0 is the first of that cycle.
    let cardinality = 2 + (7 * 37) % 500;
    let padding = 4 + (7 * 13) % 120;
    for i in 0..3 {
        let row = ids.value(i) as usize;
        let expected = format!("k07-{:0padding$}", row % cardinality, padding = padding);
        assert_eq!(values.value(i), expected, "row {row}");
    }
}

/// A whole-object read still gets the whole object, with every sub-field populated.
#[tokio::test]
async fn indexed_path_still_reads_the_whole_object_when_asked() {
    let (batches, _) = run("SELECT attributes FROM t LIMIT 1").await;

    let batch = batches.first().expect("a batch");
    let object = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        object.num_columns(),
        WIDTH,
        "every sub-field must be present"
    );
    for k in 0..WIDTH {
        let name = format!("k{k:02}");
        let child = object.column_by_name(&name).unwrap();
        assert!(
            child.is_null(0) == false,
            "sub-field {name} should carry its value, not be nulled by pruning"
        );
    }
}

/// A predicate on a sub-field must read that leaf, so the evaluator can apply it. The failure this
/// guards is a predicate silently matching nothing because its leaf was pruned away and read null.
#[tokio::test]
async fn indexed_path_keeps_the_leaves_a_predicate_needs() {
    let cardinality = 2 + (7 * 37) % 500;
    let padding = 4 + (7 * 13) % 120;
    let wanted = format!("k07-{:0padding$}", 0, padding = padding);

    // The projection asks for a different sub-field than the predicate, so the leaf set has to
    // cover both. Asking for the same one would pass even if predicates were ignored.
    let (batches, _) = run_with("SELECT attributes['k00'] FROM t", |schema| {
        (subfield_equals(schema, &wanted), vec![1])
    })
    .await;

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let expected = ROWS / cardinality + usize::from(ROWS % cardinality > 0);
    assert_eq!(
        rows, expected,
        "the predicate must see real values, not nulls from a pruned leaf"
    );
}
