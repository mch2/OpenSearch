use arrow::array::RecordBatch;
use datafusion::{error::DataFusionError, functions_aggregate::{count::count, sum::sum}, prelude::{col, DataFrame, SessionContext}};

 pub async fn aggregate_batch(
    ctx: &SessionContext,
    record_batch: RecordBatch,
    term_str: &str,
) -> Result<DataFrame, DataFusionError> {

    // Process the data using DataFusion
    let result = ctx
        .read_batch(record_batch)?
        .filter(col(term_str).is_not_null())?
        .aggregate(
            vec![col(term_str).alias("ord")],
            vec![count(col(term_str)).alias("count")],
        )?
        .collect()
        .await?;

    // Convert result back to DataFrame
    Ok(ctx.read_batches(result)?)
}

// Union a list of DataFrames
fn union_df(frames: Vec<DataFrame>) -> datafusion::common::Result<DataFrame> {
    Ok(frames
        .into_iter()
        .reduce(|acc, df| match acc.union(df) {
            Ok(unioned_df) => unioned_df,
            Err(e) => panic!("Failed to union DataFrames: {}", e),
        })
        .ok_or_else(|| DataFusionError::Execution("No frames to union".to_string()))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::prelude::SessionConfig;
    use std::sync::Arc;

    fn create_test_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("category", DataType::Int64, false)]);
        let array = Int64Array::from(values);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
    }

    #[tokio::test]
    async fn test_aggregator_single_batch() {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(1));
        // let mut aggregator = DataFusionAggregator::new(ctx, "`category`".to_string());

        // Create a batch with [1, 1, 2, 2, 2]
        let batch: RecordBatch = create_test_batch(vec![1, 1, 2, 2, 2]);

        // Push batch and check results
        let df = aggregate_batch(&ctx, batch, "category").await.unwrap();

        // Get results and verify
        let batches = df.collect().await.unwrap();

        let expected = vec![
            "+-----+-------+",
            "| ord | count |",
            "+-----+-------+",
            "| 1   | 2     |",
            "| 2   | 3     |",
            "+-----+-------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
    }
}