use std::error::Error;
use std::io::Cursor;
use std::ptr::addr_of_mut;
use std::time::Duration;

use arrow::array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::array::{Array, RecordBatch, StructArray};
use arrow::datatypes::Schema;
use arrow::ffi::{self};
use arrow::ipc::writer::FileWriter;
use bytes::Bytes;
use datafusion::catalog::Session;
use datafusion::error::DataFusionError;
use datafusion::execution::{context, RecordBatchStream, SendableRecordBatchStream};
use datafusion::functions_aggregate::count::count;
use datafusion::functions_aggregate::sum::sum;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::prelude::{col, DataFrame, SessionConfig, SessionContext};
use futures::stream::TryStreamExt;
use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jint, jlong};
use jni::{AttachGuard, JNIEnv};

use std::io::BufWriter;
use tokio::runtime::Runtime;
mod provider;

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusion_load(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    ctx: jlong,
    array_ptr: jlong,
    schema_ptr: jlong,
    term: JString,
    callback: JObject,
) {
    let context = unsafe { &mut *(ctx as *mut SessionContext) };
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let term_str: String = format!(
        "`{}`",
        env.get_string(&term)
            .expect("Invalid term string")
            .to_string_lossy()
            .into_owned()
    );
    // Take ownership of FFI structs immediately outside the async block
    let array = unsafe { ffi::FFI_ArrowArray::from_raw(array_ptr as *mut _) };
    let schema = unsafe { ffi::FFI_ArrowSchema::from_raw(schema_ptr as *mut _) };

    runtime.block_on(async {
        let result = unsafe {
            let data = arrow::ffi::from_ffi(array, &schema).unwrap();
            let arrow_array = arrow::array::make_array(data);

            // Create DataFrame
            let struct_array = arrow_array
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .unwrap();
            let record_batch = RecordBatch::try_from(struct_array).unwrap();
            let df = context
                .read_batch(record_batch)
                .and_then(|df| df.filter(col(&term_str).is_not_null()))
                .and_then(|df| {
                    df.aggregate(
                        vec![col(&term_str).alias("ord")],
                        vec![
                            datafusion::functions_aggregate::count::count(col(&term_str))
                                .alias("count"),
                        ],
                    )
                })
                .and_then(|agg_df| agg_df.sort(vec![col("count").sort(true, false)]))
                .and_then(|sorted| sorted.limit(0, Some(500)))
                .and_then(|limited| limited.sort(vec![col("ord").sort(false, true)]));

            df
        };

        let addr = result.map(|df| Box::into_raw(Box::new(df)));
        set_object_result(&mut env, callback, addr);
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusion_query(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    ctx: jlong,
    ticket: JByteArray,
    callback: JObject,
) {
    let input = env.convert_byte_array(&ticket).unwrap();
    let context: &mut SessionContext = unsafe { &mut *(ctx as *mut SessionContext) };
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };

    runtime.block_on(async {
        let result = provider::read_aggs(
            context.clone(),
            Bytes::from(input),
            "http://localhost:9450".to_owned(),
            5,
        )
        .await;
        let addr = result.map(|df| Box::into_raw(Box::new(df)));
        set_object_result(&mut env, callback, addr);
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusion_agg(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    ctx: jlong,
    ticket: JByteArray,
    size: jint,
    callback: JObject,
) {
    let input = env.convert_byte_array(&ticket).unwrap();
    let context = unsafe { &mut *(ctx as *mut SessionContext) };
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };

    runtime.block_on(async {
        let result = provider::read_aggs(
            context.clone(),
            Bytes::from(input),
            "http://localhost:9450".to_owned(),
            size.try_into().unwrap(),
        )
        .await;
        let addr = result.map(|df| Box::into_raw(Box::new(df)));
        set_object_result(&mut env, callback, addr);
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusion_collect(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    df: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let dataframe = unsafe { &mut *(df as *mut DataFrame) };

    let schema = dataframe.schema().into();
    runtime.block_on(async {
        let batches = dataframe
            .clone()
            .collect()
            .await
            .expect("failed to collect dataframe");

        let mut buff = Cursor::new(vec![0; 0]);

        {
            let mut writer = FileWriter::try_new(BufWriter::new(&mut buff), &schema)
                .expect("failed to create writer");
            for batch in batches {
                writer.write(&batch).expect("failed to write batch");
            }
            writer.finish().expect("failed to finish");
        }

        let err_message = env
            .new_string("".to_string())
            .expect("Couldn't create java string!");

        let ba = env
            .byte_array_from_slice(buff.get_ref())
            .expect("cannot create empty byte array");

        env.call_method(
            callback,
            "accept",
            "(Ljava/lang/Object;Ljava/lang/Object;)V",
            &[(&err_message).into(), (&ba).into()],
        )
        .expect("failed to call method");
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_SessionContext_destroySessionContext(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut SessionContext) };
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_SessionContext_createSessionContext(
    _env: JNIEnv,
    _class: JClass,
    size: jint,
) -> jlong {
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config(config);
    Box::into_raw(Box::new(context)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_SessionContext_createRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    if let Ok(runtime) = Runtime::new() {
        Box::into_raw(Box::new(runtime)) as jlong
    } else {
        // TODO error handling
        -1
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_SessionContext_destroyRuntime(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let runtime = unsafe { Box::from_raw(pointer as *mut Runtime) };
    runtime.shutdown_timeout(Duration::from_millis(100));
}

pub fn set_object_result<T, Err: Error>(
    env: &mut JNIEnv,
    callback: JObject,
    address: Result<*mut T, Err>,
) {
    match address {
        Ok(address) => set_object_result_ok(env, callback, address),
        Err(err) => set_object_result_error(env, callback, &err),
    };
}

/// Set success result by calling an ObjectResultCallback
pub fn set_object_result_ok<T>(env: &mut JNIEnv, callback: JObject, address: *mut T) {
    let err_message = JObject::null();
    env.call_method(
        callback,
        "callback",
        "(Ljava/lang/String;J)V",
        &[(&err_message).into(), (address as jlong).into()],
    )
    .expect("Failed to call object result callback with address");
}

/// Set error result by calling an ObjectResultCallback
pub fn set_object_result_error<T: Error>(env: &mut JNIEnv, callback: JObject, error: &T) {
    let err_message = env
        .new_string(error.to_string())
        .expect("Couldn't create java string for error message");
    let address = -1 as jlong;
    env.call_method(
        callback,
        "callback",
        "(Ljava/lang/String;J)V",
        &[(&err_message).into(), address.into()],
    )
    .expect("Failed to call object result callback with error");
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusion_executeStream(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    dataframe: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let dataframe = unsafe { &mut *(dataframe as *mut DataFrame) };
    runtime.block_on(async {
        let stream_result = dataframe.clone().execute_stream().await;
        set_object_result(
            &mut env,
            callback,
            stream_result.map(|stream| Box::into_raw(Box::new(stream))),
        );
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_next(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    stream: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    runtime.block_on(async {
        let next: Result<Option<arrow::array::RecordBatch>, datafusion::error::DataFusionError> =
            stream.try_next().await;
        match next {
            Ok(Some(batch)) => {
                // Convert to struct array for compatibility with FFI
                let struct_array: StructArray = batch.into();
                let array_data = struct_array.into_data();
                let mut ffi_array = FFI_ArrowArray::new(&array_data);
                // ffi_array must remain alive until after the callback is called
                set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_array));
            }
            Ok(None) => {
                set_object_result_ok(&mut env, callback, 0 as *mut FFI_ArrowSchema);
            }
            Err(err) => {
                set_object_result_error(&mut env, callback, &err);
            }
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_destroy(
    mut env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut SendableRecordBatchStream) };
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_getSchema(
    mut env: JNIEnv,
    _class: JClass,
    stream: jlong,
    callback: JObject,
) {
    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    let schema = stream.schema();
    let ffi_schema = FFI_ArrowSchema::try_from(&*schema);
    match ffi_schema {
        Ok(mut ffi_schema) => {
            // ffi_schema must remain alive until after the callback is called
            set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_schema));
        }
        Err(err) => {
            set_object_result_error(&mut env, callback, &err);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFrame_destroyDataFrame(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut DataFrame) };
}

pub struct DataFusionAggregator {
    context: SessionContext,
    current_aggregation: Option<DataFrame>,
    term_column: String,
}

impl DataFusionAggregator {
    pub fn new(context: SessionContext, term_column: String) -> Self {
        DataFusionAggregator {
            context,
            current_aggregation: None,
            term_column,
        }
    }

    pub async fn push_batch(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        // agg and collect the new batch immediately, we need to pre-aggregate the incoming batch
        // so that union works, otherwise we have 2 cols union with 1.
        let aggregated = self
            .context
            .read_batch(batch)?
            .filter(col(&self.term_column).is_not_null())?
            .aggregate(
                vec![col(&self.term_column).alias("ord")],
                vec![count(col(&self.term_column))
                .alias("count")],
            )?
            .collect()
            .await?;

        // Convert collected batches back to DataFrame
        let incoming_df = self.context.read_batches(aggregated)?;

        // Merge with existing aggregation if we have one
        self.current_aggregation = match self.current_aggregation.take() {
            Some(existing) => {
                let merged = existing
                    .union(incoming_df)?
                    .collect()
                    .await?;
                Some(self.context.read_batches(merged)?)
            }
            None => Some(incoming_df),
        };

        Ok(())
    }

    pub fn get_results(&self) -> Option<DataFrame> {
        self.current_aggregation
            .as_ref()
            .and_then(|df| df.clone()
            .aggregate(vec![col("ord")], vec![sum(col("count")).alias("count")]).unwrap()
            .sort(vec![col("ord").sort(true, false)]).ok())
    }

    pub fn get_results_with_limit(&self, limit: i32) -> Option<DataFrame> {
        self.current_aggregation.as_ref().and_then(|df| {
            if limit > 0 {
                df.clone()
                    .limit(0, Some(limit as usize))
                    .and_then(|limited| limited.sort(vec![col("ord").sort(false, false)]))
                    .ok()
            } else {
                Some(df.clone())
            }
        })
    }

    pub fn take_results(&mut self) -> Option<DataFrame> {
        self.current_aggregation.take()
    }
}

// JNI bindings
#[no_mangle]
pub extern "system" fn Java_org_opensearch_search_stream_collector_DataFusionAggregator_create(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    term: JString,
) -> jlong {
    let context = unsafe { &*(ctx as *const SessionContext) };
    let term_str = format!(
        "`{}`",
        env.get_string(&term)
            .expect("Invalid term string")
            .to_string_lossy()
            .into_owned()
    );

    let aggregator = DataFusionAggregator::new(context.clone(), term_str);
    Box::into_raw(Box::new(aggregator)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_search_stream_collector_DataFusionAggregator_pushBatch(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    agg_ptr: jlong,
    array_ptr: jlong,
    schema_ptr: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let aggregator = unsafe { &mut *(agg_ptr as *mut DataFusionAggregator) };

    let array = unsafe { ffi::FFI_ArrowArray::from_raw(array_ptr as *mut _) };
    let schema = unsafe { ffi::FFI_ArrowSchema::from_raw(schema_ptr as *mut _) };

    runtime.block_on(async {
        let result = unsafe {
            let data = arrow::ffi::from_ffi(array, &schema).unwrap();
            let arrow_array = arrow::array::make_array(data);
            let struct_array = arrow_array
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .unwrap();
            let record_batch = RecordBatch::try_from(struct_array).unwrap();

            aggregator
                .push_batch(record_batch)
                .await
                .map(|_| Box::into_raw(Box::new(1i32)))
        };

        set_object_result(&mut env, callback, result);
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_search_stream_collector_DataFusionAggregator_getResults(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    agg_ptr: jlong,
    limit: jint,
    callback: JObject,
) {
    let aggregator = unsafe { &mut *(agg_ptr as *mut DataFusionAggregator) };

    let result = aggregator
        .get_results()
        .map(|df| Box::into_raw(Box::new(df)))
        .ok_or_else(|| DataFusionError::Execution("No results available".to_string()));

    set_object_result::<DataFrame, DataFusionError>(&mut env, callback, result);
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_search_stream_collector_DataFusionAggregator_destroy(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut DataFusionAggregator) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_sorted_eq;
    use std::sync::Arc;

    fn create_test_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("category", DataType::Int64, false)]);
        let array = Int64Array::from(values);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
    }

    #[tokio::test]
    async fn test_aggregator_single_batch() {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(1));
        let mut aggregator = DataFusionAggregator::new(ctx, "`category`".to_string());

        // Create a batch with [1, 1, 2, 2, 2]
        let batch: RecordBatch = create_test_batch(vec![1, 1, 2, 2, 2]);

        // Push batch and check results
        aggregator.push_batch(batch).await.unwrap();

        // Get results and verify
        let batches = aggregator.take_results().unwrap().collect().await.unwrap();

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

    #[tokio::test]
    async fn test_aggregator_multiple_batches() {
        let ctx = SessionContext::new();
        let mut aggregator = DataFusionAggregator::new(ctx, "`category`".to_string());

        // Push multiple batches
        aggregator
            .push_batch(create_test_batch(vec![1, 1]))
            .await
            .unwrap();

        let batches = aggregator.get_results().unwrap().collect().await.unwrap();
        let expected = vec![
            "+-----+-------+",
            "| ord | count |",
            "+-----+-------+",
            "| 1   | 2     |",
            "+-----+-------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        aggregator
            .push_batch(create_test_batch(vec![2, 2]))
            .await
            .unwrap();

        let batches = aggregator.get_results().unwrap().collect().await.unwrap();
        let expected = vec![
            "+-----+-------+",
            "| ord | count |",
            "+-----+-------+",
            "| 1   | 2     |",
            "| 2   | 2     |",
            "+-----+-------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        aggregator
            .push_batch(create_test_batch(vec![2]))
            .await
            .unwrap();

        // Get results and verify
        let batches = aggregator.get_results().unwrap().collect().await.unwrap();
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

    #[tokio::test]
    async fn test_aggregator_empty_batch() {
        let ctx = SessionContext::new();
        let mut aggregator = DataFusionAggregator::new(ctx, "`category`".to_string());

        // Push empty batch
        let batch = create_test_batch(vec![]);
        aggregator.push_batch(batch).await.unwrap();

        // Get results and verify
        let batches = aggregator.take_results().unwrap().collect().await.unwrap();
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn test_aggregator_limit_and_sort() {
        let ctx = SessionContext::new();
        let mut aggregator = DataFusionAggregator::new(ctx, "`category`".to_string());

        // Create batch with multiple values
        let batch = create_test_batch(vec![1, 1, 1, 1, 2, 2, 3, 3, 3, 4]);
        aggregator.push_batch(batch).await.unwrap();

        // Get results with limit
        let batches = aggregator
            .get_results_with_limit(1)
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected = vec![
            "+-----+-------+",
            "| ord | count |",
            "+-----+-------+",
            "| 1   | 4     |",
            "+-----+-------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        let batches = aggregator
            .get_results_with_limit(4)
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected = vec![
            "+-----+-------+",
            "| ord | count |",
            "+-----+-------+",
            "| 1   | 4     |",
            "| 2   | 2     |",
            "| 3   | 3     |",
            "| 4   | 1     |",
            "+-----+-------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_aggregator_ordering() {
        let ctx = SessionContext::new();
        let mut aggregator = DataFusionAggregator::new(ctx, "`category`".to_string());

        // Push values in random order
        aggregator
            .push_batch(create_test_batch(vec![3, 1]))
            .await
            .unwrap();
        aggregator
            .push_batch(create_test_batch(vec![2, 4]))
            .await
            .unwrap();

        // Get results and verify ordering
        let batches = aggregator.take_results().unwrap().collect().await.unwrap();
        let expected = vec![
            "+-----+-------+",
            "| ord | count |",
            "+-----+-------+",
            "| 1   | 1     |",
            "| 2   | 1     |",
            "| 3   | 1     |",
            "| 4   | 1     |",
            "+-----+-------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_aggregator_null_handling() {
        let ctx = SessionContext::new();
        let mut aggregator = DataFusionAggregator::new(ctx, "`category`".to_string());

        // Create a batch with some null values
        let schema = Schema::new(vec![Field::new("category", DataType::Int64, true)]);
        let array = Int64Array::from(vec![Some(1), None, Some(2), None, Some(1)]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        aggregator.push_batch(batch).await.unwrap();

        // Get results and verify
        let batches = aggregator.take_results().unwrap().collect().await.unwrap();
        let expected = vec![
            "+-----+-------+",
            "| ord | count |",
            "+-----+-------+",
            "| 1   | 2     |",
            "| 2   | 1     |",
            "+-----+-------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
    }
}

// I WAS TRYING SOME WEIRD SHIT BELOW THIS LINE, IT DOES NOT WORK BUT IM LEAVING IT FOR ANOTHER TIME WHEN
// I WANT TO TRY WEIRD SHIT AGAIN

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::Stream;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{context::SessionState, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};

use jni::JavaVM;

use jni::objects::GlobalRef;

struct CollectorStream {
    collector_ref: GlobalRef,
    schema: SchemaRef,
    finished: bool,
    metrics: MetricsSet,
    jvm: Arc<JavaVM>, // Store JavaVM instead of AttachGuard
}

impl Stream for CollectorStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.finished {
            return Poll::Ready(None);
        }

        let mut env_guard = match this.jvm.attach_current_thread() {
            Ok(guard) => guard,
            Err(e) => return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
        };

        let result = env_guard.call_method(
            this.collector_ref.as_obj(),
            "getNextBatch",
            "()LBatchPointers;",
            &[],
        );

        match result {
            Ok(output) => match output.l() {
                Ok(obj) => {
                    if obj.is_null() {
                        this.finished = true;
                        Poll::Ready(None)
                    } else {
                        let schema_ptr = env_guard
                            .get_field(&obj, "schemaPtr", "J")
                            .map_err(|e| DataFusionError::External(Box::new(e)))?
                            .j()
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                        let array_ptr = env_guard
                            .get_field(&obj, "arrayPtr", "J")
                            .map_err(|e| DataFusionError::External(Box::new(e)))?
                            .j()
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                        match convert_to_record_batch(schema_ptr, array_ptr, &this.jvm) {
                            Ok(batch) => Poll::Ready(Some(Ok(batch))),
                            Err(e) => Poll::Ready(Some(Err(e))),
                        }
                    }
                }
                Err(e) => Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
            },
            Err(e) => Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
        }
    }
}

#[derive(Debug)]
struct CollectorScan {
    collector_ref: GlobalRef,
    jvm: Arc<JavaVM>, // Wrap JavaVM in Arc here too
    projected_schema: SchemaRef,
    metrics: MetricsSet,
    properties: PlanProperties,
}

impl CollectorScan {
    fn new(collector_ref: GlobalRef, jvm: Arc<JavaVM>, schema: SchemaRef) -> Self {
        CollectorScan {
            collector_ref,
            jvm,
            projected_schema: schema.clone(),
            metrics: MetricsSet::new(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::RoundRobinBatch(5),
                datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                datafusion::physical_plan::execution_plan::Boundedness::Bounded,
            ),
        }
    }
}

impl DisplayAs for CollectorScan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "CollectorScan")
            }
            DisplayFormatType::Verbose => todo!(),
        }
    }
}

impl<'a> RecordBatchStream for CollectorStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

unsafe impl<'a> Send for CollectorStream {}

impl ExecutionPlan for CollectorScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, DataFusionError> {
        // let env_guard = self.jvm.attach_current_thread()
        //     .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let stream = CollectorStream {
            collector_ref: self.collector_ref.clone(),
            schema: self.projected_schema.clone(),
            finished: false,
            metrics: self.metrics.clone(),
            jvm: self.jvm.clone(),
        };

        // Just use one Box with pin
        Ok(Box::pin(stream))
    }

    fn name(&self) -> &str {
        "CollectorScan"
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
}

#[derive(Debug)]
pub struct CollectorTable {
    collector_ref: GlobalRef,
    schema: SchemaRef,
    jvm: Arc<JavaVM>, // Wrap JavaVM in Arc
}

impl CollectorTable {
    pub fn new(collector_ref: GlobalRef, schema: SchemaRef, jvm: JavaVM) -> Self {
        Self {
            collector_ref,
            schema,
            jvm: Arc::new(jvm), // Wrap in Arc when creating
        }
    }
}

#[async_trait]
impl TableProvider for CollectorTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(CollectorScan::new(
            self.collector_ref.clone(),
            self.jvm.clone(),
            project_schema(&self.schema, projection),
        )))
    }
}

fn convert_to_record_batch(
    array_ptr: i64,
    schema_ptr: i64,
    jvm: &JavaVM,
) -> Result<RecordBatch, DataFusionError> {
    let mut env = jvm
        .attach_current_thread()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let array = unsafe { ffi::FFI_ArrowArray::from_raw(array_ptr as *mut _) };
    let schema = unsafe { ffi::FFI_ArrowSchema::from_raw(schema_ptr as *mut _) };

    unsafe {
        let data = arrow::ffi::from_ffi(array, &schema)?;
        let arrow_array = arrow::array::make_array(data);
        let struct_array = arrow_array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Failed to convert to StructArray".to_string())
            })?;

        RecordBatch::try_from(struct_array.clone()).map_err(|e| {
            DataFusionError::Internal(format!("Failed to convert to RecordBatch: {}", e))
        })
    }
}

fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
    match projection {
        Some(proj) => Arc::new(schema.project(proj).unwrap()),
        None => schema.clone(),
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_search_stream_collector_StreamingCollector_create(
    mut env: JNIEnv,
    _class: JClass,
    ctx: jlong,
    collector: JObject,
    limit: jint,
    term: JString,
    schema_ptr: jlong,
) -> jlong {
    let context = unsafe { &*(ctx as *const SessionContext) };
    let schema = unsafe { &*(schema_ptr as *const Schema) };
    let jvm = env.get_java_vm().unwrap();

    let term_str: String = format!(
        "`{}`",
        env.get_string(&term)
            .expect("Invalid term string")
            .to_string_lossy()
            .into_owned()
    );

    let collector_ref = match env.new_global_ref(collector) {
        Ok(global) => global,
        Err(_) => return -1,
    };

    let table = Arc::new(CollectorTable::new(
        collector_ref,
        Arc::new(schema.clone()),
        jvm,
    ));

    let df = match context.read_table(table) {
        Ok(df) => df
            .aggregate(
                vec![col(&term_str).alias("ord")],
                vec![count(col("ord")).alias("count")],
            )
            .and_then(|agg_df| agg_df.sort(vec![col("count").sort(true, false)]))
            .and_then(|sorted| sorted.limit(0, Some(limit as usize)))
            .and_then(|limited| limited.sort(vec![col("ord").sort(false, true)])),
        Err(_) => return -1,
    };

    match df {
        Ok(plan) => Box::into_raw(Box::new(plan)) as jlong,
        Err(_) => -1,
    }
}
