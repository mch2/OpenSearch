use std::error::Error;
use std::ffi::CString;
use std::io::Cursor;
use std::ptr::addr_of_mut;
use std::sync::atomic::{AtomicBool,Ordering};
use std::sync::Arc;

use arrow::array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::array::{Array, RecordBatch, StructArray};
use arrow::ffi::{self};
use arrow::ipc::writer::FileWriter;
use bytes::Bytes;

use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::functions_aggregate::count::count;
use datafusion::functions_aggregate::sum::sum;

use datafusion::prelude::{col, DataFrame, SessionConfig, SessionContext};
use futures::stream::TryStreamExt;
use jni::objects::{GlobalRef, JByteArray, JClass, JLongArray, JObject, JString, JValue, ReleaseMode};
use jni::sys::{jint, jlong};
use jni::{JNIEnv, JavaVM};
use tokio::time::Duration;

use std::io::BufWriter;
use tokio::runtime::Runtime;
mod provider;

#[no_mangle]
pub extern "system" fn Java_org_opensearch_search_stream_collector_DataFusionAggregator_processBatch(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    ctx: jlong,
    array_ptr: jlong,
    schema_ptr: jlong,
    term: JString,
    callback: JObject,
) -> () {
    // Convert the raw term string from Java
    let term_str: String = format!(
        "`{}`",
        env.get_string(&term)
            .expect("Invalid term string")
            .to_string_lossy()
            .into_owned()
    );

    // Safely get runtime and context
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let context = unsafe { &*(ctx as *const SessionContext) };

    // Create FFI arrays from raw pointers
    let array = unsafe { ffi::FFI_ArrowArray::from_raw(array_ptr as *mut _) };
    let schema = unsafe { ffi::FFI_ArrowSchema::from_raw(schema_ptr as *mut _) };

    // Create global references for the async block
    let callback_ref = env
        .new_global_ref(callback)
        .expect("Failed to create global reference");
    let java_vm = Arc::new(env.get_java_vm().expect("Failed to get JavaVM"));

    // Spawn the async task
    runtime.spawn(async move {
        let result = process_data(context, array, schema, &term_str).await;
        
        match result {
            Ok(df) => {
                let df_box = Box::new(df);
                let df_ptr = Box::into_raw(df_box);
                set_object_result_async(java_vm, callback_ref, df_ptr);
            }
            Err(e) => {
                // set_object_result_async(java_vm, callback_ref, std::ptr::null_mut());
                println!("ERROR {:?}", e);
            }
        }
    });
}

async fn process_data(
    context: &SessionContext,
    array: ffi::FFI_ArrowArray,
    schema: ffi::FFI_ArrowSchema,
    term_str: &str,
) -> Result<DataFrame, DataFusionError> {
    // Convert FFI array to Arrow array
    let data = unsafe { arrow::ffi::from_ffi(array, &schema)? };
    let arrow_array = arrow::array::make_array(data);
    
    // Convert to struct array and then to record batch
    let struct_array = arrow_array
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .ok_or_else(|| DataFusionError::Internal("Failed to downcast to StructArray".to_string()))?;
    
    let record_batch = RecordBatch::try_from(struct_array).unwrap();

    // Process the data using DataFusion
    let result = context
        .read_batch(record_batch)?
        .filter(col(term_str).is_not_null())?
        .aggregate(
            vec![col(term_str).alias("ord")],
            vec![count(col(term_str)).alias("count")],
        )?
        .collect()
        .await?;

    // Convert result back to DataFrame
    Ok(context.read_batches(result)?)
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
    env: JNIEnv,
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


        // Create global references for the async block
        let callback_ref = env
        .new_global_ref(callback)
        .expect("Failed to create global reference");
    let java_vm = Arc::new(env.get_java_vm().expect("Failed to get JavaVM"));

    runtime.spawn(async move {
        let result = provider::read_aggs(
            context.clone(),
            Bytes::from(input),
            "http://localhost:9450".to_owned(),
            size.try_into().unwrap(),
        ).await;
                
        match result {
            Ok(df) => {
                let df_box = Box::new(df);
                let df_ptr = Box::into_raw(df_box);
                set_object_result_async(java_vm, callback_ref, df_ptr);
            }
            Err(e) => {
                // set_object_result_async(java_vm, callback_ref, std::ptr::null_mut());
                println!("ERROR {:?}", e);
            }
        }
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
    println!("Destroy ctx {}", pointer);
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
    let ctx = Box::into_raw(Box::new(context)) as jlong;
    print!("Context created {:?}", ctx);
    ctx
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
    let callback_ref = env
    .new_global_ref(callback)
    .expect("Failed to create global reference");
    let java_vm = Arc::new(env.get_java_vm().expect("Failed to get JavaVM"));

    runtime.spawn(async move {
        let result = dataframe.clone().execute_stream().await;
        match result {
            Ok(stream) => {
                let stream_box = Box::new(stream);
                let stream_ptr = Box::into_raw(stream_box);
                set_object_result_async(java_vm, callback_ref, stream_ptr);
            }
            Err(e) => {
                // set_object_result_async(java_vm, callback_ref, std::ptr::null_mut());
                println!("ERROR {:?}", e);
            }
        }
        // let mut address = stream_result.map(|stream| Box::into_raw(Box::new(stream)));
        // set_object_result_async(java_vm, callback_ref, addr_of_mut!(address));
        // set_object_result(
        //     &mut env,
        //     callback,
        //     stream_result.map(|stream| Box::into_raw(Box::new(stream))),
        // );
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

    // Create global references to pass into the async block
    let callback_ref = env
        .new_global_ref(callback)
        .expect("Failed to create global reference");
    let java_vm = Arc::new(env.get_java_vm().expect("Failed to get JavaVM"));

    runtime.spawn(async move {
        let next: Option<RecordBatch> = stream.try_next().await.unwrap();
        match next {
            Some(batch) => {
                let struct_array: StructArray = batch.into();
                let array_data = struct_array.into_data();
                let mut ffi_array = FFI_ArrowArray::new(&array_data);
                // ffi_array must remain alive until after the callback is called
                set_object_result_async(java_vm, callback_ref, addr_of_mut!(ffi_array));
            }
            None => {
                set_object_result_async(java_vm, callback_ref, 0 as *mut FFI_ArrowSchema);
            }
        }
    });
}

fn set_object_result_async<T>(java_vm: Arc<JavaVM>, callback: GlobalRef, address: *mut T) {
    // Attach to the JVM from this thread
    let mut env = java_vm
        .attach_current_thread()
        .expect("Failed to attach to JVM");

    let err_message = JObject::null();
    env.call_method(
        callback,
        "callback",
        "(Ljava/lang/String;J)V",
        &[(&err_message).into(), (address as jlong).into()],
    )
    .expect("Failed to call object result callback with address");
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_destroy(
    mut env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    println!("Destroy recordbatchStream {:?}", pointer);
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
    println!("Destroy dataFrame {:?}", pointer);
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
                vec![count(col(&self.term_column)).alias("count")],
            )?
            .collect()
            .await?;

        // Convert collected batches back to DataFrame
        let incoming_df = self.context.read_batches(aggregated)?;

        // Merge with existing aggregation if we have one
        self.current_aggregation = match self.current_aggregation.take() {
            Some(existing) => {
                let merged = existing.union(incoming_df)?.collect().await?;
                Some(self.context.read_batches(merged)?)
            }
            None => Some(incoming_df),
        };

        Ok(())
    }

    pub fn get_results(&self) -> Option<DataFrame> {
        self.current_aggregation.as_ref().and_then(|df| {
            df.clone()
                .aggregate(vec![col("ord")], vec![sum(col("count")).alias("count")])
                .unwrap()
                .sort(vec![col("ord").sort(false, false)])
                .ok()
        })
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

// // Function to manually trigger a heap dump
// fn force_jemalloc_dump() {
//     unsafe {
//         let dump_str = CString::new("prof.dump").unwrap();
//         jemalloc_sys::mallctl(
//             dump_str.as_ptr(),
//             std::ptr::null_mut(),
//             std::ptr::null_mut(),
//             std::ptr::null_mut(),
//             0,
//         );
//         eprintln!("Manually triggered jemalloc heap dump");
//     }
// }

#[no_mangle]
pub extern "system" fn Java_org_opensearch_search_stream_collector_DataFusionAggregator_unionFrames(
    mut env: JNIEnv,
    _class: JClass,
    _runtime: jlong,
    _ctx: jlong,
    frame_ptrs: JLongArray,
    _limit: jint,
    callback: JObject,
) -> () {
    // Get the array elements
    let elements = unsafe {
        env.get_array_elements(&frame_ptrs, ReleaseMode::NoCopyBack)
            .expect("Failed to get array elements")
    };
    
    if elements.is_empty() {
        // set_object_result(&mut env, callback, Ok(std::ptr::null_mut()));
        return;
    }

    // Get first frame
    let first_frame = unsafe { 
        &*(elements[0] as *const DataFrame) 
    };
    let mut result = first_frame.clone();
    
    // Union remaining frames directly from iterator
    let result: Result<DataFrame, DataFusionError> = (|| {
        // Skip first frame since we already used it
        for ptr in elements.iter().skip(1) {
            let frame = unsafe { 
                &*(*ptr as *const DataFrame) 
            };
            result = result.union(frame.clone())?;
        }
        Ok(result)
    })();

    let ptr_result = result.map(|df| {
        let df_box = Box::new(df);
        Box::into_raw(df_box)
    });
    // force_jemalloc_dump();
    set_object_result(&mut env, callback, ptr_result);
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
    let ptr = Box::into_raw(Box::new(aggregator)) as jlong;
    println!("Create aggregator {}", ptr);
    ptr
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_search_stream_collector_DataFusionAggregator_destroy(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    println!("Destroy aggregator {}", pointer);
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
