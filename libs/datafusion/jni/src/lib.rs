use std::error::Error;
use std::io::Cursor;
use std::ptr::{self, addr_of_mut};
use std::sync::Arc;
use arrow::array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::array::{Array, RecordBatch, StructArray};
use arrow::ffi::{self};
use arrow::ipc::writer::FileWriter;
use bytes::Bytes;

use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::functions_aggregate::count::count;

use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use futures::stream::TryStreamExt;
use jni::objects::{GlobalRef, JByteArray, JClass, JLongArray, JObject, JString, ReleaseMode};
use jni::sys::{jint, jlong};
use jni::{JNIEnv, JavaVM};

use std::io::BufWriter;
use tokio::runtime::Runtime;

mod provider;
mod aggregator; 

/// Coordinator specific JNI CALLS (QUERY/AGG) to set up tables & plans
/// Returns a pointer to a DataFrame.
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
    let java_vm: JavaVM = env.get_java_vm().expect("Failed to get JavaVM");

    runtime.spawn(async move {

        let result = provider::read_aggs(
            context.clone(),
            Bytes::from(input),
            "http://localhost:9450".to_owned(),
            size.try_into().unwrap(),
        ).await;
        let mut env = java_vm.attach_current_thread().unwrap();

        match result {
            Ok(df) => {
                let df_box = Box::new(df);
                let df_ptr = Box::into_raw(df_box);
                set_object_result_async_ok(&mut env, &callback_ref, df_ptr);
            }
            Err(e) => {
                set_object_result_async_err(&mut env, &callback_ref, &e);
            }
        }
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
    let ctx = Box::into_raw(Box::new(context)) as jlong;
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
    let _ = unsafe { Box::from_raw(pointer as *mut Runtime) };
}

fn set_object_result<T, Err: Error>(
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
fn set_object_result_ok<T>(env: &mut JNIEnv, callback: JObject, address: *mut T) {
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
fn set_object_result_error<T: Error>(env: &mut JNIEnv, callback: JObject, error: &T) {
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
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_next(
    env: JNIEnv,
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
        let next  = stream.try_next().await;
        let mut env = java_vm.attach_current_thread().unwrap();
        match next.unwrap() {
            Some(batch) => {
                let struct_array: StructArray = batch.into();
                let array_data = struct_array.into_data();
                let mut ffi_array = FFI_ArrowArray::new(&array_data);
                // ffi_array must remain alive until after the callback is called
                println!("Return Data");
                set_object_result_async_ok(&mut env, &callback_ref, addr_of_mut!(ffi_array));
            }
            None => {
                // return null ptr 0 to indicate end of stream.
                println!("Return empty");
                set_object_result_async_ok(&mut env, &callback_ref, ptr::null_mut::<FFI_ArrowArray>());
            }
        }
    });
}

fn set_object_result_async_ok<T>(env: &mut JNIEnv, callback: &GlobalRef, address: *mut T) {
    let err_message = JObject::null();
    env.call_method(
        callback,
        "callback",
        "(Ljava/lang/String;J)V",
        &[(&err_message).into(), (address as jlong).into()],
    )
    .expect("Failed to call object result callback with address");
}

fn set_object_result_async_err<E: Error>(env: &mut JNIEnv, callback: &GlobalRef, err: E) {
    let err_message = env
        .new_string(err.to_string())
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

/// RECORDBATCHSTREAM METHODS (Lifecycle, getSchema, next)


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

/// Destruction methods
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFrame_destroyDataFrame(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    println!("Destroy dataFrame {:?}", pointer);
    let _ = unsafe { Box::from_raw(pointer as *mut DataFrame) };
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_destroy(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    if pointer != 0 {
        println!("Destroying RecordBatchStream at {:?}", pointer);
        let _ = unsafe { Box::from_raw(pointer as *mut SendableRecordBatchStream) };
    } else {
        println!("Attempt to destroy null RecordBatchStream pointer");
    }
}

// Data node aggregation helpers

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

    runtime.spawn(async move {
        let data = unsafe { arrow::ffi::from_ffi(array, &schema) };
        let arrow_array = arrow::array::make_array(data.unwrap());
        let struct_array = arrow_array
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .ok_or_else(|| DataFusionError::Internal("Failed to downcast to StructArray".to_string()));
        let record_batch = RecordBatch::try_from(struct_array.unwrap()).unwrap();
        let result = aggregator::aggregate_batch(context, record_batch, &term_str).await;
        let mut env = java_vm.attach_current_thread().unwrap();

        match result {
            Ok(df) => {
                let df_box = Box::new(df);
                let df_ptr = Box::into_raw(df_box);
                set_object_result_async_ok(&mut env, &callback_ref, df_ptr);
            }
            Err(e) => {
                set_object_result_async_err(&mut env, &callback_ref, e);
            }
        }
    });
}



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
        set_object_result_ok(&mut env, callback, std::ptr::null_mut::<jlong>());
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
    set_object_result(&mut env, callback, ptr_result);
}

// DataFrame methods - Collect & execute_stream.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFrame_executeStream(
    env: JNIEnv,
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
    let java_vm = env.get_java_vm().expect("Failed to get JavaVM");

    runtime.spawn(async move {
        let result = dataframe.clone().execute_stream().await;
        let mut env = java_vm.attach_current_thread().unwrap();

        match result {
            Ok(stream) => {
                let stream_box = Box::new(stream);
                let stream_ptr = Box::into_raw(stream_box);
                set_object_result_async_ok(&mut env, &callback_ref, stream_ptr);
            }
            Err(e) => {
                set_object_result_async_err(&mut env, &callback_ref, e);
            }
        }
    });
}

// Collect a DataFrame, returning a pointer to a 
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFrame_collect(
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
