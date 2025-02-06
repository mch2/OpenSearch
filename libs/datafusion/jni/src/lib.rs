use std::error::Error;
use std::io::Cursor;
use std::ptr::addr_of_mut;
use std::time::Duration;

use arrow::array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::array::{Array, RecordBatch, StructArray};
use arrow::ffi::{self};
use arrow::ipc::writer::FileWriter;
use bytes::Bytes;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::{col, DataFrame, SessionConfig, SessionContext};
use futures::stream::TryStreamExt;
use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
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
    let term_str: String = format!("`{}`", env.get_string(&term)
        .expect("Invalid term string")
        .to_string_lossy()
        .into_owned());
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
                .and_then(|df| df.sort(vec![col("ord").sort(false, true)]));

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
    size: jint
) -> jlong {
    let config = SessionConfig::new().with_batch_size(size.try_into().unwrap());
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
        let next: Result<Option<arrow::array::RecordBatch>, datafusion::error::DataFusionError> = stream.try_next().await;
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
    pointer: jlong
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

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFrame_union(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    df1: jlong,
    df2: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let dataframe1 = unsafe { &mut *(df1 as *mut DataFrame) };
    let dataframe2 = unsafe { &mut *(df2 as *mut DataFrame) };

    runtime.block_on(async {
        let result = dataframe1.clone()
            .union(dataframe2.clone())
            .map(|df| Box::into_raw(Box::new(df)));
        set_object_result(&mut env, callback, result);
    });
}