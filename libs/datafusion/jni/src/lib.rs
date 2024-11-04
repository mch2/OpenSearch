use std::error::Error;
use std::io::Cursor;
use std::ptr::addr_of_mut;
use std::time::Duration;

use arrow::array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::array::{Array, StructArray};
use arrow::ipc::writer::FileWriter;
use bytes::Bytes;
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use futures::stream::TryStreamExt;
use jni::objects::{JByteArray, JClass, JList, JObject, JObjectArray, JString};
use jni::sys::{jbyteArray, jlong, jobject};
use jni::JNIEnv;
use std::io::BufWriter;
use tokio::runtime::Runtime;
mod provider;

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusion_query(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    ctx: jlong,
    byte_array_list: JObject,
    callback: JObject,
) {
    let list: JList<'_, '_, '_> = env.get_list(&byte_array_list).unwrap();

    let size: usize = list.size(&mut env).unwrap() as usize;
    let mut tickets: Vec<Bytes> = Vec::with_capacity(size);

    for i in 0..size {
        let byte_array: JByteArray = list.get(&mut env, i as i32).unwrap().unwrap().into();
        let input = env.convert_byte_array(&byte_array).unwrap();
        let ticket = Bytes::from(input);
        tickets.push(ticket);
    }
    let context = unsafe { &mut *(ctx as *mut SessionContext) };
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };

    runtime.block_on(async {
        let result = provider::query(context.clone(), tickets).await;
        let addr = result.map(|df| Box::into_raw(Box::new(df)));
        set_object_result(&mut env, callback, addr);
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusion_join(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    ctx: jlong,
    join_field: JString,
    left_tickets: JObject,
    right_tickets: JObject,
    callback: JObject,
) {
    let left = unwrap_tickets(&mut env, left_tickets);
    let right = unwrap_tickets(&mut env, right_tickets);
    let field = env
        .get_string(&join_field)
        .expect("Error fetching join field")
        .into();

    let context = unsafe { &mut *(ctx as *mut SessionContext) };
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };

    runtime.block_on(async {
        let result = provider::join(context.clone(), left, right, field).await;
        let addr = result.map(|df| Box::into_raw(Box::new(df)));
        set_object_result(&mut env, callback, addr);
    });
}

fn unwrap_tickets<'local>(env: &mut JNIEnv, tickets: JObject) -> Vec<Bytes> {
    let list: JList<'_, '_, '_> = env.get_list(&tickets).unwrap();

    let size: usize = list.size(env).unwrap() as usize;
    let mut tickets: Vec<Bytes> = Vec::with_capacity(size);

    for i in 0..size {
        let byte_array: JByteArray = list.get(env, i as i32).unwrap().unwrap().into();
        let input = env.convert_byte_array(&byte_array).unwrap();
        let ticket = Bytes::from(input);
        tickets.push(ticket);
    }
    tickets
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
) -> jlong {
    let config = SessionConfig::new().with_batch_size(512);
    let context = SessionContext::new_with_config(config);
    Box::into_raw(Box::new(context)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_SessionContext_createRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    if let Ok(runtime) = Runtime::new() {
        // println!("successfully created tokio runtime");
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
    // println!("successfully shutdown tokio runtime");
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{array::{Int32Array, RecordBatch, StringArray}, datatypes::{DataType, Field, Schema}};
    use datafusion::{common::JoinType, prelude::{col, SessionContext}};


    #[tokio::test]
    async fn test_unwrap_tickets() {
        let ctx = SessionContext::new();

    // Define schemas
    let employee_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    let department_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("department_name", DataType::Utf8, false),
    ]));

    // Create employee RecordBatch
    let employee_batch = RecordBatch::try_new(
        employee_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["John", "Jane", "Bob"])),
            Arc::new(Int32Array::from(vec![30, 25, 35])),
        ],
    ).unwrap();

    // Create department RecordBatch
    let department_batch = RecordBatch::try_new(
        department_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Engineering", "Marketing", "Sales"])),
        ],
    ).unwrap();

    // Create DataFrames from RecordBatches
    let employee_df = ctx.read_batch(employee_batch).unwrap();
    let department_df = ctx.read_batch(department_batch).unwrap().select(vec![
        col("id").alias("dept_id"),
        col("department_name")
    ]).unwrap();

    // Perform join using DataFrame API
    let joined_df = employee_df
        .join(
            department_df,
            JoinType::Inner,
            &["id"],  // join columns from left DataFrame
            &["id"],  // join columns from right DataFrame
            None,     // additional join filter
        ).unwrap().select(vec![col("id"), col("name"), col("age"), col("department_name")]).unwrap();

        joined_df.show().await;
    // Execute and collect results
    // let results = joined_df.collect().await.unwrap();

    // // Print results
    // println!("Join Results:");
    // for batch in results {
    //     println!("{:?}", batch);
    // }

    
    }
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
        let next = stream.try_next().await;
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
