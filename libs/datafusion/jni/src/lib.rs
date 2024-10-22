use std::error::Error;
use std::io::Cursor;
use std::time::Duration;

use arrow::ipc::writer::FileWriter;
use datafusion::prelude::{DataFrame, SessionContext};
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JList, JObject, JObjectArray};
use jni::sys::{jbyteArray, jlong, jobject};
use bytes::Bytes;
use std::io::BufWriter;
use tokio::runtime::Runtime;
mod provider;

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusion_query(
    mut env: JNIEnv, _class: JClass, runtime: jlong, ctx: jlong, byte_array_list: JObject, callback: JObject) {
    println!("Get me those IDs!");

    let list: JList<'_, '_, '_> = env.get_list(&byte_array_list).unwrap();

    let size: usize = list.size(&mut env).unwrap() as usize;
    let mut tickets: Vec<Bytes> = Vec::with_capacity(size);

    println!("Size of list {}", size);

    for i in 0..size {
        let byte_array: JByteArray = list.get(&mut env, i as i32)
        .unwrap()
        .unwrap()
        .into();
        let input = env.convert_byte_array(&byte_array).unwrap();
        let ticket = Bytes::from(input);
        tickets.push(ticket);
    }
    let context = unsafe { &mut *(ctx as *mut SessionContext) };
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };

    runtime.block_on(async {
        print!("Fetching streams");
        let result = provider::collect(context.clone(), tickets).await;
        let addr = result.map(|df| Box::into_raw(Box::new(df)));
        set_object_result(&mut env, callback, addr);
    });

}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusion_collect(
    mut env: JNIEnv, _class: JClass, runtime: jlong, df: jlong, callback: JObject) {
        
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
    let context = SessionContext::new();
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
    #[test]
    fn test_unwrap_tickets() {
        println!("hi");
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