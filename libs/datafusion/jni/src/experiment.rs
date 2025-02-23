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
use datafusion::execution::{context::SessionState};
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
                    if (obj).is_null() {
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
    let _env = jvm
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
    _: jint,  // explicitly mark as unused with underscore prefix
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
            .and_then(|sorted| sorted.limit(0, Some(500)))
            .and_then(|limited| limited.sort(vec![col("ord").sort(false, true)])),
        Err(_) => return -1,
    };

    match df {
        Ok(plan) => Box::into_raw(Box::new(plan)) as jlong,
        Err(_) => -1,
    }
}