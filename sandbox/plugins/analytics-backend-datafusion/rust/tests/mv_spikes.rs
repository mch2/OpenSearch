/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Spikes validating the two load-bearing bets in `MULTI_VALUE_DESIGN.md`.
//!
//! Spike A — the generic `mv_<fn>` elementwise wrapper. The whole plan rests on being able to lift
//! any length-preserving elementwise scalar UDF over a `List<T>` by invoking the inner kernel on the
//! child array and rewrapping with the original offsets and validity. If that does not hold, the
//! elementwise bucket becomes ~90 hand-written adapters.
//!
//! Spike B — per-element grouping. `stats count() by tags` must produce one bucket per element, and
//! `LogicalPlan::Unnest` is the only mechanism available. This proves DataFusion can do it and
//! measures the row multiplication it costs.
//!
//! These are throwaway proofs, not production code. They stay in `tests/` so they never link into
//! the shipped library.

use std::sync::Arc;

use arrow::buffer::OffsetBuffer;
use arrow_array::builder::{Int64Builder, ListBuilder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, StringViewArray,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::{col, lit, SessionContext};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};

// ── Spike A: the generic mv_<fn> wrapper ─────────────────────────────────────────────────────────

/// Lifts a scalar UDF over a `List<T>` column: run the inner kernel on the child array once, then
/// rewrap with the source list's offsets and validity.
///
/// This is the whole trick the plan depends on. A `ListArray`'s values are one contiguous child
/// array, so a length-preserving kernel applied to the child produces exactly the elements the
/// output list needs, in the same positions. Offsets and the list-level null buffer carry over
/// untouched, which is what preserves `[]` (zero-length, non-null) and an absent field (null list).
fn mv_map(
    ctx: &SessionContext,
    func_name: &str,
    list: &ListArray,
    extra_args: Vec<ColumnarValue>,
    extra_fields: Vec<Arc<Field>>,
) -> ListArray {
    let udf = ctx
        .state()
        .scalar_functions()
        .get(func_name)
        .cloned()
        .expect("udf registered");

    // The child array is sliced to the range the offsets actually address. A ListArray built by a
    // builder has child length == last offset, but one arriving from a sliced parent does not, and
    // feeding the kernel unaddressed trailing values would be wasted work at best.
    let child = list.values();
    let child_len = child.len();

    let mut args: Vec<ColumnarValue> = vec![ColumnarValue::Array(Arc::clone(child))];
    args.extend(extra_args);
    let mut arg_fields = vec![Arc::new(Field::new(
        "element",
        child.data_type().clone(),
        true,
    ))];
    arg_fields.extend(extra_fields);

    let return_type = udf
        .return_type(
            &arg_fields
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>(),
        )
        .expect("return type");
    let return_field = Arc::new(Field::new("element", return_type, true));

    let out = udf
        .invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            // number_rows is the CHILD length, not the row count. This is the assumption the spike
            // is here to check: the kernel is being asked about elements, not documents.
            number_rows: child_len,
            return_field: Arc::clone(&return_field),
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        })
        .expect("kernel invoke");

    let mapped: ArrayRef = match out {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(s) => s.to_array_of_size(child_len).expect("scalar to array"),
    };
    assert_eq!(mapped.len(), child_len, "kernel must be length-preserving");

    ListArray::new(
        return_field,
        OffsetBuffer::new(list.offsets().inner().clone()),
        mapped,
        list.nulls().cloned(),
    )
}

/// `["alpha", null]`, `[]`, null, `["beta"]` — every cardinality case the read path has to survive.
fn string_list_fixture() -> ListArray {
    let mut b = ListBuilder::new(StringBuilder::new());
    b.values().append_value("alpha");
    b.values().append_null();
    b.append(true);
    b.append(true); // []
    b.append(false); // null list
    b.values().append_value("beta");
    b.append(true);
    b.finish()
}

fn int_list_fixture() -> ListArray {
    let mut b = ListBuilder::new(Int64Builder::new());
    b.values().append_value(-7);
    b.values().append_null();
    b.append(true);
    b.append(true);
    b.append(false);
    b.values().append_value(3);
    b.append(true);
    b.finish()
}

/// Renders a list column the way an assertion can read it: `None` is a null list.
fn debug_lists(list: &ListArray) -> Vec<Option<Vec<Option<String>>>> {
    (0..list.len())
        .map(|i| {
            if list.is_null(i) {
                return None;
            }
            let v = list.value(i);
            let out = (0..v.len())
                .map(|j| {
                    if v.is_null(j) {
                        None
                    } else if let Some(s) = v.as_any().downcast_ref::<StringArray>() {
                        Some(s.value(j).to_string())
                    } else if let Some(s) = v.as_any().downcast_ref::<StringViewArray>() {
                        // Some kernels return Utf8View even for a Utf8 input (`substr` does), so a
                        // wrapper cannot assume the output element type matches the input's.
                        Some(s.value(j).to_string())
                    } else if let Some(n) = v.as_any().downcast_ref::<Int64Array>() {
                        Some(n.value(j).to_string())
                    } else if let Some(n) = v.as_any().downcast_ref::<Int32Array>() {
                        Some(n.value(j).to_string())
                    } else {
                        panic!("unhandled element type {:?}", v.data_type())
                    }
                })
                .collect();
            Some(out)
        })
        .collect()
}

#[test]
fn spike_a_unary_string_kernel_lifts_over_list() {
    let ctx = SessionContext::new();
    let out = mv_map(&ctx, "upper", &string_list_fixture(), vec![], vec![]);

    assert_eq!(
        debug_lists(&out),
        vec![
            Some(vec![Some("ALPHA".to_string()), None]),
            Some(vec![]),
            None,
            Some(vec![Some("BETA".to_string())]),
        ],
        "null inside a list, empty list, and null list must all survive"
    );
}

#[test]
fn spike_a_unary_numeric_kernel_lifts_over_list() {
    let ctx = SessionContext::new();
    let out = mv_map(&ctx, "abs", &int_list_fixture(), vec![], vec![]);

    assert_eq!(
        debug_lists(&out),
        vec![
            Some(vec![Some("7".to_string()), None]),
            Some(vec![]),
            None,
            Some(vec![Some("3".to_string())])
        ],
    );
}

#[test]
fn spike_a_binary_kernel_with_scalar_second_arg() {
    // `substr(f, 2)` — the common shape where the field is multi-valued and the other operands are
    // scalars. A scalar ColumnarValue broadcasts across the child array, so it needs no per-row
    // expansion.
    let ctx = SessionContext::new();
    let out = mv_map(
        &ctx,
        "substr",
        &string_list_fixture(),
        vec![ColumnarValue::Scalar(
            datafusion_common::ScalarValue::Int64(Some(2)),
        )],
        vec![Arc::new(Field::new("pos", DataType::Int64, true))],
    );

    assert_eq!(
        debug_lists(&out),
        vec![
            Some(vec![Some("lpha".to_string()), None]),
            Some(vec![]),
            None,
            Some(vec![Some("eta".to_string())]),
        ],
    );
}

#[test]
fn spike_a_return_type_may_differ_from_element_type() {
    // `character_length(List<Utf8>)` must produce `List<Int32>`, so the wrapper cannot assume the
    // output element type matches the input's.
    let ctx = SessionContext::new();
    let out = mv_map(
        &ctx,
        "character_length",
        &string_list_fixture(),
        vec![],
        vec![],
    );
    match out.data_type() {
        DataType::List(f) => assert!(
            matches!(f.data_type(), DataType::Int32 | DataType::Int64),
            "expected an integer element type, got {:?}",
            f.data_type()
        ),
        other => panic!("expected a list, got {other:?}"),
    }
    assert_eq!(
        debug_lists(&out)[0],
        Some(vec![Some("5".to_string()), None])
    );
}

#[test]
fn spike_a_sliced_list_keeps_its_own_offsets() {
    // A ListArray arriving mid-plan is often a slice of a larger array. Rewrapping with
    // `list.offsets()` and `list.nulls()` from the slice is what keeps the mapping aligned; this
    // fails loudly if the wrapper ever reaches for the unsliced child instead.
    let ctx = SessionContext::new();
    let full = string_list_fixture();
    let sliced = full.slice(2, 2); // [null, ["beta"]]
    let sliced = sliced
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("still a list")
        .clone();

    let out = mv_map(&ctx, "upper", &sliced, vec![], vec![]);
    assert_eq!(
        debug_lists(&out),
        vec![None, Some(vec![Some("BETA".to_string())])]
    );
}

#[test]
fn spike_a_registry_walk_finds_the_elementwise_family() {
    // The plan's claim is that one module can register `mv_<fn>` for the whole elementwise surface
    // by walking the registry. This checks the registry is walkable and that the functions the
    // rewrite catalogue names are actually present under the names it uses.
    let ctx = SessionContext::new();
    let names: Vec<String> = ctx.state().scalar_functions().keys().cloned().collect();
    assert!(
        names.len() > 100,
        "expected a populated registry, got {}",
        names.len()
    );

    for wanted in [
        "upper",
        "lower",
        "btrim",
        "ltrim",
        "rtrim",
        "substr",
        "character_length",
        "replace",
        "reverse",
        "md5",
        "abs",
        "round",
        "floor",
        "ceil",
        "trunc",
        "signum",
        "ln",
        "log10",
        "exp",
        "date_part",
        "to_char",
    ] {
        assert!(
            names.iter().any(|n| n == wanted),
            "elementwise function [{wanted}] missing from the registry"
        );
    }
}

#[test]
fn spike_a_volatile_functions_must_be_excluded() {
    // `random()` is length-preserving but not index-independent, so lifting it over a list changes
    // how many values get drawn. The registry exposes volatility, which is the filter the generic
    // registration needs — this asserts the signal exists rather than that we use it yet.
    let ctx = SessionContext::new();
    let random = ctx
        .state()
        .scalar_functions()
        .get("random")
        .cloned()
        .expect("random registered");
    assert_eq!(
        random.signature().volatility,
        datafusion_expr::Volatility::Volatile,
        "volatility is the discriminator the generic registration filters on"
    );

    let upper = ctx
        .state()
        .scalar_functions()
        .get("upper")
        .cloned()
        .expect("upper registered");
    assert_eq!(
        upper.signature().volatility,
        datafusion_expr::Volatility::Immutable
    );
}

// ── Spike B: per-element grouping via Unnest ─────────────────────────────────────────────────────

fn tags_table(ctx: &SessionContext, rows: Vec<Option<Vec<&str>>>) -> datafusion::error::Result<()> {
    let mut b = ListBuilder::new(StringBuilder::new());
    for row in &rows {
        match row {
            None => b.append(false),
            Some(values) => {
                for v in values {
                    b.values().append_value(v);
                }
                b.append(true);
            }
        }
    }
    let tags = Arc::new(b.finish()) as ArrayRef;
    let ids = Arc::new(Int64Array::from((0..rows.len() as i64).collect::<Vec<_>>())) as ArrayRef;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tags", tags.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![ids, tags])?;
    ctx.register_table(
        "docs",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;
    Ok(())
}

#[tokio::test]
async fn spike_b_group_by_unnested_column_buckets_per_element() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    // The matrix fixture: one doc with [alpha], one with [alpha, beta], one with
    // [alpha, beta, alpha], plus [] and absent. DSL terms-agg semantics are alpha:3, beta:2.
    tags_table(
        &ctx,
        vec![
            Some(vec!["alpha"]),
            Some(vec!["alpha", "beta"]),
            Some(vec!["alpha", "beta", "alpha"]),
            Some(vec![]),
            None,
        ],
    )?;

    // preserve_nulls MUST be false. The default is true, which emits a null row for `[]` and for an
    // absent field, producing a spurious null bucket that a DSL terms agg would never return.
    let opts = datafusion_common::UnnestOptions::new().with_preserve_nulls(false);
    let df = ctx
        .table("docs")
        .await?
        .unnest_columns_with_options(&["tags"], opts)?
        .aggregate(
            vec![col("tags")],
            vec![datafusion::functions_aggregate::expr_fn::count(lit(1))],
        )?
        .sort_by(vec![col("tags")])?;

    let batches = df.collect().await?;
    assert_eq!(
        pretty(&batches),
        vec![("alpha".to_string(), 4), ("beta".to_string(), 2)],
        "unnest counts VALUES: alpha appears 4 times across 3 documents. A DSL terms agg counts \
         DOCUMENTS (alpha:3). Matching the DSL needs count(distinct doc) per bucket, not count(*)."
    );
    Ok(())
}

#[tokio::test]
async fn spike_b_preserve_nulls_default_emits_a_spurious_bucket() -> datafusion::error::Result<()> {
    // Pins the trap: with the default options, `[]` and an absent field each yield a null row, so
    // grouping gains a null bucket. Two documents with no values, one bucket of 2.
    let ctx = SessionContext::new();
    tags_table(&ctx, vec![Some(vec!["only"]), Some(vec![]), None])?;

    let default_buckets = ctx
        .table("docs")
        .await?
        .unnest_columns(&["tags"])?
        .aggregate(
            vec![col("tags")],
            vec![datafusion::functions_aggregate::expr_fn::count(lit(1))],
        )?
        .collect()
        .await?;
    let with_nulls: usize = default_buckets.iter().map(|b| b.num_rows()).sum();

    let opts = datafusion_common::UnnestOptions::new().with_preserve_nulls(false);
    let suppressed = ctx
        .table("docs")
        .await?
        .unnest_columns_with_options(&["tags"], opts)?
        .aggregate(
            vec![col("tags")],
            vec![datafusion::functions_aggregate::expr_fn::count(lit(1))],
        )?
        .collect()
        .await?;
    let without_nulls: usize = suppressed.iter().map(|b| b.num_rows()).sum();

    assert_eq!(
        with_nulls, 2,
        "default preserve_nulls=true adds a null bucket"
    );
    assert_eq!(
        without_nulls, 1,
        "preserve_nulls=false matches DSL terms-agg semantics"
    );
    Ok(())
}

#[tokio::test]
async fn spike_b_document_counting_needs_distinct_over_a_doc_key() -> datafusion::error::Result<()>
{
    // The DSL-parity form. Unnest duplicates a document once per value, so `count(*)` per bucket
    // over-counts a document holding the same value twice. Counting distinct document ids restores
    // terms-agg semantics: alpha:3, not alpha:4.
    let ctx = SessionContext::new();
    tags_table(
        &ctx,
        vec![
            Some(vec!["alpha"]),
            Some(vec!["alpha", "beta"]),
            Some(vec!["alpha", "beta", "alpha"]),
            Some(vec![]),
            None,
        ],
    )?;

    let opts = datafusion_common::UnnestOptions::new().with_preserve_nulls(false);
    let batches = ctx
        .table("docs")
        .await?
        .unnest_columns_with_options(&["tags"], opts)?
        .aggregate(
            vec![col("tags")],
            vec![datafusion::functions_aggregate::expr_fn::count_distinct(
                col("id"),
            )],
        )?
        .sort_by(vec![col("tags")])?
        .collect()
        .await?;

    assert_eq!(
        pretty(&batches),
        vec![("alpha".to_string(), 3), ("beta".to_string(), 2)],
        "count(distinct id) over the unnested rows reproduces the DSL terms aggregation exactly"
    );
    Ok(())
}

#[tokio::test]
async fn spike_b_row_multiplication_is_the_element_count() -> datafusion::error::Result<()> {
    // Unnest multiplies rows by the average cardinality before aggregation. This measures the
    // factor so the design can decide whether per-element grouping is safe as a silent default on a
    // wide multi-value field.
    let ctx = SessionContext::new();
    let wide: Vec<&str> = vec!["v"; 1000];
    tags_table(
        &ctx,
        vec![Some(wide.clone()), Some(wide.clone()), Some(vec!["x"])],
    )?;

    let unnested = ctx
        .table("docs")
        .await?
        .unnest_columns(&["tags"])?
        .collect()
        .await?;
    let rows: usize = unnested.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 2001,
        "3 documents become 2001 rows; the factor is the total element count"
    );
    Ok(())
}

fn pretty(batches: &[RecordBatch]) -> Vec<(String, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let keys = b
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string key");
        let counts = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64 count");
        for i in 0..b.num_rows() {
            out.push((keys.value(i).to_string(), counts.value(i)));
        }
    }
    out.sort();
    out
}

// ── Spike B2: can an unnest cross the Substrait wire? ────────────────────────────────────────────
//
// The architectural risk. `datafusion-substrait` 54 refuses `LogicalPlan::Unnest` in both
// directions, so per-element grouping needs a new rel. This proves the extension-rel route works:
// a hand-built `ExtensionSingleRel` whose detail names the column decodes through
// `from_substrait_plan`, and the decoded node converts into a real `LogicalPlan::Unnest` that
// executes with DSL-parity semantics.

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::{Extension, UserDefinedLogicalNodeCore};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use substrait::proto::rel::RelType;
use substrait::proto::{ExtensionSingleRel, Rel};

/// The wire contract: `os_unnest` + a JSON detail naming the column to unnest.
const OS_UNNEST_TYPE_URL: &str = "os_unnest";

/// Marker node the consumer produces. It carries no execution behavior — a later pass turns it into
/// `LogicalPlan::Unnest`, so DataFusion's own unnest implementation does the work.
#[derive(Debug, Clone)]
struct MvUnnestNode {
    input: LogicalPlan,
    column: String,
}

impl PartialEq for MvUnnestNode {
    fn eq(&self, other: &Self) -> bool {
        self.column == other.column && self.input == other.input
    }
}
impl Eq for MvUnnestNode {}
// DFSchema and LogicalPlan have no PartialOrd, so order on the one field that does. Required by
// UserDefinedLogicalNodeCore.
impl PartialOrd for MvUnnestNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.column.partial_cmp(&other.column)
    }
}
impl Hash for MvUnnestNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.column.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for MvUnnestNode {
    fn name(&self) -> &str {
        "MvUnnest"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    // Passthrough: the marker does not change the row type, only the cardinality. The real Unnest
    // this becomes does change the column type, which is why the conversion has to happen before
    // anything above it is type-checked.
    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        self.input.schema()
    }
    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MvUnnest: column={}", self.column)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            input: inputs.into_iter().next().expect("one input"),
            column: self.column.clone(),
        })
    }
}

#[derive(Debug)]
struct MvUnnestSerializerRegistry;

impl datafusion_expr::registry::SerializerRegistry for MvUnnestSerializerRegistry {
    fn serialize_logical_plan(
        &self,
        node: &dyn datafusion_expr::UserDefinedLogicalNode,
    ) -> datafusion::error::Result<Vec<u8>> {
        let n = node
            .as_any()
            .downcast_ref::<MvUnnestNode>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::NotImplemented(node.name().to_string())
            })?;
        Ok(format!("{{\"column\":\"{}\"}}", n.column).into_bytes())
    }

    fn deserialize_logical_plan(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> datafusion::error::Result<Arc<dyn datafusion_expr::UserDefinedLogicalNode>> {
        if name != OS_UNNEST_TYPE_URL {
            return Err(datafusion_common::DataFusionError::NotImplemented(
                name.to_string(),
            ));
        }
        let json = String::from_utf8(bytes.to_vec()).expect("utf8 detail");
        let column = json
            .split("\"column\":\"")
            .nth(1)
            .and_then(|rest| rest.split('"').next())
            .expect("column in detail")
            .to_string();
        // The consumer supplies the input separately via with_exprs_and_inputs, so the placeholder
        // input here is replaced before the node is used.
        Ok(Arc::new(MvUnnestNode {
            input: LogicalPlanBuilder::empty(false).build()?,
            column,
        }))
    }
}

/// Wraps a substrait plan's root input in `ExtensionSingleRel{detail: Any{os_unnest, json}}`.
fn inject_unnest_marker(plan: &mut substrait::proto::Plan, column: &str) {
    let root = plan.relations.first_mut().expect("one relation");
    let rel_root = match root.rel_type.as_mut().expect("rel_type") {
        substrait::proto::plan_rel::RelType::Root(r) => r,
        substrait::proto::plan_rel::RelType::Rel(_) => panic!("expected a RelRoot"),
    };
    let inner = rel_root.input.take().expect("root input");
    let detail = pbjson_types::Any {
        type_url: OS_UNNEST_TYPE_URL.to_string(),
        value: format!("{{\"column\":\"{column}\"}}").into_bytes().into(),
    };
    rel_root.input = Some(Rel {
        rel_type: Some(RelType::ExtensionSingle(Box::new(ExtensionSingleRel {
            common: None,
            input: Some(Box::new(inner)),
            detail: Some(detail),
        }))),
    });
}

/// Replaces every `MvUnnestNode` with a real `LogicalPlan::Unnest`, suppressing null rows so `[]`
/// and an absent field produce no bucket (see spike_b_preserve_nulls_default_emits_a_spurious_bucket).
fn lower_markers(plan: LogicalPlan) -> datafusion::error::Result<LogicalPlan> {
    plan.transform_up(|node| {
        if let LogicalPlan::Extension(Extension { node: ref udln }) = node {
            if let Some(mv) = udln.as_any().downcast_ref::<MvUnnestNode>() {
                let opts = datafusion_common::UnnestOptions::new().with_preserve_nulls(false);
                let lowered = LogicalPlanBuilder::from(mv.input.clone())
                    .unnest_columns_with_options(vec![mv.column.as_str().into()], opts)?
                    .build()?;
                return Ok(Transformed::yes(lowered));
            }
        }
        Ok(Transformed::no(node))
    })
    .map(|t| t.data)
}

fn spike_ctx() -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_serializer_registry(Arc::new(MvUnnestSerializerRegistry))
        .build();
    SessionContext::new_with_state(state)
}

#[tokio::test]
async fn spike_b2_extension_rel_carries_unnest_across_substrait() -> datafusion::error::Result<()> {
    let ctx = spike_ctx();
    tags_table(
        &ctx,
        vec![
            Some(vec!["alpha"]),
            Some(vec!["alpha", "beta"]),
            Some(vec!["alpha", "beta", "alpha"]),
            Some(vec![]),
            None,
        ],
    )?;

    // Produce a plain substrait plan, then splice the marker in — this is exactly what the Java side
    // would do when it sees an ARRAY-typed grouping key.
    let base = ctx
        .table("docs")
        .await?
        .select(vec![col("id"), col("tags")])?
        .into_optimized_plan()?;
    let mut proto = (*to_substrait_plan(&base, &ctx.state())?).clone();
    inject_unnest_marker(&mut proto, "tags");

    // Decode. This is the step that would fail if DefaultSubstraitConsumer did not route
    // ExtensionSingleRel to the serializer registry.
    let decoded = from_substrait_plan(&ctx.state(), &proto).await?;
    let explained = format!("{}", decoded.display_indent());
    assert!(
        explained.contains("MvUnnest: column=tags"),
        "marker did not survive the wire:\n{explained}"
    );

    // Lower to a real Unnest and run it.
    let lowered = lower_markers(decoded)?;
    let batches = ctx
        .execute_logical_plan(lowered)
        .await?
        .aggregate(
            vec![col("tags")],
            vec![datafusion::functions_aggregate::expr_fn::count_distinct(
                col("id"),
            )],
        )?
        .sort_by(vec![col("tags")])?
        .collect()
        .await?;

    assert_eq!(
        pretty(&batches),
        vec![("alpha".to_string(), 3), ("beta".to_string(), 2)],
        "an unnest that crossed Substrait as an extension rel reproduces DSL terms-agg semantics"
    );
    Ok(())
}

#[tokio::test]
async fn spike_b2_unknown_extension_detail_fails_loudly() -> datafusion::error::Result<()> {
    // A detail the registry does not recognize must be an error rather than a silently dropped rel,
    // otherwise a version skew between coordinator and data node would quietly change the answer.
    let ctx = spike_ctx();
    tags_table(&ctx, vec![Some(vec!["alpha"])])?;

    let base = ctx
        .table("docs")
        .await?
        .select(vec![col("tags")])?
        .into_optimized_plan()?;
    let mut proto = (*to_substrait_plan(&base, &ctx.state())?).clone();
    let root = proto.relations.first_mut().expect("one relation");
    if let substrait::proto::plan_rel::RelType::Root(r) = root.rel_type.as_mut().expect("rel_type")
    {
        let inner = r.input.take().expect("root input");
        r.input = Some(Rel {
            rel_type: Some(RelType::ExtensionSingle(Box::new(ExtensionSingleRel {
                common: None,
                input: Some(Box::new(inner)),
                detail: Some(pbjson_types::Any {
                    type_url: "os_not_a_thing".to_string(),
                    value: b"{}".to_vec().into(),
                }),
            }))),
        });
    }

    assert!(
        from_substrait_plan(&ctx.state(), &proto).await.is_err(),
        "unknown extension detail must error"
    );
    Ok(())
}

#[tokio::test]
async fn spike_b_dedup_before_unnest_gives_document_counts_without_a_doc_key(
) -> datafusion::error::Result<()> {
    // The cheap route to DSL doc_count semantics. Unnesting `array_distinct(tags)` collapses a
    // document's repeated value to one row, so plain `count(*)` per bucket counts documents —
    // no per-document identity column needed in the plan, which matters because the shard plan does
    // not carry one outside the late-materialization path.
    let ctx = spike_ctx();
    tags_table(
        &ctx,
        vec![
            Some(vec!["alpha"]),
            Some(vec!["alpha", "beta"]),
            Some(vec!["alpha", "beta", "alpha"]),
            Some(vec![]),
            None,
        ],
    )?;

    let opts = datafusion_common::UnnestOptions::new().with_preserve_nulls(false);
    let batches = ctx
        .table("docs")
        .await?
        .select(vec![
            col("id"),
            datafusion::functions_nested::expr_fn::array_distinct(col("tags")).alias("tags"),
        ])?
        .unnest_columns_with_options(&["tags"], opts)?
        .aggregate(
            vec![col("tags")],
            vec![datafusion::functions_aggregate::expr_fn::count(lit(1))],
        )?
        .sort_by(vec![col("tags")])?
        .collect()
        .await?;

    assert_eq!(
        pretty(&batches),
        vec![("alpha".to_string(), 3), ("beta".to_string(), 2)],
        "array_distinct before unnest reproduces DSL terms-agg doc_count with a plain count(*)"
    );
    Ok(())
}

// ── Spike E: what unnest costs, and what it costs columns that ride along ────────────────────────
//
// `UnnestExec` computes per-row list lengths, sums them for the output row count, builds an Int64
// take-index array repeating row i once per element, then calls `arrow::take` once per NON-unnested
// column (see repeat_arrs_from_indices in datafusion-physical-plan). The unnested column itself is
// cheap — its child array is already flat. The cost is the gather on every other projected column,
// which is why projecting down before the unnest matters.

/// A table with one multi-value column of fixed cardinality plus `extra_cols` scalar payload
/// columns, standing in for the other fields a real query carries along.
fn wide_table(
    ctx: &SessionContext,
    name: &str,
    rows: usize,
    cardinality: usize,
    extra_cols: usize,
) -> datafusion::error::Result<()> {
    let mut b = ListBuilder::new(StringBuilder::new());
    for r in 0..rows {
        for c in 0..cardinality {
            // Distinct values per row so array_distinct cannot collapse them; the tag space is
            // bounded so grouping produces a realistic bucket count.
            b.values().append_value(format!("tag{}", (r + c) % 50));
        }
        b.append(true);
    }
    let tags = Arc::new(b.finish()) as ArrayRef;

    let mut fields = vec![Field::new("tags", tags.data_type().clone(), true)];
    let mut columns: Vec<ArrayRef> = vec![tags];
    for c in 0..extra_cols {
        fields.push(Field::new(format!("payload{c}"), DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())) as ArrayRef);
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    ctx.register_table(
        name,
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;
    Ok(())
}

async fn unnested_row_count(
    ctx: &SessionContext,
    table: &str,
    project_down: bool,
    dedup: bool,
) -> datafusion::error::Result<(usize, usize, std::time::Duration)> {
    let opts = datafusion_common::UnnestOptions::new().with_preserve_nulls(false);
    let mut df = ctx.table(table).await?;
    if project_down {
        // What a `stats count() by tags` needs: nothing but the grouping column.
        let expr = if dedup {
            datafusion::functions_nested::expr_fn::array_distinct(col("tags")).alias("tags")
        } else {
            col("tags")
        };
        df = df.select(vec![expr])?;
    } else if dedup {
        let mut exprs =
            vec![datafusion::functions_nested::expr_fn::array_distinct(col("tags")).alias("tags")];
        for f in ctx.table(table).await?.schema().fields().iter().skip(1) {
            exprs.push(col(f.name()));
        }
        df = df.select(exprs)?;
    }

    let start = std::time::Instant::now();
    let batches = df
        .unnest_columns_with_options(&["tags"], opts)?
        .collect()
        .await?;
    let elapsed = start.elapsed();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let cols = batches.first().map(|b| b.num_columns()).unwrap_or(0);
    Ok((rows, cols, elapsed))
}

#[tokio::test]
async fn spike_e_unnest_expands_every_projected_column() -> datafusion::error::Result<()> {
    let ctx = spike_ctx();
    let rows = 20_000;
    let cardinality = 5;
    wide_table(&ctx, "wide", rows, cardinality, 9)?;

    let (wide_rows, wide_cols, wide_time) = unnested_row_count(&ctx, "wide", false, false).await?;
    let (narrow_rows, narrow_cols, narrow_time) =
        unnested_row_count(&ctx, "wide", true, false).await?;

    assert_eq!(
        wide_rows,
        rows * cardinality,
        "output rows are the total element count"
    );
    assert_eq!(
        narrow_rows,
        rows * cardinality,
        "projecting down does not change the row count"
    );
    assert_eq!(
        wide_cols, 10,
        "all 10 columns get gathered to the expanded length"
    );
    assert_eq!(
        narrow_cols, 1,
        "projected down, only the grouping column is materialized"
    );

    // Cells materialized is the deterministic cost measure; wall time is informational because a
    // unit test is a poor benchmark harness.
    println!(
        "unnest cost: 10-col {} cells in {:?} | 1-col {} cells in {:?} | cell ratio {}x",
        wide_rows * wide_cols,
        wide_time,
        narrow_rows * narrow_cols,
        narrow_time,
        (wide_rows * wide_cols) / (narrow_rows * narrow_cols)
    );
    Ok(())
}

#[tokio::test]
async fn spike_e_dedup_caps_expansion_at_distinct_values() -> datafusion::error::Result<()> {
    // The array_distinct the group-by rewrite inserts for doc-count semantics also bounds the
    // expansion: a row holding the same value repeatedly expands once, not once per copy.
    let ctx = spike_ctx();
    let mut b = ListBuilder::new(StringBuilder::new());
    for _ in 0..10_000 {
        for _ in 0..20 {
            b.values().append_value("same");
        }
        b.append(true);
    }
    let tags = Arc::new(b.finish()) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "tags",
        tags.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![tags])?;
    ctx.register_table(
        "dupes",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;

    let (plain, _, _) = unnested_row_count(&ctx, "dupes", true, false).await?;
    let (deduped, _, _) = unnested_row_count(&ctx, "dupes", true, true).await?;

    assert_eq!(plain, 200_000, "20 copies per row expand to 20 rows each");
    assert_eq!(
        deduped, 10_000,
        "array_distinct collapses them to one row each"
    );
    Ok(())
}

#[tokio::test]
async fn spike_e_no_multi_value_column_means_no_unnest_cost() -> datafusion::error::Result<()> {
    // The guarantee non-multi-value queries need: a plan with no unnest in it pays nothing. This is
    // trivially true here, and the assertion exists so the production rule has something to break
    // if it ever inserts an unnest unconditionally.
    let ctx = spike_ctx();
    wide_table(&ctx, "plain", 20_000, 5, 9)?;

    let batches = ctx
        .table("plain")
        .await?
        .aggregate(
            vec![col("payload0")],
            vec![datafusion::functions_aggregate::expr_fn::count(lit(1))],
        )?
        .collect()
        .await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 20_000,
        "grouping on a scalar column sees the unexpanded row count"
    );
    Ok(())
}
