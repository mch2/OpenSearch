/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! JSON scalar UDFs. Names mirror PPL's JSON functions:
//! - `json(str)` — validate; return the input if parseable, NULL otherwise.
//! - `json_array_length(str)` — array length, NULL if input is not a JSON array.
//! - `json_keys(str)` — JSON array string of object keys, NULL if not an object.
//! - `json_array(v, ...)` — construct a JSON array from variadic args. Each arg
//!   that is itself a parseable JSON value is embedded as-is, otherwise it is
//!   embedded as a JSON string.
//! - `json_extract(str, path, ...)` — extract via OpenSearch path syntax
//!   (`a.b{}.c{2}`). Single path → single match (or jsonized list if multiple);
//!   multiple paths → list of per-path results.
//! - `json_extract_all(str, path)` — JSON array of all matches at path.
//! - `json_set(str, path, val, ...)` — set value(s) at path(s).
//! - `json_delete(str, path, ...)` — delete value(s) at path(s).
//! - `json_append(str, path, val, ...)` — append val to array at path.
//! - `json_extend(str, path, val, ...)` — like append but flatten arrays.
//!
//! Path syntax (OpenSearch flavor, NOT JSONPath):
//!   `a` field, `.` separator, `{N}` array index, `{}` array wildcard.
//!   e.g. `students{}.name` → all elements of `students[*].name`.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Builder, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(JsonUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonArrayLengthUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonKeysUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonArrayUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonExtractAllUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonSetUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonDeleteUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonAppendUdf::new()));
    ctx.register_udf(ScalarUDF::from(JsonExtendUdf::new()));
    log::info!(
        "OpenSearch JSON UDFs registered: json, json_array_length, json_keys, json_array, \
         json_extract, json_extract_all, json_set, json_delete, json_append, json_extend"
    );
}

// ---- path parsing ---------------------------------------------------------

#[derive(Debug, Clone)]
enum Step {
    Field(String),
    Index(i64),
    Wildcard,
}

/// Parse OpenSearch JSON path like `a.b{}.c{2}`.
fn parse_path(input: &str) -> Result<Vec<Step>> {
    let mut steps = Vec::new();
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c == '.' {
            i += 1;
            continue;
        }
        if c == '{' {
            let end = match input[i..].find('}') {
                Some(e) => i + e,
                None => return Err(DataFusionError::Execution(format!("unmatched {{ in path: {}", input))),
            };
            let inner = input[i + 1..end].trim();
            if inner.is_empty() {
                steps.push(Step::Wildcard);
            } else {
                let n: i64 = inner.parse().map_err(|_| {
                    DataFusionError::Execution(format!("invalid array index in path: {}", input))
                })?;
                steps.push(Step::Index(n));
            }
            i = end + 1;
        } else {
            let start = i;
            while i < bytes.len() && bytes[i] != b'.' && bytes[i] != b'{' {
                i += 1;
            }
            steps.push(Step::Field(input[start..i].to_string()));
        }
    }
    Ok(steps)
}

/// Walk path, collecting every matching subtree by reference. Returns empty if
/// any step misses; wildcards over arrays expand to all elements.
fn walk<'a>(value: &'a Value, steps: &[Step]) -> Vec<&'a Value> {
    let mut current: Vec<&Value> = vec![value];
    for step in steps {
        let mut next: Vec<&Value> = Vec::new();
        for node in current {
            match step {
                Step::Field(name) => {
                    if let Some(v) = node.as_object().and_then(|o| o.get(name.as_str())) {
                        next.push(v);
                    }
                }
                Step::Index(idx) => {
                    if let Some(arr) = node.as_array() {
                        let len = arr.len() as i64;
                        let i = if *idx < 0 { len + idx } else { *idx };
                        if i >= 0 && i < len {
                            next.push(&arr[i as usize]);
                        }
                    }
                }
                Step::Wildcard => {
                    if let Some(arr) = node.as_array() {
                        for v in arr {
                            next.push(v);
                        }
                    }
                }
            }
        }
        current = next;
    }
    current
}

/// Mutating walker. For each leaf at `path`, invoke `mutate(parent_array_or_object, key)`.
/// `mutate` takes the *parent* container so it can replace, delete, or append at the indicated key.
/// Returns the number of mutations applied.
fn mutate_at_path<F>(value: &mut Value, steps: &[Step], mutate: &mut F) -> usize
where
    F: FnMut(&mut Value, &Step),
{
    if steps.is_empty() {
        return 0;
    }
    let last_idx = steps.len() - 1;
    mutate_recursive(value, steps, 0, last_idx, mutate)
}

fn mutate_recursive<F>(node: &mut Value, steps: &[Step], depth: usize, last: usize, mutate: &mut F) -> usize
where
    F: FnMut(&mut Value, &Step),
{
    if depth == last {
        let step = &steps[depth];
        // Verify the leaf exists before invoking the mutator.
        let exists = match step {
            Step::Field(name) => node.as_object().and_then(|o| o.get(name.as_str())).is_some(),
            Step::Index(idx) => {
                if let Some(arr) = node.as_array() {
                    let len = arr.len() as i64;
                    let i = if *idx < 0 { len + idx } else { *idx };
                    i >= 0 && i < len
                } else {
                    false
                }
            }
            Step::Wildcard => node.is_array(),
        };
        if !exists {
            return 0;
        }
        match step {
            Step::Wildcard => {
                if let Some(arr) = node.as_array_mut() {
                    let n = arr.len();
                    // For wildcard at leaf, mutator is invoked once for each element by passing
                    // a transient parent of {"_": arr[i]} via Step::Field("_"). Use an Index step instead.
                    for i in 0..n {
                        mutate(node, &Step::Index(i as i64));
                    }
                    return n;
                }
                0
            }
            _ => {
                mutate(node, step);
                1
            }
        }
    } else {
        let step = &steps[depth];
        let mut count = 0;
        match step {
            Step::Field(name) => {
                if let Some(child) = node.as_object_mut().and_then(|o| o.get_mut(name.as_str())) {
                    count += mutate_recursive(child, steps, depth + 1, last, mutate);
                }
            }
            Step::Index(idx) => {
                if let Some(arr) = node.as_array_mut() {
                    let len = arr.len() as i64;
                    let i = if *idx < 0 { len + idx } else { *idx };
                    if i >= 0 && i < len {
                        count += mutate_recursive(&mut arr[i as usize], steps, depth + 1, last, mutate);
                    }
                }
            }
            Step::Wildcard => {
                if let Some(arr) = node.as_array_mut() {
                    for child in arr.iter_mut() {
                        count += mutate_recursive(child, steps, depth + 1, last, mutate);
                    }
                }
            }
        }
        count
    }
}

// ---- arg helpers ----------------------------------------------------------

fn string_col(args: &ScalarFunctionArgs, idx: usize) -> Result<StringArray> {
    let arr = args
        .args
        .get(idx)
        .ok_or_else(|| DataFusionError::Internal(format!("UDF missing arg {}", idx)))?;
    let arr = arr.clone().into_array(args.number_rows)?;
    arr.as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Internal(format!("arg {} expected Utf8, got {:?}", idx, arr.data_type()))
        })
}

// ---- json(str) ------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonUdf {
    signature: Signature,
}

impl JsonUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for JsonUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arr = string_col(&args, 0)?;
        let mut builder = StringBuilder::with_capacity(arr.len(), arr.len() * 32);
        for i in 0..arr.len() {
            if arr.is_null(i) {
                builder.append_null();
                continue;
            }
            let s = arr.value(i);
            match serde_json::from_str::<Value>(s) {
                Ok(v) => builder.append_value(v.to_string()),
                Err(_) => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- json_array_length(str) -----------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonArrayLengthUdf {
    signature: Signature,
}

impl JsonArrayLengthUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for JsonArrayLengthUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_array_length" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Int32) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arr = string_col(&args, 0)?;
        let mut builder = Int32Builder::with_capacity(arr.len());
        for i in 0..arr.len() {
            if arr.is_null(i) {
                builder.append_null();
                continue;
            }
            match serde_json::from_str::<Value>(arr.value(i)) {
                Ok(Value::Array(a)) => builder.append_value(a.len() as i32),
                _ => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- json_keys(str) -------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonKeysUdf {
    signature: Signature,
}

impl JsonKeysUdf {
    fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for JsonKeysUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_keys" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arr = string_col(&args, 0)?;
        let mut builder = StringBuilder::with_capacity(arr.len(), arr.len() * 32);
        for i in 0..arr.len() {
            if arr.is_null(i) {
                builder.append_null();
                continue;
            }
            match serde_json::from_str::<Value>(arr.value(i)) {
                Ok(Value::Object(map)) => {
                    let keys: Vec<Value> = map.keys().map(|k| Value::String(k.clone())).collect();
                    builder.append_value(Value::Array(keys).to_string());
                }
                _ => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- json_array(...) ------------------------------------------------------

/// Build a JSON array from variadic string args. Each arg is embedded as a
/// parsed JSON value if it parses, otherwise as a JSON string.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonArrayUdf {
    signature: Signature,
}

impl JsonArrayUdf {
    fn new() -> Self {
        Self { signature: Signature::variadic(vec![DataType::Utf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for JsonArrayUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_array" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let arity = args.args.len();
        let mut builder = StringBuilder::with_capacity(n, n * 32);
        if arity == 0 {
            for _ in 0..n {
                builder.append_value("[]");
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef));
        }
        let cols: Vec<StringArray> = (0..arity).map(|i| string_col(&args, i)).collect::<Result<_>>()?;
        for row in 0..n {
            let mut elems = Vec::with_capacity(arity);
            for c in &cols {
                if c.is_null(row) {
                    elems.push(Value::Null);
                } else {
                    let s = c.value(row);
                    elems.push(serde_json::from_str::<Value>(s).unwrap_or_else(|_| Value::String(s.to_string())));
                }
            }
            builder.append_value(Value::Array(elems).to_string());
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- json_extract(str, path, ...) -----------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonExtractUdf {
    signature: Signature,
}

impl JsonExtractUdf {
    fn new() -> Self {
        Self { signature: Signature::variadic(vec![DataType::Utf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for JsonExtractUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_extract" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let arity = args.args.len();
        let mut builder = StringBuilder::with_capacity(n, n * 64);
        if arity < 2 {
            for _ in 0..n {
                builder.append_null();
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef));
        }
        let json_col = string_col(&args, 0)?;
        let path_cols: Vec<StringArray> =
            (1..arity).map(|i| string_col(&args, i)).collect::<Result<_>>()?;
        for row in 0..n {
            if json_col.is_null(row) {
                builder.append_null();
                continue;
            }
            let value: Value = match serde_json::from_str(json_col.value(row)) {
                Ok(v) => v,
                Err(_) => {
                    builder.append_null();
                    continue;
                }
            };
            let mut per_path_results: Vec<Value> = Vec::with_capacity(path_cols.len());
            for col in &path_cols {
                if col.is_null(row) {
                    per_path_results.push(Value::Null);
                    continue;
                }
                let raw_path = col.value(row);
                let steps = match parse_path(raw_path) {
                    Ok(s) => s,
                    Err(_) => {
                        per_path_results.push(Value::Null);
                        continue;
                    }
                };
                let has_wildcard = steps.iter().any(|s| matches!(s, Step::Wildcard));
                let matches = walk(&value, &steps);
                let path_result = if has_wildcard {
                    // Wildcards always produce an array of all matches.
                    Value::Array(matches.into_iter().cloned().collect())
                } else {
                    match matches.len() {
                        0 => Value::Null,
                        1 => matches[0].clone(),
                        _ => Value::Array(matches.into_iter().cloned().collect()),
                    }
                };
                per_path_results.push(path_result);
            }
            let result = if per_path_results.len() == 1 {
                per_path_results.into_iter().next().unwrap()
            } else {
                Value::Array(per_path_results)
            };
            match result {
                Value::Null => builder.append_null(),
                Value::String(s) => builder.append_value(s),
                Value::Number(n) => builder.append_value(n.to_string()),
                Value::Bool(b) => builder.append_value(b.to_string()),
                other => builder.append_value(other.to_string()),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- json_extract_all(str, path) ------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonExtractAllUdf {
    signature: Signature,
}

impl JsonExtractAllUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for JsonExtractAllUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_extract_all" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let json_col = string_col(&args, 0)?;
        let path_col = string_col(&args, 1)?;
        let n = args.number_rows;
        let mut builder = StringBuilder::with_capacity(n, n * 64);
        for row in 0..n {
            if json_col.is_null(row) || path_col.is_null(row) {
                builder.append_null();
                continue;
            }
            let value: Value = match serde_json::from_str(json_col.value(row)) {
                Ok(v) => v,
                Err(_) => {
                    builder.append_null();
                    continue;
                }
            };
            let steps = match parse_path(path_col.value(row)) {
                Ok(s) => s,
                Err(_) => {
                    builder.append_null();
                    continue;
                }
            };
            let matches = walk(&value, &steps);
            let arr: Vec<Value> = matches.into_iter().cloned().collect();
            builder.append_value(Value::Array(arr).to_string());
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- json_set(str, path, val, ...) ----------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonSetUdf {
    signature: Signature,
}

impl JsonSetUdf {
    fn new() -> Self {
        Self { signature: Signature::variadic(vec![DataType::Utf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for JsonSetUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_set" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        run_path_value_pairs(args, MutationOp::Set)
    }
}

// ---- json_delete(str, path, ...) ------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonDeleteUdf {
    signature: Signature,
}

impl JsonDeleteUdf {
    fn new() -> Self {
        Self { signature: Signature::variadic(vec![DataType::Utf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for JsonDeleteUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_delete" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let arity = args.args.len();
        let mut builder = StringBuilder::with_capacity(n, n * 64);
        if arity < 2 {
            for _ in 0..n {
                builder.append_null();
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef));
        }
        let json_col = string_col(&args, 0)?;
        let path_cols: Vec<StringArray> =
            (1..arity).map(|i| string_col(&args, i)).collect::<Result<_>>()?;
        for row in 0..n {
            if json_col.is_null(row) {
                builder.append_null();
                continue;
            }
            let mut value: Value = match serde_json::from_str(json_col.value(row)) {
                Ok(v) => v,
                Err(_) => {
                    builder.append_null();
                    continue;
                }
            };
            for col in &path_cols {
                if col.is_null(row) {
                    continue;
                }
                let steps = match parse_path(col.value(row)) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let mut delete = |parent: &mut Value, step: &Step| {
                    match step {
                        Step::Field(name) => {
                            if let Some(o) = parent.as_object_mut() {
                                o.remove(name.as_str());
                            }
                        }
                        Step::Index(idx) => {
                            if let Some(a) = parent.as_array_mut() {
                                let len = a.len() as i64;
                                let i = if *idx < 0 { len + idx } else { *idx };
                                if i >= 0 && (i as usize) < a.len() {
                                    a.remove(i as usize);
                                }
                            }
                        }
                        Step::Wildcard => {
                            if let Some(a) = parent.as_array_mut() {
                                a.clear();
                            }
                        }
                    }
                };
                mutate_at_path(&mut value, &steps, &mut delete);
            }
            builder.append_value(value.to_string());
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

// ---- json_append(str, path, val, ...) -------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonAppendUdf {
    signature: Signature,
}

impl JsonAppendUdf {
    fn new() -> Self {
        Self { signature: Signature::variadic(vec![DataType::Utf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for JsonAppendUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_append" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        run_path_value_pairs(args, MutationOp::Append)
    }
}

// ---- json_extend(str, path, val, ...) -------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonExtendUdf {
    signature: Signature,
}

impl JsonExtendUdf {
    fn new() -> Self {
        Self { signature: Signature::variadic(vec![DataType::Utf8], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for JsonExtendUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "json_extend" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        run_path_value_pairs(args, MutationOp::Extend)
    }
}

#[derive(Copy, Clone)]
enum MutationOp {
    Set,
    Append,
    Extend,
}

/// Shared driver for json_set/json_append/json_extend. Each takes (json, path, val, path, val, ...)
/// pairs and applies the operation. Path/value arity must be even.
fn run_path_value_pairs(args: ScalarFunctionArgs, op: MutationOp) -> Result<ColumnarValue> {
    let n = args.number_rows;
    let arity = args.args.len();
    let mut builder = StringBuilder::with_capacity(n, n * 64);
    if arity < 3 || (arity - 1) % 2 != 0 {
        for _ in 0..n {
            builder.append_null();
        }
        return Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef));
    }
    let json_col = string_col(&args, 0)?;
    let cols: Vec<StringArray> =
        (1..arity).map(|i| string_col(&args, i)).collect::<Result<_>>()?;
    for row in 0..n {
        if json_col.is_null(row) {
            builder.append_null();
            continue;
        }
        let mut value: Value = match serde_json::from_str(json_col.value(row)) {
            Ok(v) => v,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let pair_count = cols.len() / 2;
        for p in 0..pair_count {
            let path_col = &cols[p * 2];
            let val_col = &cols[p * 2 + 1];
            if path_col.is_null(row) {
                continue;
            }
            let steps = match parse_path(path_col.value(row)) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let val = if val_col.is_null(row) {
                Value::Null
            } else {
                let s = val_col.value(row);
                serde_json::from_str::<Value>(s).unwrap_or_else(|_| Value::String(s.to_string()))
            };
            apply_mutation(&mut value, &steps, op, &val);
        }
        builder.append_value(value.to_string());
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
}

fn apply_mutation(value: &mut Value, steps: &[Step], op: MutationOp, new_val: &Value) {
    if steps.is_empty() {
        return;
    }
    let mut mutator = |parent: &mut Value, step: &Step| {
        match step {
            Step::Field(name) => match op {
                MutationOp::Set => {
                    if let Some(o) = parent.as_object_mut() {
                        o.insert(name.clone(), new_val.clone());
                    }
                }
                MutationOp::Append => {
                    if let Some(o) = parent.as_object_mut() {
                        if let Some(existing) = o.get_mut(name.as_str()) {
                            if let Some(arr) = existing.as_array_mut() {
                                arr.push(new_val.clone());
                            }
                        }
                    }
                }
                MutationOp::Extend => {
                    if let Some(o) = parent.as_object_mut() {
                        if let Some(existing) = o.get_mut(name.as_str()) {
                            if let Some(arr) = existing.as_array_mut() {
                                if let Some(items) = new_val.as_array() {
                                    for item in items {
                                        arr.push(item.clone());
                                    }
                                } else {
                                    arr.push(new_val.clone());
                                }
                            }
                        }
                    }
                }
            },
            Step::Index(idx) => match op {
                MutationOp::Set => {
                    if let Some(a) = parent.as_array_mut() {
                        let len = a.len() as i64;
                        let i = if *idx < 0 { len + idx } else { *idx };
                        if i >= 0 && (i as usize) < a.len() {
                            a[i as usize] = new_val.clone();
                        }
                    }
                }
                MutationOp::Append | MutationOp::Extend => {
                    if let Some(a) = parent.as_array_mut() {
                        let len = a.len() as i64;
                        let i = if *idx < 0 { len + idx } else { *idx };
                        if i >= 0 && (i as usize) < a.len() {
                            if let Some(inner) = a[i as usize].as_array_mut() {
                                if matches!(op, MutationOp::Extend) {
                                    if let Some(items) = new_val.as_array() {
                                        for item in items {
                                            inner.push(item.clone());
                                        }
                                        return;
                                    }
                                }
                                inner.push(new_val.clone());
                            }
                        }
                    }
                }
            },
            Step::Wildcard => {}
        }
    };
    mutate_at_path(value, steps, &mut mutator);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_path_basic() {
        let steps = parse_path("a.b{}.c{2}").unwrap();
        assert_eq!(steps.len(), 5);
        assert!(matches!(steps[0], Step::Field(ref s) if s == "a"));
        assert!(matches!(steps[1], Step::Field(ref s) if s == "b"));
        assert!(matches!(steps[2], Step::Wildcard));
        assert!(matches!(steps[3], Step::Field(ref s) if s == "c"));
        assert!(matches!(steps[4], Step::Index(2)));
    }

    #[test]
    fn walk_index() {
        let v: Value = serde_json::from_str("[10,20,30]").unwrap();
        let steps = parse_path("{1}").unwrap();
        let r = walk(&v, &steps);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0], &Value::from(20));
    }

    #[test]
    fn walk_wildcard() {
        let v: Value = serde_json::from_str("{\"a\":[1,2,3]}").unwrap();
        let steps = parse_path("a{}").unwrap();
        let r = walk(&v, &steps);
        assert_eq!(r.len(), 3);
    }
}
