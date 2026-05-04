/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `rex_extract(field, pattern, group)` — extract a regex capture group by
//! name or 1-based index. Mirrors sql-repo
//! `org.opensearch.sql.expression.function.udf.RexExtractFunction`.
//!
//! Rationale (UDF over decomposition): named-group extraction in DataFusion
//! needs the group's 1-based index, but the Java RexExtractAdapter could only
//! resolve that when both pattern and group name were `RexLiteral`. Runtime
//! patterns (column-valued, or e.g. a literal pattern after a subquery projection
//! escapes constant folding) threw `UnsupportedOperationException`. The Rust UDF
//! parses the pattern per-row with the `regex` crate's native named-group
//! support (`(?P<name>...)`), then directly looks up by name or index.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use regex::Regex;

use super::{coerce_slot, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(RexExtractUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RexExtractUdf {
    signature: Signature,
}

impl RexExtractUdf {
    pub fn new() -> Self {
        // PPL emits rex_extract with field/pattern as Utf8/LargeUtf8/Utf8View
        // and group as either Utf8 (named capture) or any integer type
        // (1-based index). Exact signatures only matched {Utf8, Utf8, Utf8}
        // or {Utf8, Utf8, Int64} which the audit showed too narrow.
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for RexExtractUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for RexExtractUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "rex_extract"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!(
                "rex_extract expects 3 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 3 {
            return plan_err!(
                "rex_extract expects 3 arguments, got {}",
                arg_types.len()
            );
        }
        // field + pattern always go to Utf8.
        let field = coerce_slot("rex_extract", 0, &arg_types[0], CoerceMode::Utf8)?;
        let pattern = coerce_slot("rex_extract", 1, &arg_types[1], CoerceMode::Utf8)?;
        // group: Utf8 family → Utf8, integer types → Int64. Reject anything else.
        let group = match &arg_types[2] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => DataType::Int64,
            other => {
                return plan_err!(
                    "rex_extract: arg 2 (group) must be a string (named capture) or integer (1-based index), got {other:?}"
                );
            }
        };
        Ok(vec![field, pattern, group])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return plan_err!(
                "rex_extract expects 3 arguments, got {}",
                args.args.len()
            );
        }
        let n = args.number_rows;
        let field = args.args[0].clone().into_array(n)?;
        let pattern = args.args[1].clone().into_array(n)?;
        let group = args.args[2].clone().into_array(n)?;

        let field = field
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "rex_extract: field expected Utf8, got {:?}",
                    field.data_type()
                ))
            })?;
        let pattern = pattern
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "rex_extract: pattern expected Utf8, got {:?}",
                    pattern.data_type()
                ))
            })?;

        let mut builder = StringBuilder::with_capacity(n, n * 16);

        // Single-regex fast path: when pattern is a constant-valued column
        // (the typical PPL case where `rex` ships a literal), we compile
        // once and reuse. Detect by: all non-null rows have the same value.
        let single_pattern = if pattern.null_count() == 0 && n > 0 {
            let first = pattern.value(0);
            if (0..n).all(|i| pattern.value(i) == first) {
                Some(first.to_string())
            } else {
                None
            }
        } else {
            None
        };
        let compiled_once = single_pattern.as_deref().and_then(|p| Regex::new(p).ok());

        match group.data_type() {
            DataType::Utf8 => {
                let group = group
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("rex_extract: group Utf8 cast".into())
                    })?;
                for i in 0..n {
                    if field.is_null(i) || pattern.is_null(i) || group.is_null(i) {
                        builder.append_null();
                        continue;
                    }
                    match extract_by_name(
                        compiled_once.as_ref(),
                        pattern.value(i),
                        field.value(i),
                        group.value(i),
                    )? {
                        Some(s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
            }
            DataType::Int64 => {
                let group = group
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("rex_extract: group i64 cast".into())
                    })?;
                for i in 0..n {
                    if field.is_null(i) || pattern.is_null(i) || group.is_null(i) {
                        builder.append_null();
                        continue;
                    }
                    match extract_by_index(
                        compiled_once.as_ref(),
                        pattern.value(i),
                        field.value(i),
                        group.value(i),
                    )? {
                        Some(s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
            }
            other => {
                return plan_err!("rex_extract: unsupported group type {:?}", other);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Extract capture group by name. Returns `Ok(None)` if the field doesn't match
/// or the named group isn't present; `Err` only on pattern compilation error.
fn extract_by_name(
    precompiled: Option<&Regex>,
    pattern: &str,
    field: &str,
    group_name: &str,
) -> Result<Option<String>> {
    let regex = compile_or_reuse(precompiled, pattern)?;
    Ok(regex
        .captures(field)
        .and_then(|c| c.name(group_name))
        .map(|m| m.as_str().to_string()))
}

/// Extract capture group by 1-based index.
fn extract_by_index(
    precompiled: Option<&Regex>,
    pattern: &str,
    field: &str,
    group_index: i64,
) -> Result<Option<String>> {
    if group_index < 1 {
        return Ok(None);
    }
    let regex = compile_or_reuse(precompiled, pattern)?;
    // regex crate's capture group 0 is the whole match; group 1 is the first
    // parenthesised capture. Matches Java's Matcher.group(i) semantics, where
    // index 0 is the whole match and rex_extract passes 1-based indices.
    let idx: usize = match group_index.try_into() {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    Ok(regex
        .captures(field)
        .and_then(|c| c.get(idx))
        .map(|m| m.as_str().to_string()))
}

fn compile_or_reuse<'a>(precompiled: Option<&'a Regex>, pattern: &str) -> Result<std::borrow::Cow<'a, Regex>> {
    if let Some(r) = precompiled {
        return Ok(std::borrow::Cow::Borrowed(r));
    }
    Regex::new(pattern)
        .map(std::borrow::Cow::Owned)
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "rex_extract: invalid regex {:?}: {}",
                pattern, e
            ))
        })
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_group_extracts() {
        let out = extract_by_name(None, r"(?P<num>\d+)", "abc123def", "num").unwrap();
        assert_eq!(out.as_deref(), Some("123"));
    }

    #[test]
    fn named_group_missing_returns_none() {
        let out = extract_by_name(None, r"(?P<num>\d+)", "abc123def", "other").unwrap();
        assert_eq!(out, None);
    }

    #[test]
    fn named_group_no_match_returns_none() {
        let out = extract_by_name(None, r"(?P<num>\d+)", "hello", "num").unwrap();
        assert_eq!(out, None);
    }

    #[test]
    fn indexed_group_is_one_based() {
        // Group 1 = first paren; group 2 = second paren.
        let out = extract_by_index(None, r"(\w+)=(\d+)", "foo=42", 1).unwrap();
        assert_eq!(out.as_deref(), Some("foo"));
        let out = extract_by_index(None, r"(\w+)=(\d+)", "foo=42", 2).unwrap();
        assert_eq!(out.as_deref(), Some("42"));
    }

    #[test]
    fn indexed_group_out_of_range_returns_none() {
        assert_eq!(
            extract_by_index(None, r"(\w+)", "foo", 5).unwrap(),
            None
        );
        assert_eq!(
            extract_by_index(None, r"(\w+)", "foo", 0).unwrap(),
            None
        );
    }

    #[test]
    fn invalid_pattern_returns_err() {
        let err = extract_by_name(None, "(unclosed", "x", "y").unwrap_err();
        assert!(format!("{err}").contains("invalid regex"));
    }

    // Coercion: field and pattern are Utf8-family strings; group is either a
    // string (named capture) or an integer (1-based index). Previously the
    // signature enumerated the two variants but was exact on Utf8 and Int64 —
    // LargeUtf8 or Int32 literals failed. Normalize: field/pattern → Utf8,
    // group → Utf8 if a string type, Int64 if an integer, reject otherwise.
    #[test]
    fn coerce_types_accepts_string_group() {
        let udf = RexExtractUdf::new();
        let out = udf
            .coerce_types(&[DataType::LargeUtf8, DataType::Utf8, DataType::Utf8View])
            .unwrap();
        assert_eq!(out, vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]);
    }

    #[test]
    fn coerce_types_accepts_int_group() {
        let udf = RexExtractUdf::new();
        let out = udf
            .coerce_types(&[DataType::Utf8, DataType::Utf8, DataType::Int32])
            .unwrap();
        assert_eq!(out, vec![DataType::Utf8, DataType::Utf8, DataType::Int64]);
    }

    #[test]
    fn coerce_types_rejects_non_string_field() {
        let udf = RexExtractUdf::new();
        assert!(udf
            .coerce_types(&[DataType::Int64, DataType::Utf8, DataType::Utf8])
            .is_err());
    }

    #[test]
    fn coerce_types_rejects_boolean_group() {
        let udf = RexExtractUdf::new();
        assert!(udf
            .coerce_types(&[DataType::Utf8, DataType::Utf8, DataType::Boolean])
            .is_err());
    }

    #[test]
    fn coerce_types_rejects_wrong_arity() {
        let udf = RexExtractUdf::new();
        assert!(udf.coerce_types(&[DataType::Utf8, DataType::Utf8]).is_err());
    }

    #[test]
    fn invoke_batch_name_variant() {
        let udf = RexExtractUdf::new();
        let field = StringArray::from(vec![Some("abc123def"), Some("no-numbers"), None]);
        let pattern = StringArray::from(vec![
            Some(r"(?P<num>\d+)"),
            Some(r"(?P<num>\d+)"),
            Some(r"(?P<num>\d+)"),
        ]);
        let group = StringArray::from(vec![Some("num"), Some("num"), Some("num")]);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(field)),
                ColumnarValue::Array(Arc::new(pattern)),
                ColumnarValue::Array(Arc::new(group)),
            ],
            number_rows: 3,
            arg_fields: vec![],
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "out",
                DataType::Utf8,
                true,
            )),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!(),
        };
        let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "123");
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }

    #[test]
    fn invoke_batch_index_variant_with_varying_patterns() {
        // Deliberate: first row uses one pattern, second another → forces
        // per-row regex compile (no fast-path).
        let udf = RexExtractUdf::new();
        let field = StringArray::from(vec![Some("foo=42"), Some("bar-99")]);
        let pattern = StringArray::from(vec![
            Some(r"(\w+)=(\d+)"),
            Some(r"(\w+)-(\d+)"),
        ]);
        let group = Int64Array::from(vec![Some(2_i64), Some(1_i64)]);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(field)),
                ColumnarValue::Array(Arc::new(pattern)),
                ColumnarValue::Array(Arc::new(group)),
            ],
            number_rows: 2,
            arg_fields: vec![],
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "out",
                DataType::Utf8,
                true,
            )),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!(),
        };
        let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "42");
        assert_eq!(arr.value(1), "bar");
    }
}
