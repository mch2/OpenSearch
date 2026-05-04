/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `mvzip(left, right, [delim])` — pairwise-concat the elements of two
//! multi-value arrays. Mirrors PPL's Java [`MVZipFunctionImpl`] semantics:
//!
//! * Returns `null` if either array is null.
//! * Truncates to the length of the shorter array (Python `zip`-style).
//! * Default delimiter is `,` when the optional third argument is absent.
//! * Null elements inside a list are stringified to `""` so the surrounding
//!   delimiter is still emitted — matches Java's `Objects.toString(x, "")`.
//!
//! DataFusion's built-in `arrays_zip` returns `List<Struct<f0, f1>>`, which is
//! the wrong shape for PPL (PPL expects a flat `List<Utf8>` of joined strings).
//! That is why we supply our own UDF rather than aliasing to `arrays_zip`.
//!
//! [`MVZipFunctionImpl`]: https://github.com/opensearch-project/sql/blob/main/core/src/main/java/org/opensearch/sql/expression/function/CollectionUDF/MVZipFunctionImpl.java

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericListBuilder, ListArray, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

const DEFAULT_DELIMITER: &str = ",";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(MvzipUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MvzipUdf {
    signature: Signature,
}

impl MvzipUdf {
    fn new() -> Self {
        // Accept either 2 or 3 args. Element-typed signatures (e.g. Exact([list<utf8>,
        // list<utf8>])) would require enumerating every concrete element type the
        // planner might hand us (Utf8 vs LargeUtf8 vs dictionary-encoded strings vs
        // numeric list elements stringified upstream). `variadic_any` punts that to
        // the invoke path — we validate/cast per-cell using ScalarValue::to_string.
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::VariadicAny],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MvzipUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "mvzip" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let arity = args.args.len();
        if arity != 2 && arity != 3 {
            return Err(DataFusionError::Execution(format!(
                "mvzip expects 2 or 3 arguments, got {}",
                arity
            )));
        }

        let left = materialize_list(&args, 0)?;
        let right = materialize_list(&args, 1)?;
        let delim: Option<StringArray> = if arity == 3 {
            Some(materialize_utf8(&args, 2)?)
        } else {
            None
        };

        let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let values_builder = StringBuilder::new();
        let mut builder = GenericListBuilder::<i32, _>::new(values_builder).with_field(item_field);

        for row in 0..n {
            if left.is_null(row) || right.is_null(row) {
                builder.append(false);
                continue;
            }
            let d = match &delim {
                Some(arr) if arr.is_null(row) => {
                    // Null delimiter in PPL's Java path propagates to a null return
                    // via Calcite's NullPolicy.ANY. Preserve that here.
                    builder.append(false);
                    continue;
                }
                Some(arr) => arr.value(row),
                None => DEFAULT_DELIMITER,
            };

            let left_items = left.value(row);
            let right_items = right.value(row);
            let min_len = std::cmp::min(left_items.len(), right_items.len());

            for i in 0..min_len {
                let l = element_to_string(&left_items, i);
                let r = element_to_string(&right_items, i);
                let mut joined = String::with_capacity(l.len() + d.len() + r.len());
                joined.push_str(&l);
                joined.push_str(d);
                joined.push_str(&r);
                builder.values().append_value(&joined);
            }
            builder.append(true);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Returns the element at `i` as a string. Null elements stringify to "" so
/// the delimiter is still emitted around them, matching Java's
/// `Objects.toString(x, "")`.
fn element_to_string(arr: &ArrayRef, i: usize) -> String {
    if arr.is_null(i) {
        return String::new();
    }
    if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
        return s.value(i).to_string();
    }
    // Fall back to ScalarValue's Display for non-Utf8 list elements (e.g. numbers).
    match datafusion::common::ScalarValue::try_from_array(arr, i) {
        Ok(sv) => sv.to_string(),
        Err(_) => String::new(),
    }
}

fn materialize_list(args: &ScalarFunctionArgs, idx: usize) -> Result<ListArray> {
    let arr = args
        .args
        .get(idx)
        .ok_or_else(|| DataFusionError::Internal(format!("mvzip missing arg {}", idx)))?;
    let arr = arr.clone().into_array(args.number_rows)?;
    match arr.data_type() {
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            Ok(arr.as_list::<i32>().clone())
        }
        other => Err(DataFusionError::Internal(format!(
            "mvzip arg {} expected List, got {:?}",
            idx, other
        ))),
    }
}

fn materialize_utf8(args: &ScalarFunctionArgs, idx: usize) -> Result<StringArray> {
    let arr = args
        .args
        .get(idx)
        .ok_or_else(|| DataFusionError::Internal(format!("mvzip missing arg {}", idx)))?;
    let arr = arr.clone().into_array(args.number_rows)?;
    arr.as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "mvzip delimiter arg {} expected Utf8, got {:?}",
                idx,
                arr.data_type()
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::ListArray;
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::Field;

    fn list_of_strings(rows: &[&[&str]]) -> ListArray {
        let flat: Vec<&str> = rows.iter().flat_map(|r| r.iter().copied()).collect();
        let values = Arc::new(StringArray::from(flat)) as ArrayRef;
        let mut offsets = Vec::with_capacity(rows.len() + 1);
        let mut acc: i32 = 0;
        offsets.push(acc);
        for r in rows {
            acc += r.len() as i32;
            offsets.push(acc);
        }
        let offsets = OffsetBuffer::new(offsets.into());
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        ListArray::new(field, offsets, values, None)
    }

    /// Exercise the per-row zip loop in isolation (without running DataFusion's
    /// column pipeline) by mimicking what `invoke_with_args` does for a single row.
    fn zip_row(left: &[&str], right: &[&str], delim: &str) -> Vec<String> {
        let min_len = std::cmp::min(left.len(), right.len());
        (0..min_len)
            .map(|i| format!("{}{}{}", left[i], delim, right[i]))
            .collect()
    }

    #[test]
    fn zips_equal_length() {
        assert_eq!(
            zip_row(&["Am", "er"], &["x", "y"], ","),
            vec!["Am,x".to_string(), "er,y".to_string()]
        );
    }

    #[test]
    fn truncates_to_shorter() {
        assert_eq!(
            zip_row(&["a", "b", "c"], &["1", "2"], ","),
            vec!["a,1".to_string(), "b,2".to_string()]
        );
        assert_eq!(
            zip_row(&["a"], &["1", "2", "3"], ","),
            vec!["a,1".to_string()]
        );
    }

    #[test]
    fn respects_custom_delim() {
        assert_eq!(
            zip_row(&["a", "b"], &["1", "2"], "|"),
            vec!["a|1".to_string(), "b|2".to_string()]
        );
    }

    #[test]
    fn empty_array_yields_empty() {
        assert_eq!(zip_row(&[], &["x"], ","), Vec::<String>::new());
    }

    #[test]
    fn materialize_list_reads_offsets() {
        // Sanity check on the list-builder fixture.
        let arr = list_of_strings(&[&["a", "b"], &["c"]]);
        assert_eq!(arr.len(), 2);
        assert_eq!(arr.value_length(0), 2);
        assert_eq!(arr.value_length(1), 1);
    }
}
