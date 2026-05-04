/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `min_by(value, key)` / `max_by(value, key)` — return the `value` column's
//! entry at the row where `key` is minimum (resp. maximum). Backs PPL's
//! `earliest(field, ts)` and `latest(field, ts)` aggregates, which Calcite
//! emits as `ARG_MIN` / `ARG_MAX`; `NAME_ALIASES` remaps them to
//! `min_by` / `max_by` at substrait emission time.
//!
//! DataFusion has built-in `min_by` / `max_by`, but its substrait consumer
//! resolves them against its typed aggregate registry using the declared
//! argument types. Our YAML entries declare `any1, any2` wildcards, which the
//! typed registry can't direct-match. Registering these as UDAFs here takes
//! priority over the built-in lookup, letting the wildcard signature survive
//! the wire and bind at execution time.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};

/// Whether the accumulator keeps the row with the smaller or larger key.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
enum Extremum {
    Min,
    Max,
}

/// `min_by(value, key)` — emits `value` from the row with the smallest `key`.
pub struct MinByUdaf {
    signature: Signature,
}

/// `max_by(value, key)` — emits `value` from the row with the largest `key`.
pub struct MaxByUdaf {
    signature: Signature,
}

impl MinByUdaf {
    pub fn new() -> Self {
        Self { signature: Signature::any(2, Volatility::Immutable) }
    }
}
impl MaxByUdaf {
    pub fn new() -> Self {
        Self { signature: Signature::any(2, Volatility::Immutable) }
    }
}
impl Default for MinByUdaf {
    fn default() -> Self { Self::new() }
}
impl Default for MaxByUdaf {
    fn default() -> Self { Self::new() }
}

// Singleton-equal (same contract as TakeUdaf).
impl PartialEq for MinByUdaf { fn eq(&self, _: &Self) -> bool { true } }
impl Eq for MinByUdaf {}
impl Hash for MinByUdaf { fn hash<H: Hasher>(&self, state: &mut H) { "min_by".hash(state) } }
impl Debug for MinByUdaf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { f.write_str("MinByUdaf") }
}
impl PartialEq for MaxByUdaf { fn eq(&self, _: &Self) -> bool { true } }
impl Eq for MaxByUdaf {}
impl Hash for MaxByUdaf { fn hash<H: Hasher>(&self, state: &mut H) { "max_by".hash(state) } }
impl Debug for MaxByUdaf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { f.write_str("MaxByUdaf") }
}

impl AggregateUDFImpl for MinByUdaf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "min_by" }
    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() < 2 {
            return exec_err!("min_by requires (value, key)");
        }
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let value_type = acc_args.expr_fields.get(0).map(|f| f.data_type().clone())
            .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                "min_by(): missing value field".to_string()))?;
        let key_type = acc_args.expr_fields.get(1).map(|f| f.data_type().clone())
            .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                "min_by(): missing key field".to_string()))?;
        Ok(Box::new(ExtremumByAccumulator::new(value_type, key_type, Extremum::Min)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        state_fields("min_by", &args)
    }
}

impl AggregateUDFImpl for MaxByUdaf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "max_by" }
    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() < 2 {
            return exec_err!("max_by requires (value, key)");
        }
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let value_type = acc_args.expr_fields.get(0).map(|f| f.data_type().clone())
            .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                "max_by(): missing value field".to_string()))?;
        let key_type = acc_args.expr_fields.get(1).map(|f| f.data_type().clone())
            .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                "max_by(): missing key field".to_string()))?;
        Ok(Box::new(ExtremumByAccumulator::new(value_type, key_type, Extremum::Max)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        state_fields("max_by", &args)
    }
}

fn state_fields(name: &str, args: &StateFieldsArgs) -> Result<Vec<FieldRef>> {
    let value_type = args.input_fields.get(0).map(|f| f.data_type().clone())
        .unwrap_or(DataType::Null);
    let key_type = args.input_fields.get(1).map(|f| f.data_type().clone())
        .unwrap_or(DataType::Null);
    Ok(vec![
        Arc::new(Field::new(format!("{}[{}][value]", args.name, name), value_type, true)),
        Arc::new(Field::new(format!("{}[{}][key]", args.name, name), key_type, true)),
    ])
}

/// State: the currently-best (value, key) pair seen so far. `None` means no
/// non-null key has been observed yet.
struct ExtremumByAccumulator {
    value_type: DataType,
    key_type: DataType,
    extremum: Extremum,
    best: Option<(ScalarValue, ScalarValue)>,
}

impl ExtremumByAccumulator {
    fn new(value_type: DataType, key_type: DataType, extremum: Extremum) -> Self {
        Self { value_type, key_type, extremum, best: None }
    }

    fn keep_left(&self, left: &ScalarValue, right: &ScalarValue) -> Result<bool> {
        match self.extremum {
            Extremum::Min => Ok(left <= right),
            Extremum::Max => Ok(left >= right),
        }
    }

    fn consider(&mut self, value: ScalarValue, key: ScalarValue) -> Result<()> {
        if key.is_null() {
            return Ok(());
        }
        match &self.best {
            None => self.best = Some((value, key)),
            Some((_, cur_key)) => {
                if !self.keep_left(cur_key, &key)? {
                    self.best = Some((value, key));
                }
            }
        }
        Ok(())
    }
}

impl Debug for ExtremumByAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtremumByAccumulator")
            .field("extremum", &self.extremum)
            .field("has_best", &self.best.is_some())
            .finish()
    }
}

impl Accumulator for ExtremumByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() < 2 {
            return exec_err!("min_by/max_by update_batch expects 2 columns");
        }
        let value_col = &values[0];
        let key_col = &values[1];
        let n = key_col.len();
        for i in 0..n {
            if key_col.is_null(i) {
                continue;
            }
            let key = ScalarValue::try_from_array(key_col, i)?;
            let value = ScalarValue::try_from_array(value_col, i)?;
            self.consider(value, key)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match &self.best {
            Some((value, _)) => Ok(value.clone()),
            None => ScalarValue::try_from(&self.value_type),
        }
    }

    fn size(&self) -> usize {
        let best_size = self.best.as_ref()
            .map(|(v, k)| v.size() + k.size())
            .unwrap_or(0);
        std::mem::size_of_val(self) + best_size
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let (value, key) = match &self.best {
            Some((v, k)) => (v.clone(), k.clone()),
            None => (
                ScalarValue::try_from(&self.value_type)?,
                ScalarValue::try_from(&self.key_type)?,
            ),
        };
        Ok(vec![value, key])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() < 2 {
            return exec_err!("min_by/max_by merge_batch expects 2 state columns");
        }
        let value_col = &states[0];
        let key_col = &states[1];
        let n = key_col.len();
        for i in 0..n {
            if key_col.is_null(i) {
                continue;
            }
            let key = ScalarValue::try_from_array(key_col, i)?;
            let value = ScalarValue::try_from_array(value_col, i)?;
            self.consider(value, key)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};

    fn strings(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec())) as ArrayRef
    }

    fn ints(values: &[Option<i32>]) -> ArrayRef {
        Arc::new(Int32Array::from(values.to_vec())) as ArrayRef
    }

    fn acc(extremum: Extremum) -> ExtremumByAccumulator {
        ExtremumByAccumulator::new(DataType::Utf8, DataType::Int32, extremum)
    }

    /// `min_by("b", 1)` given rows ("a",3), ("b",1), ("c",2) → "b".
    #[test]
    fn min_by_returns_value_at_min_key() {
        let mut a = acc(Extremum::Min);
        a.update_batch(&[
            strings(&[Some("a"), Some("b"), Some("c")]),
            ints(&[Some(3), Some(1), Some(2)]),
        ]).expect("update");
        let out = a.evaluate().expect("evaluate");
        assert_eq!(out, ScalarValue::Utf8(Some("b".to_string())));
    }

    /// `max_by` picks the row with the largest key.
    #[test]
    fn max_by_returns_value_at_max_key() {
        let mut a = acc(Extremum::Max);
        a.update_batch(&[
            strings(&[Some("a"), Some("b"), Some("c")]),
            ints(&[Some(3), Some(1), Some(2)]),
        ]).expect("update");
        let out = a.evaluate().expect("evaluate");
        assert_eq!(out, ScalarValue::Utf8(Some("a".to_string())));
    }

    /// Rows with null keys are ignored — consistent with SQL min_by semantics.
    #[test]
    fn null_key_rows_are_skipped() {
        let mut a = acc(Extremum::Min);
        a.update_batch(&[
            strings(&[Some("ignored"), Some("picked"), Some("other")]),
            ints(&[None, Some(5), Some(10)]),
        ]).expect("update");
        let out = a.evaluate().expect("evaluate");
        assert_eq!(out, ScalarValue::Utf8(Some("picked".to_string())));
    }

    /// Empty input (or all-null keys) → null result, not an error.
    #[test]
    fn all_null_keys_yield_null() {
        let mut a = acc(Extremum::Min);
        a.update_batch(&[
            strings(&[Some("x"), Some("y")]),
            ints(&[None, None]),
        ]).expect("update");
        let out = a.evaluate().expect("evaluate");
        assert!(out.is_null(), "expected null, got {out:?}");
    }

    /// Merge of two partial states trims to the global extremum.
    #[test]
    fn merge_batch_picks_global_extremum() {
        let mut a1 = acc(Extremum::Min);
        a1.update_batch(&[strings(&[Some("a")]), ints(&[Some(5)])]).unwrap();
        let mut a2 = acc(Extremum::Min);
        a2.update_batch(&[strings(&[Some("b")]), ints(&[Some(2)])]).unwrap();

        let s1 = a1.state().unwrap();
        let s2 = a2.state().unwrap();
        // Build 2-row state arrays from the pair of single-value states.
        let value_col = ScalarValue::iter_to_array(vec![s1[0].clone(), s2[0].clone()]).unwrap();
        let key_col = ScalarValue::iter_to_array(vec![s1[1].clone(), s2[1].clone()]).unwrap();

        let mut final_acc = acc(Extremum::Min);
        final_acc.merge_batch(&[value_col, key_col]).unwrap();
        let out = final_acc.evaluate().unwrap();
        assert_eq!(out, ScalarValue::Utf8(Some("b".to_string())));
    }
}
