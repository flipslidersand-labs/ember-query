use anyhow::{anyhow, Result};
use arrow::array::{
    ArrayRef, Float64Array, Float64Builder, Int64Array, Int64Builder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

use super::{evaluate, resolve_column, Executor};
use crate::sql::ast::{AggFunc, Expr};

/// A typed MIN/MAX candidate (keeps the input type, no f64 round trip).
#[derive(Debug, Clone, PartialEq)]
enum Val {
    Int(i64),
    Float(f64),
    Str(String),
}

impl Val {
    /// Total order for MIN/MAX. Floats use `f64::total_cmp` (same as ORDER BY),
    /// so NaN no longer makes the result depend on row order (#94).
    fn total_cmp(&self, other: &Val) -> std::cmp::Ordering {
        match (self, other) {
            (Val::Int(a), Val::Int(b)) => a.cmp(b),
            (Val::Float(a), Val::Float(b)) => a.total_cmp(b),
            (Val::Str(a), Val::Str(b)) => a.cmp(b),
            // One column has one type, so mixed variants never occur.
            _ => std::cmp::Ordering::Equal,
        }
    }
}

/// Per-group accumulator state.
#[derive(Debug)]
struct AggState {
    count: i64,
    sum_i: i64,
    sum_f: f64,
    min: Option<Val>,
    max: Option<Val>,
}

impl AggState {
    fn new() -> Self {
        AggState {
            count: 0,
            sum_i: 0,
            sum_f: 0.0,
            min: None,
            max: None,
        }
    }

    fn update_count_only(&mut self) {
        self.count += 1;
    }

    fn update_minmax(&mut self, v: Val) {
        if self.min.as_ref().is_none_or(|m| v.total_cmp(m).is_lt()) {
            self.min = Some(v.clone());
        }
        if self.max.as_ref().is_none_or(|m| v.total_cmp(m).is_gt()) {
            self.max = Some(v);
        }
    }
}

/// Argument type of an aggregate, inferred by evaluating over an empty batch.
fn check_agg_type(func: &AggFunc, dt: &DataType, name: &str) -> Result<()> {
    let ok = match func {
        AggFunc::Count => true,
        AggFunc::Sum | AggFunc::Avg => matches!(dt, DataType::Int64 | DataType::Float64),
        AggFunc::Min | AggFunc::Max => {
            matches!(dt, DataType::Int64 | DataType::Float64 | DataType::Utf8)
        }
    };
    if ok {
        Ok(())
    } else {
        Err(anyhow!(
            "type error: {:?} is not supported for {:?} (column '{}')",
            dt,
            func,
            name
        ))
    }
}

pub struct HashAggExec {
    input: Box<dyn Executor>,
    group_by: Vec<String>,
    /// (function, argument expression, output column name)
    aggs: Vec<(AggFunc, Expr, String)>,
    out_schema: SchemaRef,
    result: Option<RecordBatch>,
    emitted: bool,
}

impl HashAggExec {
    pub fn new(
        input: Box<dyn Executor>,
        group_by: Vec<String>,
        aggs: Vec<(AggFunc, Expr, String)>,
    ) -> Result<Self> {
        let in_schema = input.schema();

        // Build output schema: group_by columns + agg output columns
        let mut fields = Vec::new();
        for col in &group_by {
            let i = resolve_column(&in_schema, col)
                .map_err(|e| anyhow!("GROUP BY column '{}': {}", col, e))?;
            fields.push(in_schema.field(i).clone());
        }
        let empty = RecordBatch::new_empty(in_schema.clone());
        for (func, expr, name) in &aggs {
            let dt = if matches!(func, AggFunc::Count) {
                DataType::Int64
            } else {
                let arg_dt = evaluate(&empty, expr)?.data_type().clone();
                check_agg_type(func, &arg_dt, name)?;
                match func {
                    AggFunc::Avg => DataType::Float64,
                    _ => arg_dt,
                }
            };
            fields.push(Field::new(name, dt, true));
        }

        let out_schema = Arc::new(Schema::new(fields));

        Ok(Self {
            input,
            group_by,
            aggs,
            out_schema,
            result: None,
            emitted: false,
        })
    }

    fn run(&mut self) -> Result<RecordBatch> {
        // group key -> (per-agg states)
        let mut map: HashMap<Vec<ScalarKey>, Vec<AggState>> = HashMap::new();
        // Preserve insertion order for deterministic output
        let mut key_order: Vec<Vec<ScalarKey>> = Vec::new();

        while let Some(batch) = self.input.next()? {
            // Extract group_by column arrays
            let group_arrays: Vec<ArrayRef> = self
                .group_by
                .iter()
                .map(|col| {
                    resolve_column(&batch.schema(), col)
                        .map(|i| batch.column(i).clone())
                        .map_err(|e| anyhow!("GROUP BY column '{}': {}", col, e))
                })
                .collect::<Result<_>>()?;

            // Extract agg argument arrays
            let agg_arrays: Vec<ArrayRef> = self
                .aggs
                .iter()
                .map(|(_, expr, _)| evaluate(&batch, expr))
                .collect::<Result<_>>()?;

            for row in 0..batch.num_rows() {
                // Build group key
                let key: Vec<ScalarKey> =
                    group_arrays.iter().map(|arr| scalar_at(arr, row)).collect();

                let states = map.entry(key.clone()).or_insert_with(|| {
                    key_order.push(key.clone());
                    self.aggs.iter().map(|_| AggState::new()).collect()
                });

                // Update each aggregate
                for (agg_idx, (func, expr, _)) in self.aggs.iter().enumerate() {
                    let arr = &agg_arrays[agg_idx];
                    match func {
                        AggFunc::Count => {
                            // COUNT(*) counts every row; COUNT(col) skips NULLs.
                            if matches!(expr, Expr::Wildcard) || !arr.is_null(row) {
                                states[agg_idx].update_count_only();
                            }
                        }
                        _ => {
                            if !arr.is_null(row) {
                                update_value(&mut states[agg_idx], func, arr, row)?;
                            }
                        }
                    }
                }
            }
        }

        // If no input rows and no group_by, emit one row for COUNT(*) etc.
        if key_order.is_empty() && self.group_by.is_empty() {
            key_order.push(vec![]);
            map.insert(vec![], self.aggs.iter().map(|_| AggState::new()).collect());
        }

        // Build output RecordBatch
        let n_rows = key_order.len();
        let mut columns: Vec<ArrayRef> = Vec::new();

        // Group-by columns
        // The first `group_by.len()` output fields are the group columns, in order.
        for col_idx in 0..self.group_by.len() {
            let field = self.out_schema.field(col_idx);
            let arr = build_group_column(field.data_type(), &key_order, col_idx, n_rows)?;
            columns.push(arr);
        }

        // Aggregate result columns
        let gb_len = self.group_by.len();
        for (agg_idx, (func, _, _)) in self.aggs.iter().enumerate() {
            let arr = build_agg_column(
                func,
                self.out_schema.field(gb_len + agg_idx).data_type(),
                &key_order,
                &map,
                agg_idx,
            )?;
            columns.push(arr);
        }

        RecordBatch::try_new(self.out_schema.clone(), columns).map_err(anyhow::Error::from)
    }
}

impl Executor for HashAggExec {
    fn schema(&self) -> SchemaRef {
        self.out_schema.clone()
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;

        if let Some(batch) = self.result.take() {
            return Ok(Some(batch));
        }

        let batch = self.run()?;
        Ok(Some(batch))
    }
}

// ── Scalar key type ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ScalarKey {
    Int(i64),
    Str(String),
    Bool(bool),
    /// Float bits; -0.0 is folded into 0.0 and all NaNs into one NaN so they group together.
    Float(u64),
    Null,
}

fn scalar_at(arr: &ArrayRef, row: usize) -> ScalarKey {
    if arr.is_null(row) {
        return ScalarKey::Null;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return ScalarKey::Int(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return ScalarKey::Str(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::BooleanArray>() {
        return ScalarKey::Bool(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        let v = a.value(row);
        let v = if v.is_nan() {
            f64::NAN
        } else if v == 0.0 {
            0.0
        } else {
            v
        };
        return ScalarKey::Float(v.to_bits());
    }
    ScalarKey::Null
}

/// Fold one non-NULL value into the state. Int64 sums use checked_add.
fn update_value(st: &mut AggState, func: &AggFunc, arr: &ArrayRef, row: usize) -> Result<()> {
    let any = arr.as_any();
    let val = if let Some(a) = any.downcast_ref::<Int64Array>() {
        Val::Int(a.value(row))
    } else if let Some(a) = any.downcast_ref::<Float64Array>() {
        Val::Float(a.value(row))
    } else if let Some(a) = any.downcast_ref::<StringArray>() {
        Val::Str(a.value(row).to_string())
    } else {
        return Err(anyhow!(
            "type error: {:?} is not supported for {:?}",
            arr.data_type(),
            func
        ));
    };
    st.count += 1;
    match (func, val) {
        (AggFunc::Sum, Val::Int(i)) => {
            st.sum_i = st
                .sum_i
                .checked_add(i)
                .ok_or_else(|| anyhow!("integer overflow in SUM"))?;
        }
        (AggFunc::Sum | AggFunc::Avg, Val::Float(f)) => st.sum_f += f,
        (AggFunc::Avg, Val::Int(i)) => st.sum_f += i as f64,
        (AggFunc::Min | AggFunc::Max, v) => st.update_minmax(v),
        _ => {
            return Err(anyhow!(
                "type error: {:?} is not supported for {:?}",
                arr.data_type(),
                func
            ))
        }
    }
    Ok(())
}

// ── Column builders ───────────────────────────────────────────────────────────

fn build_group_column(
    dt: &DataType,
    key_order: &[Vec<ScalarKey>],
    col_idx: usize,
    _n: usize,
) -> Result<ArrayRef> {
    match dt {
        DataType::Int64 => {
            let mut b = Int64Builder::new();
            for key in key_order {
                match key.get(col_idx) {
                    Some(ScalarKey::Int(i)) => b.append_value(*i),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::new();
            for key in key_order {
                match key.get(col_idx) {
                    Some(ScalarKey::Str(s)) => b.append_value(s),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        DataType::Float64 => {
            let mut b = Float64Builder::new();
            for key in key_order {
                match key.get(col_idx) {
                    Some(ScalarKey::Float(bits)) => b.append_value(f64::from_bits(*bits)),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        DataType::Boolean => {
            let mut b = arrow::array::BooleanBuilder::new();
            for key in key_order {
                match key.get(col_idx) {
                    Some(ScalarKey::Bool(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        _ => {
            let mut b = StringBuilder::new();
            for key in key_order {
                match key.get(col_idx) {
                    Some(ScalarKey::Str(s)) => b.append_value(s),
                    Some(ScalarKey::Int(i)) => b.append_value(i.to_string()),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
    }
}

fn build_agg_column(
    func: &AggFunc,
    dt: &DataType,
    key_order: &[Vec<ScalarKey>],
    map: &HashMap<Vec<ScalarKey>, Vec<AggState>>,
    agg_idx: usize,
) -> Result<ArrayRef> {
    let states = key_order.iter().map(|k| &map[k][agg_idx]);
    // NULL when the group saw no non-NULL value.
    let pick = |s: &AggState| -> Option<Val> {
        if s.count == 0 {
            return None;
        }
        match func {
            AggFunc::Sum if dt == &DataType::Int64 => Some(Val::Int(s.sum_i)),
            AggFunc::Sum => Some(Val::Float(s.sum_f)),
            AggFunc::Avg => Some(Val::Float(s.sum_f / s.count as f64)),
            AggFunc::Min => s.min.clone(),
            _ => s.max.clone(),
        }
    };
    match (func, dt) {
        (AggFunc::Count, _) => {
            let mut b = Int64Builder::new();
            for s in states {
                b.append_value(s.count);
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        (_, DataType::Int64) => {
            let mut b = Int64Builder::new();
            for s in states {
                match pick(s) {
                    Some(Val::Int(i)) => b.append_value(i),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        (_, DataType::Float64) => {
            let mut b = Float64Builder::new();
            for s in states {
                match pick(s) {
                    Some(Val::Float(f)) => b.append_value(f),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        _ => {
            let mut b = StringBuilder::new();
            for s in states {
                match pick(s) {
                    Some(Val::Str(v)) => b.append_value(v),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockExec {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    }

    impl Executor for MockExec {
        fn next(&mut self) -> Result<Option<RecordBatch>> {
            Ok(if self.batches.is_empty() {
                None
            } else {
                Some(self.batches.remove(0))
            })
        }
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    fn run_agg(vals: Vec<Option<i64>>, aggs: Vec<(AggFunc, Expr, String)>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batches = if vals.is_empty() {
            vec![]
        } else {
            let arr: Int64Array = vals.into_iter().collect();
            vec![RecordBatch::try_new(schema.clone(), vec![Arc::new(arr) as ArrayRef]).unwrap()]
        };
        let input = Box::new(MockExec { schema, batches });
        let mut exec = HashAggExec::new(input, vec![], aggs).unwrap();
        exec.next().unwrap().unwrap()
    }

    fn col(name: &str) -> Expr {
        Expr::Column(name.to_string())
    }

    #[test]
    fn count_column_skips_nulls_but_count_star_does_not() {
        let out = run_agg(
            vec![Some(1), None, Some(3)],
            vec![
                (AggFunc::Count, col("v"), "c".into()),
                (AggFunc::Count, Expr::Wildcard, "s".into()),
            ],
        );
        let c = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let s = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(c.value(0), 2);
        assert_eq!(s.value(0), 3);
    }

    #[test]
    fn sum_is_null_for_zero_rows_and_all_null() {
        for vals in [vec![], vec![None, None]] {
            let out = run_agg(vals, vec![(AggFunc::Sum, col("v"), "t".into())]);
            assert!(out.column(0).is_null(0));
        }
    }

    #[test]
    fn sum_ignores_nulls() {
        let out = run_agg(
            vec![Some(1), None, Some(3)],
            vec![(AggFunc::Sum, col("v"), "t".into())],
        );
        let t = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(t.value(0), 4);
    }

    // ── #12: grouped / value / edge-case tests ──

    use arrow::array::{BooleanArray, Float64Array as F64};

    fn make_exec(cols: Vec<(&str, ArrayRef)>, empty: bool) -> Box<dyn Executor> {
        let schema = Arc::new(Schema::new(
            cols.iter()
                .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batches = if empty {
            vec![]
        } else {
            let arrays = cols.into_iter().map(|(_, a)| a).collect();
            vec![RecordBatch::try_new(schema.clone(), arrays).unwrap()]
        };
        Box::new(MockExec { schema, batches })
    }

    fn run_group(
        cols: Vec<(&str, ArrayRef)>,
        group_by: &[&str],
        aggs: Vec<(AggFunc, Expr, String)>,
    ) -> RecordBatch {
        let input = make_exec(cols, false);
        let gb = group_by.iter().map(|s| s.to_string()).collect();
        HashAggExec::new(input, gb, aggs)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
    }

    fn ints(v: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(v.into_iter().collect::<Int64Array>())
    }

    fn f64s(out: &RecordBatch, i: usize) -> &F64 {
        out.column(i).as_any().downcast_ref::<F64>().unwrap()
    }

    fn i64s(out: &RecordBatch, i: usize) -> &Int64Array {
        out.column(i).as_any().downcast_ref::<Int64Array>().unwrap()
    }

    fn all_aggs() -> Vec<(AggFunc, Expr, String)> {
        vec![
            (AggFunc::Sum, col("v"), "sum".into()),
            (AggFunc::Avg, col("v"), "avg".into()),
            (AggFunc::Min, col("v"), "min".into()),
            (AggFunc::Max, col("v"), "max".into()),
        ]
    }

    #[test]
    fn group_by_string_keeps_first_seen_order() {
        let k: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "a"]));
        let out = run_group(
            vec![("k", k), ("v", ints(vec![Some(1), Some(2), Some(3)]))],
            &["k"],
            vec![
                (AggFunc::Sum, col("v"), "t".into()),
                (AggFunc::Count, Expr::Wildcard, "c".into()),
            ],
        );
        let keys = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!((keys.value(0), keys.value(1)), ("a", "b"));
        assert_eq!((i64s(&out, 1).value(0), i64s(&out, 1).value(1)), (4, 2));
        assert_eq!((i64s(&out, 2).value(0), i64s(&out, 2).value(1)), (2, 1));
    }

    #[test]
    fn group_by_int_column() {
        let k = ints(vec![Some(10), Some(20), Some(10), Some(20), Some(10)]);
        let v = ints(vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
        let out = run_group(
            vec![("k", k), ("v", v)],
            &["k"],
            vec![(AggFunc::Sum, col("v"), "t".into())],
        );
        assert_eq!(out.num_rows(), 2);
        assert_eq!(i64s(&out, 0).value(0), 10);
        assert_eq!(i64s(&out, 0).value(1), 20);
        assert_eq!(i64s(&out, 1).value(0), 9);
        assert_eq!(i64s(&out, 1).value(1), 6);
    }

    #[test]
    fn group_by_bool_column() {
        let k: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));
        let out = run_group(
            vec![("k", k), ("v", ints(vec![Some(1), Some(2), Some(3)]))],
            &["k"],
            vec![(AggFunc::Sum, col("v"), "t".into())],
        );
        let keys = out
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(out.num_rows(), 2);
        assert!(keys.value(0));
        assert!(!keys.value(1));
        assert_eq!(i64s(&out, 1).value(0), 4);
        assert_eq!(i64s(&out, 1).value(1), 2);
    }

    #[test]
    fn group_by_float_column_keeps_float64_type() {
        let k: ArrayRef = Arc::new(F64::from(vec![
            Some(1.5),
            Some(2.25),
            Some(1.5),
            None,
            Some(-0.0),
            Some(0.0),
        ]));
        let v = ints(vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)]);
        let out = run_group(
            vec![("k", k), ("v", v)],
            &["k"],
            vec![(AggFunc::Sum, col("v"), "t".into())],
        );
        assert_eq!(out.schema().field(0).data_type(), &DataType::Float64);
        assert_eq!(out.column(0).data_type(), &DataType::Float64);
        let keys = f64s(&out, 0);
        // 1.5, 2.25, NULL, 0.0 (-0.0 and 0.0 are the same group)
        assert_eq!(out.num_rows(), 4);
        assert_eq!((keys.value(0), keys.value(1)), (1.5, 2.25));
        assert!(arrow::array::Array::is_null(keys, 2));
        assert_eq!(keys.value(3), 0.0);
        assert_eq!(i64s(&out, 1).value(0), 4);
        assert_eq!(i64s(&out, 1).value(3), 11);
    }

    #[test]
    fn null_group_key_forms_its_own_group() {
        let k = ints(vec![Some(1), None, None, Some(1)]);
        let out = run_group(
            vec![
                ("k", k),
                ("v", ints(vec![Some(1), Some(2), Some(3), Some(4)])),
            ],
            &["k"],
            vec![(AggFunc::Sum, col("v"), "t".into())],
        );
        assert_eq!(out.num_rows(), 2);
        assert_eq!(i64s(&out, 0).value(0), 1);
        assert!(out.column(0).is_null(1));
        assert_eq!(i64s(&out, 1).value(0), 5);
        assert_eq!(i64s(&out, 1).value(1), 5);
    }

    #[test]
    fn sum_avg_min_max_with_negative_values() {
        let out = run_agg(vec![Some(-5), Some(-1), Some(-3)], all_aggs());
        assert_eq!(i64s(&out, 0).value(0), -9);
        assert_eq!(f64s(&out, 1).value(0), -3.0);
        // max must be -1, not an initial 0.
        assert_eq!(i64s(&out, 2).value(0), -5);
        assert_eq!(i64s(&out, 3).value(0), -1);
    }

    #[test]
    fn min_max_with_all_positive_values() {
        // min must be 5, not an initial 0.
        let out = run_agg(vec![Some(5), Some(9), Some(7)], all_aggs());
        assert_eq!(i64s(&out, 2).value(0), 5);
        assert_eq!(i64s(&out, 3).value(0), 9);
    }

    #[test]
    fn all_equal_values() {
        let out = run_agg(vec![Some(7), Some(7), Some(7)], all_aggs());
        assert_eq!(i64s(&out, 0).value(0), 21);
        assert_eq!(f64s(&out, 1).value(0), 7.0);
        assert_eq!(i64s(&out, 2).value(0), 7);
        assert_eq!(i64s(&out, 3).value(0), 7);
    }

    #[test]
    fn single_row_min_max() {
        let out = run_agg(vec![Some(-2)], all_aggs());
        assert_eq!(i64s(&out, 2).value(0), -2);
        assert_eq!(i64s(&out, 3).value(0), -2);
    }

    #[test]
    fn avg_min_max_skip_nulls() {
        // AVG = (1 + 3) / 2, not / 3.
        let out = run_agg(vec![Some(1), None, Some(3)], all_aggs());
        assert_eq!(f64s(&out, 1).value(0), 2.0);
        assert_eq!(i64s(&out, 2).value(0), 1);
        assert_eq!(i64s(&out, 3).value(0), 3);
    }

    #[test]
    fn avg_min_max_are_null_when_all_values_null() {
        let out = run_agg(vec![None, None], all_aggs());
        for i in 0..4 {
            assert!(out.column(i).is_null(0), "column {i} should be NULL");
        }
    }

    #[test]
    fn empty_input_without_group_by_yields_one_row() {
        let mut aggs = all_aggs();
        aggs.push((AggFunc::Count, Expr::Wildcard, "c".into()));
        let out = run_agg(vec![], aggs);
        assert_eq!(out.num_rows(), 1);
        for i in 0..4 {
            assert!(out.column(i).is_null(0), "column {i} should be NULL");
        }
        assert_eq!(i64s(&out, 4).value(0), 0);
    }

    #[test]
    fn empty_input_with_group_by_yields_zero_rows() {
        let k: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
        let input = make_exec(vec![("k", k), ("v", ints(vec![]))], true);
        let mut exec = HashAggExec::new(
            input,
            vec!["k".to_string()],
            vec![(AggFunc::Count, Expr::Wildcard, "c".into())],
        )
        .unwrap();
        let out = exec.next().unwrap().unwrap();
        assert_eq!(out.num_rows(), 0);
        assert_eq!(out.num_columns(), 2);
    }

    #[test]
    fn unknown_group_by_column_is_an_error() {
        let input = make_exec(vec![("v", ints(vec![Some(1)]))], false);
        let res = HashAggExec::new(
            input,
            vec!["nope".to_string()],
            vec![(AggFunc::Count, Expr::Wildcard, "c".into())],
        );
        let err = res.err().expect("expected error");
        assert!(err.to_string().contains("nope"), "{err}");
    }

    #[test]
    fn output_schema_types() {
        let k: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
        let input = make_exec(vec![("k", k), ("v", ints(vec![Some(1)]))], false);
        let mut aggs = all_aggs();
        aggs.push((AggFunc::Count, Expr::Wildcard, "c".into()));
        let exec = HashAggExec::new(input, vec!["k".to_string()], aggs).unwrap();
        let s = exec.schema();
        let types: Vec<_> = s.fields().iter().map(|f| f.data_type().clone()).collect();
        assert_eq!(
            types,
            vec![
                DataType::Utf8,
                DataType::Int64,
                DataType::Float64,
                DataType::Int64,
                DataType::Int64,
                DataType::Int64
            ]
        );
    }

    // ── #43: typed SUM/MIN/MAX/AVG ──

    const BIG: i64 = (1i64 << 53) + 1;

    fn try_agg(
        cols: Vec<(&str, ArrayRef)>,
        aggs: Vec<(AggFunc, Expr, String)>,
    ) -> Result<RecordBatch> {
        HashAggExec::new(make_exec(cols, false), vec![], aggs)?
            .next()
            .map(|b| b.unwrap())
    }

    #[test]
    fn int64_sum_min_max_are_exact_above_2_pow_53() {
        let out = run_agg(vec![Some(BIG), Some(BIG + 2)], {
            vec![
                (AggFunc::Sum, col("v"), "s".into()),
                (AggFunc::Min, col("v"), "mn".into()),
                (AggFunc::Max, col("v"), "mx".into()),
            ]
        });
        assert_eq!(i64s(&out, 0).value(0), 2 * BIG + 2);
        assert_eq!(i64s(&out, 1).value(0), BIG);
        assert_eq!(i64s(&out, 2).value(0), BIG + 2);
    }

    #[test]
    fn utf8_min_max_return_strings() {
        let v: ArrayRef = Arc::new(StringArray::from(vec![Some("pear"), None, Some("apple")]));
        let out = try_agg(
            vec![("v", v)],
            vec![
                (AggFunc::Min, col("v"), "mn".into()),
                (AggFunc::Max, col("v"), "mx".into()),
            ],
        )
        .unwrap();
        let mn = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mx = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!((mn.value(0), mx.value(0)), ("apple", "pear"));
    }

    #[test]
    fn sum_and_avg_of_utf8_are_type_errors() {
        for f in [AggFunc::Sum, AggFunc::Avg] {
            let v: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
            let res = try_agg(vec![("v", v)], vec![(f, col("v"), "x".into())]);
            assert!(res.is_err(), "Utf8 must be a type error");
        }
    }

    #[test]
    fn sum_of_bool_is_a_type_error() {
        let v: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        let res = try_agg(vec![("v", v)], vec![(AggFunc::Sum, col("v"), "x".into())]);
        assert!(res.is_err());
    }

    #[test]
    fn int64_sum_overflow_is_an_error() {
        let res = try_agg(
            vec![("v", ints(vec![Some(i64::MAX), Some(1)]))],
            vec![(AggFunc::Sum, col("v"), "x".into())],
        );
        let err = res.expect_err("expected overflow error");
        assert!(err.to_string().contains("integer overflow"), "{err}");
    }

    #[test]
    fn float64_aggregates_keep_float64_and_skip_nulls() {
        let v: ArrayRef = Arc::new(F64::from(vec![Some(1.5), None, Some(2.5)]));
        let out = try_agg(
            vec![("v", v)],
            vec![
                (AggFunc::Sum, col("v"), "s".into()),
                (AggFunc::Avg, col("v"), "a".into()),
                (AggFunc::Min, col("v"), "mn".into()),
                (AggFunc::Max, col("v"), "mx".into()),
            ],
        )
        .unwrap();
        let got: Vec<f64> = (0..4).map(|i| f64s(&out, i).value(0)).collect();
        assert_eq!(got, vec![4.0, 2.0, 1.5, 2.5]);
    }

    #[test]
    fn utf8_min_max_are_null_when_all_null() {
        let v: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>, None]));
        let out = try_agg(vec![("v", v)], vec![(AggFunc::Max, col("v"), "x".into())]).unwrap();
        assert!(out.column(0).is_null(0));
    }

    // #94: MIN/MAX must not depend on row order when NaN is present
    // (total order, same as ORDER BY: NaN is the largest value).
    #[test]
    fn float_min_max_with_nan_are_row_order_independent() {
        let orders: Vec<Vec<f64>> = vec![
            vec![f64::NAN, 1.5, 3.0],
            vec![1.5, f64::NAN, 3.0],
            vec![1.5, 3.0, f64::NAN],
            vec![3.0, 1.5, f64::NAN],
        ];
        for vals in orders {
            let v: ArrayRef = Arc::new(F64::from(vals.clone()));
            let out = try_agg(
                vec![("v", v)],
                vec![
                    (AggFunc::Min, col("v"), "mn".into()),
                    (AggFunc::Max, col("v"), "mx".into()),
                ],
            )
            .unwrap();
            assert_eq!(f64s(&out, 0).value(0), 1.5, "min for {vals:?}");
            assert!(f64s(&out, 1).value(0).is_nan(), "max for {vals:?}");
        }
    }

    #[test]
    fn float_min_max_all_nan_is_nan() {
        let v: ArrayRef = Arc::new(F64::from(vec![f64::NAN, f64::NAN]));
        let out = try_agg(
            vec![("v", v)],
            vec![
                (AggFunc::Min, col("v"), "mn".into()),
                (AggFunc::Max, col("v"), "mx".into()),
            ],
        )
        .unwrap();
        assert!(f64s(&out, 0).value(0).is_nan());
        assert!(f64s(&out, 1).value(0).is_nan());
    }
}
