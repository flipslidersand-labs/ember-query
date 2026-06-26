use anyhow::{anyhow, Result};
use arrow::array::{
    ArrayRef, Float64Builder, Int64Array, Int64Builder, Float64Array, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

use super::{evaluate, Executor};
use crate::sql::ast::{AggFunc, Expr};

/// Per-group accumulator state.
#[derive(Debug, Default)]
struct AggState {
    count: i64,
    sum: f64,
    min: f64,
    max: f64,
    first: bool, // true = min/max not yet set
}

impl AggState {
    fn new() -> Self {
        AggState {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first: true,
        }
    }

    fn update(&mut self, val: f64) {
        self.count += 1;
        self.sum += val;
        if self.first || val < self.min {
            self.min = val;
        }
        if self.first || val > self.max {
            self.max = val;
        }
        self.first = false;
    }

    fn update_count_only(&mut self) {
        self.count += 1;
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
            let f = in_schema
                .field_with_name(col)
                .map_err(|_| anyhow!("GROUP BY column '{}' not found", col))?;
            fields.push(f.clone());
        }
        for (func, _, name) in &aggs {
            let dt = match func {
                AggFunc::Count => DataType::Int64,
                AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => DataType::Float64,
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
                    batch
                        .schema()
                        .index_of(col)
                        .map(|i| batch.column(i).clone())
                        .map_err(|_| anyhow!("GROUP BY column '{}' not found", col))
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
                let key: Vec<ScalarKey> = group_arrays
                    .iter()
                    .map(|arr| scalar_at(arr, row))
                    .collect();

                let states = map.entry(key.clone()).or_insert_with(|| {
                    key_order.push(key.clone());
                    self.aggs.iter().map(|_| AggState::new()).collect()
                });

                // Update each aggregate
                for (agg_idx, (func, _, _)) in self.aggs.iter().enumerate() {
                    let arr = &agg_arrays[agg_idx];
                    match func {
                        AggFunc::Count => {
                            // COUNT(*) — arg is Wildcard → BooleanArray of true
                            states[agg_idx].update_count_only();
                        }
                        _ => {
                            if let Some(v) = f64_at(arr, row) {
                                states[agg_idx].update(v);
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
        for (col_idx, col_name) in self.group_by.iter().enumerate() {
            let field = self.out_schema.field_with_name(col_name)?;
            let arr = build_group_column(
                field.data_type(),
                &key_order,
                col_idx,
                n_rows,
            )?;
            columns.push(arr);
        }

        // Aggregate result columns
        let gb_len = self.group_by.len();
        for (agg_idx, (func, _, _)) in self.aggs.iter().enumerate() {
            let arr = build_agg_column(func, &key_order, &map, agg_idx, n_rows)?;
            columns.push(arr);
        }
        let _ = gb_len;

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
    // Float — convert to ordered bits for hashing (lossy but workable for GROUP BY)
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return ScalarKey::Str(a.value(row).to_string());
    }
    ScalarKey::Null
}

fn f64_at(arr: &ArrayRef, row: usize) -> Option<f64> {
    if arr.is_null(row) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return Some(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::BooleanArray>() {
        return Some(if a.value(row) { 1.0 } else { 0.0 });
    }
    None
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
    key_order: &[Vec<ScalarKey>],
    map: &HashMap<Vec<ScalarKey>, Vec<AggState>>,
    agg_idx: usize,
    _n: usize,
) -> Result<ArrayRef> {
    match func {
        AggFunc::Count => {
            let mut b = Int64Builder::new();
            for key in key_order {
                let state = &map[key][agg_idx];
                b.append_value(state.count);
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        AggFunc::Sum => {
            let mut b = Float64Builder::new();
            for key in key_order {
                let state = &map[key][agg_idx];
                b.append_value(state.sum);
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        AggFunc::Avg => {
            let mut b = Float64Builder::new();
            for key in key_order {
                let state = &map[key][agg_idx];
                if state.count > 0 {
                    b.append_value(state.sum / state.count as f64);
                } else {
                    b.append_null();
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        AggFunc::Min => {
            let mut b = Float64Builder::new();
            for key in key_order {
                let state = &map[key][agg_idx];
                if state.count > 0 {
                    b.append_value(state.min);
                } else {
                    b.append_null();
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        AggFunc::Max => {
            let mut b = Float64Builder::new();
            for key in key_order {
                let state = &map[key][agg_idx];
                if state.count > 0 {
                    b.append_value(state.max);
                } else {
                    b.append_null();
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }
    }
}
