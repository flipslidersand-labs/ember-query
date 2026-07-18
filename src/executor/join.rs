use anyhow::{anyhow, Result};
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

use super::{evaluate, evaluate_bool, Executor};
use crate::sql::ast::Expr;

/// Hash inner join executor.
///
/// Builds a hash map from the right (probe) side keyed by a hash of all right-side
/// values in the ON equality column, then probes with each left row.
///
/// For Phase 5 the ON expr must be `left_col = right_col`.
pub struct HashJoinExec {
    /// Merged schema: left fields then right fields (with table prefix on collision)
    schema: SchemaRef,
    /// All joined batches (materialised)
    batches: Vec<RecordBatch>,
    cursor: usize,
    done: bool,
}

impl HashJoinExec {
    pub fn new(
        mut left: Box<dyn Executor>,
        mut right: Box<dyn Executor>,
        on: &Expr,
    ) -> Result<Self> {
        // Extract equality columns from the ON expression
        let (left_col, right_col) = extract_eq_columns(on)
            .ok_or_else(|| anyhow!("JOIN ON must be an equality expression (left_col = right_col)"))?;

        // Collect all right batches into a hash map: right_key_value → [row_index, batch_index]
        let mut right_batches: Vec<RecordBatch> = Vec::new();
        while let Some(rb) = right.next()? {
            right_batches.push(rb);
        }

        // Build hash map: string-key → Vec<(batch_idx, row_idx)>
        let mut build_map: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
        for (b_idx, batch) in right_batches.iter().enumerate() {
            let key_arr = eval_column_str(batch, &right_col)?;
            for r_idx in 0..batch.num_rows() {
                let key = array_value_str(&key_arr, r_idx);
                build_map.entry(key).or_default().push((b_idx, r_idx));
            }
        }

        // Build merged schema (left || right, with "right." prefix on name conflicts)
        let left_schema = left.schema();
        let right_schema = right.schema();
        let merged_schema = merge_schemas(&left_schema, &right_schema);
        let schema = Arc::new(merged_schema);

        // Probe: for each left row, find matching right rows and emit merged rows
        let mut result_batches: Vec<RecordBatch> = Vec::new();
        while let Some(left_batch) = left.next()? {
            let probe_arr = eval_column_str(&left_batch, &left_col)?;
            let mut matched_left_indices: Vec<usize> = Vec::new();
            let mut matched_right_batch_indices: Vec<usize> = Vec::new();
            let mut matched_right_row_indices: Vec<usize> = Vec::new();

            for l_row in 0..left_batch.num_rows() {
                let key = array_value_str(&probe_arr, l_row);
                if let Some(matches) = build_map.get(&key) {
                    for &(rb_idx, rr_idx) in matches {
                        matched_left_indices.push(l_row);
                        matched_right_batch_indices.push(rb_idx);
                        matched_right_row_indices.push(rr_idx);
                    }
                }
            }

            if matched_left_indices.is_empty() {
                continue;
            }

            // Build output batch by taking matching rows
            let n = matched_left_indices.len();
            let mut columns: Vec<ArrayRef> = Vec::new();

            // Left columns
            for col in left_batch.columns() {
                let selected: Vec<ArrayRef> = matched_left_indices
                    .iter()
                    .map(|&idx| col.slice(idx, 1))
                    .collect();
                let refs: Vec<&dyn Array> = selected.iter().map(|a| a.as_ref()).collect();
                columns.push(arrow::compute::concat(&refs)?);
            }

            // Right columns (from potentially different batches)
            for col_idx in 0..right_batches[0].num_columns() {
                let mut parts: Vec<ArrayRef> = Vec::new();
                for i in 0..n {
                    let rb = &right_batches[matched_right_batch_indices[i]];
                    let row = matched_right_row_indices[i];
                    parts.push(rb.column(col_idx).slice(row, 1));
                }
                let refs: Vec<&dyn Array> = parts.iter().map(|a| a.as_ref()).collect();
                columns.push(arrow::compute::concat(&refs)?);
            }

            result_batches.push(RecordBatch::try_new(schema.clone(), columns)?);
        }

        Ok(HashJoinExec {
            schema,
            batches: result_batches,
            cursor: 0,
            done: false,
        })
    }
}

impl Executor for HashJoinExec {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.done || self.cursor >= self.batches.len() {
            self.done = true;
            return Ok(None);
        }
        let batch = self.batches[self.cursor].clone();
        self.cursor += 1;
        Ok(Some(batch))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Extract (left_col, right_col) from `left_col = right_col` ON expression.
fn extract_eq_columns(on: &Expr) -> Option<(String, String)> {
    if let Expr::BinOp {
        op: crate::sql::ast::BinOp::Eq,
        left,
        right,
    } = on
    {
        let l = column_name(left)?;
        let r = column_name(right)?;
        Some((l, r))
    } else {
        None
    }
}

fn column_name(expr: &Expr) -> Option<String> {
    if let Expr::Column(name) = expr {
        Some(name.clone())
    } else {
        None
    }
}

/// Evaluate a column (possibly qualified like "t1.col") and return as StringArray for hashing.
fn eval_column_str(batch: &RecordBatch, col: &str) -> Result<ArrayRef> {
    // Try exact name first, then try unqualified suffix
    if let Ok(arr) = evaluate(batch, &Expr::Column(col.to_string())) {
        return Ok(arr);
    }
    // Try stripping the table prefix (e.g., "orders.id" → "id")
    if let Some(unqualified) = col.split('.').last() {
        if let Ok(arr) = evaluate(batch, &Expr::Column(unqualified.to_string())) {
            return Ok(arr);
        }
    }
    Err(anyhow!("column '{}' not found in batch", col))
}

fn array_value_str(arr: &ArrayRef, idx: usize) -> String {
    use arrow::array::*;
    if arr.is_null(idx) {
        return "NULL".to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return a.value(idx).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return a.value(idx).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return a.value(idx).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return a.value(idx).to_string();
    }
    format!("{:?}[{}]", arr.data_type(), idx)
}

fn merge_schemas(left: &Schema, right: &Schema) -> Schema {
    let left_names: std::collections::HashSet<&str> =
        left.fields().iter().map(|f| f.name().as_str()).collect();
    let mut fields: Vec<Field> = left.fields().iter().map(|f| f.as_ref().clone()).collect();
    for f in right.fields() {
        if left_names.contains(f.name().as_str()) {
            fields.push(Field::new(
                format!("right_{}", f.name()),
                f.data_type().clone(),
                f.is_nullable(),
            ));
        } else {
            fields.push(f.as_ref().clone());
        }
    }
    Schema::new(fields)
}

// suppress unused import warning for evaluate_bool
#[allow(unused_imports)]
use super::evaluate_bool as _;
