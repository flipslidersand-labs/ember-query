use anyhow::{anyhow, Result};
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use super::Executor;
use crate::sql::ast::OrderByItem;

/// Sort executor — collects all input rows, sorts in-memory, then streams output.
pub struct SortExec {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    cursor: usize,
    done: bool,
}

impl SortExec {
    pub fn new(mut input: Box<dyn Executor>, order_by: Vec<OrderByItem>) -> Result<Self> {
        let schema = input.schema();
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        while let Some(rb) = input.next()? {
            all_batches.push(rb);
        }

        if all_batches.is_empty() {
            return Ok(SortExec { schema, batches: vec![], cursor: 0, done: false });
        }

        // Concatenate all batches into one for sorting
        let cols: Vec<Vec<ArrayRef>> = (0..all_batches[0].num_columns())
            .map(|ci| all_batches.iter().map(|b| b.column(ci).clone()).collect())
            .collect();

        let merged_columns: Vec<ArrayRef> = cols
            .into_iter()
            .map(|parts| {
                let refs: Vec<&dyn Array> = parts.iter().map(|a| a.as_ref()).collect();
                arrow::compute::concat(&refs).map_err(anyhow::Error::from)
            })
            .collect::<Result<_>>()?;

        let merged = RecordBatch::try_new(schema.clone(), merged_columns)?;

        // Sort: build index vector [(sort_key, original_row_idx)] and sort it
        let n = merged.num_rows();
        let mut indices: Vec<usize> = (0..n).collect();

        // Find sort column indices
        let sort_cols: Vec<(usize, bool)> = order_by
            .iter()
            .map(|ob| {
                let col_name = strip_table_prefix(&ob.column);
                let idx = schema
                    .index_of(col_name)
                    .or_else(|_| schema.index_of(&ob.column))
                    .map_err(|_| anyhow!("ORDER BY column '{}' not found", ob.column))?;
                Ok((idx, ob.asc))
            })
            .collect::<Result<_>>()?;

        indices.sort_by(|&a, &b| {
            for &(col_idx, asc) in &sort_cols {
                let arr = merged.column(col_idx);
                let ord = compare_rows(arr, a, b);
                let ord = if asc { ord } else { ord.reverse() };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });

        // Reorder rows according to sorted indices
        let sorted_columns: Vec<ArrayRef> = merged
            .columns()
            .iter()
            .map(|col| take_rows(col, &indices))
            .collect::<Result<_>>()?;

        let sorted = RecordBatch::try_new(schema.clone(), sorted_columns)?;

        Ok(SortExec {
            schema,
            batches: vec![sorted],
            cursor: 0,
            done: false,
        })
    }
}

impl Executor for SortExec {
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

fn strip_table_prefix(col: &str) -> &str {
    col.split('.').last().unwrap_or(col)
}

fn compare_rows(arr: &ArrayRef, a: usize, b: usize) -> std::cmp::Ordering {
    if arr.is_null(a) && arr.is_null(b) {
        return std::cmp::Ordering::Equal;
    }
    if arr.is_null(a) {
        return std::cmp::Ordering::Greater;
    }
    if arr.is_null(b) {
        return std::cmp::Ordering::Less;
    }
    if let Some(a_arr) = arr.as_any().downcast_ref::<Int64Array>() {
        return a_arr.value(a).cmp(&a_arr.value(b));
    }
    if let Some(a_arr) = arr.as_any().downcast_ref::<Float64Array>() {
        return a_arr.value(a).partial_cmp(&a_arr.value(b)).unwrap_or(std::cmp::Ordering::Equal);
    }
    if let Some(a_arr) = arr.as_any().downcast_ref::<StringArray>() {
        return a_arr.value(a).cmp(a_arr.value(b));
    }
    std::cmp::Ordering::Equal
}

fn take_rows(arr: &ArrayRef, indices: &[usize]) -> Result<ArrayRef> {
    let take_indices: arrow::array::Int64Array = indices.iter().map(|&i| Some(i as i64)).collect();
    Ok(arrow::compute::take(arr.as_ref(), &take_indices, None)?)
}
