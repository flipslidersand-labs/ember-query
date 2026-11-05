use anyhow::{anyhow, Result};
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

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
            return Ok(SortExec {
                schema,
                batches: vec![],
                cursor: 0,
                done: false,
            });
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
    col.split('.').next_back().unwrap_or(col)
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
        return a_arr
            .value(a)
            .partial_cmp(&a_arr.value(b))
            .unwrap_or(std::cmp::Ordering::Equal);
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    struct VecExec {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    }

    impl Executor for VecExec {
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

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("price", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn batch(ids: Vec<Option<i64>>, prices: Vec<f64>, names: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Float64Array::from(prices)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    fn sorted(batches: Vec<RecordBatch>, order: Vec<(&str, bool)>) -> Result<Vec<RecordBatch>> {
        let input = Box::new(VecExec {
            schema: schema(),
            batches,
        });
        let order_by = order
            .into_iter()
            .map(|(c, asc)| OrderByItem {
                column: c.to_string(),
                asc,
            })
            .collect();
        let mut exec = SortExec::new(input, order_by)?;
        let mut out = vec![];
        while let Some(b) = exec.next()? {
            out.push(b);
        }
        Ok(out)
    }

    fn ids(out: &[RecordBatch]) -> Vec<Option<i64>> {
        let a = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..a.len())
            .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
            .collect()
    }

    #[test]
    fn sorts_int_ascending_across_batches_with_nulls_last() {
        let b1 = batch(vec![Some(3), None], vec![1.0, 2.0], vec!["c", "n"]);
        let b2 = batch(vec![Some(1), Some(2)], vec![3.0, 4.0], vec!["a", "b"]);
        let out = sorted(vec![b1, b2], vec![("id", true)]).unwrap();
        assert_eq!(ids(&out), vec![Some(1), Some(2), Some(3), None]);
    }

    #[test]
    fn sorts_descending_with_table_prefix() {
        let b = batch(vec![Some(1), Some(3), Some(2)], vec![0.0; 3], vec!["a"; 3]);
        let out = sorted(vec![b], vec![("t.id", false)]).unwrap();
        assert_eq!(ids(&out), vec![Some(3), Some(2), Some(1)]);
    }

    #[test]
    fn sorts_by_float_then_string_tiebreak() {
        let b = batch(
            vec![Some(1), Some(2), Some(3)],
            vec![2.0, 1.0, 1.0],
            vec!["x", "z", "y"],
        );
        let out = sorted(vec![b], vec![("price", true), ("name", true)]).unwrap();
        assert_eq!(ids(&out), vec![Some(3), Some(2), Some(1)]);
    }

    #[test]
    fn empty_input_yields_no_batches() {
        assert!(sorted(vec![], vec![("id", true)]).unwrap().is_empty());
    }

    #[test]
    fn unknown_column_errors() {
        let b = batch(vec![Some(1)], vec![0.0], vec!["a"]);
        let err = sorted(vec![b], vec![("nope", true)]).err().unwrap();
        assert!(err.to_string().contains("not found"));
    }
}
