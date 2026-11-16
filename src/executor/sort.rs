use anyhow::{anyhow, Result};
use arrow::array::{Array, ArrayRef, UInt32Array};
use arrow::compute::{concat_batches, take_record_batch, SortOptions};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};

use super::{resolve_column, Executor};
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

        // 入力が 0 行でも存在しない ORDER BY 列は検出する
        for ob in &order_by {
            resolve_column(&schema, &ob.column)
                .map_err(|e| anyhow!("ORDER BY column '{}': {}", ob.column, e))?;
        }

        if all_batches.is_empty() {
            return Ok(SortExec {
                schema,
                batches: vec![],
                cursor: 0,
                done: false,
            });
        }

        let sort_cols: Vec<(usize, bool)> = order_by
            .iter()
            .map(|ob| {
                let idx = resolve_column(&schema, &ob.column)
                    .map_err(|e| anyhow!("ORDER BY column '{}': {}", ob.column, e))?;
                Ok((idx, ob.asc))
            })
            .collect::<Result<_>>()?;

        let merged = concat_batches(&schema, &all_batches)?;

        // Row format: byte-comparable keys with total float order; NULLS LAST for both directions.
        let fields = sort_cols
            .iter()
            .map(|&(ci, asc)| {
                let dt = merged.column(ci).data_type().clone();
                if !matches!(
                    dt,
                    DataType::Int64 | DataType::Float64 | DataType::Utf8 | DataType::Boolean
                ) {
                    return Err(anyhow!("ORDER BY on column type {:?} is not supported", dt));
                }
                Ok(SortField::new_with_options(
                    dt,
                    SortOptions {
                        descending: !asc,
                        nulls_first: false,
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let key_cols: Vec<ArrayRef> = sort_cols
            .iter()
            .map(|&(ci, _)| merged.column(ci).clone())
            .collect();
        let rows = RowConverter::new(fields)?.convert_columns(&key_cols)?;

        // Stable sort keeps input order for equal keys.
        let mut indices: Vec<u32> = (0..merged.num_rows() as u32).collect();
        indices.sort_by(|&a, &b| rows.row(a as usize).cmp(&rows.row(b as usize)));
        let sorted = take_record_batch(&merged, &UInt32Array::from(indices))?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
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

    fn flag_out(schema: SchemaRef, batch: RecordBatch, asc: bool) -> Result<Vec<Option<i64>>> {
        let input = Box::new(VecExec {
            schema,
            batches: vec![batch],
        });
        let order_by = vec![OrderByItem {
            column: "flag".to_string(),
            asc,
        }];
        let mut exec = SortExec::new(input, order_by)?;
        let out = exec.next()?.unwrap();
        let a = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        Ok((0..a.len()).map(|i| Some(a.value(i))).collect())
    }

    fn flag_batch() -> (SchemaRef, RecordBatch) {
        use arrow::array::BooleanArray;
        let s = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("flag", DataType::Boolean, true),
        ]));
        let b = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    None,
                    Some(false),
                    Some(true),
                ])),
            ],
        )
        .unwrap();
        (s, b)
    }

    #[test]
    fn sorts_boolean_ascending_false_before_true_nulls_last() {
        let (s, b) = flag_batch();
        assert_eq!(
            flag_out(s, b, true).unwrap(),
            vec![Some(3), Some(1), Some(4), Some(2)]
        );
    }

    #[test]
    fn sorts_boolean_descending_nulls_last() {
        let (s, b) = flag_batch();
        assert_eq!(
            flag_out(s, b, false).unwrap(),
            vec![Some(1), Some(4), Some(3), Some(2)]
        );
    }

    #[test]
    fn unsupported_column_type_errors() {
        let s = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("flag", DataType::Int32, true),
        ]));
        let b = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(arrow::array::Int32Array::from(vec![2, 1])),
            ],
        )
        .unwrap();
        assert!(flag_out(s, b, true).is_err());
    }

    #[test]
    fn descending_keeps_nulls_last() {
        let b = batch(
            vec![Some(2), None, Some(3), Some(1)],
            vec![0.0; 4],
            vec!["a"; 4],
        );
        let out = sorted(vec![b], vec![("id", false)]).unwrap();
        assert_eq!(ids(&out), vec![Some(3), Some(2), Some(1), None]);
    }

    #[test]
    fn float_nan_has_total_order() {
        let b = batch(
            vec![Some(1), Some(2), Some(3), Some(4)],
            vec![f64::NAN, 2.0, 1.0, 3.0],
            vec!["a"; 4],
        );
        let out = sorted(vec![b], vec![("price", true)]).unwrap();
        assert_eq!(ids(&out), vec![Some(3), Some(2), Some(4), Some(1)]);
    }

    #[test]
    fn equal_keys_keep_input_order_across_batches() {
        // 同値キーは入力順を保つ(ASC/DESC とも)。id が入力順の目印。
        let b1 = batch(
            vec![Some(1), Some(2), Some(3), Some(4)],
            vec![1.0, 2.0, 1.0, 2.0],
            vec!["x", "x", "y", "y"],
        );
        let b2 = batch(
            vec![Some(5), Some(6), Some(7), None],
            vec![1.0, 2.0, 1.0, 2.0],
            vec!["x", "x", "y", "y"],
        );
        let asc = sorted(vec![b1.clone(), b2.clone()], vec![("price", true)]).unwrap();
        assert_eq!(
            ids(&asc),
            vec![
                Some(1),
                Some(3),
                Some(5),
                Some(7),
                Some(2),
                Some(4),
                Some(6),
                None
            ]
        );
        let desc = sorted(vec![b1.clone(), b2.clone()], vec![("price", false)]).unwrap();
        assert_eq!(
            ids(&desc),
            vec![
                Some(2),
                Some(4),
                Some(6),
                None,
                Some(1),
                Some(3),
                Some(5),
                Some(7)
            ]
        );
        // 複数キー: price ASC, name DESC、残りは入力順
        let multi = sorted(vec![b1, b2], vec![("price", true), ("name", false)]).unwrap();
        assert_eq!(
            ids(&multi),
            vec![
                Some(3),
                Some(7),
                Some(1),
                Some(5),
                Some(4),
                None,
                Some(2),
                Some(6)
            ]
        );
    }

    #[test]
    fn utf8_sorts_bytewise_both_directions() {
        let b = batch(
            vec![Some(1), Some(2), Some(3), Some(4)],
            vec![0.0; 4],
            vec!["b", "B", "ab", "b"],
        );
        let asc = sorted(vec![b.clone()], vec![("name", true)]).unwrap();
        assert_eq!(ids(&asc), vec![Some(2), Some(3), Some(1), Some(4)]);
        let desc = sorted(vec![b], vec![("name", false)]).unwrap();
        assert_eq!(ids(&desc), vec![Some(1), Some(4), Some(3), Some(2)]);
    }

    #[test]
    fn float_negative_zero_orders_before_zero_and_nan_desc() {
        let b = batch(
            vec![Some(1), Some(2), Some(3), Some(4), Some(5)],
            vec![0.0, -0.0, f64::NAN, -1.0, -0.0],
            vec!["a"; 5],
        );
        let asc = sorted(vec![b.clone()], vec![("price", true)]).unwrap();
        assert_eq!(ids(&asc), vec![Some(4), Some(2), Some(5), Some(1), Some(3)]);
        let desc = sorted(vec![b], vec![("price", false)]).unwrap();
        assert_eq!(
            ids(&desc),
            vec![Some(3), Some(1), Some(2), Some(5), Some(4)]
        );
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
