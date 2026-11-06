use anyhow::Result;
use arrow::compute;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::{evaluate_bool, Executor};
use crate::sql::ast::Expr;

pub struct FilterExec {
    input: Box<dyn Executor>,
    predicate: Expr,
}

impl FilterExec {
    pub fn new(input: Box<dyn Executor>, predicate: Expr) -> Self {
        Self { input, predicate }
    }
}

impl Executor for FilterExec {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            match self.input.next()? {
                None => return Ok(None),
                Some(batch) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let mask = evaluate_bool(&batch, &self.predicate)?;
                    let filtered = filter_batch(&batch, &mask)?;
                    if filtered.num_rows() > 0 {
                        return Ok(Some(filtered));
                    }
                    // all rows were filtered out — try next batch
                }
            }
        }
    }
}

fn filter_batch(batch: &RecordBatch, mask: &arrow::array::BooleanArray) -> Result<RecordBatch> {
    let columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| compute::filter(col.as_ref(), mask).map_err(anyhow::Error::from))
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(batch.schema(), columns).map_err(anyhow::Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{BinOp, Value};
    use arrow::array::{Array, Int64Array};
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
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]))
    }

    fn batch(xs: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(xs))]).unwrap()
    }

    fn gt(n: i64) -> Expr {
        Expr::BinOp {
            op: BinOp::Gt,
            left: Box::new(Expr::Column("x".into())),
            right: Box::new(Expr::Literal(Value::Int(n))),
        }
    }

    fn exec(batches: Vec<RecordBatch>, pred: Expr) -> FilterExec {
        FilterExec::new(
            Box::new(VecExec {
                schema: schema(),
                batches,
            }),
            pred,
        )
    }

    fn values(b: &RecordBatch) -> Vec<Option<i64>> {
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        (0..a.len())
            .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
            .collect()
    }

    #[test]
    fn null_predicate_rows_are_excluded() {
        let mut f = exec(
            vec![batch(vec![Some(1), None, Some(10), None, Some(7)])],
            gt(5),
        );
        let out = f.next().unwrap().unwrap();
        assert_eq!(values(&out), vec![Some(10), Some(7)]);
        assert!(f.next().unwrap().is_none());
    }

    #[test]
    fn fully_filtered_first_batch_is_skipped() {
        let mut f = exec(
            vec![batch(vec![Some(1), Some(2)]), batch(vec![Some(9), Some(3)])],
            gt(5),
        );
        let out = f.next().unwrap().unwrap();
        assert_eq!(values(&out), vec![Some(9)]);
        assert!(f.next().unwrap().is_none());
    }

    #[test]
    fn all_batches_filtered_returns_none() {
        let mut f = exec(
            vec![batch(vec![Some(1)]), batch(vec![None, Some(2)])],
            gt(5),
        );
        assert!(f.next().unwrap().is_none());
    }

    #[test]
    fn non_boolean_predicate_is_error() {
        let mut f = exec(vec![batch(vec![Some(1)])], Expr::Column("x".into()));
        let err = f.next().unwrap_err();
        assert!(err.to_string().contains("boolean"), "{err}");
    }

    #[test]
    fn empty_batches_are_skipped() {
        let mut f = exec(vec![batch(vec![]), batch(vec![Some(8)])], gt(5));
        let out = f.next().unwrap().unwrap();
        assert_eq!(values(&out), vec![Some(8)]);
        assert!(f.next().unwrap().is_none());
    }

    #[test]
    fn only_empty_batch_returns_none() {
        // A non-boolean predicate would error if the empty batch were evaluated.
        let mut f = exec(vec![batch(vec![])], Expr::Column("x".into()));
        assert!(f.next().unwrap().is_none());
    }
}
