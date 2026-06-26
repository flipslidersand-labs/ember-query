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
