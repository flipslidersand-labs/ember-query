use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::Executor;

pub struct LimitExec {
    input: Box<dyn Executor>,
    remaining: u64,
    done: bool,
}

impl LimitExec {
    pub fn new(input: Box<dyn Executor>, n: u64) -> Self {
        Self {
            input,
            remaining: n,
            done: false,
        }
    }
}

impl Executor for LimitExec {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.done || self.remaining == 0 {
            return Ok(None);
        }

        match self.input.next()? {
            None => {
                self.done = true;
                Ok(None)
            }
            Some(batch) => {
                let rows = batch.num_rows() as u64;
                if rows <= self.remaining {
                    self.remaining -= rows;
                    if self.remaining == 0 {
                        self.done = true;
                    }
                    Ok(Some(batch))
                } else {
                    // Slice to exactly `remaining` rows
                    let take = self.remaining as usize;
                    let sliced = batch.slice(0, take);
                    self.remaining = 0;
                    self.done = true;
                    Ok(Some(sliced))
                }
            }
        }
    }
}
