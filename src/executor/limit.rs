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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Stub that yields prepared batches and counts `next()` calls.
    struct VecExec {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        calls: Arc<AtomicUsize>,
    }

    impl Executor for VecExec {
        fn next(&mut self) -> Result<Option<RecordBatch>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
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
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch(range: std::ops::Range<i64>) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from(range.collect::<Vec<_>>()))],
        )
        .unwrap()
    }

    /// Run LimitExec over `batches`; returns (output batches, input next() call count).
    fn run(batches: Vec<RecordBatch>, n: u64) -> (Vec<RecordBatch>, usize) {
        let calls = Arc::new(AtomicUsize::new(0));
        let input = Box::new(VecExec {
            schema: schema(),
            batches,
            calls: calls.clone(),
        });
        let mut exec = LimitExec::new(input, n);
        let mut out = vec![];
        while let Some(b) = exec.next().unwrap() {
            out.push(b);
        }
        // Extra calls after exhaustion must stay None.
        assert!(exec.next().unwrap().is_none());
        let c = calls.load(Ordering::SeqCst);
        (out, c)
    }

    fn ids(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn limit_zero_returns_no_batches_and_does_not_read_input() {
        let (out, calls) = run(vec![batch(0..5)], 0);
        assert!(out.is_empty());
        assert_eq!(calls, 0);
    }

    #[test]
    fn limit_equals_batch_size_stops_without_reading_next() {
        let (out, calls) = run(vec![batch(0..5), batch(5..10)], 5);
        assert_eq!(ids(&out), vec![0, 1, 2, 3, 4]);
        assert_eq!(out.len(), 1);
        assert_eq!(calls, 1);
    }

    #[test]
    fn limit_smaller_than_first_batch_slices() {
        let (out, calls) = run(vec![batch(0..5), batch(5..10)], 3);
        assert_eq!(ids(&out), vec![0, 1, 2]);
        assert_eq!(calls, 1);
    }

    #[test]
    fn limit_spanning_batches_slices_last_batch() {
        let (out, calls) = run(vec![batch(0..4), batch(4..8), batch(8..12)], 6);
        assert_eq!(ids(&out), vec![0, 1, 2, 3, 4, 5]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[1].num_rows(), 2);
        assert_eq!(calls, 2);
    }

    #[test]
    fn limit_at_multi_batch_boundary_stops_without_reading_next() {
        let (out, calls) = run(vec![batch(0..4), batch(4..8), batch(8..12)], 8);
        assert_eq!(ids(&out), (0..8).collect::<Vec<_>>());
        assert_eq!(out.len(), 2);
        assert_eq!(calls, 2);
    }

    #[test]
    fn limit_larger_than_total_returns_all_rows() {
        let (out, calls) = run(vec![batch(0..4), batch(4..8)], 100);
        assert_eq!(ids(&out), (0..8).collect::<Vec<_>>());
        // two batches + one call that observed end of input
        assert_eq!(calls, 3);
    }

    #[test]
    fn empty_input_returns_nothing() {
        let (out, calls) = run(vec![], 10);
        assert!(out.is_empty());
        assert_eq!(calls, 1);
    }

    #[test]
    fn schema_is_passed_through() {
        let input = Box::new(VecExec {
            schema: schema(),
            batches: vec![],
            calls: Arc::new(AtomicUsize::new(0)),
        });
        assert_eq!(LimitExec::new(input, 1).schema(), schema());
    }
}
