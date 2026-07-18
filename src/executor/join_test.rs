#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use crate::executor::{build_executor, Executor};
    use crate::planner::logical::LogicalPlan;
    use crate::catalog::TableSchema;
    use crate::sql::ast::{BinOp, Expr};
    use crate::executor::join::HashJoinExec;

    fn make_schema(fields: Vec<(&str, DataType)>) -> Arc<Schema> {
        Arc::new(Schema::new(
            fields.into_iter().map(|(n, t)| Field::new(n, t, true)).collect::<Vec<_>>()
        ))
    }

    fn make_batch(schema: Arc<Schema>, cols: Vec<Vec<i64>>) -> RecordBatch {
        let arrays: Vec<Arc<dyn arrow::array::Array>> = cols.into_iter()
            .map(|v| Arc::new(Int64Array::from(v)) as _)
            .collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    struct VecExec {
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
        idx: usize,
    }

    impl VecExec {
        fn new(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
            VecExec { schema, batches, idx: 0 }
        }
    }

    impl Executor for VecExec {
        fn next(&mut self) -> anyhow::Result<Option<RecordBatch>> {
            if self.idx >= self.batches.len() {
                return Ok(None);
            }
            let b = self.batches[self.idx].clone();
            self.idx += 1;
            Ok(Some(b))
        }
        fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }
    }

    #[test]
    fn inner_join_matches_on_id() {
        // orders: id, amount
        let o_schema = make_schema(vec![("id", DataType::Int64), ("amount", DataType::Int64)]);
        let o_batch = make_batch(o_schema.clone(), vec![vec![1, 2, 3], vec![100, 200, 300]]);

        // customers: id, name  (as Int64 for simplicity)
        let c_schema = make_schema(vec![("id", DataType::Int64), ("score", DataType::Int64)]);
        let c_batch = make_batch(c_schema.clone(), vec![vec![2, 3, 4], vec![20, 30, 40]]);

        // ON orders.id = customers.id
        let on = Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Column("id".into())),
            right: Box::new(Expr::Column("id".into())),
        };

        let left = Box::new(VecExec::new(o_schema, vec![o_batch]));
        let right = Box::new(VecExec::new(c_schema, vec![c_batch]));
        let mut exec = HashJoinExec::new(left, right, &on).unwrap();

        let mut total_rows = 0;
        while let Some(batch) = exec.next().unwrap() {
            total_rows += batch.num_rows();
        }
        // ids 2 and 3 match → 2 rows
        assert_eq!(total_rows, 2, "expected 2 matching rows for ids 2 and 3");
    }

    #[test]
    fn inner_join_no_matches_returns_empty() {
        let s = make_schema(vec![("id", DataType::Int64)]);
        let left_b = make_batch(s.clone(), vec![vec![1, 2]]);
        let right_b = make_batch(s.clone(), vec![vec![5, 6]]);

        let on = Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Column("id".into())),
            right: Box::new(Expr::Column("id".into())),
        };

        let left = Box::new(VecExec::new(s.clone(), vec![left_b]));
        let right = Box::new(VecExec::new(s, vec![right_b]));
        let mut exec = HashJoinExec::new(left, right, &on).unwrap();

        let mut total_rows = 0;
        while let Some(batch) = exec.next().unwrap() {
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 0, "no matches expected");
    }

    #[test]
    fn non_eq_on_returns_error() {
        use crate::sql::ast::BinOp;
        let s = make_schema(vec![("id", DataType::Int64)]);
        let on = Expr::BinOp {
            op: BinOp::Lt,  // not equality
            left: Box::new(Expr::Column("id".into())),
            right: Box::new(Expr::Column("id".into())),
        };
        let left = Box::new(VecExec::new(s.clone(), vec![]));
        let right = Box::new(VecExec::new(s, vec![]));
        assert!(HashJoinExec::new(left, right, &on).is_err());
    }
}
