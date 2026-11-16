#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use crate::executor::join::HashJoinExec;
    use crate::executor::Executor;

    use crate::sql::ast::{BinOp, Expr};

    fn make_schema(fields: Vec<(&str, DataType)>) -> Arc<Schema> {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(n, t)| Field::new(n, t, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn make_batch(schema: Arc<Schema>, cols: Vec<Vec<i64>>) -> RecordBatch {
        let arrays: Vec<Arc<dyn arrow::array::Array>> = cols
            .into_iter()
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
            VecExec {
                schema,
                batches,
                idx: 0,
            }
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
            op: BinOp::Lt, // not equality
            left: Box::new(Expr::Column("id".into())),
            right: Box::new(Expr::Column("id".into())),
        };
        let left = Box::new(VecExec::new(s.clone(), vec![]));
        let right = Box::new(VecExec::new(s, vec![]));
        assert!(HashJoinExec::new(left, right, &on).is_err());
    }

    // ── #36: NULL / 型違いキー・出力値の検証 ─────────────────────────────

    use arrow::array::{ArrayRef, Float64Array, StringArray};

    /// 2 列 (k, v) のバッチ。k は任意型、v は Int64。
    fn kv_batch(k: ArrayRef, v: Vec<i64>) -> RecordBatch {
        let schema = make_schema(vec![("k", k.data_type().clone()), ("v", DataType::Int64)]);
        RecordBatch::try_new(schema, vec![k, Arc::new(Int64Array::from(v))]).unwrap()
    }

    /// ON k = k で結合し、出力バッチ列を返す。
    fn run_join(left: Vec<RecordBatch>, right: Vec<RecordBatch>) -> Vec<RecordBatch> {
        let on = Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Column("k".into())),
            right: Box::new(Expr::Column("k".into())),
        };
        let ls = left[0].schema();
        let rs = right[0].schema();
        let mut exec = HashJoinExec::new(
            Box::new(VecExec::new(ls, left)),
            Box::new(VecExec::new(rs, right)),
            &on,
        )
        .unwrap();
        let mut out = Vec::new();
        while let Some(b) = exec.next().unwrap() {
            out.push(b);
        }
        out
    }

    fn rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn i64_col(b: &RecordBatch, name: &str) -> Vec<i64> {
        let idx = b.schema().index_of(name).unwrap();
        let a = b.column(idx).as_any().downcast_ref::<Int64Array>().unwrap();
        (0..a.len()).map(|i| a.value(i)).collect()
    }

    #[test]
    fn null_keys_do_not_match_each_other() {
        let l = kv_batch(
            Arc::new(Int64Array::from(vec![Some(1), None])),
            vec![10, 20],
        );
        let r = kv_batch(
            Arc::new(Int64Array::from(vec![None, Some(1)])),
            vec![100, 200],
        );
        let out = run_join(vec![l], vec![r]);
        assert_eq!(rows(&out), 1, "NULL = NULL must not match");
        assert_eq!(i64_col(&out[0], "k"), vec![1]);
        assert_eq!(i64_col(&out[0], "v"), vec![10]);
        assert_eq!(i64_col(&out[0], "right_k"), vec![1]);
        assert_eq!(i64_col(&out[0], "right_v"), vec![200]);
    }

    #[test]
    fn utf8_null_string_does_not_match_null() {
        let l = kv_batch(
            Arc::new(StringArray::from(vec![None, Some("a")])),
            vec![1, 2],
        );
        let r = kv_batch(
            Arc::new(StringArray::from(vec![Some("NULL"), Some("a")])),
            vec![10, 20],
        );
        let out = run_join(vec![l], vec![r]);
        assert_eq!(rows(&out), 1, "NULL must not match the string 'NULL'");
        assert_eq!(i64_col(&out[0], "v"), vec![2]);
        assert_eq!(i64_col(&out[0], "right_v"), vec![20]);
    }

    #[test]
    fn utf8_null_string_matches_itself() {
        let l = kv_batch(Arc::new(StringArray::from(vec!["NULL"])), vec![1]);
        let r = kv_batch(Arc::new(StringArray::from(vec!["NULL"])), vec![2]);
        assert_eq!(rows(&run_join(vec![l], vec![r])), 1);
    }

    #[test]
    fn int64_and_utf8_same_text_do_not_match() {
        let l = kv_batch(Arc::new(Int64Array::from(vec![1])), vec![1]);
        let r = kv_batch(Arc::new(StringArray::from(vec!["1"])), vec![2]);
        assert_eq!(rows(&run_join(vec![l], vec![r])), 0);
    }

    #[test]
    fn int64_and_float64_match_by_value() {
        // #130: Int64 は Float64 に昇格して比較する(WHERE と同じ)
        let l = kv_batch(Arc::new(Int64Array::from(vec![1, 2])), vec![1, 2]);
        let r = kv_batch(Arc::new(Float64Array::from(vec![1.0, 2.5])), vec![10, 20]);
        let out = run_join(vec![l], vec![r]);
        assert_eq!(rows(&out), 1);
        assert_eq!(i64_col(&out[0], "right_v"), vec![10]);
    }

    #[test]
    fn float64_keys_compare_by_value_bits() {
        let l = kv_batch(
            Arc::new(Float64Array::from(vec![0.0, f64::NAN, 1.5])),
            vec![1, 2, 3],
        );
        let r = kv_batch(
            Arc::new(Float64Array::from(vec![-0.0, f64::NAN, 2.5])),
            vec![10, 20, 30],
        );
        let out = run_join(vec![l], vec![r]);
        assert_eq!(rows(&out), 2, "-0.0 == 0.0 and NaN == NaN");
        assert_eq!(i64_col(&out[0], "v"), vec![1, 2]);
        assert_eq!(i64_col(&out[0], "right_v"), vec![10, 20]);
    }

    #[test]
    fn duplicate_right_keys_multi_batch_values() {
        // left 2 バッチ、right 2 バッチ。right に重複キー (1:N)。
        let l1 = kv_batch(Arc::new(Int64Array::from(vec![1, 2])), vec![10, 20]);
        let l2 = kv_batch(Arc::new(Int64Array::from(vec![1])), vec![30]);
        let r1 = kv_batch(Arc::new(Int64Array::from(vec![1, 1])), vec![100, 101]);
        let r2 = kv_batch(Arc::new(Int64Array::from(vec![1, 3])), vec![102, 103]);
        let out = run_join(vec![l1, l2], vec![r1, r2]);

        assert_eq!(rows(&out), 6);
        assert_eq!(out.len(), 2, "one output batch per left batch with matches");
        let names: Vec<String> = out[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, vec!["k", "v", "right_k", "right_v"]);

        assert_eq!(i64_col(&out[0], "k"), vec![1, 1, 1]);
        assert_eq!(i64_col(&out[0], "v"), vec![10, 10, 10]);
        assert_eq!(i64_col(&out[0], "right_k"), vec![1, 1, 1]);
        assert_eq!(i64_col(&out[0], "right_v"), vec![100, 101, 102]);
        assert_eq!(i64_col(&out[1], "v"), vec![30, 30, 30]);
        assert_eq!(i64_col(&out[1], "right_v"), vec![100, 101, 102]);
    }

    // ── #133: 結果は逐次(バッチ単位)に生成する ─────────────────────────

    #[test]
    fn duplicate_keys_first_next_builds_only_one_bounded_batch() {
        // 全行 k=1 の 1000 x 1000 (全結果は 100 万行)。next() 1 回で全件を作らない。
        let n = 1000;
        let l = kv_batch(Arc::new(Int64Array::from(vec![1; n])), vec![0; n]);
        let r = kv_batch(Arc::new(Int64Array::from(vec![1; n])), vec![0; n]);
        let on = Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Column("k".into())),
            right: Box::new(Expr::Column("k".into())),
        };
        let (ls, rs) = (l.schema(), r.schema());
        let mut exec = HashJoinExec::new(
            Box::new(VecExec::new(ls, vec![l])),
            Box::new(VecExec::new(rs, vec![r])),
            &on,
        )
        .unwrap();
        let first = exec.next().unwrap().unwrap();
        assert!(first.num_rows() > 0 && first.num_rows() <= 8192);
    }

    #[test]
    fn large_fanout_is_split_into_batches_preserving_order() {
        // 左 3 行 x 右 5000 行 (同一キー) = 15000 行。分割しても順序・総数は同じ。
        let l = kv_batch(Arc::new(Int64Array::from(vec![1, 1, 1])), vec![1, 2, 3]);
        let rv: Vec<i64> = (0..5000).collect();
        let r = kv_batch(Arc::new(Int64Array::from(vec![1; 5000])), rv.clone());
        let out = run_join(vec![l], vec![r]);
        assert!(out.len() > 1);
        assert!(out.iter().all(|b| b.num_rows() <= 8192));
        assert_eq!(rows(&out), 15000);
        let lv: Vec<i64> = out.iter().flat_map(|b| i64_col(b, "v")).collect();
        let rr: Vec<i64> = out.iter().flat_map(|b| i64_col(b, "right_v")).collect();
        let exp_l: Vec<i64> = (1..=3).flat_map(|x| vec![x; 5000]).collect();
        let exp_r: Vec<i64> = (0..3).flat_map(|_| rv.clone()).collect();
        assert_eq!(lv, exp_l);
        assert_eq!(rr, exp_r);
    }

    /// 1 回目の next() はエラー、以降は同じバッチを返す左入力。
    struct FailOnceExec {
        schema: Arc<Schema>,
        batch: RecordBatch,
        failed: bool,
    }

    impl Executor for FailOnceExec {
        fn next(&mut self) -> anyhow::Result<Option<RecordBatch>> {
            if !self.failed {
                self.failed = true;
                anyhow::bail!("left failed");
            }
            Ok(Some(self.batch.clone()))
        }
        fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }
    }

    #[test]
    fn next_after_left_error_stays_finished() {
        let l = kv_batch(Arc::new(Int64Array::from(vec![1])), vec![1]);
        let r = kv_batch(Arc::new(Int64Array::from(vec![1])), vec![2]);
        let on = Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Column("k".into())),
            right: Box::new(Expr::Column("k".into())),
        };
        let left = Box::new(FailOnceExec {
            schema: l.schema(),
            batch: l,
            failed: false,
        });
        let right = Box::new(VecExec::new(r.schema(), vec![r]));
        let mut exec = HashJoinExec::new(left, right, &on).unwrap();
        assert!(exec.next().is_err());
        assert!(exec.next().unwrap().is_none(), "no retry after an error");
    }

    /// 左 n 行 (キー 1..=n, 各 1 件マッチ) x 右 (キー 1..=n)。
    fn unique_key_join(n: i64) -> Vec<RecordBatch> {
        let keys: Vec<i64> = (1..=n).collect();
        let l = kv_batch(Arc::new(Int64Array::from(keys.clone())), keys.clone());
        let r = kv_batch(Arc::new(Int64Array::from(keys.clone())), keys);
        run_join(vec![l], vec![r])
    }

    #[test]
    fn output_of_exactly_one_full_batch_has_no_trailing_batch() {
        let out = unique_key_join(8192);
        assert_eq!(out.iter().map(|b| b.num_rows()).collect::<Vec<_>>(), [8192]);
    }

    #[test]
    fn output_one_row_over_the_limit_spills_into_a_second_batch() {
        let out = unique_key_join(8193);
        assert_eq!(
            out.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            [8192, 1]
        );
        let lv: Vec<i64> = out.iter().flat_map(|b| i64_col(b, "v")).collect();
        assert_eq!(lv, (1..=8193).collect::<Vec<i64>>());
    }

    #[test]
    fn resumes_correctly_across_left_batches() {
        // 左 2 バッチ。1 つ目の途中 (行の途中) で出力が満杯になり、続きから再開する。
        let l1 = kv_batch(Arc::new(Int64Array::from(vec![1, 2])), vec![10, 20]);
        let l2 = kv_batch(Arc::new(Int64Array::from(vec![2, 3])), vec![30, 40]);
        let rv: Vec<i64> = (0..5000).collect();
        let mut rk = vec![1; 5000];
        rk.extend(vec![2; 5000]);
        rk.push(3);
        let mut rvals = rv.clone();
        rvals.extend(rv.clone());
        rvals.push(0);
        let r = kv_batch(Arc::new(Int64Array::from(rk)), rvals);
        let out = run_join(vec![l1, l2], vec![r]);
        assert!(out.iter().all(|b| b.num_rows() <= 8192));
        // 5000 + 5000 (l1) + 5000 (l2 key 2) + 1 (l2 key 3)
        assert_eq!(rows(&out), 15001);
        let lv: Vec<i64> = out.iter().flat_map(|b| i64_col(b, "v")).collect();
        let mut exp: Vec<i64> = vec![10; 5000];
        exp.extend(vec![20; 5000]);
        exp.extend(vec![30; 5000]);
        exp.push(40);
        assert_eq!(lv, exp);
    }

    #[test]
    fn empty_right_input_yields_no_rows() {
        let l = kv_batch(Arc::new(Int64Array::from(vec![1, 2])), vec![1, 2]);
        let empty = kv_batch(Arc::new(Int64Array::from(Vec::<i64>::new())), vec![]);
        assert_eq!(rows(&run_join(vec![l.clone()], vec![empty.clone()])), 0);

        // 右が 0 バッチ (空のストリーム) でも同様。
        let on = Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Column("k".into())),
            right: Box::new(Expr::Column("k".into())),
        };
        let mut exec = HashJoinExec::new(
            Box::new(VecExec::new(l.schema(), vec![l])),
            Box::new(VecExec::new(empty.schema(), vec![])),
            &on,
        )
        .unwrap();
        assert!(exec.next().unwrap().is_none());
    }
}
