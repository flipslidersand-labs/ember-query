pub mod csv_scan;
pub mod filter;
pub mod hash_agg;
pub mod join;
#[cfg(test)]
#[path = "join_test.rs"]
mod join_test;
pub mod limit;
pub mod project;
pub mod sort;

use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute;
use arrow::compute::kernels::numeric as numeric_kernels;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::sql::ast::{BinOp, Expr, UnaryOp, Value};

/// Executor trait — produces `RecordBatch`es one at a time.
pub trait Executor {
    fn next(&mut self) -> Result<Option<RecordBatch>>;
    fn schema(&self) -> SchemaRef;
}

// ── Expression evaluation ─────────────────────────────────────────────────────

/// Evaluate an expression over a `RecordBatch`, returning an `ArrayRef`.
pub fn evaluate(batch: &RecordBatch, expr: &Expr) -> Result<ArrayRef> {
    match expr {
        Expr::Column(name) => batch
            .schema()
            .index_of(name)
            .map(|i| batch.column(i).clone())
            .map_err(|_| {
                anyhow!(
                    "column '{}' not found in schema {:?}",
                    name,
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                )
            }),

        Expr::Literal(val) => make_scalar_array(val, batch.num_rows()),

        Expr::BinOp { op, left, right } => {
            let l = evaluate(batch, left)?;
            let r = evaluate(batch, right)?;
            apply_binop(op, &l, &r)
        }

        Expr::Unary { op, expr } => {
            let arr = evaluate(batch, expr)?;
            match op {
                UnaryOp::Not => {
                    let bool_arr = arr
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| anyhow!("NOT requires boolean array"))?;
                    Ok(Arc::new(compute::not(bool_arr)?) as ArrayRef)
                }
                UnaryOp::Neg => {
                    // Negate numeric column
                    if let Some(i64_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                        let negated: Int64Array = i64_arr
                            .iter()
                            .map(|v| {
                                v.map(|x| {
                                    x.checked_neg().ok_or_else(|| {
                                        anyhow!("integer overflow: cannot negate {x}")
                                    })
                                })
                                .transpose()
                            })
                            .collect::<Result<_>>()?;
                        Ok(Arc::new(negated) as ArrayRef)
                    } else if let Some(f64_arr) = arr.as_any().downcast_ref::<Float64Array>() {
                        let negated: Float64Array = f64_arr.iter().map(|v| v.map(|x| -x)).collect();
                        Ok(Arc::new(negated) as ArrayRef)
                    } else {
                        Err(anyhow!("NEG requires numeric array"))
                    }
                }
            }
        }

        Expr::Wildcard => {
            // COUNT(*) wildcard — return an all-true boolean array
            let arr: BooleanArray = (0..batch.num_rows()).map(|_| Some(true)).collect();
            Ok(Arc::new(arr) as ArrayRef)
        }

        Expr::Agg { .. } => Err(anyhow!(
            "Agg expressions must be handled by the hash_agg executor"
        )),
    }
}

/// Evaluate an expression and return a `BooleanArray` (for filter predicates).
pub fn evaluate_bool(batch: &RecordBatch, expr: &Expr) -> Result<BooleanArray> {
    let arr = evaluate(batch, expr)?;
    if let Some(b) = arr.as_any().downcast_ref::<BooleanArray>() {
        Ok(b.clone())
    } else {
        Err(anyhow!(
            "predicate did not evaluate to boolean; got {:?}",
            arr.data_type()
        ))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_scalar_array(val: &Value, len: usize) -> Result<ArrayRef> {
    match val {
        Value::Int(i) => {
            let arr: Int64Array = (0..len).map(|_| Some(*i)).collect();
            Ok(Arc::new(arr) as ArrayRef)
        }
        Value::Float(f) => {
            let arr: Float64Array = (0..len).map(|_| Some(*f)).collect();
            Ok(Arc::new(arr) as ArrayRef)
        }
        Value::Str(s) => {
            let arr: StringArray = (0..len).map(|_| Some(s.as_str())).collect();
            Ok(Arc::new(arr) as ArrayRef)
        }
        Value::Bool(b) => {
            let arr: BooleanArray = (0..len).map(|_| Some(*b)).collect();
            Ok(Arc::new(arr) as ArrayRef)
        }
        Value::Null => {
            let arr: Int64Array = (0..len).map(|_| None).collect();
            Ok(Arc::new(arr) as ArrayRef)
        }
    }
}

fn apply_binop(op: &BinOp, l: &ArrayRef, r: &ArrayRef) -> Result<ArrayRef> {
    use arrow::compute::kernels::cmp as kcmp;
    use arrow::datatypes::DataType;

    // Int64 と Float64 の混在は Int64 側を Float64 に揃える(arrow は暗黙変換しない)
    let (l, r) = match (l.data_type(), r.data_type()) {
        (DataType::Int64, DataType::Float64) => (&compute::cast(l, &DataType::Float64)?, r),
        (DataType::Float64, DataType::Int64) => (l, &compute::cast(r, &DataType::Float64)?),
        _ => (l, r),
    };

    match op {
        // Arithmetic
        BinOp::Add => Ok(numeric_kernels::add(l, r)?),
        BinOp::Sub => Ok(numeric_kernels::sub(l, r)?),
        BinOp::Mul => Ok(numeric_kernels::mul(l, r)?),
        BinOp::Div => Ok(numeric_kernels::div(l, r)?),

        // Comparison — returns BooleanArray
        BinOp::Eq => Ok(Arc::new(kcmp::eq(l, r)?) as ArrayRef),
        BinOp::Ne => Ok(Arc::new(kcmp::neq(l, r)?) as ArrayRef),
        BinOp::Lt => Ok(Arc::new(kcmp::lt(l, r)?) as ArrayRef),
        BinOp::Le => Ok(Arc::new(kcmp::lt_eq(l, r)?) as ArrayRef),
        BinOp::Gt => Ok(Arc::new(kcmp::gt(l, r)?) as ArrayRef),
        BinOp::Ge => Ok(Arc::new(kcmp::gt_eq(l, r)?) as ArrayRef),

        // Logical
        BinOp::And => {
            let la = l
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("AND requires boolean lhs"))?;
            let ra = r
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("AND requires boolean rhs"))?;
            Ok(Arc::new(compute::and(la, ra)?) as ArrayRef)
        }
        BinOp::Or => {
            let la = l
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("OR requires boolean lhs"))?;
            let ra = r
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("OR requires boolean rhs"))?;
            Ok(Arc::new(compute::or(la, ra)?) as ArrayRef)
        }
    }
}

// ── Plan → Executor dispatch ──────────────────────────────────────────────────

use crate::planner::logical::LogicalPlan;

pub fn build_executor(plan: LogicalPlan) -> Result<Box<dyn Executor>> {
    match plan {
        LogicalPlan::Scan { path, schema } => {
            Ok(Box::new(csv_scan::CsvScanExec::new(&path, schema)?))
        }
        LogicalPlan::Filter { input, predicate } => {
            let child = build_executor(*input)?;
            Ok(Box::new(filter::FilterExec::new(child, predicate)))
        }
        LogicalPlan::Project { input, exprs } => {
            let child = build_executor(*input)?;
            Ok(Box::new(project::ProjectExec::new(child, exprs)?))
        }
        LogicalPlan::Limit { input, n } => {
            let child = build_executor(*input)?;
            Ok(Box::new(limit::LimitExec::new(child, n)))
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
        } => {
            let child = build_executor(*input)?;
            Ok(Box::new(hash_agg::HashAggExec::new(child, group_by, aggs)?))
        }
        LogicalPlan::Join {
            left,
            right_path,
            right_schema,
            on,
            ..
        } => {
            let left_exec = build_executor(*left)?;
            let right_exec = Box::new(csv_scan::CsvScanExec::new(&right_path, right_schema)?);
            Ok(Box::new(join::HashJoinExec::new(
                left_exec, right_exec, &on,
            )?))
        }
        LogicalPlan::Sort { input, order_by } => {
            let child = build_executor(*input)?;
            Ok(Box::new(sort::SortExec::new(child, order_by)?))
        }
    }
}

#[cfg(test)]
mod binop_mixed_type_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Float64, false),
            Field::new("qty", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(vec![5.5, 10.0, 20.25])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn bin(op: BinOp, l: Expr, r: Expr) -> Expr {
        Expr::BinOp {
            op,
            left: Box::new(l),
            right: Box::new(r),
        }
    }

    fn col(n: &str) -> Expr {
        Expr::Column(n.to_string())
    }

    #[test]
    fn float_column_gt_int_literal() {
        let e = bin(BinOp::Gt, col("price"), Expr::Literal(Value::Int(10)));
        let got = evaluate_bool(&batch(), &e).unwrap();
        assert_eq!(got, BooleanArray::from(vec![false, false, true]));
    }

    #[test]
    fn int_column_lt_float_literal() {
        let e = bin(BinOp::Lt, col("qty"), Expr::Literal(Value::Float(1.5)));
        let got = evaluate_bool(&batch(), &e).unwrap();
        assert_eq!(got, BooleanArray::from(vec![true, false, false]));
    }

    #[test]
    fn float_column_times_int_literal() {
        let e = bin(BinOp::Mul, col("price"), Expr::Literal(Value::Int(2)));
        let got = evaluate(&batch(), &e).unwrap();
        let got = got.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(got, &Float64Array::from(vec![11.0, 20.0, 40.5]));
    }

    #[test]
    fn float_column_plus_int_column() {
        let e = bin(BinOp::Add, col("price"), col("qty"));
        let got = evaluate(&batch(), &e).unwrap();
        let got = got.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(got, &Float64Array::from(vec![6.5, 12.0, 23.25]));
    }
}

#[cfg(test)]
mod neg_tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};

    fn neg(e: Expr) -> Expr {
        Expr::Unary {
            op: UnaryOp::Neg,
            expr: Box::new(e),
        }
    }

    fn batch_of(col: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            col.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    #[test]
    fn neg_i64_min_is_overflow_error() {
        let b = batch_of(Arc::new(Int64Array::from(vec![1, i64::MIN])) as ArrayRef);
        let err = evaluate(&b, &neg(Expr::Column("v".into()))).unwrap_err();
        assert!(err.to_string().contains("integer overflow"), "{err}");
    }

    #[test]
    fn neg_i64_with_null_and_normal_values() {
        let b = batch_of(Arc::new(Int64Array::from(vec![Some(5), None, Some(-3)])) as ArrayRef);
        let got = evaluate(&b, &neg(Expr::Column("v".into()))).unwrap();
        let got = got.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(got, &Int64Array::from(vec![Some(-5), None, Some(3)]));
    }

    #[test]
    fn neg_f64_unchanged() {
        let b =
            batch_of(Arc::new(Float64Array::from(vec![Some(1.5), None, Some(-2.0)])) as ArrayRef);
        let got = evaluate(&b, &neg(Expr::Column("v".into()))).unwrap();
        let got = got.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(got, &Float64Array::from(vec![Some(-1.5), None, Some(2.0)]));
    }
}
