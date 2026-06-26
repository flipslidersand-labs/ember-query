pub mod csv_scan;
pub mod filter;
pub mod hash_agg;
pub mod limit;
pub mod project;

use anyhow::{anyhow, Result};
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
};
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
        Expr::Column(name) => {
            batch
                .schema()
                .index_of(name)
                .map(|i| batch.column(i).clone())
                .map_err(|_| anyhow!("column '{}' not found in schema {:?}", name, batch.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>()))
        }

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
                        let negated: Int64Array = i64_arr.iter().map(|v| v.map(|x| -x)).collect();
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
    }
}
