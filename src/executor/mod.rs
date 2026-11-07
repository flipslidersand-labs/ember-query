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
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::sql::ast::{BinOp, Expr, UnaryOp, Value};

/// Executor trait — produces `RecordBatch`es one at a time.
pub trait Executor {
    fn next(&mut self) -> Result<Option<RecordBatch>>;
    fn schema(&self) -> SchemaRef;
}

// ── Column resolution ─────────────────────────────────────────────────────────

/// Whether two column references name the same column: equal, or one is
/// unqualified and equals the other's last segment (`t.col` ~ `col`).
/// `t1.col` and `t2.col` are different columns.
pub fn same_column(a: &str, b: &str) -> bool {
    let bare = |s: &str| s.rsplit('.').next().unwrap_or(s).to_string();
    a == b || (!a.contains('.') && a == bare(b)) || (!b.contains('.') && b == bare(a))
}

/// Field metadata keys recording a column's source table and bare name. Set
/// by the CSV scan; kept through JOIN renames (`right_<col>`) and plain
/// column projections.
pub const TABLE_KEY: &str = "table";
pub const COLUMN_KEY: &str = "column";

/// Tag `f` as column `column` of table `table`.
pub fn tag_field(f: Field, table: &str, column: &str) -> Field {
    f.with_metadata(std::collections::HashMap::from([
        (TABLE_KEY.to_string(), table.to_string()),
        (COLUMN_KEY.to_string(), column.to_string()),
    ]))
}

fn field_tag(f: &Field) -> Option<(&str, &str)> {
    let m = f.metadata();
    Some((m.get(TABLE_KEY)?.as_str(), m.get(COLUMN_KEY)?.as_str()))
}

/// A reference naming `f` unambiguously: `table.column` when tagged.
pub fn field_ref(f: &Field) -> String {
    field_tag(f).map_or_else(|| f.name().clone(), |(t, c)| format!("{t}.{c}"))
}

/// Candidates by source-table tag. `t.col` on a schema tagged with table `t`
/// matches only `t`'s `col`; a bare `col` matches every table's `col` plus an
/// exact field name. `None` (tags cannot decide: untagged schema, unknown
/// qualifier, or no tagged match for a bare name) falls back to name matching.
fn tagged_hits(schema: &Schema, name: &str) -> Option<Vec<usize>> {
    let tags: Vec<_> = schema.fields().iter().map(|f| field_tag(f)).collect();
    let (table, col) = match name.rsplit_once('.') {
        Some((t, c)) => (Some(t), c),
        None => (None, name),
    };
    if table.is_some() && !tags.iter().flatten().any(|(t, _)| Some(*t) == table) {
        return None;
    }
    let hits: Vec<usize> = (0..tags.len())
        .filter(|&i| match tags[i] {
            Some((t, c)) if c == col && table.is_none_or(|q| q == t) => true,
            _ => table.is_none() && schema.field(i).name() == name,
        })
        .collect();
    (table.is_some() || hits.iter().any(|&i| tags[i].is_some())).then_some(hits)
}

/// Resolve a (possibly qualified) column reference to a field index. Tagged
/// schemas resolve by (table, column) first (see `tagged_hits`); otherwise
/// exact name first, then a unique `same_column` match. Several matches
/// are an error rather than a silent pick. Without tags, a JOIN's
/// `right_<col>` rename means a qualified `t.col` that also has a
/// `right_col` could mean either table and is ambiguous too.
pub fn resolve_column(schema: &Schema, name: &str) -> Result<usize> {
    let names: Vec<&String> = schema.fields().iter().map(|f| f.name()).collect();
    let tagged = tagged_hits(schema, name);
    if tagged.is_none() {
        if let Ok(i) = schema.index_of(name) {
            return Ok(i);
        }
    }
    let mut hits: Vec<usize> = tagged.clone().unwrap_or_else(|| {
        (0..names.len())
            .filter(|&i| same_column(names[i], name))
            .collect()
    });
    let untagged_hit = tagged.is_none() && !hits.is_empty();
    if let (true, Some((_, bare))) = (untagged_hit, name.rsplit_once('.')) {
        hits.extend(schema.index_of(&format!("right_{bare}")).ok());
    }
    match hits.as_slice() {
        [i] => Ok(*i),
        [] => Err(anyhow!("column '{}' not found in schema {:?}", name, names)),
        _ => Err(anyhow!(
            "column '{}' is ambiguous: matches {:?}",
            name,
            hits.iter().map(|&i| names[i]).collect::<Vec<_>>()
        )),
    }
}

// ── Expression evaluation ─────────────────────────────────────────────────────

/// Evaluate an expression over a `RecordBatch`, returning an `ArrayRef`.
pub fn evaluate(batch: &RecordBatch, expr: &Expr) -> Result<ArrayRef> {
    match expr {
        Expr::Column(name) => {
            resolve_column(&batch.schema(), name).map(|i| batch.column(i).clone())
        }

        Expr::Literal(val) => make_scalar_array(val, batch.num_rows()),

        Expr::BinOp { op, left, right } => {
            let l = evaluate_operand(batch, left)?;
            let r = evaluate_operand(batch, right)?;
            apply_binop(op, &l, &r)
        }

        Expr::Unary { op, expr } => {
            let arr = evaluate_operand(batch, expr)?;
            match op {
                UnaryOp::Not => {
                    let arr = coerce_null(&arr, &DataType::Boolean)?;
                    let bool_arr = arr
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| anyhow!("NOT requires boolean array"))?;
                    Ok(Arc::new(compute::not(bool_arr)?) as ArrayRef)
                }
                UnaryOp::Neg => {
                    let arr = coerce_null(&arr, &DataType::Int64)?;
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
    } else if arr.null_count() == arr.len() && !matches!(arr.data_type(), DataType::Boolean) {
        // 全行 NULL の配列(NULL リテラル由来の Int64 など)は NULL の述語として扱い、全行を落とす
        Ok(BooleanArray::new_null(arr.len()))
    } else {
        Err(anyhow!(
            "predicate did not evaluate to boolean; got {:?}",
            arr.data_type()
        ))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// 演算子のオペランドを評価する。NULL リテラルだけは型未定(`DataType::Null`)のまま返し、
/// 演算側で相手の型に合わせる。単独の `SELECT NULL` は従来どおり Int64 の NULL 配列。
fn evaluate_operand(batch: &RecordBatch, expr: &Expr) -> Result<ArrayRef> {
    match expr {
        Expr::Literal(Value::Null) => Ok(arrow::array::new_null_array(
            &DataType::Null,
            batch.num_rows(),
        )),
        _ => evaluate(batch, expr),
    }
}

fn coerce_null(arr: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    if matches!(arr.data_type(), DataType::Null) {
        Ok(compute::cast(arr, target)?)
    } else {
        Ok(arr.clone())
    }
}

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

    // NULL リテラル(DataType::Null)は相手の型に揃える。論理演算は Boolean、両辺 NULL の算術・比較は Int64
    let (nl, nr) = (
        matches!(l.data_type(), DataType::Null),
        matches!(r.data_type(), DataType::Null),
    );
    let target = match op {
        BinOp::And | BinOp::Or => DataType::Boolean,
        _ if nl && !nr => r.data_type().clone(),
        _ if nr && !nl => l.data_type().clone(),
        _ => DataType::Int64,
    };
    let (l, r) = (&coerce_null(l, &target)?, &coerce_null(r, &target)?);

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
        BinOp::Div => {
            // arrow の Float 除算は inf / NaN を返す。Int(Divide by zero エラー)に揃える。
            // NULL の行は検出対象外(NULL のまま)。0.0 と -0.0 は `== 0.0` で両方一致。
            if let Some(d) = r.as_any().downcast_ref::<Float64Array>() {
                if d.iter().any(|v| v == Some(0.0)) {
                    return Err(arrow::error::ArrowError::DivideByZero.into());
                }
            }
            Ok(numeric_kernels::div(l, r)?)
        }

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
            Ok(Arc::new(compute::and_kleene(la, ra)?) as ArrayRef)
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
            Ok(Arc::new(compute::or_kleene(la, ra)?) as ArrayRef)
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

#[cfg(test)]
mod eval_semantics_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    const T: Option<bool> = Some(true);
    const F: Option<bool> = Some(false);
    const N: Option<bool> = None;

    /// a/b は 3x3 の全組合せ(true,false,NULL)。
    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("n", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(vec![T, T, T, F, F, F, N, N, N])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![T, F, N, T, F, N, T, F, N])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(0),
                    None,
                    Some(7),
                    Some(-4),
                    Some(2),
                    Some(3),
                    Some(5),
                    Some(9),
                ])) as ArrayRef,
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    Some(0.0),
                    None,
                    Some(1.0),
                    Some(0.0),
                    None,
                    Some(1.0),
                    Some(0.0),
                    None,
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("apple"),
                    Some("banana"),
                    None,
                    Some("apple"),
                    Some("banana"),
                    None,
                    Some("apple"),
                    Some("banana"),
                    None,
                ])) as ArrayRef,
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

    fn un(op: UnaryOp, e: Expr) -> Expr {
        Expr::Unary {
            op,
            expr: Box::new(e),
        }
    }

    fn col(n: &str) -> Expr {
        Expr::Column(n.to_string())
    }

    fn lit(v: Value) -> Expr {
        Expr::Literal(v)
    }

    fn bools(arr: &ArrayRef) -> Vec<Option<bool>> {
        arr.as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean array")
            .iter()
            .collect()
    }

    // ── NOT ──

    #[test]
    fn not_true_false_null() {
        let b = batch();
        let got = evaluate(&b, &un(UnaryOp::Not, col("a"))).unwrap();
        assert_eq!(bools(&got), vec![F, F, F, T, T, T, N, N, N]);
    }

    #[test]
    fn not_on_int_is_error() {
        let err = evaluate(&batch(), &un(UnaryOp::Not, col("n"))).unwrap_err();
        assert!(err.to_string().contains("NOT requires boolean"), "{err}");
    }

    #[test]
    fn not_null_literal_is_null() {
        let got = evaluate(&batch(), &un(UnaryOp::Not, lit(Value::Null))).unwrap();
        assert_eq!(bools(&got), vec![N; 9]);
    }

    // ── Neg(i64::MIN は neg_tests 側) ──

    #[test]
    fn neg_on_utf8_is_error() {
        let err = evaluate(&batch(), &un(UnaryOp::Neg, col("s"))).unwrap_err();
        assert!(err.to_string().contains("NEG requires numeric"), "{err}");
    }

    #[test]
    fn neg_on_bool_is_error() {
        assert!(evaluate(&batch(), &un(UnaryOp::Neg, col("a"))).is_err());
    }

    // ── Literal(Null) ──

    #[test]
    fn null_literal_is_all_null_with_batch_length() {
        let got = evaluate(&batch(), &lit(Value::Null)).unwrap();
        assert_eq!(got.len(), 9);
        assert_eq!(got.null_count(), 9);
    }

    #[test]
    fn null_propagates_through_arithmetic_and_comparison() {
        let b = batch();
        let add = evaluate(&b, &bin(BinOp::Add, col("n"), lit(Value::Int(1)))).unwrap();
        assert!(add.is_null(2));
        assert!(!add.is_null(0));
        let eq = evaluate(&b, &bin(BinOp::Eq, col("n"), lit(Value::Int(1)))).unwrap();
        assert_eq!(bools(&eq)[0], T);
        assert_eq!(bools(&eq)[2], N);
        // NULL リテラルとの比較は NULL
        let eq_null = evaluate(&b, &bin(BinOp::Eq, col("n"), lit(Value::Null))).unwrap();
        assert_eq!(bools(&eq_null), vec![N; 9]);
    }

    // ── AND / OR ──

    #[test]
    fn and_three_valued_logic_table() {
        let e = bin(BinOp::And, col("a"), col("b"));
        let got = evaluate(&batch(), &e).unwrap();
        // 期待: FALSE AND NULL = FALSE、NULL AND FALSE = FALSE(Kleene)
        assert_eq!(bools(&got), vec![T, F, N, F, F, F, N, F, N]);
    }

    #[test]
    fn or_three_valued_logic_table() {
        let e = bin(BinOp::Or, col("a"), col("b"));
        let got = evaluate(&batch(), &e).unwrap();
        // 期待: TRUE OR NULL = TRUE、NULL OR TRUE = TRUE(Kleene)
        assert_eq!(bools(&got), vec![T, T, T, T, F, N, T, N, N]);
    }

    #[test]
    fn and_or_null_rows_where_kleene_and_current_agree() {
        // Kleene と現状実装で結果が一致する NULL 行(NULL 伝播の回帰検出用)
        let b = batch();
        let and = bools(&evaluate(&b, &bin(BinOp::And, col("a"), col("b"))).unwrap());
        let or = bools(&evaluate(&b, &bin(BinOp::Or, col("a"), col("b"))).unwrap());
        // (a,b) = (T,N) (N,T) (N,N) の AND は NULL
        assert_eq!([and[2], and[6], and[8]], [N, N, N]);
        // (F,N) (N,F) (N,N) の OR は NULL
        assert_eq!([or[5], or[7], or[8]], [N, N, N]);
    }

    #[test]
    fn and_or_without_nulls_two_valued() {
        let b = batch();
        let and = evaluate(&b, &bin(BinOp::And, col("a"), col("b"))).unwrap();
        let or = evaluate(&b, &bin(BinOp::Or, col("a"), col("b"))).unwrap();
        // NULL を含まない行(0,1,3,4)だけ確認
        for (i, (ea, eo)) in [(0, (T, T)), (1, (F, T)), (3, (F, T)), (4, (F, F))] {
            assert_eq!(bools(&and)[i], ea, "AND row {i}");
            assert_eq!(bools(&or)[i], eo, "OR row {i}");
        }
    }

    #[test]
    fn and_or_on_non_boolean_is_error() {
        let b = batch();
        for op in [BinOp::And, BinOp::Or] {
            let lhs = evaluate(&b, &bin(op.clone(), col("n"), col("a"))).unwrap_err();
            assert!(lhs.to_string().contains("requires boolean lhs"), "{lhs}");
            let rhs = evaluate(&b, &bin(op, col("a"), col("n"))).unwrap_err();
            assert!(rhs.to_string().contains("requires boolean rhs"), "{rhs}");
        }
    }

    #[test]
    fn and_or_with_null_literal() {
        let b = batch();
        // TRUE AND NULL = NULL, FALSE AND NULL = FALSE
        let and = evaluate(&b, &bin(BinOp::And, col("a"), lit(Value::Null))).unwrap();
        assert_eq!(bools(&and), vec![N, N, N, F, F, F, N, N, N]);
        // TRUE OR NULL = TRUE, FALSE OR NULL = NULL
        let or = evaluate(&b, &bin(BinOp::Or, col("a"), lit(Value::Null))).unwrap();
        assert_eq!(bools(&or), vec![T, T, T, N, N, N, N, N, N]);
    }

    // ── ゼロ除算 ──

    #[test]
    fn int_division_by_zero_is_error() {
        let e = bin(BinOp::Div, lit(Value::Int(1)), lit(Value::Int(0)));
        let err = evaluate(&batch(), &e).unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("divide by zero"),
            "{err}"
        );
    }

    #[test]
    fn int_column_division_by_zero_row_is_error() {
        // n = [1, 0, ...] なので 1 / n の 2 行目でゼロ除算
        let e = bin(BinOp::Div, lit(Value::Int(1)), col("n"));
        assert!(evaluate(&batch(), &e).is_err());
    }

    #[test]
    fn float_division_by_zero_is_not_inf() {
        let e = bin(BinOp::Div, lit(Value::Float(1.0)), lit(Value::Float(0.0)));
        match evaluate(&batch(), &e) {
            Err(_) => {}
            Ok(arr) => {
                let f = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                assert!(f.iter().all(|v| v.is_none_or(|x| x.is_finite())));
            }
        }
    }

    fn div_f(l: Float64Array, r: Float64Array) -> Result<ArrayRef> {
        let b = RecordBatch::try_from_iter([
            ("l", Arc::new(l) as ArrayRef),
            ("r", Arc::new(r) as ArrayRef),
        ])
        .unwrap();
        evaluate(&b, &bin(BinOp::Div, col("l"), col("r")))
    }

    #[test]
    fn float_division_by_zero_matches_int_error() {
        for z in [0.0, -0.0] {
            let err =
                div_f(Float64Array::from(vec![1.0]), Float64Array::from(vec![z])).unwrap_err();
            assert!(
                err.to_string().to_lowercase().contains("divide by zero"),
                "{err}"
            );
        }
        // 0.0 / 0.0 (NaN になる場合)も同様
        assert!(div_f(Float64Array::from(vec![0.0]), Float64Array::from(vec![0.0])).is_err());
        // 行の途中のゼロ
        assert!(div_f(
            Float64Array::from(vec![1.0, 2.0]),
            Float64Array::from(vec![2.0, 0.0])
        )
        .is_err());
    }

    #[test]
    fn mixed_int_float_division_by_zero_is_error() {
        // Int 列 / Float 0.0 (昇格後)
        let e = bin(BinOp::Div, col("n"), lit(Value::Float(0.0)));
        assert!(evaluate(&batch(), &e).is_err());
        // Float / Int 0
        let e = bin(BinOp::Div, lit(Value::Float(1.0)), lit(Value::Int(0)));
        assert!(evaluate(&batch(), &e).is_err());
    }

    #[test]
    fn float_division_null_divisor_stays_null() {
        // 除数 NULL の行はエラーにせず NULL。非ゼロ除数は通常どおり
        let got = div_f(
            Float64Array::from(vec![Some(1.0), Some(2.0), None]),
            Float64Array::from(vec![Some(2.0), None, Some(4.0)]),
        )
        .unwrap();
        let f = got.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(f, &Float64Array::from(vec![Some(0.5), None, None]));
    }

    // ── 文字列比較 ──

    #[test]
    fn utf8_eq_and_lt() {
        let b = batch();
        let eq = evaluate(
            &b,
            &bin(BinOp::Eq, col("s"), lit(Value::Str("apple".into()))),
        )
        .unwrap();
        assert_eq!(bools(&eq), vec![T, F, N, T, F, N, T, F, N]);
        let lt = evaluate(&b, &bin(BinOp::Lt, col("s"), lit(Value::Str("b".into())))).unwrap();
        assert_eq!(bools(&lt), vec![T, F, N, T, F, N, T, F, N]);
        // 境界: "banana" < "banana" は false(Le への変異を検出)
        let lt_eq_val = evaluate(
            &b,
            &bin(BinOp::Lt, col("s"), lit(Value::Str("banana".into()))),
        )
        .unwrap();
        assert_eq!(bools(&lt_eq_val), vec![T, F, N, T, F, N, T, F, N]);
        let ne = evaluate(
            &b,
            &bin(BinOp::Ne, col("s"), lit(Value::Str("apple".into()))),
        )
        .unwrap();
        assert_eq!(bools(&ne), vec![F, T, N, F, T, N, F, T, N]);
    }

    #[test]
    fn int_column_eq_string_literal_is_error() {
        let e = bin(BinOp::Eq, col("n"), lit(Value::Str("1".into())));
        assert!(evaluate(&batch(), &e).is_err());
    }

    #[test]
    fn string_column_lt_int_literal_is_error() {
        let e = bin(BinOp::Lt, col("s"), lit(Value::Int(1)));
        assert!(evaluate(&batch(), &e).is_err());
    }

    // ── 未知カラム ──

    #[test]
    fn unknown_column_error_message_lists_schema() {
        let err = evaluate(&batch(), &col("nope")).unwrap_err().to_string();
        assert!(err.contains("column 'nope' not found"), "{err}");
        for name in ["a", "b", "n", "f", "s"] {
            assert!(err.contains(&format!("\"{name}\"")), "{err}");
        }
    }

    #[test]
    fn unknown_column_nested_in_binop_propagates() {
        let e = bin(BinOp::Add, col("n"), col("nope"));
        let err = evaluate(&batch(), &e).unwrap_err().to_string();
        assert!(err.contains("column 'nope' not found"), "{err}");
    }

    // ── evaluate_bool ──

    #[test]
    fn evaluate_bool_rejects_non_boolean() {
        let err = evaluate_bool(&batch(), &col("n")).unwrap_err();
        assert!(
            err.to_string().contains("did not evaluate to boolean"),
            "{err}"
        );
    }

    #[test]
    fn evaluate_bool_treats_all_null_int_as_null_boolean() {
        let b = evaluate_bool(&batch(), &Expr::Literal(Value::Null)).unwrap();
        let a: ArrayRef = Arc::new(b);
        assert_eq!(a.len(), batch().num_rows());
        assert_eq!(a.null_count(), a.len());
    }
}

#[cfg(test)]
mod resolve_column_tests {
    use super::{resolve_column, same_column};
    use arrow::datatypes::{DataType, Field, Schema};

    fn schema(names: &[&str]) -> Schema {
        Schema::new(
            names
                .iter()
                .map(|n| Field::new(*n, DataType::Int64, true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn same_column_matches_qualifier_against_bare_only() {
        assert!(same_column("id", "id"));
        assert!(same_column("t.id", "id"));
        assert!(same_column("id", "t.id"));
        assert!(!same_column("t1.id", "t2.id"));
        assert!(!same_column("right_id", "t.id"));
    }

    #[test]
    fn resolves_exact_then_last_segment() {
        let s = schema(&["id", "t.qty"]);
        assert_eq!(resolve_column(&s, "id").unwrap(), 0);
        assert_eq!(resolve_column(&s, "orders.id").unwrap(), 0);
        assert_eq!(resolve_column(&s, "qty").unwrap(), 1);
        assert_eq!(resolve_column(&s, "t.qty").unwrap(), 1);
        let err = resolve_column(&s, "orders.nope").unwrap_err().to_string();
        assert!(err.contains("column 'orders.nope' not found"), "{err}");
    }

    #[test]
    fn ambiguous_match_is_an_error() {
        let s = schema(&["a.id", "b.id"]);
        let err = resolve_column(&s, "id").unwrap_err().to_string();
        assert!(err.contains("ambiguous"), "{err}");
        assert_eq!(resolve_column(&s, "b.id").unwrap(), 1);
    }

    #[test]
    fn qualified_name_colliding_with_join_rename_resolves_by_table_tag() {
        let f = |n: &str, t: &str, c: &str| {
            super::tag_field(Field::new(n, DataType::Int64, true), t, c)
        };
        let s = Schema::new(vec![
            f("id", "t1", "id"),
            f("name", "t1", "name"),
            f("right_id", "t2", "id"),
            f("t1_id", "t2", "t1_id"),
        ]);
        assert_eq!(resolve_column(&s, "t2.id").unwrap(), 2);
        assert_eq!(resolve_column(&s, "t1.id").unwrap(), 0);
        assert_eq!(resolve_column(&s, "right_id").unwrap(), 2);
        assert_eq!(resolve_column(&s, "t2.t1_id").unwrap(), 3);
        assert_eq!(resolve_column(&s, "t1_id").unwrap(), 3);
        let err = resolve_column(&s, "id").unwrap_err().to_string();
        assert!(err.contains("ambiguous"), "{err}");
        // A known table lacking the column never falls back to another table's.
        let err = resolve_column(&s, "t2.name").unwrap_err().to_string();
        assert!(err.contains("not found"), "{err}");
        // Untagged schemas keep the conservative fallback.
        let untagged = schema(&["id", "right_id"]);
        let err = resolve_column(&untagged, "t2.id").unwrap_err().to_string();
        assert!(err.contains("ambiguous"), "{err}");
        // A lone `right_x` column is not a fallback target for `t.x`.
        let lone = schema(&["right_x"]);
        assert!(resolve_column(&lone, "t.x").is_err());
    }
}
