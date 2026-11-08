use anyhow::{anyhow, Result};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

use super::{evaluate, Executor};
use crate::sql::ast::Expr;

/// Hash inner join executor.
///
/// Builds a hash map from the right (probe) side keyed by a hash of all right-side
/// values in the ON equality column, then probes with each left row.
///
/// For Phase 5 the ON expr must be `left_col = right_col`.
pub struct HashJoinExec {
    /// Merged schema: left fields then right fields (with table prefix on collision)
    schema: SchemaRef,
    /// All joined batches (materialised)
    batches: Vec<RecordBatch>,
    cursor: usize,
    done: bool,
}

impl HashJoinExec {
    pub fn new(
        mut left: Box<dyn Executor>,
        mut right: Box<dyn Executor>,
        on: &Expr,
    ) -> Result<Self> {
        // Extract equality columns from the ON expression
        let (mut left_col, mut right_col) = extract_eq_columns(on).ok_or_else(|| {
            anyhow!("JOIN ON must be an equality expression (left_col = right_col)")
        })?;
        // Orient the ON sides by table tag: `b.id = a.v` probes with `a.v`.
        // A side naming only the other input's table must never fall back to
        // name matching there (a self-JOIN would silently swap columns, #92).
        let (ls, rs) = (left.schema(), right.schema());
        let misplaced = |l: &str, r: &str| {
            let only = |c: &str, here: &Schema, there: &Schema| {
                tagged_in(here, c) == Some(true) && tagged_in(there, c) == Some(false)
            };
            only(l, &rs, &ls) || only(r, &ls, &rs)
        };
        if misplaced(&left_col, &right_col) {
            std::mem::swap(&mut left_col, &mut right_col);
            if misplaced(&left_col, &right_col) {
                return Err(anyhow!(
                    "JOIN ON '{right_col} = {left_col}' must compare a column of each side"
                ));
            }
        }

        // Fail early with both schemas so a reversed or misspelled ON is diagnosable (#102).
        let names = |s: &Schema| {
            s.fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        };
        for (col, schema) in [(&left_col, &ls), (&right_col, &rs)] {
            if let Err(e) = crate::executor::resolve_column(schema, col) {
                if !e.to_string().contains("not found") {
                    return Err(e);
                }
                return Err(anyhow!(
                    "column '{col}' not found in JOIN ON: left schema {:?}, right schema {:?}",
                    names(&ls),
                    names(&rs)
                ));
            }
        }

        // Collect all right batches into a hash map: right_key_value → [row_index, batch_index]
        let mut right_batches: Vec<RecordBatch> = Vec::new();
        while let Some(rb) = right.next()? {
            right_batches.push(rb);
        }

        // Build hash map: typed key → Vec<(batch_idx, row_idx)>. NULL keys are skipped.
        let mut build_map: HashMap<JoinKey, Vec<(usize, usize)>> = HashMap::new();
        for (b_idx, batch) in right_batches.iter().enumerate() {
            let key_arr = eval_column_str(batch, &right_col)?;
            for r_idx in 0..batch.num_rows() {
                if let Some(key) = join_key(&key_arr, r_idx)? {
                    build_map.entry(key).or_default().push((b_idx, r_idx));
                }
            }
        }

        // Build merged schema (left || right, with "right." prefix on name conflicts)
        let left_schema = left.schema();
        let right_schema = right.schema();
        let merged_schema = merge_schemas(&left_schema, &right_schema);
        let schema = Arc::new(merged_schema);

        // Probe: for each left row, find matching right rows and emit merged rows
        let mut result_batches: Vec<RecordBatch> = Vec::new();
        while let Some(left_batch) = left.next()? {
            let probe_arr = eval_column_str(&left_batch, &left_col)?;
            let mut matched_left_indices: Vec<usize> = Vec::new();
            let mut matched_right_batch_indices: Vec<usize> = Vec::new();
            let mut matched_right_row_indices: Vec<usize> = Vec::new();

            for l_row in 0..left_batch.num_rows() {
                let Some(key) = join_key(&probe_arr, l_row)? else {
                    continue; // NULL never matches
                };
                if let Some(matches) = build_map.get(&key) {
                    for &(rb_idx, rr_idx) in matches {
                        matched_left_indices.push(l_row);
                        matched_right_batch_indices.push(rb_idx);
                        matched_right_row_indices.push(rr_idx);
                    }
                }
            }

            if matched_left_indices.is_empty() {
                continue;
            }

            // Build output batch by taking matching rows
            let n = matched_left_indices.len();
            let mut columns: Vec<ArrayRef> = Vec::new();

            // Left columns
            for col in left_batch.columns() {
                let selected: Vec<ArrayRef> = matched_left_indices
                    .iter()
                    .map(|&idx| col.slice(idx, 1))
                    .collect();
                let refs: Vec<&dyn Array> = selected.iter().map(|a| a.as_ref()).collect();
                columns.push(arrow::compute::concat(&refs)?);
            }

            // Right columns (from potentially different batches)
            for col_idx in 0..right_batches[0].num_columns() {
                let mut parts: Vec<ArrayRef> = Vec::new();
                for i in 0..n {
                    let rb = &right_batches[matched_right_batch_indices[i]];
                    let row = matched_right_row_indices[i];
                    parts.push(rb.column(col_idx).slice(row, 1));
                }
                let refs: Vec<&dyn Array> = parts.iter().map(|a| a.as_ref()).collect();
                columns.push(arrow::compute::concat(&refs)?);
            }

            result_batches.push(RecordBatch::try_new(schema.clone(), columns)?);
        }

        Ok(HashJoinExec {
            schema,
            batches: result_batches,
            cursor: 0,
            done: false,
        })
    }
}

impl Executor for HashJoinExec {
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

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Extract (left_col, right_col) from `left_col = right_col` ON expression.
fn extract_eq_columns(on: &Expr) -> Option<(String, String)> {
    if let Expr::BinOp {
        op: crate::sql::ast::BinOp::Eq,
        left,
        right,
    } = on
    {
        let l = column_name(left)?;
        let r = column_name(right)?;
        Some((l, r))
    } else {
        None
    }
}

/// Whether `col`'s qualifier is a table tag of `schema`; `None` if unqualified.
fn tagged_in(schema: &Schema, col: &str) -> Option<bool> {
    let (q, _) = col.rsplit_once('.')?;
    let tag = |f: &Arc<Field>| f.metadata().get(super::TABLE_KEY).cloned();
    Some(schema.fields().iter().any(|f| tag(f).as_deref() == Some(q)))
}

fn column_name(expr: &Expr) -> Option<String> {
    if let Expr::Column(name) = expr {
        Some(name.clone())
    } else {
        None
    }
}

/// Evaluate a join key column (possibly qualified like "t1.col").
fn eval_column_str(batch: &RecordBatch, col: &str) -> Result<ArrayRef> {
    evaluate(batch, &Expr::Column(col.to_string()))
}

/// Typed join key: values of different types never compare equal.
#[derive(Debug, PartialEq, Eq, Hash)]
enum JoinKey {
    Int(i64),
    /// Normalised bits (-0.0 → 0.0, all NaNs → one NaN)
    Float(u64),
    Str(String),
    Bool(bool),
}

/// Returns `None` for NULL (SQL: NULL matches nothing).
fn join_key(arr: &ArrayRef, idx: usize) -> Result<Option<JoinKey>> {
    use arrow::array::*;
    if arr.is_null(idx) {
        return Ok(None);
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Ok(Some(JoinKey::Int(a.value(idx))));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        let v = a.value(idx);
        let v = if v.is_nan() {
            f64::NAN
        } else if v == 0.0 {
            0.0
        } else {
            v
        };
        return Ok(Some(JoinKey::Float(v.to_bits())));
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(JoinKey::Str(a.value(idx).to_string())));
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Ok(Some(JoinKey::Bool(a.value(idx))));
    }
    Err(anyhow!("unsupported JOIN key type: {:?}", arr.data_type()))
}

/// Output schema of a JOIN. A right column whose name is taken gets a
/// `right_` prefix; if that is taken too (3+ table JOINs, #101) the prefix
/// becomes `right2_`, `right3_`, ... so output names stay unique. The
/// (table, column) tag is kept so `t.col` still resolves (#40).
fn merge_schemas(left: &Schema, right: &Schema) -> Schema {
    let mut used: std::collections::HashSet<String> =
        left.fields().iter().map(|f| f.name().clone()).collect();
    let mut fields: Vec<Field> = left.fields().iter().map(|f| f.as_ref().clone()).collect();
    for f in right.fields() {
        if !used.contains(f.name().as_str()) {
            used.insert(f.name().clone());
            fields.push(f.as_ref().clone());
            continue;
        }
        let name = std::iter::once("right_".to_string())
            .chain((2..).map(|n| format!("right{n}_")))
            .map(|p| format!("{p}{}", f.name()))
            .find(|n| !used.contains(n))
            .expect("unbounded iterator");
        used.insert(name.clone());
        fields.push(
            Field::new(name, f.data_type().clone(), f.is_nullable())
                .with_metadata(f.metadata().clone()),
        );
    }
    Schema::new(fields)
}

// suppress unused import warning for evaluate_bool
#[allow(unused_imports)]
use super::evaluate_bool as _;
