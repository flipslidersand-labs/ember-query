use anyhow::{anyhow, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

use super::{evaluate, scalar_key, Executor, ScalarKey};
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
    left: Box<dyn Executor>,
    left_col: String,
    /// Whole build (right) side as one batch
    right_all: RecordBatch,
    build_map: HashMap<ScalarKey, Vec<u32>>,
    /// Probe position in the current left batch
    cur: Option<Probe>,
    done: bool,
}

/// Upper bound on rows per output batch, so a duplicate-key fan-out is
/// produced incrementally instead of all at once (#133).
const MAX_OUTPUT_ROWS: usize = 8192;

struct Probe {
    batch: RecordBatch,
    probe: ArrayRef,
    row: usize,
    /// Offset into the matches of `row` already emitted
    m_off: usize,
}

impl HashJoinExec {
    pub fn new(left: Box<dyn Executor>, mut right: Box<dyn Executor>, on: &Expr) -> Result<Self> {
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

        // Build side: collect the right input into one batch (input-proportional).
        let right_schema = right.schema();
        let mut right_batches: Vec<RecordBatch> = Vec::new();
        while let Some(rb) = right.next()? {
            right_batches.push(rb);
        }
        let right_all = arrow::compute::concat_batches(&right_schema, &right_batches)?;
        drop(right_batches);

        // Build hash map: typed key → right row indices (in order). NULL keys are skipped.
        let mut build_map: HashMap<ScalarKey, Vec<u32>> = HashMap::new();
        if right_all.num_rows() > 0 {
            let key_arr = eval_column_str(&right_all, &right_col)?;
            for r_idx in 0..right_all.num_rows() {
                if let Some(key) = join_key(&key_arr, r_idx)? {
                    build_map
                        .entry(key)
                        .or_default()
                        .push(u32::try_from(r_idx)?);
                }
            }
        }

        // Build merged schema (left || right, with "right." prefix on name conflicts)
        let schema = Arc::new(merge_schemas(&left.schema(), &right_schema));

        Ok(HashJoinExec {
            schema,
            left,
            left_col,
            right_all,
            build_map,
            cur: None,
            done: false,
        })
    }

    /// Load the next left batch that has rows. Returns false at end of input.
    fn advance_left(&mut self) -> Result<bool> {
        while let Some(batch) = self.left.next()? {
            if batch.num_rows() == 0 {
                continue;
            }
            let probe = eval_column_str(&batch, &self.left_col)?;
            self.cur = Some(Probe {
                batch,
                probe,
                row: 0,
                m_off: 0,
            });
            return Ok(true);
        }
        Ok(false)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        while !self.done {
            if self.cur.is_none() && !self.advance_left()? {
                self.done = true;
                break;
            }
            if let Some(b) = self.emit()? {
                return Ok(Some(b));
            }
        }
        Ok(None)
    }

    /// Build at most MAX_OUTPUT_ROWS rows from the current left batch.
    fn emit(&mut self) -> Result<Option<RecordBatch>> {
        let Some(cur) = self.cur.as_mut() else {
            return Ok(None);
        };
        let mut left_idx: Vec<u32> = Vec::new();
        let mut right_idx: Vec<u32> = Vec::new();
        let n_rows = cur.batch.num_rows();
        while cur.row < n_rows && left_idx.len() < MAX_OUTPUT_ROWS {
            let matches = match join_key(&cur.probe, cur.row)? {
                Some(key) => self.build_map.get(&key),
                None => None, // NULL never matches
            };
            if let Some(m) = matches {
                let take = (m.len() - cur.m_off).min(MAX_OUTPUT_ROWS - left_idx.len());
                for &r in &m[cur.m_off..cur.m_off + take] {
                    left_idx.push(u32::try_from(cur.row)?);
                    right_idx.push(r);
                }
                cur.m_off += take;
                if cur.m_off < m.len() {
                    break; // output full mid-row; resume here
                }
            }
            cur.row += 1;
            cur.m_off = 0;
        }
        let exhausted = cur.row >= n_rows;
        let result = if left_idx.is_empty() {
            None
        } else {
            let li = arrow::array::UInt32Array::from(left_idx);
            let ri = arrow::array::UInt32Array::from(right_idx);
            let mut columns: Vec<ArrayRef> = Vec::new();
            for col in cur.batch.columns() {
                columns.push(arrow::compute::take(col.as_ref(), &li, None)?);
            }
            for col in self.right_all.columns() {
                columns.push(arrow::compute::take(col.as_ref(), &ri, None)?);
            }
            Some(RecordBatch::try_new(self.schema.clone(), columns)?)
        };
        if exhausted {
            self.cur = None;
        }
        Ok(result)
    }
}

impl Executor for HashJoinExec {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        let result = self.next_batch();
        if result.is_err() {
            // An error ends the stream; a later call must not resume.
            self.done = true;
            self.cur = None;
        }
        result
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

/// Returns `None` for NULL (SQL: NULL matches nothing).
fn join_key(arr: &ArrayRef, idx: usize) -> Result<Option<ScalarKey>> {
    Ok(scalar_key(arr, idx)?.map(|k| match k {
        // Int64 = Float64 は Float64 に昇格して比較する(WHERE と同じ規則, #130)。
        // ただし 2^53 超などで f64 に正確に表せない Int64 は別の値と同一視
        // されないよう Int のまま持つ(Float とは一致しない)。
        ScalarKey::Int(i) => {
            let f = i as f64;
            // f < 2^63: i64::MAX は f64 で 2^63 に丸まり `as i64` が飽和するため除外
            let lim = 2f64.powi(63);
            if (-lim..lim).contains(&f) && f as i64 == i {
                ScalarKey::Float(f.to_bits())
            } else {
                ScalarKey::Int(i)
            }
        }
        k => k,
    }))
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

#[cfg(test)]
mod key_tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};

    fn k(a: ArrayRef) -> Option<ScalarKey> {
        join_key(&a, 0).unwrap()
    }
    fn i(v: i64) -> ArrayRef {
        Arc::new(Int64Array::from(vec![v]))
    }
    fn f(v: f64) -> ArrayRef {
        Arc::new(Float64Array::from(vec![v]))
    }

    #[test]
    fn int_and_float_equal_values_match() {
        assert_eq!(k(i(1)), k(f(1.0)));
        assert_eq!(k(i(0)), k(f(-0.0)));
        assert_eq!(k(i(-5)), k(f(-5.0)));
        assert_ne!(k(i(1)), k(f(1.5)));
    }

    #[test]
    fn large_int_is_not_conflated_with_neighbours() {
        let big = 1_i64 << 53;
        // exactly representable: matches the equal float
        assert_eq!(k(i(big)), k(f(big as f64)));
        // not representable as f64: must not match the rounded float
        assert_ne!(k(i(big + 1)), k(f(big as f64)));
        assert_ne!(k(i(big + 1)), k(i(big)));
        assert_ne!(k(i(i64::MAX)), k(f(i64::MAX as f64)));
        assert_eq!(k(i(i64::MAX)), k(i(i64::MAX)));
    }

    #[test]
    fn other_types_stay_distinct_and_null_never_matches() {
        assert_ne!(k(Arc::new(StringArray::from(vec!["1"]))), k(i(1)));
        assert_ne!(k(Arc::new(BooleanArray::from(vec![true]))), k(i(1)));
        assert_eq!(k(f(f64::NAN)), k(f(-f64::NAN)));
        assert_eq!(k(Arc::new(Int64Array::from(vec![None::<i64>]))), None);
        assert_ne!(k(Arc::new(StringArray::from(vec!["NULL"]))), None);
    }
}
