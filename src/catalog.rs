use anyhow::Result;
use arrow::datatypes::DataType;
use std::path::Path;

/// Column metadata.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

/// Schema inferred from a CSV file's header + sample values.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnDef>,
}

/// Infer the schema of a CSV file by reading the header row and a sample of
/// data rows.  Type priority: i64 → f64 → String.
pub fn infer_schema(path: &Path) -> Result<TableSchema> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)?;

    let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();

    // Scan every row once, keeping only per-column flags (no records retained),
    // so the inferred type always matches what the scan will parse.
    let mut states = vec![ColState::default(); headers.len()];
    for result in reader.records() {
        let record = result?;
        if record.len() > headers.len() {
            anyhow::bail!(
                "record has {} fields, expected at most {}",
                record.len(),
                headers.len()
            );
        }
        for (state, val) in states.iter_mut().zip(record.iter()) {
            state.observe(val);
        }
    }

    let mut columns = Vec::with_capacity(headers.len());
    for (name, state) in headers.iter().zip(states.iter()) {
        columns.push(ColumnDef {
            name: name.clone(),
            data_type: state.data_type(),
        });
    }

    Ok(TableSchema { columns })
}

/// Bool として受理する表記は `true`/`false`(大文字小文字不問)のみ。型推論とスキャンで共通。
pub(crate) fn parse_bool_str(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn is_strict_bool_str(s: &str) -> bool {
    parse_bool_str(s).is_some()
}

#[derive(Clone)]
struct ColState {
    can_be_int: bool,
    can_be_float: bool,
    can_be_bool: bool,
    has_non_empty: bool,
}

impl Default for ColState {
    fn default() -> Self {
        ColState {
            can_be_int: true,
            can_be_float: true,
            can_be_bool: true,
            has_non_empty: false,
        }
    }
}

impl ColState {
    fn observe(&mut self, val: &str) {
        if val.is_empty() {
            return; // null — doesn't constrain type
        }
        self.has_non_empty = true;
        if self.can_be_bool && !is_strict_bool_str(val) {
            self.can_be_bool = false;
        }
        if self.can_be_int && val.parse::<i64>().is_err() {
            self.can_be_int = false;
        }
        if self.can_be_float && val.parse::<f64>().is_err() {
            self.can_be_float = false;
        }
    }

    fn data_type(&self) -> DataType {
        if !self.has_non_empty {
            return DataType::Utf8;
        }
        // Bool takes priority over int (1/0 values would pass int check but prefer bool if only bools present)
        if self.can_be_bool {
            DataType::Boolean
        } else if self.can_be_int {
            DataType::Int64
        } else if self.can_be_float {
            DataType::Float64
        } else {
            DataType::Utf8
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TmpCsv(PathBuf);

    impl TmpCsv {
        fn new(content: &str) -> Self {
            static N: AtomicUsize = AtomicUsize::new(0);
            let p = std::env::temp_dir().join(format!(
                "ember_query_catalog_{}_{}.csv",
                std::process::id(),
                N.fetch_add(1, Ordering::SeqCst)
            ));
            std::fs::write(&p, content).unwrap();
            TmpCsv(p)
        }
    }

    impl Drop for TmpCsv {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn types(content: &str) -> Vec<DataType> {
        let f = TmpCsv::new(content);
        infer_schema(&f.0)
            .unwrap()
            .columns
            .into_iter()
            .map(|c| c.data_type)
            .collect()
    }

    #[test]
    fn basic_type_priority() {
        let t = types("i,f,s,b\n1,1.5,a,true\n2,2,b,FALSE\n");
        assert_eq!(
            t,
            vec![
                DataType::Int64,
                DataType::Float64,
                DataType::Utf8,
                DataType::Boolean
            ]
        );
    }

    #[test]
    fn all_empty_column_is_utf8() {
        let t = types("a,b\n1,\n2,\n");
        assert_eq!(t, vec![DataType::Int64, DataType::Utf8]);
    }

    #[test]
    fn header_only_gives_utf8_columns_with_names() {
        let f = TmpCsv::new("x,y\n");
        let ts = infer_schema(&f.0).unwrap();
        assert_eq!(ts.columns.len(), 2);
        assert_eq!(ts.columns[0].name, "x");
        assert_eq!(ts.columns[1].name, "y");
        assert!(ts.columns.iter().all(|c| c.data_type == DataType::Utf8));
    }

    #[test]
    fn nulls_do_not_constrain_type() {
        assert_eq!(types("n\n\n5\n\n"), vec![DataType::Int64]);
    }

    #[test]
    fn numeric_one_zero_is_int_not_bool() {
        assert_eq!(types("n\n1\n0\n1\n"), vec![DataType::Int64]);
    }

    #[test]
    fn int_and_float_mix_is_float() {
        assert_eq!(types("n\n1\n2.5\n"), vec![DataType::Float64]);
    }

    fn n_rows_then(n: usize, tail: &str) -> String {
        let mut s = String::from("n\n");
        for i in 0..n {
            s.push_str(&format!("{}\n", i));
        }
        s.push_str(tail);
        s
    }

    #[test]
    fn text_after_row_100_makes_utf8() {
        assert_eq!(types(&n_rows_then(100, "N/A\n")), vec![DataType::Utf8]);
        assert_eq!(types(&n_rows_then(5000, "N/A\n")), vec![DataType::Utf8]);
    }

    #[test]
    fn decimal_after_row_100_makes_float() {
        assert_eq!(types(&n_rows_then(150, "2.5\n")), vec![DataType::Float64]);
    }

    #[test]
    fn all_ints_beyond_100_rows_stay_int() {
        assert_eq!(types(&n_rows_then(1000, "")), vec![DataType::Int64]);
    }

    #[test]
    fn nonexistent_file_is_error() {
        assert!(infer_schema(Path::new("/nonexistent/ember_query/nope.csv")).is_err());
    }
}
