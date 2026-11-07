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

    // 空名・重複名は列が黙って参照不能になるためエラー(大文字小文字違いは別名)。
    let mut seen: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for (i, h) in headers.iter().enumerate() {
        if h.is_empty() {
            anyhow::bail!(
                "empty column name at column {} in header of {}",
                i + 1,
                path.display()
            );
        }
        if let Some(first) = seen.insert(h.as_str(), i + 1) {
            anyhow::bail!(
                "duplicate column name '{}' (column {} and column {}) in header of {}",
                h,
                first,
                i + 1,
                path.display()
            );
        }
    }

    // Collect up to 100 sample rows for type inference.
    let mut samples: Vec<Vec<String>> = Vec::new();
    for result in reader.records().take(100) {
        let record = result?;
        if record.len() > headers.len() {
            anyhow::bail!(
                "record has {} fields, expected at most {}",
                record.len(),
                headers.len()
            );
        }
        samples.push(record.iter().map(|f| f.to_string()).collect());
    }

    let mut columns = Vec::with_capacity(headers.len());
    for (col_idx, name) in headers.iter().enumerate() {
        let data_type = infer_column_type(&samples, col_idx);
        columns.push(ColumnDef {
            name: name.clone(),
            data_type,
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

fn infer_column_type(samples: &[Vec<String>], col_idx: usize) -> DataType {
    let mut can_be_int = true;
    let mut can_be_float = true;
    let mut can_be_bool = true;
    let mut has_non_empty = false;

    for row in samples {
        let val = match row.get(col_idx) {
            Some(v) => v.as_str(),
            None => continue,
        };

        if val.is_empty() {
            continue; // null — doesn't constrain type
        }
        has_non_empty = true;

        if can_be_bool && !is_strict_bool_str(val) {
            can_be_bool = false;
        }
        if can_be_int && val.parse::<i64>().is_err() {
            can_be_int = false;
        }
        if can_be_float && val.parse::<f64>().is_err() {
            can_be_float = false;
        }

        if !can_be_int && !can_be_float && !can_be_bool {
            break;
        }
    }

    if !has_non_empty {
        return DataType::Utf8;
    }

    // Bool takes priority over int (1/0 values would pass int check but prefer bool if only bools present)
    if can_be_bool {
        DataType::Boolean
    } else if can_be_int {
        DataType::Int64
    } else if can_be_float {
        DataType::Float64
    } else {
        DataType::Utf8
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

    #[test]
    fn sample_is_limited_to_first_100_rows() {
        // 100 ints then text: inference only sees the ints.
        let mut s = String::from("n\n");
        for i in 0..100 {
            s.push_str(&format!("{}\n", i));
        }
        s.push_str("abc\n");
        assert_eq!(types(&s), vec![DataType::Int64]);

        // Text inside the first 100 rows is seen.
        let mut s = String::from("n\n");
        for i in 0..99 {
            s.push_str(&format!("{}\n", i));
        }
        s.push_str("abc\n");
        assert_eq!(types(&s), vec![DataType::Utf8]);
    }

    #[test]
    fn duplicate_header_is_error() {
        let f = TmpCsv::new("id,name,id\n1,a,2\n");
        let err = infer_schema(&f.0).unwrap_err().to_string();
        assert!(err.contains("duplicate"), "{err}");
        assert!(err.contains("'id'"), "{err}");
        assert!(err.contains("column 3"), "{err}");
        assert!(err.contains(&f.0.display().to_string()), "{err}");
    }

    #[test]
    fn empty_header_is_error() {
        let f = TmpCsv::new("a,,c\n1,2,3\n");
        let err = infer_schema(&f.0).unwrap_err().to_string();
        assert!(err.contains("empty"), "{err}");
        assert!(err.contains("column 2"), "{err}");
        assert!(err.contains(&f.0.display().to_string()), "{err}");
    }

    #[test]
    fn headers_differing_only_in_case_are_ok() {
        let f = TmpCsv::new("Id,id\n1,2\n");
        let ts = infer_schema(&f.0).unwrap();
        assert_eq!(ts.columns[0].name, "Id");
        assert_eq!(ts.columns[1].name, "id");
    }

    #[test]
    fn distinct_headers_unchanged() {
        let f = TmpCsv::new("a,b,c\n1,2,3\n");
        let ts = infer_schema(&f.0).unwrap();
        let names: Vec<&str> = ts.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["a", "b", "c"]);
    }

    #[test]
    fn nonexistent_file_is_error() {
        assert!(infer_schema(Path::new("/nonexistent/ember_query/nope.csv")).is_err());
    }
}
