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
        .from_path(path)?;

    let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_string()).collect();

    // Collect up to 100 sample rows for type inference.
    let mut samples: Vec<Vec<String>> = Vec::new();
    for result in reader.records().take(100) {
        let record = result?;
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

fn is_strict_bool_str(s: &str) -> bool {
    matches!(s.to_lowercase().as_str(), "true" | "false")
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
    fn nonexistent_file_is_error() {
        assert!(infer_schema(Path::new("/nonexistent/ember_query/nope.csv")).is_err());
    }
}
