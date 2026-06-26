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

    let headers: Vec<String> = reader
        .headers()?
        .iter()
        .map(|h| h.to_string())
        .collect();

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
