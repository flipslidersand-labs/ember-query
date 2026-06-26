use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

/// Pretty-print a `RecordBatch` as an aligned ASCII table.
pub fn print_batch(batch: &RecordBatch) {
    let schema = batch.schema();
    let n_cols = batch.num_columns();
    let n_rows = batch.num_rows();

    // Collect all values as strings
    let mut col_data: Vec<Vec<String>> = Vec::with_capacity(n_cols);
    for i in 0..n_cols {
        let arr = batch.column(i);
        let field = schema.field(i);
        let values: Vec<String> = (0..n_rows)
            .map(|row| format_value(arr.as_ref(), row, field.data_type()))
            .collect();
        col_data.push(values);
    }

    // Compute column widths
    let col_widths: Vec<usize> = (0..n_cols)
        .map(|i| {
            let header_len = schema.field(i).name().len();
            let max_val = col_data[i].iter().map(|s| s.len()).max().unwrap_or(0);
            header_len.max(max_val)
        })
        .collect();

    // Print header
    let header: String = (0..n_cols)
        .map(|i| {
            let name = schema.field(i).name();
            format!("{:<width$}", name, width = col_widths[i])
        })
        .collect::<Vec<_>>()
        .join(" | ");

    let separator: String = col_widths
        .iter()
        .map(|&w| "-".repeat(w))
        .collect::<Vec<_>>()
        .join("-+-");

    println!("{}", header);
    println!("{}", separator);

    // Print rows
    for row in 0..n_rows {
        let line: String = (0..n_cols)
            .map(|i| format!("{:<width$}", col_data[i][row], width = col_widths[i]))
            .collect::<Vec<_>>()
            .join(" | ");
        println!("{}", line);
    }
}

fn format_value(arr: &dyn Array, row: usize, dt: &DataType) -> String {
    if arr.is_null(row) {
        return "NULL".to_string();
    }
    match dt {
        DataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            a.value(row).to_string()
        }
        DataType::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            format!("{:.4}", a.value(row))
        }
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            a.value(row).to_string()
        }
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            a.value(row).to_string()
        }
        _ => format!("<{:?}>", dt),
    }
}
