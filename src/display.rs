use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

/// Pretty-print batches as one aligned ASCII table on stdout.
pub fn print_batch(batches: &[RecordBatch]) {
    print!("{}", format_batch(batches));
}

/// Render all batches as a single table: one header, widths decided over every row.
/// Returns an empty string when there are no batches (no schema to print).
/// Widths use `str::len` (bytes), so multi-byte text is not width-aligned.
pub fn format_batch(batches: &[RecordBatch]) -> String {
    let Some(first) = batches.first() else {
        return String::new();
    };
    let schema = first.schema();
    let n_cols = first.num_columns();

    // Stringify every cell, column-major, across all batches.
    let mut col_data: Vec<Vec<String>> = vec![Vec::new(); n_cols];
    for batch in batches {
        for (i, values) in col_data.iter_mut().enumerate() {
            let arr = batch.column(i);
            let dt = schema.field(i).data_type();
            values.extend((0..batch.num_rows()).map(|row| format_value(arr.as_ref(), row, dt)));
        }
    }

    let col_widths: Vec<usize> = (0..n_cols)
        .map(|i| {
            let max_val = col_data[i].iter().map(|s| s.len()).max().unwrap_or(0);
            schema.field(i).name().len().max(max_val)
        })
        .collect();

    let pad_line = |cells: &mut dyn Iterator<Item = &str>| -> String {
        cells
            .zip(&col_widths)
            .map(|(c, &w)| format!("{:<w$}", c))
            .collect::<Vec<_>>()
            .join(" | ")
    };

    let mut out = String::new();
    out.push_str(&pad_line(
        &mut schema.fields().iter().map(|f| f.name().as_str()),
    ));
    out.push('\n');
    out.push_str(
        &col_widths
            .iter()
            .map(|&w| "-".repeat(w))
            .collect::<Vec<_>>()
            .join("-+-"),
    );
    out.push('\n');
    let n_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    for row in 0..n_rows {
        out.push_str(&pad_line(&mut col_data.iter().map(|c| c[row].as_str())));
        out.push('\n');
    }
    out
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, BinaryArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn int_batch(vals: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vals)) as ArrayRef]).unwrap()
    }

    #[test]
    fn header_printed_once_for_1025_rows_in_two_batches() {
        let b1 = int_batch((0..1024).collect());
        let b2 = int_batch(vec![99999]);
        let out = format_batch(&[b1, b2]);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2 + 1025);
        assert_eq!(out.matches("id").count(), 1);
        assert_eq!(out.matches("---").count(), 1);
    }

    #[test]
    fn widths_decided_across_batches() {
        let out = format_batch(&[int_batch(vec![1]), int_batch(vec![123456])]);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "id    ");
        assert_eq!(lines[1], "------");
        assert_eq!(lines[2], "1     ");
        assert_eq!(lines[3], "123456");
    }

    #[test]
    fn null_float_bool_utf8_and_unsupported() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f", DataType::Float64, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("x", DataType::Binary, true),
        ]));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![Some(1.23456789), None])),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(StringArray::from(vec![Some("a"), None])),
            Arc::new(BinaryArray::from_vec(vec![b"z", b"y"])),
        ];
        let out = format_batch(&[RecordBatch::try_new(schema, cols).unwrap()]);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[2], "1.2346 | true | a    | <Binary>");
        assert_eq!(lines[3], "NULL   | NULL | NULL | <Binary>");
    }

    #[test]
    fn zero_row_batch_prints_header_only() {
        let out = format_batch(&[int_batch(vec![])]);
        assert_eq!(out, "id\n--\n");
    }

    #[test]
    fn no_batches_is_empty() {
        assert_eq!(format_batch(&[]), "");
    }

    #[test]
    fn multibyte_width_uses_byte_len_current_behaviour() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let col: ArrayRef = Arc::new(StringArray::from(vec!["日本"]));
        let out = format_batch(&[RecordBatch::try_new(schema, vec![col]).unwrap()]);
        // "日本" is 6 bytes: width follows bytes, not display columns.
        assert_eq!(out.lines().nth(1).unwrap(), "------");
    }
}
