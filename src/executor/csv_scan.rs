use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use super::Executor;
use crate::catalog::TableSchema;

const BATCH_SIZE: usize = 1024;

pub struct CsvScanExec {
    reader: csv::Reader<std::fs::File>,
    schema: SchemaRef,
    done: bool,
}

impl CsvScanExec {
    pub fn new(path: &str, table_schema: TableSchema) -> Result<Self> {
        let reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true) // short records pad with NULL; long ones are rejected in next()
            .from_path(path)
            .map_err(|e| anyhow!("cannot open CSV '{}': {}", path, e))?;

        let fields: Vec<Field> = table_schema
            .columns
            .iter()
            .map(|c| Field::new(&c.name, c.data_type.clone(), true))
            .collect();

        let schema = Arc::new(Schema::new(fields));
        Ok(Self {
            reader,
            schema,
            done: false,
        })
    }
}

impl Executor for CsvScanExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }

        let n_cols = self.schema.fields().len();
        let mut builders: Vec<ColBuilder> = self
            .schema
            .fields()
            .iter()
            .map(|f| ColBuilder::new(f.data_type()))
            .collect();

        let mut rows_read = 0usize;
        for result in self.reader.records().take(BATCH_SIZE) {
            let record = result?;
            if record.len() > n_cols {
                return Err(anyhow!(
                    "record {} has {} fields, expected at most {}",
                    rows_read + 1,
                    record.len(),
                    n_cols
                ));
            }
            for (col_idx, builder) in builders.iter_mut().enumerate() {
                let val = record.get(col_idx).unwrap_or("");
                builder.append(val)?;
            }
            rows_read += 1;
        }

        if rows_read == 0 {
            self.done = true;
            return Ok(None);
        }

        if rows_read < BATCH_SIZE {
            self.done = true;
        }

        let columns: Vec<ArrayRef> = builders
            .into_iter()
            .map(|b| b.finish())
            .collect::<Result<Vec<_>>>()?;

        // Make sure column count matches schema
        assert_eq!(columns.len(), n_cols);

        let batch = RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| anyhow!("failed to build RecordBatch: {}", e))?;

        Ok(Some(batch))
    }
}

// ── Per-column builder ────────────────────────────────────────────────────────

enum ColBuilder {
    Int(Int64Builder),
    Float(Float64Builder),
    Str(StringBuilder),
    Bool(BooleanBuilder),
}

impl ColBuilder {
    fn new(dt: &DataType) -> Self {
        match dt {
            DataType::Int64 => ColBuilder::Int(Int64Builder::new()),
            DataType::Float64 => ColBuilder::Float(Float64Builder::new()),
            DataType::Boolean => ColBuilder::Bool(BooleanBuilder::new()),
            _ => ColBuilder::Str(StringBuilder::new()),
        }
    }

    fn append(&mut self, val: &str) -> Result<()> {
        match self {
            ColBuilder::Int(b) => {
                if val.is_empty() {
                    b.append_null();
                } else {
                    b.append_value(
                        val.parse::<i64>()
                            .map_err(|_| anyhow!("cannot parse '{}' as i64", val))?,
                    );
                }
            }
            ColBuilder::Float(b) => {
                if val.is_empty() {
                    b.append_null();
                } else {
                    b.append_value(
                        val.parse::<f64>()
                            .map_err(|_| anyhow!("cannot parse '{}' as f64", val))?,
                    );
                }
            }
            ColBuilder::Bool(b) => {
                if val.is_empty() {
                    b.append_null();
                } else {
                    b.append_value(
                        crate::catalog::parse_bool_str(val)
                            .ok_or_else(|| anyhow!("cannot parse '{}' as bool", val))?,
                    );
                }
            }
            ColBuilder::Str(b) => {
                if val.is_empty() {
                    b.append_null();
                } else {
                    b.append_value(val);
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<ArrayRef> {
        Ok(match self {
            ColBuilder::Int(mut b) => Arc::new(b.finish()) as ArrayRef,
            ColBuilder::Float(mut b) => Arc::new(b.finish()) as ArrayRef,
            ColBuilder::Bool(mut b) => Arc::new(b.finish()) as ArrayRef,
            ColBuilder::Str(mut b) => Arc::new(b.finish()) as ArrayRef,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{infer_schema, ColumnDef};
    use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Temp CSV that is removed on drop.
    struct TmpCsv(PathBuf);

    impl TmpCsv {
        fn new(content: &str) -> Self {
            static N: AtomicUsize = AtomicUsize::new(0);
            let p = std::env::temp_dir().join(format!(
                "ember_query_csv_scan_{}_{}.csv",
                std::process::id(),
                N.fetch_add(1, Ordering::SeqCst)
            ));
            std::fs::write(&p, content).unwrap();
            TmpCsv(p)
        }
        fn path(&self) -> &str {
            self.0.to_str().unwrap()
        }
    }

    impl Drop for TmpCsv {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn schema_of(cols: &[(&str, DataType)]) -> TableSchema {
        TableSchema {
            columns: cols
                .iter()
                .map(|(n, t)| ColumnDef {
                    name: n.to_string(),
                    data_type: t.clone(),
                })
                .collect(),
        }
    }

    fn int_csv(rows: usize) -> String {
        let mut s = String::from("id\n");
        for i in 0..rows {
            s.push_str(&format!("{}\n", i));
        }
        s
    }

    fn scan_all(csv: &TmpCsv, ts: TableSchema) -> Result<Vec<RecordBatch>> {
        let mut exec = CsvScanExec::new(csv.path(), ts)?;
        let mut out = Vec::new();
        while let Some(b) = exec.next()? {
            out.push(b);
        }
        // After exhaustion, further calls keep returning None.
        assert!(exec.next()?.is_none());
        Ok(out)
    }

    fn check_no_loss(rows: usize, expected_batches: &[usize]) {
        let f = TmpCsv::new(&int_csv(rows));
        let batches = scan_all(&f, schema_of(&[("id", DataType::Int64)])).unwrap();
        let sizes: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();
        assert_eq!(sizes, expected_batches);
        let mut next = 0i64;
        for b in &batches {
            let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..a.len() {
                assert_eq!(a.value(i), next, "row lost or reordered");
                next += 1;
            }
        }
        assert_eq!(next as usize, rows);
    }

    #[test]
    fn rows_1024_exact_batch_then_end() {
        check_no_loss(1024, &[1024]);
    }

    #[test]
    fn rows_1025_splits_without_loss() {
        check_no_loss(1025, &[1024, 1]);
    }

    #[test]
    fn rows_2049_splits_without_loss() {
        check_no_loss(2049, &[1024, 1024, 1]);
    }

    #[test]
    fn rows_1023_single_batch() {
        check_no_loss(1023, &[1023]);
    }

    #[test]
    fn header_only_returns_none() {
        let f = TmpCsv::new("a,b\n");
        let batches = scan_all(
            &f,
            schema_of(&[("a", DataType::Int64), ("b", DataType::Utf8)]),
        )
        .unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn empty_cells_become_null() {
        let f = TmpCsv::new("i,f,s,b\n,,,\n1,2.5,x,true\n");
        let ts = schema_of(&[
            ("i", DataType::Int64),
            ("f", DataType::Float64),
            ("s", DataType::Utf8),
            ("b", DataType::Boolean),
        ]);
        let batches = scan_all(&f, ts).unwrap();
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        assert_eq!(b.num_rows(), 2);
        for c in 0..4 {
            assert!(b.column(c).is_null(0), "col {} row 0 should be NULL", c);
            assert!(!b.column(c).is_null(1), "col {} row 1 should be set", c);
        }
    }

    #[test]
    fn empty_string_is_null_not_empty_value() {
        let f = TmpCsv::new("s\n\"\"\nabc\n");
        let batches = scan_all(&f, schema_of(&[("s", DataType::Utf8)])).unwrap();
        let a = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(a.is_null(0));
        assert_eq!(a.value(1), "abc");
    }

    #[test]
    fn int_column_with_text_is_error() {
        let f = TmpCsv::new("n\n1\nabc\n3\n");
        let err = scan_all(&f, schema_of(&[("n", DataType::Int64)])).unwrap_err();
        assert!(err.to_string().contains("abc"), "got: {}", err);
    }

    #[test]
    fn float_column_with_text_is_error() {
        let f = TmpCsv::new("n\n1.5\nxyz\n");
        let err = scan_all(&f, schema_of(&[("n", DataType::Float64)])).unwrap_err();
        assert!(err.to_string().contains("xyz"), "got: {}", err);
    }

    #[test]
    fn type_deviation_after_sample_window_is_error() {
        // 101st data row deviates: inference (first 100 rows) says Int64, scan fails.
        let mut s = String::from("n\n");
        for i in 0..100 {
            s.push_str(&format!("{}\n", i));
        }
        s.push_str("oops\n");
        let f = TmpCsv::new(&s);
        let ts = infer_schema(&f.0).unwrap();
        assert_eq!(ts.columns[0].data_type, DataType::Int64);
        let err = scan_all(&f, ts).unwrap_err();
        assert!(err.to_string().contains("oops"), "got: {}", err);
    }

    #[test]
    fn bool_column_accepts_true_false_case_insensitive() {
        let f = TmpCsv::new("b\nTrue\nFALSE\ntrue\n\n");
        let batches = scan_all(&f, schema_of(&[("b", DataType::Boolean)])).unwrap();
        let a = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let got: Vec<Option<bool>> = (0..a.len())
            .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
            .collect();
        assert_eq!(got, vec![Some(true), Some(false), Some(true)]);
    }

    #[test]
    fn bool_column_rejects_non_true_false_notations() {
        for bad in ["maybe", "1", "0", "yes", "no"] {
            let f = TmpCsv::new(&format!("b\ntrue\n{}\n", bad));
            let err = scan_all(&f, schema_of(&[("b", DataType::Boolean)])).unwrap_err();
            assert!(err.to_string().contains(bad), "{}: got {}", bad, err);
        }
    }

    #[test]
    fn bool_column_empty_cell_is_null() {
        let f = TmpCsv::new("a,b\n1,true\n2,\n");
        let batches = scan_all(
            &f,
            schema_of(&[("a", DataType::Int64), ("b", DataType::Boolean)]),
        )
        .unwrap();
        let a = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!a.is_null(0));
        assert!(a.is_null(1));
    }

    #[test]
    fn nonexistent_file_is_error() {
        let err = CsvScanExec::new(
            "/nonexistent/ember_query/does_not_exist.csv",
            schema_of(&[("a", DataType::Int64)]),
        )
        .err()
        .expect("should fail");
        assert!(err.to_string().contains("cannot open CSV"), "got: {}", err);
    }

    #[test]
    fn schema_reports_nullable_fields_in_order() {
        let f = TmpCsv::new("a,b\n1,x\n");
        let exec = CsvScanExec::new(
            f.path(),
            schema_of(&[("a", DataType::Int64), ("b", DataType::Utf8)]),
        )
        .unwrap();
        let s = exec.schema();
        assert_eq!(s.field(0).name(), "a");
        assert_eq!(s.field(1).data_type(), &DataType::Utf8);
        assert!(s.field(0).is_nullable());
    }

    /// A record shorter than the header yields NULL for the missing trailing cells.
    #[test]
    fn short_record_pads_with_null() {
        let f = TmpCsv::new("a,b\n1,x\n2\n");
        let batches = scan_all(
            &f,
            schema_of(&[("a", DataType::Int64), ("b", DataType::Utf8)]),
        )
        .unwrap();
        let b = &batches[0];
        assert_eq!(b.num_rows(), 2);
        assert!(b.column(1).is_null(1));
    }

    #[test]
    fn long_record_still_errors() {
        let f = TmpCsv::new("a,b\n1,x\n2,y,z\n");
        let res = scan_all(
            &f,
            schema_of(&[("a", DataType::Int64), ("b", DataType::Utf8)]),
        );
        assert!(res.is_err());
    }
}
