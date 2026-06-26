use anyhow::{anyhow, Result};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
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
                    match val.to_lowercase().as_str() {
                        "true" | "1" | "yes" => b.append_value(true),
                        "false" | "0" | "no" => b.append_value(false),
                        _ => b.append_null(),
                    }
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
