use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use super::{evaluate, field_ref, resolve_column, Executor};
use crate::sql::ast::{Expr, SelectItem};

pub struct ProjectExec {
    input: Box<dyn Executor>,
    /// (output field name, expression)
    projections: Vec<(String, Expr)>,
    out_schema: SchemaRef,
}

impl ProjectExec {
    pub fn new(input: Box<dyn Executor>, exprs: Vec<SelectItem>) -> Result<Self> {
        let in_schema = input.schema();

        let mut projections: Vec<(String, Expr)> = Vec::new();

        for item in &exprs {
            match item {
                SelectItem::Wildcard => {
                    // Expand all columns
                    // `table.column` so a JOIN's colliding bare names stay unambiguous.
                    for field in in_schema.fields() {
                        projections.push((field.name().clone(), Expr::Column(field_ref(field))));
                    }
                }
                SelectItem::Expr(expr, alias) => {
                    let name = alias.clone().unwrap_or_else(|| expr_name(expr, &in_schema));
                    projections.push((name, expr.clone()));
                }
            }
        }

        // Build output schema
        let fields: Vec<Field> = projections
            .iter()
            .map(|(name, expr)| {
                let dt = infer_expr_type(expr, &in_schema);
                let f = Field::new(name, dt, true);
                // A plain column keeps its source tag so a Sort above can
                // still resolve `t.col` (e.g. `SELECT * ... ORDER BY t2.id`).
                match expr {
                    Expr::Column(c) => match resolve_column(&in_schema, c) {
                        Ok(i) => f.with_metadata(in_schema.field(i).metadata().clone()),
                        Err(_) => f,
                    },
                    _ => f,
                }
            })
            .collect();

        let out_schema = Arc::new(Schema::new(fields));

        Ok(Self {
            input,
            projections,
            out_schema,
        })
    }
}

impl Executor for ProjectExec {
    fn schema(&self) -> SchemaRef {
        self.out_schema.clone()
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self.input.next()? {
            None => Ok(None),
            Some(batch) => {
                let columns = self
                    .projections
                    .iter()
                    .map(|(_, expr)| evaluate(&batch, expr))
                    .collect::<Result<Vec<_>>>()?;

                RecordBatch::try_new(self.out_schema.clone(), columns)
                    .map(Some)
                    .map_err(anyhow::Error::from)
            }
        }
    }
}

fn expr_name(expr: &Expr, _schema: &Schema) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::Literal(val) => format!("{:?}", val),
        Expr::Agg { func, expr: _ } => format!("{:?}", func).to_lowercase(),
        _ => "expr".to_string(),
    }
}

/// Infer the output type by evaluating `expr` over an empty batch, so the
/// declared schema always matches what `evaluate` produces. Expressions that
/// cannot be evaluated here (e.g. aggregates) keep the legacy Utf8 fallback.
fn infer_expr_type(expr: &Expr, schema: &SchemaRef) -> DataType {
    let empty = RecordBatch::new_empty(schema.clone());
    evaluate(&empty, expr)
        .map(|a| a.data_type().clone())
        .unwrap_or(DataType::Utf8)
}
