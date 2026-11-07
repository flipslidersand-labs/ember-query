use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use super::{evaluate, resolve_column, Executor};
use crate::sql::ast::{Expr, SelectItem};

/// An output column: an input column by position, or an expression.
enum Proj {
    /// From `SELECT *`. Bound by position so a self-JOIN's `id` and
    /// `right_id`, both tagged `t1.id`, stay distinct (#92).
    Input(usize),
    Expr(Expr),
}

pub struct ProjectExec {
    input: Box<dyn Executor>,
    /// (output field name, source)
    projections: Vec<(String, Proj)>,
    out_schema: SchemaRef,
}

impl ProjectExec {
    pub fn new(input: Box<dyn Executor>, exprs: Vec<SelectItem>) -> Result<Self> {
        let in_schema = input.schema();

        let mut projections: Vec<(String, Proj)> = Vec::new();

        for item in &exprs {
            match item {
                SelectItem::Wildcard => {
                    // Expand all columns
                    for (i, field) in in_schema.fields().iter().enumerate() {
                        projections.push((field.name().clone(), Proj::Input(i)));
                    }
                }
                SelectItem::Expr(expr, alias) => {
                    let name = alias.clone().unwrap_or_else(|| expr_name(expr, &in_schema));
                    projections.push((name, Proj::Expr(expr.clone())));
                }
            }
        }

        // Build output schema
        let fields: Vec<Field> = projections
            .iter()
            .map(|(name, proj)| match proj {
                // Keeps the source tag so a Sort above can still resolve `t.col`.
                Proj::Input(i) => {
                    let src = in_schema.field(*i);
                    Field::new(name, src.data_type().clone(), true)
                        .with_metadata(src.metadata().clone())
                }
                Proj::Expr(expr) => {
                    let dt = infer_expr_type(expr, &in_schema);
                    let f = Field::new(name, dt, true);
                    // A plain, unaliased column keeps its source tag so a Sort above can
                    // still resolve `t.col` (e.g. `SELECT t2.id ... ORDER BY t2.id`).
                    match expr {
                        Expr::Column(c) => match resolve_column(&in_schema, c) {
                            Ok(i) if c == name || in_schema.field(i).name() == name => {
                                f.with_metadata(in_schema.field(i).metadata().clone())
                            }
                            _ => f,
                        },
                        _ => f,
                    }
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
                    .map(|(_, proj)| match proj {
                        Proj::Input(i) => Ok(batch.column(*i).clone()),
                        Proj::Expr(expr) => evaluate(&batch, expr),
                    })
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
