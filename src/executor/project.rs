use anyhow::Result;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use super::{evaluate, Executor};
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
                    for field in in_schema.fields() {
                        projections.push((
                            field.name().clone(),
                            Expr::Column(field.name().clone()),
                        ));
                    }
                }
                SelectItem::Expr(expr, alias) => {
                    let name = alias
                        .clone()
                        .unwrap_or_else(|| expr_name(expr, &in_schema));
                    projections.push((name, expr.clone()));
                }
            }
        }

        // Build output schema
        let fields: Vec<Field> = projections
            .iter()
            .map(|(name, expr)| {
                let dt = infer_expr_type(expr, &in_schema);
                Field::new(name, dt, true)
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

fn infer_expr_type(expr: &Expr, schema: &Schema) -> arrow::datatypes::DataType {
    use arrow::datatypes::DataType;
    match expr {
        Expr::Column(name) => schema
            .field_with_name(name)
            .map(|f| f.data_type().clone())
            .unwrap_or(DataType::Utf8),
        Expr::Literal(val) => match val {
            crate::sql::ast::Value::Int(_) => DataType::Int64,
            crate::sql::ast::Value::Float(_) => DataType::Float64,
            crate::sql::ast::Value::Bool(_) => DataType::Boolean,
            _ => DataType::Utf8,
        },
        _ => DataType::Utf8,
    }
}
