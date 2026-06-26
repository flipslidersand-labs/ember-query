use anyhow::Result;

use crate::catalog::TableSchema;
use crate::sql::ast::{AggFunc, Expr, SelectItem, SelectStmt};

/// Logical plan node.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Table scan — reads the CSV at `path` with the given schema.
    Scan {
        path: String,
        schema: TableSchema,
    },
    /// Filter rows by a predicate expression.
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    /// Project (compute output columns).
    Project {
        input: Box<LogicalPlan>,
        exprs: Vec<SelectItem>,
    },
    /// Limit the number of output rows.
    Limit {
        input: Box<LogicalPlan>,
        n: u64,
    },
    /// Hash aggregation with optional GROUP BY.
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<String>,
        /// (function, argument expression, output column name)
        aggs: Vec<(AggFunc, Expr, String)>,
    },
}

impl LogicalPlan {
    pub fn explain(&self, depth: usize) -> String {
        let indent = "  ".repeat(depth);
        match self {
            LogicalPlan::Scan { path, schema } => {
                format!(
                    "{}Scan [{}] columns=[{}]",
                    indent,
                    path,
                    schema
                        .columns
                        .iter()
                        .map(|c| format!("{}:{:?}", c.name, c.data_type))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            LogicalPlan::Filter { input, predicate } => {
                format!(
                    "{}Filter [{:?}]\n{}",
                    indent,
                    predicate,
                    input.explain(depth + 1)
                )
            }
            LogicalPlan::Project { input, exprs } => {
                format!(
                    "{}Project [{} exprs]\n{}",
                    indent,
                    exprs.len(),
                    input.explain(depth + 1)
                )
            }
            LogicalPlan::Limit { input, n } => {
                format!("{}Limit [{}]\n{}", indent, n, input.explain(depth + 1))
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggs,
            } => {
                format!(
                    "{}Aggregate [group_by={:?}, aggs={}]\n{}",
                    indent,
                    group_by,
                    aggs.iter()
                        .map(|(f, e, n)| format!("{:?}({:?}) AS {}", f, e, n))
                        .collect::<Vec<_>>()
                        .join(", "),
                    input.explain(depth + 1)
                )
            }
        }
    }
}

/// Build a logical plan from a parsed `SelectStmt`.
pub fn build(stmt: &SelectStmt, path: &str, schema: TableSchema) -> Result<LogicalPlan> {
    // 1. Scan
    let mut plan = LogicalPlan::Scan {
        path: path.to_string(),
        schema,
    };

    // 2. Filter
    if let Some(predicate) = &stmt.where_ {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: predicate.clone(),
        };
    }

    // 3. Determine whether there are aggregates in the SELECT list.
    let aggs: Vec<(AggFunc, Expr, String)> = collect_aggs(&stmt.select);

    if !aggs.is_empty() || !stmt.group_by.is_empty() {
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by: stmt.group_by.clone(),
            aggs,
        };
    } else {
        // 4. Project (non-aggregate)
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            exprs: stmt.select.clone(),
        };
    }

    // 5. Limit
    if let Some(n) = stmt.limit {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            n,
        };
    }

    Ok(plan)
}

/// Extract aggregate calls from the SELECT list.
fn collect_aggs(select: &[SelectItem]) -> Vec<(AggFunc, Expr, String)> {
    let mut result = Vec::new();
    for (i, item) in select.iter().enumerate() {
        if let SelectItem::Expr(expr, alias) = item {
            if let Expr::Agg { func, expr: arg } = expr {
                let name = alias
                    .clone()
                    .unwrap_or_else(|| format!("agg_{}", i));
                result.push((func.clone(), *arg.clone(), name));
            }
        }
    }
    result
}
