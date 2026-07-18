use anyhow::Result;

use crate::catalog::TableSchema;
use crate::sql::ast::{AggFunc, Expr, OrderByItem, SelectItem, SelectStmt};

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
    /// Hash inner join.
    Join {
        left: Box<LogicalPlan>,
        right_path: String,
        right_schema: TableSchema,
        right_alias: Option<String>,
        on: Expr,
    },
    /// Sort output rows by one or more columns.
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderByItem>,
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
            LogicalPlan::Join { left, right_path, on, .. } => {
                format!(
                    "{}Join [right={}, on={:?}]\n{}",
                    indent, right_path, on,
                    left.explain(depth + 1)
                )
            }
            LogicalPlan::Sort { input, order_by } => {
                format!(
                    "{}Sort [{:?}]\n{}",
                    indent,
                    order_by.iter().map(|o| format!("{} {}", o.column, if o.asc { "ASC" } else { "DESC" })).collect::<Vec<_>>().join(", "),
                    input.explain(depth + 1)
                )
            }
        }
    }
}

/// Build a logical plan from a parsed `SelectStmt`.
/// catalog maps table_name → (csv_path, schema).
pub fn build(
    stmt: &SelectStmt,
    path: &str,
    schema: TableSchema,
    catalog: &std::collections::HashMap<String, (String, TableSchema)>,
) -> Result<LogicalPlan> {
    // 1. Scan primary table
    let mut plan = LogicalPlan::Scan {
        path: path.to_string(),
        schema,
    };

    // 2. JOINs (before filter so ON predicate can reference both sides)
    for join in &stmt.joins {
        let (right_path, right_schema) = catalog
            .get(&join.table)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("JOIN table '{}' not found in catalog", join.table))?;
        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right_path,
            right_schema,
            right_alias: join.alias.clone(),
            on: join.on.clone(),
        };
    }

    // 3. Filter
    if let Some(predicate) = &stmt.where_ {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: predicate.clone(),
        };
    }

    // 4. Determine whether there are aggregates in the SELECT list.
    let aggs: Vec<(AggFunc, Expr, String)> = collect_aggs(&stmt.select);

    if !aggs.is_empty() || !stmt.group_by.is_empty() {
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by: stmt.group_by.clone(),
            aggs,
        };
    } else {
        // 5. Project (non-aggregate)
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            exprs: stmt.select.clone(),
        };
    }

    // 6. Sort
    if !stmt.order_by.is_empty() {
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            order_by: stmt.order_by.clone(),
        };
    }

    // 7. Limit
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
