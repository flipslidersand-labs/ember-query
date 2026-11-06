use anyhow::Result;

use crate::catalog::TableSchema;
use crate::sql::ast::{AggFunc, Expr, OrderByItem, SelectItem, SelectStmt};

/// Logical plan node.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Table scan — reads the CSV at `path` with the given schema.
    Scan { path: String, schema: TableSchema },
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
    Limit { input: Box<LogicalPlan>, n: u64 },
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
            LogicalPlan::Join {
                left,
                right_path,
                on,
                ..
            } => {
                format!(
                    "{}Join [right={}, on={:?}]\n{}",
                    indent,
                    right_path,
                    on,
                    left.explain(depth + 1)
                )
            }
            LogicalPlan::Sort { input, order_by } => {
                format!(
                    "{}Sort [{:?}]\n{}",
                    indent,
                    order_by
                        .iter()
                        .map(|o| format!("{} {}", o.column, if o.asc { "ASC" } else { "DESC" }))
                        .collect::<Vec<_>>()
                        .join(", "),
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
        plan = sort_node(plan, &stmt.order_by);
    } else if let Some(order_by) = hidden_order_by(&stmt.select, &stmt.order_by) {
        // 5/6. ORDER BY refers to columns the SELECT list drops: sort the
        // Project's input, then project.
        plan = LogicalPlan::Project {
            input: Box::new(sort_node(plan, &order_by)),
            exprs: stmt.select.clone(),
        };
    } else {
        // 5/6. Project (non-aggregate), then Sort on its output
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            exprs: stmt.select.clone(),
        };
        plan = sort_node(plan, &stmt.order_by);
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

fn sort_node(input: LogicalPlan, order_by: &[OrderByItem]) -> LogicalPlan {
    if order_by.is_empty() {
        return input;
    }
    LogicalPlan::Sort {
        input: Box::new(input),
        order_by: order_by.to_vec(),
    }
}

/// If some ORDER BY key is not an output column of the (non-aggregate)
/// SELECT list, return the keys rewritten to resolve against the Project's
/// input: an alias of a plain column maps to that column, other keys are kept.
/// Returns None when every key resolves in the output (Sort stays above).
fn hidden_order_by(select: &[SelectItem], order_by: &[OrderByItem]) -> Option<Vec<OrderByItem>> {
    if order_by.is_empty() || select.iter().any(|i| matches!(i, SelectItem::Wildcard)) {
        return None;
    }
    let out_name = |item: &SelectItem| match item {
        SelectItem::Expr(_, Some(alias)) => Some(alias.clone()),
        SelectItem::Expr(Expr::Column(c), None) => Some(c.clone()),
        _ => None,
    };
    let names: Vec<String> = select.iter().filter_map(out_name).collect();
    let in_output = |col: &str| {
        let bare = col.split('.').next_back().unwrap_or(col);
        names.iter().any(|n| n == col || n == bare)
    };
    if order_by.iter().all(|o| in_output(&o.column)) {
        return None;
    }
    Some(
        order_by
            .iter()
            .map(|o| {
                let source = select.iter().find_map(|i| match i {
                    SelectItem::Expr(Expr::Column(c), Some(a)) if *a == o.column => Some(c),
                    _ => None,
                });
                OrderByItem {
                    column: source.cloned().unwrap_or_else(|| o.column.clone()),
                    ..o.clone()
                }
            })
            .collect(),
    )
}

/// Extract aggregate calls from the SELECT list.
fn collect_aggs(select: &[SelectItem]) -> Vec<(AggFunc, Expr, String)> {
    let mut result = Vec::new();
    for (i, item) in select.iter().enumerate() {
        if let SelectItem::Expr(Expr::Agg { func, expr: arg }, alias) = item {
            let name = alias.clone().unwrap_or_else(|| format!("agg_{}", i));
            result.push((func.clone(), *arg.clone(), name));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ColumnDef;
    use crate::sql::parser::parse;
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    fn schema(cols: &[(&str, DataType)]) -> TableSchema {
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

    fn main_schema() -> TableSchema {
        schema(&[("id", DataType::Int64), ("name", DataType::Utf8)])
    }

    fn catalog() -> HashMap<String, (String, TableSchema)> {
        let mut c = HashMap::new();
        c.insert(
            "b".to_string(),
            ("b.csv".to_string(), schema(&[("id", DataType::Int64)])),
        );
        c.insert(
            "c".to_string(),
            ("c.csv".to_string(), schema(&[("id", DataType::Int64)])),
        );
        c
    }

    fn plan(sql: &str) -> Result<LogicalPlan> {
        build(&parse(sql).unwrap(), "a.csv", main_schema(), &catalog())
    }

    /// Node names from the root down to the Scan.
    fn shape(p: &LogicalPlan) -> Vec<&'static str> {
        let mut out = Vec::new();
        let mut cur = p;
        loop {
            let (name, next) = match cur {
                LogicalPlan::Scan { .. } => ("Scan", None),
                LogicalPlan::Filter { input, .. } => ("Filter", Some(input)),
                LogicalPlan::Project { input, .. } => ("Project", Some(input)),
                LogicalPlan::Limit { input, .. } => ("Limit", Some(input)),
                LogicalPlan::Aggregate { input, .. } => ("Aggregate", Some(input)),
                LogicalPlan::Join { left, .. } => ("Join", Some(left)),
                LogicalPlan::Sort { input, .. } => ("Sort", Some(input)),
            };
            out.push(name);
            match next {
                Some(n) => cur = n,
                None => return out,
            }
        }
    }

    #[test]
    fn full_non_aggregate_node_order() {
        let p = plan("SELECT id FROM a JOIN b ON a.id = b.id WHERE id > 1 ORDER BY id LIMIT 5")
            .unwrap();
        assert_eq!(
            shape(&p),
            vec!["Limit", "Sort", "Project", "Filter", "Join", "Scan"]
        );
    }

    #[test]
    fn minimal_select_is_project_over_scan() {
        let p = plan("SELECT id FROM a").unwrap();
        assert_eq!(shape(&p), vec!["Project", "Scan"]);
    }

    #[test]
    fn aggregate_replaces_project() {
        let p = plan("SELECT COUNT(*) FROM a WHERE id > 1 ORDER BY agg_0 LIMIT 1").unwrap();
        assert_eq!(
            shape(&p),
            vec!["Limit", "Sort", "Aggregate", "Filter", "Scan"]
        );
    }

    #[test]
    fn group_by_without_aggregates_is_aggregate_with_no_aggs() {
        let p = plan("SELECT name FROM a GROUP BY name").unwrap();
        assert_eq!(shape(&p), vec!["Aggregate", "Scan"]);
        match p {
            LogicalPlan::Aggregate { group_by, aggs, .. } => {
                assert_eq!(group_by, vec!["name".to_string()]);
                assert!(aggs.is_empty());
            }
            other => panic!("expected Aggregate, got {:?}", other),
        }
    }

    #[test]
    fn agg_names_use_select_position_and_alias() {
        // Position index counts every SELECT item, including non-aggregates.
        let p = plan("SELECT COUNT(*), SUM(id) AS total, MAX(id) FROM a").unwrap();
        match p {
            LogicalPlan::Aggregate { aggs, .. } => {
                let names: Vec<&str> = aggs.iter().map(|(_, _, n)| n.as_str()).collect();
                assert_eq!(names, vec!["agg_0", "total", "agg_2"]);
                assert_eq!(aggs[0].0, AggFunc::Count);
                assert_eq!(aggs[0].1, Expr::Wildcard);
                assert_eq!(aggs[1].0, AggFunc::Sum);
                assert_eq!(aggs[1].1, Expr::Column("id".to_string()));
                assert_eq!(aggs[2].0, AggFunc::Max);
            }
            other => panic!("expected Aggregate, got {:?}", other),
        }
    }

    #[test]
    fn auto_name_index_is_position_in_select_list() {
        let p = plan("SELECT name, COUNT(*) FROM a GROUP BY name").unwrap();
        match p {
            LogicalPlan::Aggregate { aggs, .. } => {
                assert_eq!(aggs.len(), 1);
                assert_eq!(aggs[0].2, "agg_1");
            }
            other => panic!("expected Aggregate, got {:?}", other),
        }
    }

    #[test]
    fn multiple_joins_nest_in_source_order() {
        let p = plan("SELECT id FROM a JOIN b ON a.id = b.id JOIN c ON a.id = c.id").unwrap();
        assert_eq!(shape(&p), vec!["Project", "Join", "Join", "Scan"]);
        let LogicalPlan::Project { input, .. } = p else {
            panic!("expected Project");
        };
        // Outermost join is the last one written (c); inner is b.
        let LogicalPlan::Join {
            left, right_path, ..
        } = *input
        else {
            panic!("expected Join");
        };
        assert_eq!(right_path, "c.csv");
        match *left {
            LogicalPlan::Join { right_path, .. } => assert_eq!(right_path, "b.csv"),
            other => panic!("expected inner Join, got {:?}", other),
        }
    }

    #[test]
    fn join_resolves_right_schema_from_catalog() {
        let p = plan("SELECT id FROM a JOIN b ON a.id = b.id").unwrap();
        let LogicalPlan::Project { input, .. } = p else {
            panic!("expected Project");
        };
        match *input {
            LogicalPlan::Join {
                right_path,
                right_schema,
                ..
            } => {
                assert_eq!(right_path, "b.csv");
                assert_eq!(right_schema.columns.len(), 1);
                assert_eq!(right_schema.columns[0].name, "id");
            }
            other => panic!("expected Join, got {:?}", other),
        }
    }

    #[test]
    fn missing_join_table_is_error() {
        let err = plan("SELECT id FROM a JOIN nope ON a.id = nope.id").unwrap_err();
        assert_eq!(err.to_string(), "JOIN table 'nope' not found in catalog");
    }

    #[test]
    fn order_by_hidden_column_sorts_below_project() {
        let p = plan("SELECT name FROM a ORDER BY id LIMIT 2").unwrap();
        assert_eq!(shape(&p), vec!["Limit", "Project", "Sort", "Scan"]);
    }

    #[test]
    fn order_by_output_column_or_alias_stays_above_project() {
        let p = plan("SELECT name FROM a ORDER BY name").unwrap();
        assert_eq!(shape(&p), vec!["Sort", "Project", "Scan"]);
        let p = plan("SELECT name AS n FROM a ORDER BY n").unwrap();
        assert_eq!(shape(&p), vec!["Sort", "Project", "Scan"]);
        let p = plan("SELECT * FROM a ORDER BY id").unwrap();
        assert_eq!(shape(&p), vec!["Sort", "Project", "Scan"]);
    }

    #[test]
    fn order_by_alias_mixed_with_hidden_column_maps_alias_to_source() {
        let p = plan("SELECT name AS n FROM a ORDER BY id, n").unwrap();
        assert_eq!(shape(&p), vec!["Project", "Sort", "Scan"]);
        match &p {
            LogicalPlan::Project { input, .. } => match input.as_ref() {
                LogicalPlan::Sort { order_by, .. } => {
                    let cols: Vec<&str> = order_by.iter().map(|o| o.column.as_str()).collect();
                    assert_eq!(cols, vec!["id", "name"]);
                }
                _ => panic!("expected Sort"),
            },
            _ => panic!("expected Project"),
        }
    }

    #[test]
    fn explain_simple_plan_format() {
        let p = plan("SELECT id FROM a ORDER BY id DESC, name LIMIT 3").unwrap();
        // `name` is not selected, so Sort moves below Project (#42).
        let expected = "Limit [3]\n  \
                        Project [1 exprs]\n    \
                        Sort [\"id DESC, name ASC\"]\n      \
                        Scan [a.csv] columns=[id:Int64, name:Utf8]";
        assert_eq!(p.explain(0), expected);
    }

    #[test]
    fn explain_aggregate_and_join_format() {
        let p =
            plan("SELECT name, COUNT(*) AS n FROM a JOIN b ON a.id = b.id GROUP BY name").unwrap();
        let out = p.explain(0);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(
            lines[0],
            "Aggregate [group_by=[\"name\"], aggs=Count(Wildcard) AS n]"
        );
        assert!(
            lines[1].starts_with("  Join [right=b.csv, on="),
            "{}",
            lines[1]
        );
        assert_eq!(lines[2], "    Scan [a.csv] columns=[id:Int64, name:Utf8]");
    }

    #[test]
    fn explain_indents_by_depth() {
        let p = plan("SELECT id FROM a").unwrap();
        assert!(p.explain(2).starts_with("    Project"));
    }
}
