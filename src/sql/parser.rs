use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;

use super::ast::*;
use anyhow::{anyhow, Result};

#[derive(Parser)]
#[grammar = "sql/grammar.pest"]
pub struct SqlParser;

/// Maximum accepted SQL length in bytes.
const MAX_SQL_LEN: usize = 64 * 1024;
/// Maximum nesting depth of parentheses plus prefix operators (`NOT`, `-`).
const MAX_NEST_DEPTH: usize = 128;

/// Rejects oversized or deeply nested input before the recursive parser runs,
/// so hostile SQL returns an error instead of overflowing the stack.
fn check_limits(sql: &str) -> Result<()> {
    if sql.len() > MAX_SQL_LEN {
        return Err(anyhow!("SQL is too long (max {MAX_SQL_LEN} bytes)"));
    }
    let b = sql.as_bytes();
    let (mut i, mut base, mut run) = (0, 0usize, 0usize);
    let mut stack: Vec<usize> = Vec::new();
    while i < b.len() {
        match b[i] {
            q @ (b'\'' | b'"') => {
                run = 0;
                i += 1;
                while i < b.len() && b[i] != q {
                    i += 1;
                }
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'-' => run += 1,
            b'(' => {
                let added = run + 1;
                base += added;
                stack.push(added);
                run = 0;
            }
            b')' => {
                base -= stack.pop().unwrap_or(0);
                run = 0;
            }
            c if c.is_ascii_alphanumeric() || c == b'_' => {
                let start = i;
                while i + 1 < b.len() && (b[i + 1].is_ascii_alphanumeric() || b[i + 1] == b'_') {
                    i += 1;
                }
                if b[start..=i].eq_ignore_ascii_case(b"NOT") {
                    run += 1;
                } else {
                    run = 0;
                }
            }
            c if c.is_ascii_whitespace() => {}
            _ => run = 0,
        }
        if base + run > MAX_NEST_DEPTH {
            return Err(anyhow!(
                "SQL is too deeply nested (max depth {MAX_NEST_DEPTH})"
            ));
        }
        i += 1;
    }
    Ok(())
}

/// Entry point — parses a SQL string into a `SelectStmt`.
pub fn parse(sql: &str) -> Result<SelectStmt> {
    check_limits(sql)?;
    let mut pairs =
        SqlParser::parse(Rule::query, sql).map_err(|e| anyhow!("Parse error: {}", e))?;

    let query = pairs.next().ok_or_else(|| anyhow!("empty parse"))?;
    let select_stmt = query
        .into_inner()
        .find(|p| p.as_rule() == Rule::select_stmt)
        .ok_or_else(|| anyhow!("no select_stmt found"))?;

    parse_select_stmt(select_stmt)
}

fn parse_select_stmt(pair: Pair<Rule>) -> Result<SelectStmt> {
    let mut inner = pair.into_inner();

    // select_list
    let select_list = inner.next().ok_or_else(|| anyhow!("missing select_list"))?;
    let select = parse_select_list(select_list)?;

    // table_ref: the primary table path is resolved by the caller, so only consume it here.
    inner.next().ok_or_else(|| anyhow!("missing table_ref"))?;

    let mut joins = Vec::new();
    let mut where_ = None;
    let mut group_by = Vec::new();
    let mut order_by = Vec::new();
    let mut limit = None;

    for p in inner {
        match p.as_rule() {
            Rule::join_clause => {
                joins.push(parse_join_clause(p)?);
            }
            Rule::where_clause => {
                let expr_pair = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| anyhow!("empty WHERE"))?;
                where_ = Some(parse_expr(expr_pair)?);
            }
            Rule::group_by_clause => {
                for col in p.into_inner() {
                    if col.as_rule() == Rule::ident {
                        group_by.push(col.as_str().to_string());
                    }
                }
            }
            Rule::order_by_clause => {
                for item in p.into_inner() {
                    if item.as_rule() == Rule::order_item {
                        order_by.push(parse_order_item(item));
                    }
                }
            }
            Rule::limit_clause => {
                let n_str = p
                    .into_inner()
                    .next()
                    .ok_or_else(|| anyhow!("empty LIMIT"))?
                    .as_str();
                limit = Some(n_str.parse::<u64>()?);
            }
            _ => {}
        }
    }

    Ok(SelectStmt {
        select,
        joins,
        where_,
        group_by,
        order_by,
        limit,
    })
}

/// Table name of a `table_ref` (the optional alias is accepted by the grammar but not used).
fn parse_table_ref(pair: Pair<Rule>) -> String {
    pair.into_inner()
        .next()
        .map(|p| p.as_str().to_string())
        .unwrap_or_default()
}

fn parse_join_clause(pair: Pair<Rule>) -> Result<super::ast::JoinClause> {
    let mut inner = pair.into_inner();
    let table_ref = inner.next().ok_or_else(|| anyhow!("JOIN missing table"))?;
    let table = parse_table_ref(table_ref);
    let on_pair = inner
        .next()
        .ok_or_else(|| anyhow!("JOIN missing ON expr"))?;
    let on = parse_expr(on_pair)?;
    Ok(super::ast::JoinClause { table, on })
}

fn parse_order_item(pair: Pair<Rule>) -> super::ast::OrderByItem {
    let mut inner = pair.into_inner();
    let column = inner
        .next()
        .map(|p| p.as_str().to_string())
        .unwrap_or_default();
    let asc = inner
        .find(|p| p.as_rule() == Rule::sort_dir)
        .is_none_or(|p| !p.as_str().eq_ignore_ascii_case("DESC"));
    super::ast::OrderByItem { column, asc }
}

fn parse_select_list(pair: Pair<Rule>) -> Result<Vec<SelectItem>> {
    let mut items = Vec::new();
    for p in pair.into_inner() {
        if p.as_rule() == Rule::select_item {
            items.push(parse_select_item(p)?);
        }
    }
    Ok(items)
}

fn parse_select_item(pair: Pair<Rule>) -> Result<SelectItem> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| anyhow!("empty select_item"))?;
    match inner.as_rule() {
        Rule::wildcard => Ok(SelectItem::Wildcard),
        Rule::aliased_expr => {
            let mut parts = inner.into_inner();
            let expr_pair = parts
                .next()
                .ok_or_else(|| anyhow!("missing expr in aliased_expr"))?;
            let expr = parse_expr(expr_pair)?;
            let alias = parts.next().map(|p| p.as_str().to_string());
            Ok(SelectItem::Expr(expr, alias))
        }
        r => Err(anyhow!("unexpected rule in select_item: {:?}", r)),
    }
}

fn parse_expr(pair: Pair<Rule>) -> Result<Expr> {
    match pair.as_rule() {
        Rule::expr => {
            let inner = pair
                .into_inner()
                .next()
                .ok_or_else(|| anyhow!("empty expr"))?;
            parse_expr(inner)
        }
        Rule::or_expr => parse_or_expr(pair),
        Rule::and_expr => parse_and_expr(pair),
        Rule::not_expr => parse_not_expr(pair),
        Rule::cmp_expr => parse_cmp_expr(pair),
        Rule::add_expr => parse_add_expr(pair),
        Rule::mul_expr => parse_mul_expr(pair),
        Rule::unary_expr => parse_unary_expr(pair),
        Rule::primary => parse_primary(pair),
        Rule::literal => parse_literal_expr(pair),
        Rule::func_call => parse_func_call(pair),
        Rule::column_ref => Ok(Expr::Column(pair.as_str().to_string())),
        r => Err(anyhow!("unexpected rule in parse_expr: {:?}", r)),
    }
}

fn parse_or_expr(pair: Pair<Rule>) -> Result<Expr> {
    let mut children = pair.into_inner();
    let mut left = parse_expr(children.next().ok_or_else(|| anyhow!("empty or_expr"))?)?;
    for right_pair in children {
        let right = parse_expr(right_pair)?;
        left = Expr::BinOp {
            op: BinOp::Or,
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_and_expr(pair: Pair<Rule>) -> Result<Expr> {
    let mut children = pair.into_inner();
    let mut left = parse_expr(children.next().ok_or_else(|| anyhow!("empty and_expr"))?)?;
    for right_pair in children {
        let right = parse_expr(right_pair)?;
        left = Expr::BinOp {
            op: BinOp::And,
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_not_expr(pair: Pair<Rule>) -> Result<Expr> {
    let mut children = pair.into_inner();
    let first = children.next().ok_or_else(|| anyhow!("empty not_expr"))?;
    match first.as_rule() {
        Rule::not_expr => {
            // NOT <not_expr>
            Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(parse_expr(first)?),
            })
        }
        _ => parse_expr(first),
    }
}

fn parse_cmp_expr(pair: Pair<Rule>) -> Result<Expr> {
    let mut children = pair.into_inner();
    let left = parse_expr(
        children
            .next()
            .ok_or_else(|| anyhow!("empty cmp_expr lhs"))?,
    )?;

    if let Some(op_pair) = children.next() {
        let op = match op_pair.as_str() {
            "=" => BinOp::Eq,
            "<>" | "!=" => BinOp::Ne,
            "<" => BinOp::Lt,
            "<=" => BinOp::Le,
            ">" => BinOp::Gt,
            ">=" => BinOp::Ge,
            s => return Err(anyhow!("unknown cmp_op: {}", s)),
        };
        let right = parse_expr(
            children
                .next()
                .ok_or_else(|| anyhow!("missing cmp_expr rhs"))?,
        )?;
        Ok(Expr::BinOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    } else {
        Ok(left)
    }
}

fn parse_add_expr(pair: Pair<Rule>) -> Result<Expr> {
    let mut children = pair.into_inner();
    let mut left = parse_expr(children.next().ok_or_else(|| anyhow!("empty add_expr"))?)?;

    while let Some(op_pair) = children.next() {
        let op = match op_pair.as_str() {
            "+" => BinOp::Add,
            "-" => BinOp::Sub,
            s => return Err(anyhow!("unknown add_op: {}", s)),
        };
        let right = parse_expr(
            children
                .next()
                .ok_or_else(|| anyhow!("missing add_expr rhs"))?,
        )?;
        left = Expr::BinOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_mul_expr(pair: Pair<Rule>) -> Result<Expr> {
    let mut children = pair.into_inner();
    let mut left = parse_expr(children.next().ok_or_else(|| anyhow!("empty mul_expr"))?)?;

    while let Some(op_pair) = children.next() {
        let op = match op_pair.as_str() {
            "*" => BinOp::Mul,
            "/" => BinOp::Div,
            s => return Err(anyhow!("unknown mul_op: {}", s)),
        };
        let right = parse_expr(
            children
                .next()
                .ok_or_else(|| anyhow!("missing mul_expr rhs"))?,
        )?;
        left = Expr::BinOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_unary_expr(pair: Pair<Rule>) -> Result<Expr> {
    let mut children = pair.into_inner();
    let first = children.next().ok_or_else(|| anyhow!("empty unary_expr"))?;
    if first.as_str() == "-" {
        let inner = children
            .next()
            .ok_or_else(|| anyhow!("missing unary_expr operand"))?;
        Ok(Expr::Unary {
            op: UnaryOp::Neg,
            expr: Box::new(parse_expr(inner)?),
        })
    } else {
        parse_expr(first)
    }
}

fn parse_primary(pair: Pair<Rule>) -> Result<Expr> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| anyhow!("empty primary"))?;
    match inner.as_rule() {
        Rule::expr => parse_expr(inner),
        Rule::func_call => parse_func_call(inner),
        Rule::literal => parse_literal_expr(inner),
        Rule::column_ref => Ok(Expr::Column(inner.as_str().to_string())),
        r => Err(anyhow!("unexpected rule in primary: {:?}", r)),
    }
}

fn parse_func_call(pair: Pair<Rule>) -> Result<Expr> {
    let mut children = pair.into_inner();
    let name_pair = children
        .next()
        .ok_or_else(|| anyhow!("missing func_name"))?;
    let func = match name_pair.as_str().to_uppercase().as_str() {
        "COUNT" => AggFunc::Count,
        "SUM" => AggFunc::Sum,
        "AVG" => AggFunc::Avg,
        "MIN" => AggFunc::Min,
        "MAX" => AggFunc::Max,
        n => return Err(anyhow!("unknown function: {}", n)),
    };

    let args_pair = children
        .next()
        .ok_or_else(|| anyhow!("missing func_args"))?;
    let arg_inner = args_pair
        .into_inner()
        .next()
        .ok_or_else(|| anyhow!("empty func_args"))?;

    let arg_expr = match arg_inner.as_rule() {
        Rule::star_arg => Expr::Wildcard,
        _ => parse_expr(arg_inner)?,
    };

    Ok(Expr::Agg {
        func,
        expr: Box::new(arg_expr),
    })
}

fn parse_literal_expr(pair: Pair<Rule>) -> Result<Expr> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| anyhow!("empty literal"))?;
    let val = match inner.as_rule() {
        Rule::null_lit => Value::Null,
        Rule::bool_lit => {
            let b = inner.as_str().to_uppercase() == "TRUE";
            Value::Bool(b)
        }
        Rule::float_lit => {
            let f: f64 = inner.as_str().parse()?;
            Value::Float(f)
        }
        Rule::integer_lit => {
            let i: i64 = inner.as_str().parse()?;
            Value::Int(i)
        }
        Rule::string_lit => {
            let s = inner.as_str();
            // Strip surrounding quotes
            let stripped = &s[1..s.len() - 1];
            Value::Str(stripped.to_string())
        }
        r => return Err(anyhow!("unexpected literal rule: {:?}", r)),
    };
    Ok(Expr::Literal(val))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(s: &str) -> Box<Expr> {
        Box::new(Expr::Column(s.to_string()))
    }

    fn eq(c: &str, n: i64) -> Expr {
        Expr::BinOp {
            op: BinOp::Eq,
            left: col(c),
            right: Box::new(Expr::Literal(Value::Int(n))),
        }
    }

    fn nested_ok(sql: &str) {
        parse(sql).unwrap();
    }

    #[test]
    fn moderate_nesting_still_parses() {
        let n = 100;
        nested_ok(&format!(
            "SELECT * FROM t WHERE {}a = 1{}",
            "(".repeat(n),
            ")".repeat(n)
        ));
        nested_ok(&format!("SELECT * FROM t WHERE {}a = 1", "NOT ".repeat(n)));
        nested_ok(&format!("SELECT * FROM t WHERE a = {}1", "- ".repeat(n)));
        nested_ok("SELECT * FROM t WHERE a = '((((((' AND b = \"))))\"");
    }

    #[test]
    fn deeply_nested_parens_are_rejected_not_aborted() {
        let n = 10_000;
        let sql = format!(
            "SELECT * FROM t WHERE {}a = 1{}",
            "(".repeat(n),
            ")".repeat(n)
        );
        let err = parse(&sql).unwrap_err().to_string();
        assert!(
            err.contains("too deeply nested") || err.contains("too long"),
            "{err}"
        );
    }

    #[test]
    fn deeply_nested_not_is_rejected_not_aborted() {
        let sql = format!("SELECT * FROM t WHERE {}a = 1", "NOT ".repeat(10_000));
        let err = parse(&sql).unwrap_err().to_string();
        assert!(
            err.contains("too deeply nested") || err.contains("too long"),
            "{err}"
        );
    }

    #[test]
    fn deeply_nested_unary_minus_is_rejected_not_aborted() {
        let sql = format!("SELECT * FROM t WHERE a = {}1", "- ".repeat(10_000));
        let err = parse(&sql).unwrap_err().to_string();
        assert!(
            err.contains("too deeply nested") || err.contains("too long"),
            "{err}"
        );
    }

    #[test]
    fn overlong_sql_is_rejected() {
        let sql = format!("SELECT * FROM t WHERE a = '{}'", "x".repeat(MAX_SQL_LEN));
        assert!(parse(&sql).unwrap_err().to_string().contains("too long"));
    }

    #[test]
    fn where_followed_by_order_by_parses() {
        let s = parse("SELECT * FROM orders WHERE qty > 1 ORDER BY qty").unwrap();
        assert!(s.where_.is_some());
        assert_eq!(s.order_by.len(), 1);
        assert_eq!(s.order_by[0].column, "qty");
    }

    #[test]
    fn join_on_followed_by_order_by_parses() {
        let s = parse("SELECT * FROM a JOIN b ON a.id = b.id ORDER BY a.id DESC").unwrap();
        assert_eq!(s.joins.len(), 1);
        assert_eq!(s.order_by.len(), 1);
        assert!(!s.order_by[0].asc);
    }

    #[test]
    fn or_and_precedence_unchanged() {
        let s = parse("SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3").unwrap();
        let expected = Expr::BinOp {
            op: BinOp::Or,
            left: Box::new(eq("a", 1)),
            right: Box::new(Expr::BinOp {
                op: BinOp::And,
                left: Box::new(eq("b", 2)),
                right: Box::new(eq("c", 3)),
            }),
        };
        assert_eq!(s.where_.unwrap(), expected);
    }

    #[test]
    fn keyword_followed_by_paren_or_space_still_works() {
        let s = parse("SELECT * FROM t WHERE NOT(a = 1) OR(b = 2) AND  c = 3").unwrap();
        assert!(s.where_.is_some());
    }

    #[test]
    fn identifiers_starting_with_keywords_are_columns() {
        let s = parse(
            "SELECT order_id, notes, nullable, true_flag, android FROM t \
             WHERE order_id = 1 OR notes = 2 AND android = 3 ORDER BY order_id",
        )
        .unwrap();
        assert_eq!(s.select.len(), 5);
        assert_eq!(
            s.select[1],
            SelectItem::Expr(Expr::Column("notes".into()), None)
        );
        assert_eq!(
            s.select[2],
            SelectItem::Expr(Expr::Column("nullable".into()), None)
        );
        assert_eq!(
            s.select[3],
            SelectItem::Expr(Expr::Column("true_flag".into()), None)
        );
        assert_eq!(s.order_by[0].column, "order_id");
    }

    #[test]
    fn alias_and_sort_direction_keywords_need_boundary() {
        let s = parse("SELECT a AS b FROM t ORDER BY a ASC LIMIT 3").unwrap();
        assert_eq!(
            s.select[0],
            SelectItem::Expr(Expr::Column("a".into()), Some("b".into()))
        );
        assert!(s.order_by[0].asc);
        assert_eq!(s.limit, Some(3));
    }

    #[test]
    fn order_by_column_ending_in_desc_is_ascending() {
        let s = parse("SELECT * FROM t ORDER BY is_desc").unwrap();
        assert!(s.order_by[0].asc);
        assert_eq!(s.order_by[0].column, "is_desc");
    }

    #[test]
    fn order_by_explicit_desc_is_descending() {
        let s = parse("SELECT * FROM t ORDER BY x DESC").unwrap();
        assert!(!s.order_by[0].asc);
        assert_eq!(s.order_by[0].column, "x");
    }
}
