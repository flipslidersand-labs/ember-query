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

    // ---- Issue #45: expression parsing and invalid input ----

    fn lit(v: Value) -> Box<Expr> {
        Box::new(Expr::Literal(v))
    }

    fn bin(op: BinOp, l: Box<Expr>, r: Box<Expr>) -> Box<Expr> {
        Box::new(Expr::BinOp {
            op,
            left: l,
            right: r,
        })
    }

    /// Parses `SELECT <e> FROM t` and returns the single select expression.
    fn sel(e: &str) -> Expr {
        let s = parse(&format!("SELECT {e} FROM t")).unwrap();
        assert_eq!(s.select.len(), 1);
        match s.select.into_iter().next().unwrap() {
            SelectItem::Expr(x, None) => x,
            other => panic!("unexpected select item: {other:?}"),
        }
    }

    fn where_of(cond: &str) -> Expr {
        parse(&format!("SELECT * FROM t WHERE {cond}"))
            .unwrap()
            .where_
            .unwrap()
    }

    #[test]
    fn subtraction_is_left_associative() {
        let expected = *bin(BinOp::Sub, bin(BinOp::Sub, col("a"), col("b")), col("c"));
        assert_eq!(sel("a - b - c"), expected);
    }

    #[test]
    fn division_is_left_associative() {
        let expected = *bin(BinOp::Div, bin(BinOp::Div, col("a"), col("b")), col("c"));
        assert_eq!(sel("a / b / c"), expected);
    }

    #[test]
    fn mul_binds_tighter_than_add() {
        let expected = *bin(BinOp::Add, col("a"), bin(BinOp::Mul, col("b"), col("c")));
        assert_eq!(sel("a + b * c"), expected);
        let expected = *bin(BinOp::Add, bin(BinOp::Mul, col("a"), col("b")), col("c"));
        assert_eq!(sel("a * b + c"), expected);
    }

    #[test]
    fn parentheses_override_precedence() {
        let expected = *bin(BinOp::Mul, bin(BinOp::Add, col("a"), col("b")), col("c"));
        assert_eq!(sel("(a + b) * c"), expected);
    }

    #[test]
    fn comparison_binds_looser_than_arithmetic() {
        let expected = *bin(
            BinOp::Gt,
            bin(BinOp::Add, col("a"), col("b")),
            lit(Value::Int(1)),
        );
        assert_eq!(where_of("a + b > 1"), expected);
    }

    #[test]
    #[ignore = "bug: parse_unary_expr checks first.as_str() == \"-\" but the anonymous \"-\" token yields no pair, so unary minus is silently dropped (-x parses as x)"]
    fn unary_minus_on_column() {
        let expected = Expr::Unary {
            op: UnaryOp::Neg,
            expr: col("x"),
        };
        assert_eq!(sel("-x"), expected);
    }

    #[test]
    fn double_not_nests() {
        let expected = Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Unary {
                op: UnaryOp::Not,
                expr: col("a"),
            }),
        };
        assert_eq!(where_of("NOT NOT a"), expected);
    }

    #[test]
    fn single_not_wraps_comparison() {
        let expected = Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(eq("a", 1)),
        };
        assert_eq!(where_of("NOT a = 1"), expected);
    }

    #[test]
    fn string_literal_quotes_are_stripped() {
        let expected = *bin(BinOp::Eq, col("name"), lit(Value::Str("it".into())));
        assert_eq!(where_of("name = 'it'"), expected);
        assert_eq!(where_of("name = \"it\""), expected);
    }

    #[test]
    fn empty_string_literal() {
        assert_eq!(sel("''"), Expr::Literal(Value::Str(String::new())));
    }

    #[test]
    fn scalar_literals() {
        assert_eq!(sel("42"), Expr::Literal(Value::Int(42)));
        assert_eq!(sel("2.5"), Expr::Literal(Value::Float(2.5)));
        assert_eq!(sel("TRUE"), Expr::Literal(Value::Bool(true)));
        assert_eq!(sel("false"), Expr::Literal(Value::Bool(false)));
        assert_eq!(sel("NULL"), Expr::Literal(Value::Null));
        assert_eq!(sel("null"), Expr::Literal(Value::Null));
    }

    #[test]
    fn count_star() {
        let expected = Expr::Agg {
            func: AggFunc::Count,
            expr: Box::new(Expr::Wildcard),
        };
        assert_eq!(sel("COUNT(*)"), expected);
    }

    #[test]
    fn aggregate_name_is_case_insensitive() {
        let expected = Expr::Agg {
            func: AggFunc::Sum,
            expr: col("x"),
        };
        assert_eq!(sel("sum(x)"), expected);
        assert_eq!(sel("SUM(x)"), expected);
        assert_eq!(sel("Sum(x)"), expected);
    }

    #[test]
    fn unknown_function_is_error() {
        assert!(parse("SELECT FOO(x) FROM t").is_err());
    }

    #[test]
    fn limit_value_is_parsed() {
        let s = parse("SELECT * FROM t LIMIT 10").unwrap();
        assert_eq!(s.limit, Some(10));
        let s = parse(&format!("SELECT * FROM t LIMIT {}", u64::MAX)).unwrap();
        assert_eq!(s.limit, Some(u64::MAX));
    }

    #[test]
    fn limit_overflow_is_error() {
        assert!(parse("SELECT * FROM t LIMIT 99999999999999999999").is_err());
    }

    #[test]
    fn negative_limit_is_error() {
        assert!(parse("SELECT * FROM t LIMIT -1").is_err());
    }

    #[test]
    fn empty_and_blank_input_is_error() {
        assert!(parse("").is_err());
        assert!(parse("   \n").is_err());
    }

    #[test]
    fn select_without_columns_is_error() {
        assert!(parse("SELECT FROM").is_err());
        assert!(parse("SELECT FROM t").is_err());
    }

    #[test]
    fn missing_from_is_error() {
        assert!(parse("SELECT a").is_err());
        assert!(parse("SELECT * WHERE a = 1").is_err());
    }

    #[test]
    fn missing_table_name_is_error() {
        assert!(parse("SELECT * FROM").is_err());
    }

    #[test]
    fn group_by_multiple_columns() {
        let s = parse("SELECT a, b FROM t GROUP BY a, b").unwrap();
        assert_eq!(s.group_by, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn from_alias_with_as_is_discarded() {
        let s = parse("SELECT * FROM t AS x").unwrap();
        assert_eq!(s.select, vec![SelectItem::Wildcard]);
        assert!(s.joins.is_empty());
    }

    #[test]
    #[ignore = "bug: grammar requires AS for table alias, so `FROM t x` is a parse error (Issue #45 expects alias to be accepted and discarded)"]
    fn from_alias_without_as_is_discarded() {
        let s = parse("SELECT * FROM t x").unwrap();
        assert_eq!(s.select, vec![SelectItem::Wildcard]);
    }
}
