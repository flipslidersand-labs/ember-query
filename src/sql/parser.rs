use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;

use super::ast::*;
use anyhow::{anyhow, Result};

#[derive(Parser)]
#[grammar = "sql/grammar.pest"]
pub struct SqlParser;

/// Entry point — parses a SQL string into a `SelectStmt`.
pub fn parse(sql: &str) -> Result<SelectStmt> {
    let mut pairs = SqlParser::parse(Rule::query, sql)
        .map_err(|e| anyhow!("Parse error: {}", e))?;

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

    // table_name
    let table = inner.next().ok_or_else(|| anyhow!("missing table_name"))?;
    let from = table.as_str().to_string();

    let mut where_ = None;
    let mut group_by = Vec::new();
    let mut limit = None;

    for p in inner {
        match p.as_rule() {
            Rule::where_clause => {
                let expr_pair = p.into_inner().next().ok_or_else(|| anyhow!("empty WHERE"))?;
                where_ = Some(parse_expr(expr_pair)?);
            }
            Rule::group_by_clause => {
                for col in p.into_inner() {
                    if col.as_rule() == Rule::ident {
                        group_by.push(col.as_str().to_string());
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
        from,
        where_,
        group_by,
        limit,
    })
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
    let inner = pair.into_inner().next().ok_or_else(|| anyhow!("empty select_item"))?;
    match inner.as_rule() {
        Rule::wildcard => Ok(SelectItem::Wildcard),
        Rule::aliased_expr => {
            let mut parts = inner.into_inner();
            let expr_pair = parts.next().ok_or_else(|| anyhow!("missing expr in aliased_expr"))?;
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
            let inner = pair.into_inner().next().ok_or_else(|| anyhow!("empty expr"))?;
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
    let left = parse_expr(children.next().ok_or_else(|| anyhow!("empty cmp_expr lhs"))?)?;

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
        let right = parse_expr(children.next().ok_or_else(|| anyhow!("missing cmp_expr rhs"))?)?;
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
        let right = parse_expr(children.next().ok_or_else(|| anyhow!("missing add_expr rhs"))?)?;
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
        let right = parse_expr(children.next().ok_or_else(|| anyhow!("missing mul_expr rhs"))?)?;
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
        let inner = children.next().ok_or_else(|| anyhow!("missing unary_expr operand"))?;
        Ok(Expr::Unary {
            op: UnaryOp::Neg,
            expr: Box::new(parse_expr(inner)?),
        })
    } else {
        parse_expr(first)
    }
}

fn parse_primary(pair: Pair<Rule>) -> Result<Expr> {
    let inner = pair.into_inner().next().ok_or_else(|| anyhow!("empty primary"))?;
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
    let name_pair = children.next().ok_or_else(|| anyhow!("missing func_name"))?;
    let func = match name_pair.as_str().to_uppercase().as_str() {
        "COUNT" => AggFunc::Count,
        "SUM" => AggFunc::Sum,
        "AVG" => AggFunc::Avg,
        "MIN" => AggFunc::Min,
        "MAX" => AggFunc::Max,
        n => return Err(anyhow!("unknown function: {}", n)),
    };

    let args_pair = children.next().ok_or_else(|| anyhow!("missing func_args"))?;
    let arg_inner = args_pair.into_inner().next().ok_or_else(|| anyhow!("empty func_args"))?;

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
    let inner = pair.into_inner().next().ok_or_else(|| anyhow!("empty literal"))?;
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
