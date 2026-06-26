/// A scalar value that can appear in a literal expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
}

/// Binary operators (arithmetic + comparison + logical).
#[derive(Debug, Clone, PartialEq)]
pub enum BinOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    // Logical
    And,
    Or,
}

/// Unary operators.
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Not,
    Neg,
}

/// Aggregate functions.
#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// An expression node in the AST.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Literal(Value),
    BinOp {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    /// Aggregate function call.  COUNT(*) uses Expr::Wildcard as the argument.
    Agg {
        func: AggFunc,
        expr: Box<Expr>,
    },
    /// Bare `*` — used as the argument to COUNT(*) and as a SELECT wildcard.
    Wildcard,
}

/// An item in the SELECT list.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// `SELECT *`
    Wildcard,
    /// `SELECT <expr> [AS alias]`
    Expr(Expr, Option<String>),
}

/// A parsed SELECT statement.
#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub select: Vec<SelectItem>,
    #[allow(dead_code)]
    pub from: String,
    pub where_: Option<Expr>,
    pub group_by: Vec<String>,
    pub limit: Option<u64>,
}
