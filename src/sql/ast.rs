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

/// The FROM table with its optional alias.
#[derive(Debug, Clone)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

impl TableRef {
    /// What columns are qualified with: the alias, else the table name.
    pub fn tag(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// An INNER JOIN clause.
#[derive(Debug, Clone)]
pub struct JoinClause {
    pub table: String,
    pub alias: Option<String>,
    pub on: Expr,
}

impl JoinClause {
    /// What columns are qualified with: the alias, else the table name.
    pub fn tag(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.table)
    }
}

/// One item in an ORDER BY clause.
#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub column: String,
    pub asc: bool,
}

/// A parsed SELECT statement.
#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub select: Vec<SelectItem>,
    pub from: TableRef,
    pub joins: Vec<JoinClause>,
    pub where_: Option<Expr>,
    pub group_by: Vec<String>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
}
