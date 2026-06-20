# Data Model — EmberQuery

## コアデータ構造

### 型システム

```rust
pub enum DataType {
    Int64,
    Float64,
    Utf8,
    Boolean,
}

pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

pub struct Schema {
    pub fields: Vec<Field>,
}
```

### AST

```rust
pub struct SelectStmt {
    pub projections: Vec<Expr>,   // SELECT 列リスト (* or 式)
    pub from: TableRef,           // FROM 句
    pub filter: Option<Expr>,     // WHERE 句
    pub limit: Option<u64>,       // LIMIT 句
    pub group_by: Vec<Expr>,      // GROUP BY (Phase 4)
    pub order_by: Vec<OrderExpr>, // ORDER BY (Phase 5)
}

pub enum Expr {
    Column(String),
    Literal(ScalarValue),
    BinaryOp { left: Box<Expr>, op: BinaryOperator, right: Box<Expr> },
    AggCall { func: AggFunc, arg: Box<Expr> },  // Phase 4
    Wildcard,
}

pub enum BinaryOperator { Eq, Ne, Lt, Le, Gt, Ge, And, Or, Add, Sub, Mul, Div }
pub enum AggFunc { Count, Sum, Avg, Min, Max }

pub enum ScalarValue {
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Boolean(bool),
    Null,
}
```

### 論理プラン (LogicalPlan)

```rust
pub enum LogicalPlan {
    Scan   { table: String, schema: Schema, projection: Option<Vec<usize>> },
    Filter { input: Box<LogicalPlan>, predicate: Expr },
    Project { input: Box<LogicalPlan>, exprs: Vec<Expr> },
    Limit  { input: Box<LogicalPlan>, n: u64 },
    Aggregate { input: Box<LogicalPlan>, group_by: Vec<Expr>, agg_exprs: Vec<Expr> }, // Phase 4
    Sort   { input: Box<LogicalPlan>, exprs: Vec<OrderExpr> },  // Phase 5
}
```

### 物理プラン (PhysicalPlan)

```rust
pub enum PhysicalPlan {
    CsvScan   { path: PathBuf, schema: Schema, projection: Option<Vec<usize>> },
    ParquetScan { path: PathBuf, schema: Schema, predicate: Option<Expr> }, // Phase 5
    Filter    { input: Box<PhysicalPlan>, predicate: Expr },
    Project   { input: Box<PhysicalPlan>, exprs: Vec<Expr> },
    Limit     { input: Box<PhysicalPlan>, n: u64 },
    HashAggregate { input: Box<PhysicalPlan>, group_by: Vec<Expr>, agg_exprs: Vec<Expr> }, // Phase 4
    Sort      { input: Box<PhysicalPlan>, exprs: Vec<OrderExpr> }, // Phase 5
}
```

## 実行結果

```rust
// Apache Arrow RecordBatch を実行の最小単位として使う
// arrow::array::RecordBatch = Schema + 列ごとの Array
```

## 状態遷移

```
SQL文字列
  → [Lexer]  → Token列
  → [Parser] → AST (SelectStmt)
  → [Planner/LogicalPlanner] → LogicalPlan
  → [Planner/PhysicalPlanner] → PhysicalPlan
  → [Executor] → RecordBatch のストリーム
  → [Printer] → テーブル形式で標準出力
```
