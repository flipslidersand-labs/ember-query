# Implementation Guide — EmberQuery

## Phase 1: Lexer + Parser (1〜2週)

### 実装内容

- `src/sql/lexer.rs` — キーワード・識別子・リテラル・記号をトークン列に変換
- `src/sql/parser.rs` — トークン列を `SelectStmt` AST に変換 (pest PEG 文法)
- `src/sql/grammar.pest` — SQL サブセットの文法定義

### 完成条件

```bash
cargo test sql::parser   # SELECT/FROM/WHERE/LIMIT パーステスト全通過
```

### 難所

- `WHERE a > 1 AND b = 'x'` の演算子優先順位 → pest の `_precedence_climbing` ルールを使う
- 文字列リテラルのエスケープ (`''`)

---

## Phase 2: 論理プラン生成 (1週)

### 実装内容

- `src/planner/logical.rs` — `SelectStmt` → `LogicalPlan` ツリー
- `src/catalog.rs` — テーブル名 → スキーマのルックアップ

### 完成条件

```bash
cargo test planner::logical   # AST → LogicalPlan 変換テスト全通過
```

---

## Phase 3: 物理プラン + CSV 実行 (1〜2週)

### 実装内容

- `src/planner/physical.rs` — `LogicalPlan` → `PhysicalPlan`
- `src/executor/csv_scan.rs` — CSV を Arrow RecordBatch に変換
- `src/executor/filter.rs` — WHERE 述語の評価
- `src/executor/project.rs` — SELECT 列の射影
- `src/executor/limit.rs` — LIMIT n 行で打ち切り
- `src/main.rs` — CLI エントリポイント

### 完成条件

```bash
# 実際の CSV に対してクエリが動く
ember-query --input tests/fixtures/vehicles.csv \
  --sql "SELECT maker, price FROM data WHERE price > 3000000 LIMIT 5"
```

### 難所

- Arrow Array の型ディスパッチ (Int64Array / StringArray 等) → `downcast_array!` マクロを活用
- NULL 値の扱い → Arrow の validity bitmap

---

## Phase 4: GROUP BY + 集計 (1〜2週)

### 実装内容

- `src/executor/hash_agg.rs` — HashMap によるグループ別集計
- COUNT / SUM / AVG / MIN / MAX の実装

### 完成条件

```bash
ember-query --input tests/fixtures/vehicles.csv \
  --sql "SELECT maker, COUNT(*), AVG(price) FROM data GROUP BY maker"
```

---

## Phase 5: Parquet + Predicate Pushdown (1〜2週)

### 実装内容

- `src/executor/parquet_scan.rs` — Parquet を Arrow RecordBatch に変換
- `src/optimizer/predicate_pushdown.rs` — WHERE 述語をスキャンまで押し下げる
- `src/optimizer/projection_pushdown.rs` — 不要な列を読まない

### 完成条件

```bash
ember-query --input tests/fixtures/vehicles.parquet \
  --sql "SELECT maker, AVG(price) FROM data GROUP BY maker"
# Parquet の行グループフィルタが効いていること (benches で確認)
```

---

## Phase 6: 並列実行 (Rayon) (1週)

### 実装内容

- `src/executor/parallel_agg.rs` — Rayon `par_iter` で RecordBatch を並列集計
- `benches/group_by.rs` — Phase 4 vs Phase 6 の速度比較

### 完成条件

```bash
cargo bench group_by
# Phase 6 が Phase 4 より高速 (4コア環境で概ね2倍以上)
```

---

## 実装順序の根拠

Lexer → Parser → 論理プラン → 物理プラン → CSV実行の順にビルドすることで、
各レイヤーを単体テストしながら積み上げられる。GROUP BY は Arrow の型システムへの
慣れが必要なため Phase 4 に据え置き、Parquet は Arrow との親和性が高いため Phase 5 で追加。
