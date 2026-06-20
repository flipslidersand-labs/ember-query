# Spec — EmberQuery

## プロジェクトの目的

CSV・JSON Lines・Parquet に対して SQL を実行できる、Rust 製の組み込み分析エンジン。
外部 DB 不要で、プロセス内に埋め込んで使える軽量クエリエンジンを実装する。

## 解決する問題

| 問題                                 | EmberQuery での解決策                          |
| ------------------------------------ | ---------------------------------------------- |
| 分析のたびに外部 DB を立ち上げる手間 | ライブラリとして埋め込み、ファイルを直接クエリ |
| Pandas では列指向最適化が効かない    | Arrow RecordBatch + 列指向実行で高速集計       |
| SQL パイプラインの学習コストが高い   | 自前実装でレイヤーを完全に理解する             |

## MVP の境界線

### やること (Phase 1〜4)

- SQL の字句解析 → AST 生成
- 論理プラン → 物理プラン変換
- CSV ファイルの読み込みと実行
- `SELECT`, `FROM`, `WHERE`, `LIMIT` のサポート
- 整数・文字列・真偽値の型システム
- 単一スレッド実行
- CLI: `ember-query --input file.csv --sql "SELECT ..."`

### やらないこと (Phase 1)

- `GROUP BY` / `ORDER BY` / `JOIN`
- Parquet・JSON Lines
- 並列実行 (Rayon)
- コスト最適化
- サブクエリ・CTE・ウィンドウ関数

## ユーザーが使うコマンド

```bash
# Phase 1 MVP
ember-query --input vehicles.csv --sql "SELECT maker, price FROM data WHERE price > 3000000 LIMIT 10"

# Phase 4 以降
ember-query --input vehicles.parquet --sql "SELECT maker, AVG(price) FROM data GROUP BY maker"

# EXPLAIN
ember-query --explain --input file.csv --sql "SELECT ..."
```

## 成功条件

| Phase   | 完成条件                                                 |
| ------- | -------------------------------------------------------- |
| Phase 1 | Lexer + Parser が SQL を AST に変換し、cargo test 全通過 |
| Phase 2 | AST → 論理プラン → 物理プラン の変換が完了               |
| Phase 3 | CSV を読んで SELECT + WHERE + LIMIT が CLI で動く        |
| Phase 4 | GROUP BY + AVG/COUNT が正しく動く                        |
| Phase 5 | Parquet 読み込み + Predicate Pushdown が有効             |
| Phase 6 | Rayon による並列集計で Phase 4 より高速化                |
