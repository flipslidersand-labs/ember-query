# ember-query

Release type: `library`

Small vectorized SQL query engine for lightweight analytical workloads on CSV files (Rust).

CSV ファイル向けの軽量分析ワークロード用ベクトル化 SQL クエリエンジン。

> 🟡 **In Development** — 基本機能は動作。API・設計は変更される可能性あり

## Status / ステータス

| Item | State | 状態 |
|------|------|------|
| Phase / フェーズ | MVP in progress | MVP 実装中 |
| Tests / テスト | Unit + E2E (`cargo test`) | 単体・E2E テストあり |
| Parquet | Not supported (planned) | 未対応(計画中) |

## Supported SQL / 対応する SQL

- `SELECT` (`*`, 式, `AS` 別名) / `FROM <表> [別名]`
- `WHERE` (`AND` `OR` `NOT`, `= <> != < <= > >=`, `+ - * /`)
- `GROUP BY` + 集計 `COUNT` `SUM` `AVG` `MIN` `MAX`
- `ORDER BY ... [ASC|DESC]`, `LIMIT n`
- `[INNER] JOIN ... ON <式>` (INNER JOIN のみ。LEFT/OUTER は未対応)

Limits / 制約:
- Input is one CSV (schema inferred) / 入力は CSV 1 ファイル(スキーマは自動推定)
- Float64 推論は有限の十進表記(数字・小数点・指数・符号)のみ。`nan`/`inf`/`infinity` は Float と見なさず、その列は Utf8 になる / Float inference accepts finite decimals only; `nan`/`inf`/`infinity` columns stay Utf8
- 端末への出力(表のセル・列名・エラー文・`--explain`)では制御文字(C0/DEL/C1。ESC・改行・タブ・CR を含む)を `\x1b` `\n` `\t` のように可視表記へエスケープする(バックスラッシュ自体は変換しない) / Control characters (C0/DEL/C1, incl. ESC, newline, tab, CR) are escaped (e.g. `\x1b`, `\n`) in terminal output
- 1 レコード(クォート内改行を含む)は 16 MiB まで、列数は 10000 まで。超えるとエラー / A record (incl. quoted newlines) is limited to 16 MiB and a header to 10000 columns; larger input is an error
- JOIN 先は `<表名>.csv` を入力 CSV と同じディレクトリから探す(無ければエラー)
- JOIN 先の `<表名>.csv` がシンボリックリンクならリンク先をそのまま読む。入力ディレクトリは信頼できる(他者が書き込めない)ものを前提とする / A JOIN `<table>.csv` that is a symlink is followed as-is; the input directory is assumed trusted (not writable by others)
- JOIN で列名が重なると右側は `right_` 接頭辞になる(例: `right_name`)
- 列名・別名は Unicode NFC に正規化して照合するが、JOIN の表名は `{table}.csv` のファイル名解決に使うため正規化しない(NFC/NFD が異なるファイルは `not found in catalog` となり、該当ファイルがあればヒントを表示) / Table names are not normalized (file lookup); a differing NFC/NFD file name yields `not found in catalog` with a hint

## Usage / 使い方

```bash
cargo run -- --input tests/fixtures/orders.csv \
  --sql "SELECT region, COUNT(*), SUM(qty) FROM orders GROUP BY region"
# region | agg_1 | agg_2
# east   | 3     | 12
# west   | 3     | 11

cargo run -- --input tests/fixtures/t1.csv \
  --sql "SELECT t1.v, right_name FROM t1 INNER JOIN t2 ON t1.id = t2.t1_id"
```

`--explain` prints the logical plan instead of running / 実行せず論理プランを表示。

```bash
cargo test   # unit + E2E tests / テスト実行
```

## Tech Stack / 技術スタック

- **Language / 言語:** Rust
- **Build / ビルドツール:** Cargo
- **Input format / 入力形式:** CSV (Parquet: planned / 計画中)

## Roadmap / 実装ロードマップ

1. **Phase 1 (Q3 2026)** — SQL parser + scan layer / SQL パーサー + スキャン層
2. **Phase 2 (Q4 2026)** — Vectorized execution engine + embedded index / ベクトル化実行エンジン + 組み込みインデックス
3. **Phase 3 (Q1 2027)** — Query optimizer + aggregations (aggregations done / 集計は実装済み) / クエリオプティマイザ + 集計
4. **Phase 4 (Q2 2027)** — Benchmarking + docs / ベンチマーク + ドキュメント

---

Docs / ドキュメント: [CHANGELOG](CHANGELOG.md) · [spec](docs/spec.md) · [data model](docs/data-model.md) · [tech stack](docs/tech-stack.md) · [ADRs](docs/adr/)

Track progress in [GitHub Issues](https://github.com/flipslidersand-labs/ember-query/issues).
