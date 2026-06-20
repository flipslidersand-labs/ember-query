# Tech Stack — EmberQuery

## 言語・バージョン

- Rust 1.78+ (edition 2021)

## 主要クレートと選定理由

| クレート    | バージョン | 役割                             | 選定理由                         |
| ----------- | ---------- | -------------------------------- | -------------------------------- |
| `arrow`     | 53         | 列指向メモリ表現 (RecordBatch)   | de facto standard, SIMD対応済み  |
| `parquet`   | 53         | Parquet 読み込み                 | arrow と同一リリースサイクル     |
| `pest`      | 2          | SQL 字句解析・構文解析 (PEG文法) | 文法ファイルが分離でき可読性高い |
| `rayon`     | 1          | データ並列処理                   | Rust で最も自然なデータ並列API   |
| `criterion` | 0.5        | ベンチマーク                     | 統計的に正確な計測               |
| `clap`      | 4          | CLI                              | derive マクロで簡潔              |
| `anyhow`    | 1          | エラーハンドリング               | ライブラリ境界以外では十分       |
| `csv`       | 1          | CSV 読み込み                     | 実績あり、Arrow との変換も容易   |

## アーキテクチャ依存関係

```
CLI (clap)
  └── Engine
        ├── Parser (pest) → AST
        ├── Planner         AST → LogicalPlan → PhysicalPlan
        └── Executor        PhysicalPlan → RecordBatch (arrow)
              ├── CsvScan (csv crate)
              ├── ParquetScan (parquet crate)  [Phase 5]
              └── Parallel (rayon)             [Phase 6]
```

## ビルドツール・実行環境

- `cargo` (ビルド・テスト・ベンチ)
- `cargo-nextest` (高速テスト実行、オプション)

## 開発ツール

| ツール             | 用途                           |
| ------------------ | ------------------------------ |
| `clippy`           | linting                        |
| `rustfmt`          | フォーマット                   |
| `criterion`        | ベンチマーク                   |
| `cargo-flamegraph` | プロファイリング (Phase 5以降) |
