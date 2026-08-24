# ember-query

Vectorized SQL query engine for lightweight analytical workloads on CSV and Parquet files (Rust).

CSV / Parquet ファイル向けの軽量分析ワークロード用ベクトル化 SQL クエリエンジン。

> 🟡 **Scaffold Phase** — 基本設計と MVP 実装準備中

## Status / ステータス

| Item | State | 状態 |
|------|------|------|
| Phase / フェーズ | Scaffold | Scaffold |
| MVP Start / 実装予定 | Q3 2026 | 2026-Q3 MVP開始 |
| Tests / テスト | Not yet | 未実装 |
| Docs / ドキュメント | Planned | 企画中 |

## Tech Stack / 技術スタック

- **Language / 言語:** Rust
- **Build / ビルドツール:** Cargo
- **Target formats / 対象形式:** CSV, Parquet

## Roadmap / 実装ロードマップ

1. **Phase 1 (Q3 2026)** — SQL parser + scan layer / SQL パーサー + スキャン層
2. **Phase 2 (Q4 2026)** — Vectorized execution engine + embedded index / ベクトル化実行エンジン + 組み込みインデックス
3. **Phase 3 (Q1 2027)** — Query optimizer + aggregations / クエリオプティマイザ + 集計
4. **Phase 4 (Q2 2027)** — Benchmarking + docs / ベンチマーク + ドキュメント

## Notes / 注意事項

- **In Development / 開発中**: API/design may change without notice / API・設計は予告なく変更される可能性があります
- **No tests yet / テスト未実装**: Quality not guaranteed until Phase 2 / 本格実装まで品質保証していません

---

Track progress in [GitHub Issues](https://github.com/flipslidersand/ember-query/issues).
