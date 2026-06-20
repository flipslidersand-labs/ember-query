# ADR-003: 実行モデルに Apache Arrow RecordBatch を使う

- **日付**: 2026-06-20
- **状態**: Accepted

## 背景

SQL エンジンの実行モデルとして、行単位 (Volcano/Iterator モデル) と列指向バッチ処理の2択がある。

## 決定

Apache Arrow `RecordBatch` を実行の最小単位とする列指向バッチモデルを採用する。

## 理由

- Arrow は SIMD 最適化済みの列指向配列を提供し、集計処理が行モデルの数十倍速い
- Parquet クレートが Arrow と直接統合しており、変換コストがない
- 将来的に DataFusion や Polars との統合・比較が可能になる
- 業界標準フォーマットなので学習価値が高い

## トレードオフ

- Volcano モデルより実装が複雑（型ディスパッチ・validity bitmap の管理が必要）
- 小規模データでは行モデルより速度が出ない場合がある
