# Changelog

All notable changes are documented here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
No git tag or release exists yet (`Cargo.toml` version `0.1.0`), so everything is under Unreleased.
Entries are derived from merged PRs; CI, tests-only, dependency-bump and merge-queue promotion PRs are omitted.

## [Unreleased]

### Added
- `INNER JOIN ... ON` and `ORDER BY` ([#2](https://github.com/flipslidersand-labs/ember-query/pull/2)).
- Table aliases without `AS` ([#89](https://github.com/flipslidersand-labs/ember-query/pull/89)); qualified columns (`t.col`) resolved by (table, column) ([#83](https://github.com/flipslidersand-labs/ember-query/pull/83), [#91](https://github.com/flipslidersand-labs/ember-query/pull/91), [#93](https://github.com/flipslidersand-labs/ember-query/pull/93)).
- Unicode (XID) identifiers and keyword boundaries ([#100](https://github.com/flipslidersand-labs/ember-query/pull/100)); full-width space (U+3000) treated as SQL whitespace ([#115](https://github.com/flipslidersand-labs/ember-query/pull/115)); CSV headers and SQL column identifiers normalized to NFC ([#117](https://github.com/flipslidersand-labs/ember-query/pull/117)), with a hint on NFC/NFD table-file mismatch ([#120](https://github.com/flipslidersand-labs/ember-query/pull/120)).
- Three-valued (Kleene) `AND`/`OR`, `NULL` literal in boolean context ([#76](https://github.com/flipslidersand-labs/ember-query/pull/76), [#81](https://github.com/flipslidersand-labs/ember-query/pull/81)).
- Rejection of over-deep expressions and oversized SQL before parsing ([#57](https://github.com/flipslidersand-labs/ember-query/pull/57)).
- Unsupported JOIN kinds are rejected instead of running as INNER ([#113](https://github.com/flipslidersand-labs/ember-query/pull/113)); JOIN column-not-found errors list both schemas ([#114](https://github.com/flipslidersand-labs/ember-query/pull/114)).

### Changed
- Schema inference scans all rows ([#107](https://github.com/flipslidersand-labs/ember-query/pull/107)).
- Duplicate or empty CSV header names are errors ([#99](https://github.com/flipslidersand-labs/ember-query/pull/99)); Bool accepts only `true`/`false` ([#29](https://github.com/flipslidersand-labs/ember-query/pull/29)); short records are padded with `NULL` ([#25](https://github.com/flipslidersand-labs/ember-query/pull/25)).
- `SUM`/`MIN`/`MAX` preserve type; `SUM`/`AVG` on non-numeric is a type error ([#60](https://github.com/flipslidersand-labs/ember-query/pull/60)); aggregate NULL semantics follow SQL ([#16](https://github.com/flipslidersand-labs/ember-query/pull/16)); mixed Int64/Float64 arithmetic yields Float64 ([#14](https://github.com/flipslidersand-labs/ember-query/pull/14)); Float division by zero is an error like Int ([#84](https://github.com/flipslidersand-labs/ember-query/pull/84)).
- `ORDER BY`: Boolean keys supported, `NULLS LAST`, total order for NaN ([#46](https://github.com/flipslidersand-labs/ember-query/pull/46)); `MIN`/`MAX` of Float with NaN is row-order independent ([#98](https://github.com/flipslidersand-labs/ember-query/pull/98)).
- `arrow` 53.4.1 → 60.1.0 ([#68](https://github.com/flipslidersand-labs/ember-query/pull/68)).

### Fixed
- Aggregate queries dropped SELECT items and ordering ([#74](https://github.com/flipslidersand-labs/ember-query/pull/74)); `ORDER BY` alias in aggregate queries ([#88](https://github.com/flipslidersand-labs/ember-query/pull/88)).
- Unary minus was discarded ([#77](https://github.com/flipslidersand-labs/ember-query/pull/77)); `i64::MIN` negation now reports integer overflow ([#48](https://github.com/flipslidersand-labs/ember-query/pull/48)).
- `ORDER BY` direction misdetected for column names ending in `desc` ([#47](https://github.com/flipslidersand-labs/ember-query/pull/47)); `ORDER BY` right after `WHERE` ([#28](https://github.com/flipslidersand-labs/ember-query/pull/28)).
- `WHERE NULL` now yields 0 rows ([#85](https://github.com/flipslidersand-labs/ember-query/pull/85)).
- Table layout broke beyond 1024 rows ([#73](https://github.com/flipslidersand-labs/ember-query/pull/73)); full-width characters misaligned columns ([#90](https://github.com/flipslidersand-labs/ember-query/pull/90)).
- JOIN: NULL / mistyped keys no longer match ([#62](https://github.com/flipslidersand-labs/ember-query/pull/62)); unique `right_` column names for 3+ table self-joins ([#112](https://github.com/flipslidersand-labs/ember-query/pull/112)); schema-inference errors propagate ([#59](https://github.com/flipslidersand-labs/ember-query/pull/59)).
- Float64 `GROUP BY` keys keep their type in output ([#24](https://github.com/flipslidersand-labs/ember-query/pull/24)).
