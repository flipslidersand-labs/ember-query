use anyhow::Result;
use arrow::datatypes::DataType;
use std::path::Path;

/// 1 レコード(クォート内改行を含む)の最大バイト数。
pub(crate) const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
/// 列数の上限。
pub(crate) const MAX_COLUMNS: usize = 10_000;

/// `BoundedRead` が追う、csv クレートと同じ規則の簡易クォート状態。
#[derive(Clone, Copy, PartialEq)]
enum QState {
    /// 欄頭(行頭または `,` の直後)。ここでの `"` だけがクォートを開く。
    FieldStart,
    /// 未クォート欄の途中。`"` は普通の文字。
    Unquoted,
    /// クォート欄の中。
    Quoted,
    /// クォート欄の中で `"` を 1 つ読んだ直後(`""` なら継続、他なら閉じ)。
    QuoteSeen,
}

/// 1 レコードのバイト数を `limit` までに制限する Read ラッパ。レコード境界は
/// クォート外の `\n` / `\r`(csv と同じく欄頭の `"` でだけクォートが開く)。
/// 超えたら `InvalidData` を返し、csv クレートが巨大な 1 レコードを
/// バッファする前に打ち切る。
pub(crate) struct BoundedRead<R> {
    inner: R,
    limit: usize,
    state: QState,
    run: usize,
}

impl<R: std::io::Read> std::io::Read for BoundedRead<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        for &b in &buf[..n] {
            use QState::*;
            let (next, boundary) = match (self.state, b) {
                (FieldStart, b'"') => (Quoted, false),
                (Quoted, b'"') => (QuoteSeen, false),
                (Quoted, _) => (Quoted, false),
                (QuoteSeen, b'"') => (Quoted, false),
                (_, b'\n' | b'\r') => (FieldStart, true),
                (_, b',') => (FieldStart, false),
                _ => (Unquoted, false),
            };
            self.state = next;
            if boundary {
                self.run = 0;
                continue;
            }
            self.run += 1;
            if self.run > self.limit {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("record exceeds the maximum size of {} bytes", self.limit),
                ));
            }
        }
        Ok(n)
    }
}

/// 入力 CSV を開く(1 レコードのサイズ上限つき)。infer と scan で共通。
pub(crate) fn open_reader(path: &Path) -> std::io::Result<csv::Reader<BoundedRead<std::fs::File>>> {
    open_reader_with_limit(path, MAX_RECORD_BYTES)
}

pub(crate) fn open_reader_with_limit(
    path: &Path,
    limit: usize,
) -> std::io::Result<csv::Reader<BoundedRead<std::fs::File>>> {
    let file = std::fs::File::open(path)?;
    Ok(csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(BoundedRead {
            inner: file,
            limit,
            state: QState::FieldStart,
            run: 0,
        }))
}

/// Column metadata.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

/// Schema inferred from a CSV file's header + every data row.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnDef>,
}

/// Infer the schema of a CSV file by reading the header row and scanning every
/// data row once.  Type priority: bool → i64 → f64 → String.
pub fn infer_schema(path: &Path) -> Result<TableSchema> {
    let mut reader = open_reader(path)?;

    let headers: Vec<String> = reader.headers()?.iter().map(crate::sql::nfc).collect();
    if headers.len() > MAX_COLUMNS {
        anyhow::bail!(
            "too many columns ({}, maximum {}) in header of {}",
            headers.len(),
            MAX_COLUMNS,
            path.display()
        );
    }
    let line_offset = physical_line_offset(&reader);

    // 空名・重複名は列が黙って参照不能になるためエラー(大文字小文字違いは別名)。
    let mut seen: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for (i, h) in headers.iter().enumerate() {
        if h.is_empty() {
            anyhow::bail!(
                "empty column name at column {} in header of {}",
                i + 1,
                path.display()
            );
        }
        if let Some(first) = seen.insert(h.as_str(), i + 1) {
            anyhow::bail!(
                "duplicate column name '{}' (column {} and column {}) in header of {}",
                h,
                first,
                i + 1,
                path.display()
            );
        }
    }

    // Scan every row once, keeping only per-column flags (no records retained),
    // so the inferred type always matches what the scan will parse.
    let mut states = vec![ColState::default(); headers.len()];
    for result in reader.records() {
        let record = result?;
        if record.len() > headers.len() {
            anyhow::bail!(
                "line {}: record has {} fields, expected at most {}",
                record_line(&record, line_offset),
                record.len(),
                headers.len()
            );
        }
        for (state, val) in states.iter_mut().zip(record.iter()) {
            state.observe(val);
        }
    }

    let mut columns = Vec::with_capacity(headers.len());
    for (name, state) in headers.iter().zip(states.iter()) {
        columns.push(ColumnDef {
            name: name.clone(),
            data_type: state.data_type(),
        });
    }

    Ok(TableSchema { columns })
}

/// csv crate は CRLF ファイルでヘッダ直後の行番号を 1 小さく数える。ヘッダ読了直後の
/// 位置(LF なら 2)との差を補正値として返す。`headers()` を読んだ後に呼ぶこと。
pub(crate) fn physical_line_offset<R: std::io::Read>(reader: &csv::Reader<R>) -> u64 {
    2u64.saturating_sub(reader.position().line())
}

/// レコード開始位置の物理行番号(ヘッダ=1)。
pub(crate) fn record_line(record: &csv::StringRecord, line_offset: u64) -> u64 {
    record.position().map(|p| p.line()).unwrap_or(0) + line_offset
}

/// Bool として受理する表記は `true`/`false`(大文字小文字不問)のみ。型推論とスキャンで共通。
pub(crate) fn parse_bool_str(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn is_strict_bool_str(s: &str) -> bool {
    parse_bool_str(s).is_some()
}

/// Float 推論・スキャン共通のパース。有限の十進表記のみ受理し、nan/inf/infinity は拒否する。
pub fn parse_finite_float(s: &str) -> Option<f64> {
    if !s
        .bytes()
        .all(|b| matches!(b, b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-'))
    {
        return None;
    }
    s.parse::<f64>().ok().filter(|v| v.is_finite())
}

#[derive(Clone)]
struct ColState {
    can_be_int: bool,
    can_be_float: bool,
    can_be_bool: bool,
    has_non_empty: bool,
}

impl Default for ColState {
    fn default() -> Self {
        ColState {
            can_be_int: true,
            can_be_float: true,
            can_be_bool: true,
            has_non_empty: false,
        }
    }
}

impl ColState {
    fn observe(&mut self, val: &str) {
        if val.is_empty() {
            return; // null — doesn't constrain type
        }
        self.has_non_empty = true;
        if self.can_be_bool && !is_strict_bool_str(val) {
            self.can_be_bool = false;
        }
        if self.can_be_int && val.parse::<i64>().is_err() {
            self.can_be_int = false;
        }
        if self.can_be_float && parse_finite_float(val).is_none() {
            self.can_be_float = false;
        }
    }

    fn data_type(&self) -> DataType {
        if !self.has_non_empty {
            return DataType::Utf8;
        }
        // Bool takes priority over int (1/0 values would pass int check but prefer bool if only bools present)
        if self.can_be_bool {
            DataType::Boolean
        } else if self.can_be_int {
            DataType::Int64
        } else if self.can_be_float {
            DataType::Float64
        } else {
            DataType::Utf8
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TmpCsv(PathBuf);

    impl TmpCsv {
        fn new(content: &str) -> Self {
            static N: AtomicUsize = AtomicUsize::new(0);
            let p = std::env::temp_dir().join(format!(
                "ember_query_catalog_{}_{}.csv",
                std::process::id(),
                N.fetch_add(1, Ordering::SeqCst)
            ));
            std::fs::write(&p, content).unwrap();
            TmpCsv(p)
        }
    }

    impl Drop for TmpCsv {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn types(content: &str) -> Vec<DataType> {
        let f = TmpCsv::new(content);
        infer_schema(&f.0)
            .unwrap()
            .columns
            .into_iter()
            .map(|c| c.data_type)
            .collect()
    }

    #[test]
    fn basic_type_priority() {
        let t = types("i,f,s,b\n1,1.5,a,true\n2,2,b,FALSE\n");
        assert_eq!(
            t,
            vec![
                DataType::Int64,
                DataType::Float64,
                DataType::Utf8,
                DataType::Boolean
            ]
        );
    }

    #[test]
    fn nan_inf_only_columns_are_utf8() {
        let t = types("a,b,c\nNaN,inf,Infinity\nnan,-inf,+INFINITY\n");
        assert_eq!(t, vec![DataType::Utf8; 3]);
    }

    #[test]
    fn float_mixed_with_nan_is_utf8() {
        let t = types("a\n1.5\nNaN\n");
        assert_eq!(t, vec![DataType::Utf8]);
    }

    #[test]
    fn exponent_and_signed_decimals_are_float64() {
        let t = types("a,b,c\n1e3,-.5,+2.\n1.5E-2,3,4\n");
        assert_eq!(t, vec![DataType::Float64; 3]);
    }

    #[test]
    fn parse_finite_float_rejects_non_decimal() {
        for s in ["nan", "inf", "-Infinity", "1e999", "", " 1", "0x10", "1_0"] {
            assert!(parse_finite_float(s).is_none(), "{s}");
        }
        assert_eq!(parse_finite_float("1e3"), Some(1000.0));
    }

    #[test]
    fn all_empty_column_is_utf8() {
        let t = types("a,b\n1,\n2,\n");
        assert_eq!(t, vec![DataType::Int64, DataType::Utf8]);
    }

    #[test]
    fn header_only_gives_utf8_columns_with_names() {
        let f = TmpCsv::new("x,y\n");
        let ts = infer_schema(&f.0).unwrap();
        assert_eq!(ts.columns.len(), 2);
        assert_eq!(ts.columns[0].name, "x");
        assert_eq!(ts.columns[1].name, "y");
        assert!(ts.columns.iter().all(|c| c.data_type == DataType::Utf8));
    }

    #[test]
    fn nulls_do_not_constrain_type() {
        assert_eq!(types("n\n\n5\n\n"), vec![DataType::Int64]);
    }

    #[test]
    fn numeric_one_zero_is_int_not_bool() {
        assert_eq!(types("n\n1\n0\n1\n"), vec![DataType::Int64]);
    }

    #[test]
    fn int_and_float_mix_is_float() {
        assert_eq!(types("n\n1\n2.5\n"), vec![DataType::Float64]);
    }

    fn n_rows_then(n: usize, tail: &str) -> String {
        let mut s = String::from("n\n");
        for i in 0..n {
            s.push_str(&format!("{}\n", i));
        }
        s.push_str(tail);
        s
    }

    #[test]
    fn text_after_row_100_makes_utf8() {
        assert_eq!(types(&n_rows_then(100, "N/A\n")), vec![DataType::Utf8]);
        assert_eq!(types(&n_rows_then(5000, "N/A\n")), vec![DataType::Utf8]);
    }

    #[test]
    fn decimal_after_row_100_makes_float() {
        assert_eq!(types(&n_rows_then(150, "2.5\n")), vec![DataType::Float64]);
    }

    #[test]
    fn all_ints_beyond_100_rows_stay_int() {
        assert_eq!(types(&n_rows_then(1000, "")), vec![DataType::Int64]);
    }

    #[test]
    fn too_many_fields_after_row_100_reports_line() {
        let mut s = String::from("a,b\n");
        for _ in 0..149 {
            s.push_str("1,2\n");
        }
        s.push_str("1,2,3\n");
        let f = TmpCsv::new(&s);
        let msg = infer_schema(&f.0).unwrap_err().to_string();
        assert!(msg.contains("line 151"), "got: {}", msg);
        assert!(msg.contains("3 fields"), "got: {}", msg);
    }

    #[test]
    fn duplicate_header_is_error() {
        let f = TmpCsv::new("id,name,id\n1,a,2\n");
        let err = infer_schema(&f.0).unwrap_err().to_string();
        assert!(err.contains("duplicate"), "{err}");
        assert!(err.contains("'id'"), "{err}");
        assert!(err.contains("column 3"), "{err}");
        assert!(err.contains(&f.0.display().to_string()), "{err}");
    }

    #[test]
    fn empty_header_is_error() {
        let f = TmpCsv::new("a,,c\n1,2,3\n");
        let err = infer_schema(&f.0).unwrap_err().to_string();
        assert!(err.contains("empty"), "{err}");
        assert!(err.contains("column 2"), "{err}");
        assert!(err.contains(&f.0.display().to_string()), "{err}");
    }

    #[test]
    fn headers_differing_only_in_case_are_ok() {
        let f = TmpCsv::new("Id,id\n1,2\n");
        let ts = infer_schema(&f.0).unwrap();
        assert_eq!(ts.columns[0].name, "Id");
        assert_eq!(ts.columns[1].name, "id");
    }

    #[test]
    fn nfd_header_is_normalized_to_nfc() {
        let f = TmpCsv::new("\u{30AB}\u{3099},b\n1,2\n");
        let schema = infer_schema(&f.0).unwrap();
        assert_eq!(schema.columns[0].name, "\u{30AC}");
    }

    #[test]
    fn distinct_headers_unchanged() {
        let f = TmpCsv::new("a,b,c\n1,2,3\n");
        let ts = infer_schema(&f.0).unwrap();
        let names: Vec<&str> = ts.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["a", "b", "c"]);
    }

    #[test]
    fn oversized_record_without_newline_is_error() {
        // /dev/zero 相当: 改行のない巨大入力はメモリを食い尽くす前にエラーにする。
        let f = TmpCsv::new(&"x".repeat(MAX_RECORD_BYTES + 1));
        let err = infer_schema(&f.0).unwrap_err().to_string();
        assert!(err.contains("record"), "{err}");
    }

    #[test]
    fn oversized_quoted_multiline_record_is_error() {
        let mut s = String::from("a\n\"");
        s.push_str(&"line\n".repeat(MAX_RECORD_BYTES / 5 + 1));
        s.push_str("\"\n");
        let f = TmpCsv::new(&s);
        assert!(infer_schema(&f.0).is_err());
    }

    #[test]
    fn many_short_lines_are_not_limited() {
        // 上限はレコード単位。総サイズでは弾かない。
        let f = TmpCsv::new(&format!("a\n{}", "1\n".repeat(MAX_RECORD_BYTES / 2 + 1)));
        assert!(infer_schema(&f.0).is_ok());
    }

    #[test]
    fn too_many_columns_is_error() {
        let header = vec!["c"; MAX_COLUMNS + 1]
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{c}{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let f = TmpCsv::new(&format!("{header}\n"));
        let err = infer_schema(&f.0).unwrap_err().to_string();
        assert!(err.contains("columns"), "{err}");
    }

    fn count_records(content: &str, limit: usize) -> Result<usize> {
        let f = TmpCsv::new(content);
        let mut r = open_reader_with_limit(&f.0, limit)?;
        let mut n = 0;
        for rec in r.records() {
            rec?;
            n += 1;
        }
        Ok(n)
    }

    #[test]
    fn stray_quote_in_unquoted_field_does_not_open_quote() {
        // 欄途中の `"` はクォートを開かない(csv クレートと同じ)。全体が上限超でも読める。
        let body = format!("a,b\n5\" pipe,1\n{}", "1,2\n".repeat(100));
        assert_eq!(count_records(&body, 100).unwrap(), 101);
    }

    #[test]
    fn quoted_field_with_escaped_quote_and_newline_is_one_record() {
        let body = "a,b\n\"x\"\"\ny\",1\n1,2\n1,2\n1,2\n1,2\n";
        assert_eq!(count_records(body, 12).unwrap(), 5);
    }

    #[test]
    fn record_limit_boundary_is_exact() {
        // "a\n" の後、改行を除いてちょうど limit バイトの行は可、+1 はエラー。
        assert_eq!(count_records("a\n12345678\n", 8).unwrap(), 1);
        assert!(count_records("a\n123456789\n", 8).is_err());
    }

    #[test]
    fn stray_quote_in_header_does_not_let_next_record_escape_limit() {
        // 見出し `a"` の `"` は普通の文字。次レコード頭の `"` はクォートを開き、
        // 閉じないまま続くので上限超過でエラーになる(上限回避にならない)。
        let body = format!("a\"\n\"{}", "x\n".repeat(100));
        assert!(count_records(&body, 20).is_err());
    }

    #[test]
    fn lone_cr_is_record_boundary() {
        // 単独 CR でも run が 0 に戻る。上限ちょうどの記録が CR 区切りで続いても通る。
        assert_eq!(
            count_records("a\r12345678\r12345678\r12345678", 8).unwrap(),
            3
        );
        assert!(count_records("a\r12345678\r123456789\r1", 8).is_err());
    }

    /// 1 回の read で 1 バイトしか返さない Read ラッパ。
    struct OneByte<R>(R);

    impl<R: std::io::Read> std::io::Read for OneByte<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = buf.len().min(1);
            self.0.read(&mut buf[..n])
        }
    }

    fn count_records_one_byte(content: &str, limit: usize) -> Result<usize> {
        let inner = OneByte(std::io::Cursor::new(content.as_bytes().to_vec()));
        let mut r = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(BoundedRead {
                inner,
                limit,
                state: QState::FieldStart,
                run: 0,
            });
        let mut n = 0;
        for rec in r.records() {
            rec?;
            n += 1;
        }
        Ok(n)
    }

    #[test]
    fn escaped_quote_across_read_boundary_continues_quote() {
        // `""` が read 境界で分かれても、クォートは継続する。
        // 継続するので、クォート内の改行は境界にならず上限超過になる。
        let long = format!("a\n\"x\"\"{}\"\n", "y\n".repeat(20));
        assert!(count_records_one_byte(&long, 10).is_err());
        // 上限内なら 1 レコード(クォート内改行を含む)として通る。
        let ok = "a\n\"x\"\"\ny\"\n1\n";
        assert_eq!(count_records_one_byte(ok, 7).unwrap(), 2);
    }

    #[test]
    fn nonexistent_file_is_error() {
        assert!(infer_schema(Path::new("/nonexistent/ember_query/nope.csv")).is_err());
    }
}
