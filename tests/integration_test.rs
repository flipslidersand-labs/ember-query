use anyhow::Result;
use arrow::record_batch::RecordBatch;
use std::path::Path;

fn run_query(csv_path: &str, sql: &str) -> Result<Vec<RecordBatch>> {
    // Reuse the same pipeline as main.rs
    let schema = ember_query::catalog::infer_schema(Path::new(csv_path))?;
    let stmt = ember_query::sql::parse(sql)?;
    let cat = std::collections::HashMap::new();
    let plan = ember_query::planner::build(&stmt, csv_path, schema, &cat)?;
    let mut exec = ember_query::executor::build_executor(plan)?;

    let mut batches = Vec::new();
    while let Some(batch) = exec.next()? {
        batches.push(batch);
    }
    Ok(batches)
}

fn sample_csv() -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    format!("{}/tests/fixtures/sample.csv", manifest)
}

#[test]
fn test_select_star_where_price_limit() -> Result<()> {
    let path = sample_csv();
    let batches = run_query(&path, "SELECT * FROM data WHERE price > 1000 LIMIT 3")?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "expected 3 rows, got {}", total_rows);
    Ok(())
}

#[test]
fn test_select_columns_where_in_stock() -> Result<()> {
    let path = sample_csv();
    let batches = run_query(&path, "SELECT name, price FROM data WHERE in_stock = true")?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 3,
        "expected 3 rows (Apple, Banana, Date), got {}",
        total_rows
    );
    Ok(())
}

#[test]
fn test_count_star() -> Result<()> {
    let path = sample_csv();
    let batches = run_query(&path, "SELECT COUNT(*) FROM data")?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "COUNT(*) should produce 1 row");

    // The count value should be 5
    let batch = &batches[0];
    let col = batch.column(0);
    let count_arr = col
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("COUNT(*) should produce Int64Array");
    assert_eq!(count_arr.value(0), 5, "COUNT(*) should be 5");
    Ok(())
}

fn types_of(sql: &str) -> Result<Vec<arrow::datatypes::DataType>> {
    let batches = run_query(&sample_csv(), sql)?;
    Ok(batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect())
}

#[test]
fn test_project_arithmetic_expr_is_int64() -> Result<()> {
    use arrow::array::Int64Array;
    let batches = run_query(&sample_csv(), "SELECT price * 2 FROM data")?;
    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<Int64Array>().expect("Int64");
    assert_eq!(arr.value(0), 3000);
    Ok(())
}

#[test]
fn test_project_comparison_expr_is_boolean() -> Result<()> {
    use arrow::datatypes::DataType;
    assert_eq!(types_of("SELECT price > 1 FROM data")?, [DataType::Boolean]);
    Ok(())
}

#[test]
fn test_project_null_literal_is_int64() -> Result<()> {
    use arrow::datatypes::DataType;
    assert_eq!(types_of("SELECT NULL FROM data")?, [DataType::Int64]);
    Ok(())
}

#[test]
fn test_project_neg_keeps_input_type() -> Result<()> {
    use arrow::datatypes::DataType;
    assert_eq!(types_of("SELECT -price FROM data")?, [DataType::Int64]);
    Ok(())
}

#[test]
fn test_project_duplicate_columns() -> Result<()> {
    let batches = run_query(&sample_csv(), "SELECT price, price FROM data")?;
    assert_eq!(batches[0].num_columns(), 2);
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 5: JOIN / ORDER BY / GROUP BY end-to-end (parse -> plan -> execute)
// ---------------------------------------------------------------------------

fn fixture(name: &str) -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    format!("{}/tests/fixtures/{}", manifest, name)
}

/// Like `run_query`, but resolves JOIN tables through the catalog
/// (`<table>.csv` under tests/fixtures), mirroring main.rs.
fn run_query_with_catalog(csv_path: &str, sql: &str) -> Result<Vec<RecordBatch>> {
    let schema = ember_query::catalog::infer_schema(Path::new(csv_path))?;
    let stmt = ember_query::sql::parse(sql)?;
    let mut cat = std::collections::HashMap::new();
    for join in &stmt.joins {
        let join_path = fixture(&format!("{}.csv", join.table));
        if let Ok(s) = ember_query::catalog::infer_schema(Path::new(&join_path)) {
            cat.insert(join.table.clone(), (join_path, s));
        }
    }
    let plan = ember_query::planner::build(&stmt, csv_path, schema, &cat)?;
    let mut exec = ember_query::executor::build_executor(plan)?;
    let mut batches = Vec::new();
    while let Some(batch) = exec.next()? {
        batches.push(batch);
    }
    Ok(batches)
}

/// Flatten batches into rows of stringified cells.
fn rows_of(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for b in batches {
        for r in 0..b.num_rows() {
            rows.push(
                (0..b.num_columns())
                    .map(|c| arrow::util::display::array_value_to_string(b.column(c), r).unwrap())
                    .collect(),
            );
        }
    }
    rows
}

fn strs(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect()
}

#[test]
fn test_inner_join_on_catalog_table() -> Result<()> {
    let batches = run_query_with_catalog(
        &sample_csv(),
        "SELECT * FROM data JOIN orders ON id = item_id",
    )?;
    // Row order of a JOIN is unspecified in SQL, so compare sorted by order_id.
    // (ORDER BY after JOIN ... ON cannot be parsed: "OR" matches the prefix of
    // "ORDER"; see PR description.)
    let mut rows = rows_of(&batches);
    rows.sort_by_key(|r| r[4].parse::<i64>().unwrap());
    // Elderberry (id 5) and order 105 (item 9) have no match and are dropped.
    assert_eq!(
        rows,
        strs(&[
            &["1", "Apple", "1500", "true", "101", "1", "3", "east"],
            &["2", "Banana", "200", "true", "102", "2", "5", "west"],
            &["1", "Apple", "1500", "true", "103", "1", "2", "west"],
            &["4", "Date", "800", "true", "104", "4", "7", "east"],
            &["3", "Cherry", "3500", "false", "106", "3", "4", "west"],
        ])
    );
    Ok(())
}

#[test]
fn test_order_by_desc_limit() -> Result<()> {
    let batches = run_query(
        &fixture("orders.csv"),
        "SELECT order_id, qty FROM orders ORDER BY qty DESC LIMIT 3",
    )?;
    assert_eq!(
        rows_of(&batches),
        strs(&[&["104", "7"], &["102", "5"], &["106", "4"]])
    );
    Ok(())
}

#[test]
fn test_order_by_column_not_in_select() -> Result<()> {
    let batches = run_query(
        &fixture("orders.csv"),
        "SELECT order_id FROM orders ORDER BY qty DESC LIMIT 3",
    )?;
    // Same ordering as test_order_by_desc_limit, but qty is not selected.
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(rows_of(&batches), strs(&[&["104"], &["102"], &["106"]]));
    Ok(())
}

#[test]
fn test_group_by_sum_order_by_key() -> Result<()> {
    let batches = run_query(
        &fixture("orders.csv"),
        "SELECT region, SUM(qty) FROM orders GROUP BY region ORDER BY region",
    )?;
    // east: 3+7+2 = 12, west: 5+2+4 = 11
    // SUM(Int64) stays Int64, so the rendered values are exact integers.
    assert_eq!(rows_of(&batches), strs(&[&["east", "12"], &["west", "11"]]));
    Ok(())
}

#[test]
fn test_error_unknown_column() {
    let res = run_query(&fixture("orders.csv"), "SELECT nope FROM orders");
    assert!(res.is_err(), "unknown column must be an error");
}

#[test]
fn test_error_unknown_join_table() {
    let res = run_query_with_catalog(
        &fixture("orders.csv"),
        "SELECT * FROM orders JOIN missing ON order_id = id",
    );
    let err = res.expect_err("unknown JOIN table must be an error");
    assert!(
        err.to_string().contains("missing"),
        "error should name the table, got: {}",
        err
    );
}

#[test]
fn test_error_unknown_from_table() {
    let res = run_query(&fixture("missing.csv"), "SELECT * FROM missing");
    assert!(res.is_err(), "unknown table file must be an error");
}

// ---------------------------------------------------------------------------
// Unary minus (#71)
// ---------------------------------------------------------------------------

#[test]
fn test_unary_minus_negates_column() -> Result<()> {
    let b = run_query(&fixture("orders.csv"), "SELECT -qty FROM orders")?;
    assert_eq!(rows_of(&b)[..3], strs(&[&["-3"], &["-5"], &["-2"]])[..]);
    Ok(())
}

#[test]
fn test_unary_minus_in_where() -> Result<()> {
    // 修正前は -qty が qty として評価され、全行が落ちる(0 件)
    let b = run_query(
        &fixture("orders.csv"),
        "SELECT order_id FROM orders WHERE -qty < 0",
    )?;
    let all = run_query(&fixture("orders.csv"), "SELECT order_id FROM orders")?;
    assert_eq!(rows_of(&b), rows_of(&all));
    let none = run_query(
        &fixture("orders.csv"),
        "SELECT order_id FROM orders WHERE -qty > 0",
    )?;
    assert!(rows_of(&none).is_empty());
    Ok(())
}

#[test]
fn test_unary_minus_double_and_binary() -> Result<()> {
    let b = run_query(
        &fixture("orders.csv"),
        "SELECT - -qty, qty - -1, qty - 1 FROM orders",
    )?;
    assert_eq!(rows_of(&b)[0], ["3", "4", "2"]);
    Ok(())
}

#[test]
fn test_unary_minus_i64_min_overflows_via_sql() {
    let res = run_query(&fixture("minint.csv"), "SELECT -v FROM minint");
    let err = res.expect_err("-i64::MIN must overflow").to_string();
    assert!(err.contains("integer overflow"), "got: {err}");
}

// 負リテラル: master では -1 が 1 と評価されていた(#71 と同根)。
// orders.csv の qty は 2 以上なので `qty > -1` 単独では master と差が出ない。
// 差が出る `qty - -1 = 4`(master は qty - 1 = 4 で qty=5 の行)を境界に使う。
#[test]
fn test_negative_literal_keeps_sign_in_where() -> Result<()> {
    let b = run_query(
        &fixture("orders.csv"),
        "SELECT order_id FROM orders WHERE qty - -1 = 4",
    )?;
    assert_eq!(rows_of(&b)[0], ["101"]); // qty=3
    let all = rows_of(&run_query(
        &fixture("orders.csv"),
        "SELECT order_id FROM orders",
    )?);
    let gt = run_query(
        &fixture("orders.csv"),
        "SELECT order_id FROM orders WHERE qty > -1",
    )?;
    assert_eq!(rows_of(&gt), all);
    Ok(())
}

#[test]
fn test_negative_literal_in_projection() -> Result<()> {
    let b = run_query(&fixture("orders.csv"), "SELECT -1, qty - -1 FROM orders")?;
    assert_eq!(rows_of(&b)[0], ["-1", "4"]);
    Ok(())
}

#[test]
fn test_negative_i64_min_literal_parses() -> Result<()> {
    let b = run_query(
        &fixture("orders.csv"),
        "SELECT -9223372036854775808 FROM orders",
    )?;
    assert_eq!(rows_of(&b)[0], ["-9223372036854775808"]);
    Ok(())
}

// 100 段の単項マイナスが評価まで届く(パースだけでなく結果が正しい)。
// 偶数段なので qty のまま、奇数段なら符号反転。
// 注: `- -qty` のような偶数段は master でも qty として通るため、検出力は
// 奇数段(test_unary_minus_negates_column / 下の 101 段)側にある。
#[test]
fn test_deep_unary_minus_evaluates() -> Result<()> {
    let even = format!("SELECT {}qty FROM orders", "- ".repeat(100));
    let odd = format!("SELECT {}qty FROM orders", "- ".repeat(101));
    assert_eq!(
        rows_of(&run_query(&fixture("orders.csv"), &even)?)[0],
        ["3"]
    );
    assert_eq!(
        rows_of(&run_query(&fixture("orders.csv"), &odd)?)[0],
        ["-3"]
    );
    Ok(())
}
