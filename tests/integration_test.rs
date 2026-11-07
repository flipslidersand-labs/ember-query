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
fn test_float_division_by_zero_is_error() {
    let err = run_query(&sample_csv(), "SELECT price / 0.0 FROM data").unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("divide by zero"),
        "{err}"
    );
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
fn test_where_or_null_keeps_true_rows() -> Result<()> {
    // in_stock = true の3行は TRUE OR NULL = TRUE で残り、false 行は NULL で落ちる
    let batches = run_query(
        &sample_csv(),
        "SELECT name FROM data WHERE in_stock OR NULL",
    )?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);
    Ok(())
}

#[test]
fn test_where_null_predicates_return_no_rows() -> Result<()> {
    for pred in ["NULL", "NOT NULL", "NULL AND TRUE"] {
        let sql = format!("SELECT name FROM data WHERE {pred}");
        let batches = run_query(&sample_csv(), &sql)?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 0, "{pred}");
    }
    Ok(())
}

#[test]
fn test_where_non_null_non_boolean_still_errors() {
    assert!(run_query(&sample_csv(), "SELECT name FROM data WHERE 1").is_err());
}

#[test]
fn test_project_not_null_is_boolean_null() -> Result<()> {
    use arrow::datatypes::DataType;
    let batches = run_query(&sample_csv(), "SELECT NOT NULL FROM data")?;
    assert_eq!(batches[0].column(0).data_type(), &DataType::Boolean);
    assert_eq!(batches[0].column(0).null_count(), batches[0].num_rows());
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

// ── #41: aggregate queries honour the SELECT list ────────────────────────────

#[test]
fn test_agg_select_order_follows_select_list() -> Result<()> {
    let batches = run_query(
        &fixture("orders.csv"),
        "SELECT SUM(qty), region FROM orders GROUP BY region ORDER BY region",
    )?;
    // Aggregate emits (region, sum); the SELECT list asks for (sum, region).
    assert_eq!(rows_of(&batches), strs(&[&["12", "east"], &["11", "west"]]));
    Ok(())
}

#[test]
fn test_agg_non_grouped_column_is_error() {
    let res = run_query(
        &fixture("orders.csv"),
        "SELECT region, COUNT(*) FROM orders",
    );
    let err = res.expect_err("non-grouped column must be an error");
    assert!(
        err.to_string().contains("must appear in the GROUP BY"),
        "{err}"
    );
}

#[test]
fn test_agg_compound_expression_is_unsupported_error() {
    let res = run_query(&fixture("orders.csv"), "SELECT SUM(qty) + 1 FROM orders");
    let err = res.expect_err("aggregate inside an expression must be an error");
    assert!(err.to_string().contains("not supported"), "{err}");
}

#[test]
fn test_agg_order_by_unselected_group_column_still_works() -> Result<()> {
    // Excluding order 105 makes the groups differ in size (east 2, west 3),
    // so ASC and DESC give different results.
    let sql = |dir: &str| {
        format!(
            "SELECT COUNT(*) FROM orders WHERE order_id != 105 GROUP BY region ORDER BY region {dir}"
        )
    };
    assert_eq!(
        rows_of(&run_query(&fixture("orders.csv"), &sql("DESC"))?),
        strs(&[&["3"], &["2"]])
    );
    assert_eq!(
        rows_of(&run_query(&fixture("orders.csv"), &sql("ASC"))?),
        strs(&[&["2"], &["3"]])
    );
    Ok(())
}

#[test]
fn test_agg_alias_colliding_with_group_column_is_error() {
    let res = run_query(
        &fixture("orders.csv"),
        "SELECT COUNT(*) AS region, region FROM orders GROUP BY region",
    );
    let err = res.expect_err("duplicate aggregate output name must be an error");
    assert!(err.to_string().contains("duplicate"), "{err}");
}

#[test]
fn test_agg_alias_colliding_with_auto_name_is_error() {
    let res = run_query(
        &fixture("orders.csv"),
        "SELECT COUNT(*) AS agg_1, SUM(qty) FROM orders",
    );
    let err = res.expect_err("duplicate aggregate output name must be an error");
    assert!(err.to_string().contains("duplicate"), "{err}");
}

// #39: qualified column names (`table.col`) resolve against a single table.

#[test]
fn test_qualified_columns_in_select_and_where() -> Result<()> {
    let all = run_query(
        &fixture("orders.csv"),
        "SELECT orders.order_id FROM orders WHERE orders.qty > 1",
    )?;
    assert_eq!(
        rows_of(&all),
        strs(&[&["101"], &["102"], &["103"], &["104"], &["105"], &["106"]])
    );
    let some = run_query(
        &fixture("orders.csv"),
        "SELECT orders.order_id FROM orders WHERE orders.qty > 3",
    )?;
    assert_eq!(rows_of(&some), strs(&[&["102"], &["104"], &["106"]]));
    Ok(())
}

#[test]
fn test_qualified_group_by_and_aggregate_argument() -> Result<()> {
    let batches = run_query(
        &fixture("orders.csv"),
        "SELECT orders.region, SUM(orders.qty) FROM orders GROUP BY orders.region ORDER BY orders.region",
    )?;
    assert_eq!(rows_of(&batches), strs(&[&["east", "12"], &["west", "11"]]));
    Ok(())
}

#[test]
fn test_qualified_and_unqualified_group_columns_match() -> Result<()> {
    let expected = strs(&[&["east", "12"], &["west", "11"]]);
    for sql in [
        "SELECT orders.region, SUM(qty) FROM orders GROUP BY region ORDER BY region",
        "SELECT region, SUM(qty) FROM orders GROUP BY orders.region ORDER BY region",
    ] {
        assert_eq!(
            rows_of(&run_query(&fixture("orders.csv"), sql)?),
            expected,
            "{sql}"
        );
    }
    Ok(())
}

#[test]
fn test_qualified_order_by() -> Result<()> {
    let cases: [(&str, &[&[&str]]); 3] = [
        (
            "SELECT orders.order_id FROM orders ORDER BY orders.qty DESC LIMIT 3",
            &[&["104"], &["102"], &["106"]],
        ),
        (
            "SELECT order_id FROM orders ORDER BY orders.order_id DESC LIMIT 2",
            &[&["106"], &["105"]],
        ),
        (
            "SELECT orders.order_id FROM orders ORDER BY order_id DESC LIMIT 2",
            &[&["106"], &["105"]],
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(
            rows_of(&run_query(&fixture("orders.csv"), sql)?),
            strs(expected),
            "{sql}"
        );
    }
    Ok(())
}

/// After a JOIN whose tables share column names, the right side's columns
/// are renamed `right_<col>`. A qualified `t2.col` must not silently resolve
/// to the left table's `col` (#39 review / #40): it is an error instead.
#[test]
fn test_join_name_collision_qualified_right_column_is_error() {
    for sql in [
        "SELECT t2.id, t2.name FROM t1 JOIN t2 ON t1.id = t2.t1_id",
        "SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t2.id = 300",
        "SELECT t2.name, COUNT(*) FROM t1 JOIN t2 ON t1.id = t2.t1_id GROUP BY t2.name",
    ] {
        let res = run_query_with_catalog(&fixture("t1.csv"), sql);
        let err = res.expect_err(sql).to_string();
        assert!(err.contains("ambiguous"), "{sql}: {err}");
    }
}

#[test]
fn test_join_qualified_unique_columns_still_resolve() -> Result<()> {
    let batches = run_query_with_catalog(
        &fixture("t1.csv"),
        "SELECT t1.v, t2.t1_id FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t1.v > 10",
    )?;
    assert_eq!(rows_of(&batches), strs(&[&["20", "2"]]));
    Ok(())
}

// #78: aggregate ORDER BY resolves SELECT aliases.

#[test]
fn test_agg_order_by_group_column_alias() -> Result<()> {
    let sql = |dir: &str| {
        format!("SELECT region AS r, COUNT(*) FROM orders GROUP BY region ORDER BY r {dir}")
    };
    assert_eq!(
        rows_of(&run_query(&fixture("orders.csv"), &sql("DESC"))?),
        strs(&[&["west", "3"], &["east", "3"]])
    );
    assert_eq!(
        rows_of(&run_query(&fixture("orders.csv"), &sql("ASC"))?),
        strs(&[&["east", "3"], &["west", "3"]])
    );
    Ok(())
}

#[test]
fn test_agg_order_by_aggregate_alias() -> Result<()> {
    let sql = |dir: &str| {
        format!(
            "SELECT region, COUNT(*) AS c FROM orders WHERE order_id != 105 GROUP BY region ORDER BY c {dir}"
        )
    };
    assert_eq!(
        rows_of(&run_query(&fixture("orders.csv"), &sql("DESC"))?),
        strs(&[&["west", "3"], &["east", "2"]])
    );
    assert_eq!(
        rows_of(&run_query(&fixture("orders.csv"), &sql("ASC"))?),
        strs(&[&["east", "2"], &["west", "3"]])
    );
    Ok(())
}

#[test]
fn test_agg_qualified_select_column_with_unqualified_group_by() -> Result<()> {
    let batches = run_query(
        &fixture("orders.csv"),
        "SELECT orders.region, COUNT(*) FROM orders GROUP BY region ORDER BY region",
    )?;
    assert_eq!(rows_of(&batches), strs(&[&["east", "3"], &["west", "3"]]));
    Ok(())
}

// -AGG(...) は #71 で Unary{Agg} になる。列が黙って消えず、#74 の
// 「集約を式に埋め込めない」エラーになること。
#[test]
fn test_agg_under_unary_minus_is_unsupported_error() {
    for sql in [
        "SELECT region, -COUNT(*) FROM orders GROUP BY region",
        "SELECT region, -SUM(qty) FROM orders GROUP BY region",
    ] {
        let err = run_query(&fixture("orders.csv"), sql).expect_err(sql);
        assert!(err.to_string().contains("not supported"), "{sql}: {err}");
    }
}

#[test]
fn test_table_alias_without_as() -> Result<()> {
    let path = sample_csv();
    let batches = run_query(&path, "SELECT * FROM data d WHERE price > 1000 LIMIT 3")?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
    let batches = run_query(&path, "SELECT * FROM data LIMIT 1")?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1);
    Ok(())
}

// #95: 先頭 100 行が整数で 101 行目が N/A の列は、全行推論で Utf8 になる。
// CSV 読み取り時の "cannot parse 'N/A' as i64" は起きず、文字列として照会できる。
// (Utf8 列と整数リテラルの比較は別問題で、現状は比較演算の型エラーになる。)
#[test]
fn test_type_mismatch_after_row_100_does_not_fail_at_runtime() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("ember_query_it95_{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("samp.csv");
    let mut s = String::from("n\n");
    for i in 0..100 {
        s.push_str(&format!("{}\n", i));
    }
    s.push_str("N/A\n");
    std::fs::write(&path, s)?;
    let p = path.to_str().unwrap();
    let result = run_query(p, "SELECT n FROM samp WHERE n = '5'");
    let issue_query = run_query(p, "SELECT n FROM samp WHERE n = 5");
    let _ = std::fs::remove_dir_all(&dir);

    let total: usize = result?.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1);
    if let Err(e) = issue_query {
        assert!(!e.to_string().contains("cannot parse"), "got: {e}");
    }
    Ok(())
}
