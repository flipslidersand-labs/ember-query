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
    let batches = run_query(
        &fixture("orders.csv"),
        "SELECT COUNT(*) FROM orders GROUP BY region ORDER BY region DESC LIMIT 1",
    )?;
    assert_eq!(rows_of(&batches), strs(&[&["3"]]));
    Ok(())
}
