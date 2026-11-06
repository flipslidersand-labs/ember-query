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
