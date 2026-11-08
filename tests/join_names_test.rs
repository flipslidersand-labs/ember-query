//! #101: output column names of 3+ table JOINs (self-joins) must not collide.

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use std::path::Path;

/// Run `sql` over CSVs `<table>.csv` written to a fresh temp dir; the first
/// file is the FROM table, the rest are resolved as JOIN tables.
fn run_query_in_dir(tag: &str, files: &[(&str, &str)], sql: &str) -> Result<Vec<RecordBatch>> {
    let dir = std::env::temp_dir().join(format!("ember_q101_{}_{tag}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    for (name, body) in files {
        std::fs::write(dir.join(format!("{name}.csv")), body)?;
    }
    let path = |t: &str| dir.join(format!("{t}.csv")).to_string_lossy().into_owned();
    let main = path(files[0].0);
    let schema = ember_query::catalog::infer_schema(Path::new(&main))?;
    let stmt = ember_query::sql::parse(sql)?;
    let mut cat = std::collections::HashMap::new();
    for join in &stmt.joins {
        let p = path(&join.table);
        let s = ember_query::catalog::infer_schema(Path::new(&p))?;
        cat.insert(join.table.clone(), (p, s));
    }
    let plan = ember_query::planner::build(&stmt, &main, schema, &cat)?;
    let mut exec = ember_query::executor::build_executor(plan)?;
    let mut batches = Vec::new();
    while let Some(b) = exec.next()? {
        batches.push(b);
    }
    std::fs::remove_dir_all(&dir)?;
    Ok(batches)
}

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

// Asymmetric: id -> next (a.nx = b.id, b.nx = c.id), distinct names per row.
const T: &str = "id,nx,name\n1,2,one\n2,3,two\n3,1,three\n";
const J3: &str = "FROM t a JOIN t b ON a.nx = b.id JOIN t c ON b.nx = c.id";

#[test]
fn test_q101_three_way_self_join_select_star_names_unique() -> Result<()> {
    let b = run_query_in_dir("star", &[("t", T)], &format!("SELECT * {J3} ORDER BY a.id"))?;
    let schema = b[0].schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let uniq: std::collections::HashSet<_> = names.iter().collect();
    assert_eq!(uniq.len(), names.len(), "duplicate output names: {names:?}");
    // 2-table naming is unchanged: second table keeps `right_`.
    assert_eq!(
        &names[..6],
        ["id", "nx", "name", "right_id", "right_nx", "right_name"]
    );
    assert_eq!(
        rows_of(&b),
        strs(&[
            &["1", "2", "one", "2", "3", "two", "3", "1", "three"],
            &["2", "3", "two", "3", "1", "three", "1", "2", "one"],
            &["3", "1", "three", "1", "2", "one", "2", "3", "two"],
        ])
    );
    Ok(())
}

#[test]
fn test_q101_three_way_self_join_qualified_columns_resolve() -> Result<()> {
    let rows = rows_of(&run_query_in_dir(
        "qual",
        &[("t", T)],
        &format!("SELECT a.id, b.id, c.id, c.name {J3} ORDER BY a.id"),
    )?);
    assert_eq!(
        rows,
        strs(&[
            &["1", "2", "3", "three"],
            &["2", "3", "1", "one"],
            &["3", "1", "2", "two"],
        ])
    );
    Ok(())
}

#[test]
fn test_q101_three_way_self_join_order_by_third_table_column() -> Result<()> {
    let rows = rows_of(&run_query_in_dir(
        "ord",
        &[("t", T)],
        &format!("SELECT a.id, c.id {J3} ORDER BY c.id DESC"),
    )?);
    assert_eq!(rows, strs(&[&["1", "3"], &["3", "2"], &["2", "1"]]));
    Ok(())
}

/// Non-colliding columns of a 3rd table keep their plain names.
#[test]
fn test_q101_three_way_distinct_tables_names_unique() -> Result<()> {
    let b = run_query_in_dir(
        "distinct",
        &[
            ("t", "id,x\n1,10\n"),
            ("u", "id,y\n1,20\n"),
            ("w", "id,z\n1,30\n"),
        ],
        "SELECT * FROM t JOIN u ON t.id = u.id JOIN w ON t.id = w.id",
    )?;
    let schema = b[0].schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["id", "x", "right_id", "y", "right2_id", "z"]);
    Ok(())
}

/// #102: a missing ON column reports both sides' schemas.
#[test]
fn join_on_missing_column_error_names_both_schemas() {
    let err = run_query_in_dir(
        "102",
        &[("t1", "id,name,v\n1,a,2\n"), ("t2", "k,t1_id\n1,1\n")],
        "SELECT * FROM t1 a JOIN t2 b ON b.t1_idd = a.id",
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("b.t1_idd"), "{err}");
    assert!(
        err.contains("left schema") && err.contains("\"name\""),
        "{err}"
    );
    assert!(
        err.contains("right schema") && err.contains("\"t1_id\""),
        "{err}"
    );
}
