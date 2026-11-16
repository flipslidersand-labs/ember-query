//! Deep expression trees must parse AND execute on a 2MB thread stack (#138, #149).
//! Unlike a parse-only check, this goes through plan build and `executor.next()`,
//! where `evaluate` recurses over the tree.

use anyhow::Result;
use std::path::Path;

fn run(sql: &str) -> Result<usize> {
    let path = format!(
        "{}/tests/fixtures/sample.csv",
        std::env::var("CARGO_MANIFEST_DIR").unwrap()
    );
    let schema = ember_query::catalog::infer_schema(Path::new(&path))?;
    let stmt = ember_query::sql::parse(sql)?;
    let cat = std::collections::HashMap::new();
    let plan = ember_query::planner::build(&stmt, &path, schema, &cat)?;
    let mut exec = ember_query::executor::build_executor(plan)?;
    let mut rows = 0;
    while let Some(b) = exec.next()? {
        rows += b.num_rows();
    }
    Ok(rows)
}

/// Runs `f` on a 2MB stack, the smallest default thread stack in use.
fn on_small_stack(f: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .expect("overflowed or panicked on a 2MB stack");
}

#[test]
fn deep_trees_parse_and_execute_on_a_2mb_stack() {
    on_small_stack(|| {
        // Largest accepted chains (MAX_EXPR_DEPTH = 500): must run to completion.
        let probe = std::env::var("EMBER_PROBE_N")
            .ok()
            .and_then(|s| s.parse().ok());
        let n = probe.unwrap_or(499usize);
        let sel = format!("SELECT price{} AS s FROM data", "+1".repeat(n));
        assert_eq!(run(&sel).unwrap(), 5);
        let wh = format!(
            "SELECT * FROM data WHERE in_stock{}",
            " AND in_stock".repeat(n)
        );
        assert_eq!(run(&wh).unwrap(), 3);
        let or = format!(
            "SELECT * FROM data WHERE in_stock{}",
            " OR in_stock".repeat(n)
        );
        assert_eq!(run(&or).unwrap(), 3);
        // Nested parens x chains: rejected or accepted, but never an abort.
        for k in [10, 30, 100] {
            if probe.is_some() {
                break;
            }
            let mut e = String::from("price");
            for _ in 0..k {
                e = format!("({e}{})", "+1".repeat(1000 / k));
            }
            let _ = run(&format!("SELECT {e} AS s FROM data"));
        }
    });
}
