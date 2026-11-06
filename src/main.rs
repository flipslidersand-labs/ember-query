mod catalog;
mod display;
mod executor;
mod planner;
mod sql;

use anyhow::{Context, Result};
use clap::Parser;
use std::path::Path;

#[derive(Parser)]
#[command(name = "ember-query", about = "A small vectorized SQL engine")]
struct Cli {
    /// Path to the input CSV file.
    #[arg(long)]
    input: String,

    /// SQL query to execute.
    #[arg(long)]
    sql: String,

    /// Print the logical plan instead of executing.
    #[arg(long, default_value_t = false)]
    explain: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // 1. Infer schema from CSV
    let path = &cli.input;
    let schema = catalog::infer_schema(Path::new(path))?;

    // 2. Parse SQL
    let stmt = sql::parse(&cli.sql)?;

    // 3. Build catalog: infer schema for each JOIN table from sibling CSVs
    let mut cat: std::collections::HashMap<String, (String, catalog::TableSchema)> =
        std::collections::HashMap::new();
    let base_dir = Path::new(path).parent().unwrap_or_else(|| Path::new(""));
    for join in &stmt.joins {
        let join_path = base_dir.join(format!("{}.csv", join.table));
        // A missing file is left to the planner's "not found in catalog" error;
        // any other failure (bad CSV, permissions) is reported with its cause.
        if !join_path.exists() {
            continue;
        }
        let s = catalog::infer_schema(&join_path).with_context(|| {
            format!(
                "failed to read JOIN table '{}' from {}",
                join.table,
                join_path.display()
            )
        })?;
        cat.insert(
            join.table.clone(),
            (join_path.to_string_lossy().into_owned(), s),
        );
    }

    // 4. Build logical plan
    let logical_plan = planner::build(&stmt, path, schema, &cat)?;

    if cli.explain {
        println!("Logical Plan:\n{}", logical_plan.explain(0));
        return Ok(());
    }

    // 4. Build executor from logical plan (physical = logical for now)
    let mut exec = executor::build_executor(logical_plan)?;

    // 5. Execute and print results
    let mut total_rows = 0usize;
    let mut first_batch = true;
    while let Some(batch) = exec.next()? {
        if first_batch {
            first_batch = false;
        }
        total_rows += batch.num_rows();
        display::print_batch(&batch);
    }

    eprintln!("({} rows)", total_rows);
    Ok(())
}
