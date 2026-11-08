//! End-to-end tests that run the real `ember-query` binary.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Creates a unique, empty scratch directory under the system temp dir.
fn scratch_dir() -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("ember-query-cli-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn run(cwd: &Path, input: &Path, sql: &str, explain: bool) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ember-query"));
    cmd.current_dir(cwd)
        .arg("--input")
        .arg(input)
        .arg("--sql")
        .arg(sql);
    if explain {
        cmd.arg("--explain");
    }
    cmd.output().unwrap()
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

#[test]
fn plain_query_prints_rows_and_count() {
    let cwd = scratch_dir();
    let out = run(&cwd, &fixture("sample.csv"), "SELECT * FROM data", false);
    assert!(out.status.success(), "stderr: {}", text(&out.stderr));
    let all = format!("{}{}", text(&out.stdout), text(&out.stderr));
    assert!(all.contains("Apple"), "output: {all}");
    assert!(all.contains(" rows)"), "output: {all}");
}

#[test]
fn explain_prints_logical_plan() {
    let cwd = scratch_dir();
    let out = run(&cwd, &fixture("sample.csv"), "SELECT * FROM data", true);
    assert!(out.status.success(), "stderr: {}", text(&out.stderr));
    assert!(text(&out.stdout).starts_with("Logical Plan:"));
}

#[test]
fn invalid_sql_exits_nonzero() {
    let cwd = scratch_dir();
    let out = run(&cwd, &fixture("sample.csv"), "NOT VALID SQL", false);
    assert!(!out.status.success());
    assert!(!text(&out.stderr).is_empty());
}

#[test]
fn broken_join_csv_reports_cause() {
    let cwd = scratch_dir();
    // Header has 2 columns, a record has 3: schema inference fails.
    std::fs::write(cwd.join("orders.csv"), "order_id,item_id\n1,1,extra\n").unwrap();
    std::fs::copy(fixture("sample.csv"), cwd.join("sample.csv")).unwrap();
    let out = run(
        &cwd,
        &cwd.join("sample.csv"),
        "SELECT * FROM data JOIN orders ON id = item_id",
        false,
    );
    assert!(!out.status.success());
    let err = text(&out.stderr);
    assert!(err.contains("3 fields"), "stderr: {err}");
    assert!(!err.contains("not found in catalog"), "stderr: {err}");
}

#[test]
fn join_csv_resolved_next_to_input_not_cwd() {
    let data_dir = scratch_dir();
    let cwd = scratch_dir(); // different directory, no orders.csv here
    std::fs::copy(fixture("sample.csv"), data_dir.join("sample.csv")).unwrap();
    std::fs::copy(fixture("orders.csv"), data_dir.join("orders.csv")).unwrap();
    let out = run(
        &cwd,
        &data_dir.join("sample.csv"),
        "SELECT * FROM data JOIN orders ON id = item_id",
        false,
    );
    assert!(out.status.success(), "stderr: {}", text(&out.stderr));
    assert!(text(&out.stdout).contains("east"));
}

#[test]
fn missing_join_csv_keeps_not_found_error() {
    let cwd = scratch_dir();
    let out = run(
        &cwd,
        &fixture("sample.csv"),
        "SELECT * FROM data JOIN nope ON id = item_id",
        false,
    );
    assert!(!out.status.success());
    assert!(text(&out.stderr).contains("not found in catalog"));
}

#[test]
fn nfc_and_nfd_identifiers_match_headers_either_way() {
    let nfc = "\u{30AC}";
    let nfd = "\u{30AB}\u{3099}";
    let dir = scratch_dir();
    for (header, label) in [(nfd, "hdr_nfd"), (nfc, "hdr_nfc")] {
        let csv = dir.join(format!("{label}.csv"));
        std::fs::write(&csv, format!("{header},b\n1,2\n")).unwrap();
        for sql_col in [nfc, nfd] {
            let sql = format!("SELECT {sql_col} FROM {label} WHERE {sql_col} = 1");
            let out = run(&dir, &csv, &sql, false);
            assert!(out.status.success(), "{label} {sql}: {}", text(&out.stderr));
            assert!(text(&out.stdout).contains('1'), "{}", text(&out.stdout));
        }
    }
}
