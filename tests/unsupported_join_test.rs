//! #109: non-INNER join kinds must be a clear error, not a silent INNER JOIN.

use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn run(sql: &str) -> (bool, String, String) {
    let dir = std::env::temp_dir().join(format!(
        "ember_q109_{}_{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("t1.csv"), "id,name\n1,a\n2,b\n3,c\n").unwrap();
    std::fs::write(dir.join("t2.csv"), "id,t1_id\n10,1\n11,2\n").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_ember-query"))
        .current_dir(&dir)
        .arg("--input")
        .arg(Path::new("t1.csv"))
        .arg("--sql")
        .arg(sql)
        .output()
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn left_join_is_rejected_not_run_as_inner() {
    let (ok, stdout, stderr) = run("SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id");
    assert!(!ok, "LEFT JOIN must fail; stdout: {stdout}");
    assert!(stderr.contains("LEFT JOIN is not supported"), "{stderr}");
}

#[test]
fn inner_join_still_runs() {
    let (ok, stdout, stderr) = run("SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.t1_id");
    assert!(ok, "{stderr}");
    assert!(stdout.contains("1  | a    | 10"), "{stdout}");
    assert!(stdout.contains("2  | b    | 11"), "{stdout}");
    assert!(stderr.contains("(2 rows)"), "{stderr}");
    assert!(!stdout.contains("| c "), "{stdout}");
}
