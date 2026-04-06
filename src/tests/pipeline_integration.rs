use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

use serde_json::{json, Value};
use tempfile::TempDir;

fn run_cli(home: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_research-radar"))
        .env("HOME", home)
        .args(args)
        .output()
        .expect("failed to run research-radar")
}

fn rpc_call(stdin: &mut dyn Write, reader: &mut BufReader<std::process::ChildStdout>, id: i64, method: &str, params: Value) -> Value {
    writeln!(stdin, "{}", json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    }))
    .unwrap();

    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    serde_json::from_str(&line).unwrap()
}

#[test]
fn full_pipeline_runs_through_mcp_and_scan_once() {
    let home = TempDir::new().unwrap();

    let add_output = run_cli(home.path(), &[
        "add",
        "https://example.com/ai-safety",
        "--title",
        "AI Safety Weekly",
        "--source-type",
        "web",
    ]);
    assert!(add_output.status.success(), "add failed: {}", String::from_utf8_lossy(&add_output.stderr));

    let mut child = Command::new(env!("CARGO_BIN_EXE_research-radar"))
        .env("HOME", home.path())
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to spawn mcp server");

    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);

    let created = rpc_call(
        &mut stdin,
        &mut reader,
        1,
        "profile_create",
        json!({
            "name": "AI radar",
            "keywords": ["AI", "Safety"],
            "score_threshold": 0.5
        }),
    );
    let profile_id = created["result"]["profile_id"].as_str().unwrap().to_string();

    let subscription = rpc_call(
        &mut stdin,
        &mut reader,
        2,
        "subscription_set",
        json!({
            "profile_id": profile_id,
            "channel": "discord",
            "config": {"webhook": "https://discord.invalid/webhook"},
            "enabled": true
        }),
    );
    assert!(subscription["error"].is_null(), "subscription_set failed: {subscription}");

    let scan_now = rpc_call(
        &mut stdin,
        &mut reader,
        3,
        "scan_now",
        json!({"profile_id": profile_id}),
    );
    let job_id = scan_now["result"]["job_id"].as_str().unwrap().to_string();
    assert_eq!(scan_now["result"]["reused"], false);

    let pending = rpc_call(
        &mut stdin,
        &mut reader,
        4,
        "scan_poll",
        json!({"job_id": job_id}),
    );
    assert_eq!(pending["result"]["status"], "pending");

    let scan_once = run_cli(home.path(), &["scan-once"]);
    assert!(scan_once.status.success(), "scan-once failed: {}", String::from_utf8_lossy(&scan_once.stderr));

    let complete = rpc_call(
        &mut stdin,
        &mut reader,
        5,
        "scan_poll",
        json!({"job_id": job_id}),
    );
    assert_eq!(complete["result"]["status"], "complete");

    let matches = rpc_call(
        &mut stdin,
        &mut reader,
        6,
        "matches_list",
        json!({"profile_id": profile_id}),
    );
    let items = matches["result"]["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert!(items[0]["score"].as_f64().unwrap() >= 0.5);
    assert_eq!(items[0]["disposition"], "new");

    drop(stdin);
    let status = child.wait().unwrap();
    assert!(status.success());
}
