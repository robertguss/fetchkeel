use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::{Value, json};
use std::fs;
use tempfile::tempdir;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn fetchkeel() -> Command {
    Command::cargo_bin("fetchkeel").expect("fetchkeel binary built")
}

#[tokio::test]
async fn canvas_link_header_pagination_429_retry_and_incremental_watermark() {
    let server = MockServer::start().await;
    let dir = tempdir().unwrap();

    let page2_url = format!("{}/api/v1/courses?page=2&per_page=2", server.uri());

    // First hit to page 1 returns HTTP 429 with Retry-After: 0 (tests automatic retry!)
    Mock::given(method("GET"))
        .and(path("/api/v1/courses"))
        .and(header("authorization", "Bearer test_canvas_token"))
        .and(query_param("per_page", "2"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Second hit to page 1 returns 2 rows + RFC 5988 Link rel="next" header
    Mock::given(method("GET"))
        .and(path("/api/v1/courses"))
        .and(header("authorization", "Bearer test_canvas_token"))
        .and(query_param("per_page", "2"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Link", format!("<{page2_url}>; rel=\"next\""))
                .set_body_json(json!([
                    {"id": 101, "name": "Old Testament I", "updated_at": "2026-11-10T10:00:00Z"},
                    {"id": 102, "name": "New Testament I", "updated_at": "2026-11-12T15:30:00Z"}
                ])),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Page 2 returns 1 final row (no Link header -> stops)
    Mock::given(method("GET"))
        .and(path("/api/v1/courses"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": 103, "name": "Systematic Theology", "updated_at": "2026-11-18T08:45:00Z"}
        ])))
        .mount(&server)
        .await;

    let manifest_path = dir.path().join("canvas.toml");
    let out_path = dir.path().join("courses.jsonl");
    let state_path = dir.path().join("state.json");

    fs::write(
        &manifest_path,
        format!(
            r#"
[source]
name = "canvas"
base_url = "{}"
auth = "bearer"
auth_env = "TEST_CANVAS_TOKEN"
max_retries = 3
base_backoff_ms = 10

[[streams]]
name = "courses"
path = "/api/v1/courses"
pagination = "link_header"
per_page = 2
watermark_field = "updated_at"
watermark_param = "updated_since"
output = "{}"
"#,
            server.uri(),
            out_path.display()
        ),
    )
    .unwrap();

    // Run extraction!
    let out = fetchkeel()
        .env("TEST_CANVAS_TOKEN", "test_canvas_token")
        .args([
            "run",
            manifest_path.to_str().unwrap(),
            "--state",
            state_path.to_str().unwrap(),
            "--format",
            "agent",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let report: Value = serde_json::from_slice(&out).expect("valid JSON");
    assert_eq!(report["ok"], true);
    assert_eq!(report["total_pages"], 2);
    assert_eq!(report["total_rows"], 3);
    assert_eq!(report["total_retries"], 1);
    assert_eq!(
        report["streams"][0]["new_watermark"],
        "2026-11-18T08:45:00Z"
    );

    let jsonl = fs::read_to_string(&out_path).unwrap();
    assert_eq!(jsonl.lines().count(), 3);

    // Dry-run next incremental sync verifies saved watermark is injected into URL
    let dry = fetchkeel()
        .env("TEST_CANVAS_TOKEN", "test_canvas_token")
        .args([
            "run",
            manifest_path.to_str().unwrap(),
            "--state",
            state_path.to_str().unwrap(),
            "--dry-run",
            "--format",
            "agent",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let dry_rep: Value = serde_json::from_slice(&dry).unwrap();
    let resolved = dry_rep["streams"][0]["resolved_url"].as_str().unwrap();
    assert!(
        resolved.contains("updated_since=2026-11-18T08%3A45%3A00Z"),
        "expected watermark in url: {resolved}"
    );
}

#[tokio::test]
async fn cursor_pagination_data_path_and_stdout_piping() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api2/students"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "meta": {"next_cursor": "cur_page_2"},
            "data": [
                {"student_id": "STU-1001", "gpa": 3.82}
            ]
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api2/students"))
        .and(query_param("cursor", "cur_page_2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "meta": {"next_cursor": null},
            "data": [
                {"student_id": "STU-1002", "gpa": 3.15}
            ]
        })))
        .mount(&server)
        .await;

    // Ad-hoc `fetchkeel get` piping pure JSONL to stdout
    let out = fetchkeel()
        .args([
            "get",
            &format!("{}/api2/students", server.uri()),
            "--paginate",
            "cursor",
            "--data-path",
            "data",
            "--cursor-path",
            "meta.next_cursor",
            "-o",
            "-",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let lines: Vec<&str> = std::str::from_utf8(&out).unwrap().lines().collect();
    assert_eq!(lines.len(), 2);
    let r0: Value = serde_json::from_str(lines[0]).unwrap();
    let r1: Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(r0["student_id"], "STU-1001");
    assert_eq!(r1["student_id"], "STU-1002");
}

#[test]
fn missing_env_returns_exit_2_and_skill_installs() {
    let dir = tempdir().unwrap();
    let manifest = dir.path().join("populi.toml");
    fs::write(
        &manifest,
        r#"
[source]
name = "populi"
base_url = "https://example.populiweb.com"
auth = "bearer"
auth_env = "MISSING_POPULI_KEY_XYZ"

[[streams]]
name = "students"
path = "/api2/students"
"#,
    )
    .unwrap();

    let stderr = fetchkeel()
        .env_remove("MISSING_POPULI_KEY_XYZ")
        .args([
            "run",
            manifest.to_str().unwrap(),
            "--dry-run",
            "--format",
            "agent",
        ])
        .assert()
        .code(2)
        .get_output()
        .stderr
        .clone();
    let env: Value = serde_json::from_slice(&stderr).expect("valid JSON error envelope");
    assert_eq!(env["ok"], false);
    assert_eq!(env["error"]["code"], "FK_MISSING_ENV");

    let skill_dir = dir.path().join(".agents/skills/fetchkeel");
    fetchkeel()
        .args(["skill", "--install", skill_dir.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("SKILL.md"));
    assert!(skill_dir.join("SKILL.md").exists());
}
