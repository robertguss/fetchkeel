use crate::error::{FetchKeelError, Result};
use std::fs;
use std::path::{Path, PathBuf};

pub const SKILL_MD: &str = r#"---
name: fetchkeel
description: Declaratively extract paginated REST APIs (Canvas LMS, Populi SIS, Stripe, GitHub, etc.) into clean JSONL with automatic RFC 5988 Link-header / cursor / page / offset pagination, HTTP 429 Retry-After backoff, and atomic incremental watermark state. Use instead of hand-writing fragile Python pagination/retry loops.
---

# FetchKeel Agent Manual

## Identity & Scope

`fetchkeel` is the extraction leg of the **Keel Data Stack** (`fetchkeel` extracts → `datakeel` validates → `loadkeel` bulk-loads → `sqlkeel` verifies SQL). Define sources in a declarative `.toml` or `.yaml` manifest (with zero hardcoded secrets) and stream clean JSONL directly to files or `stdout` pipes.

## Output & Exit Code Contract

- **Secret hygiene:** API tokens and base URLs come strictly from environment variables (`auth_env`, `base_url_env`)—never hardcode credentials in manifests.
- **Stream separation:** When `-o -` is used, `stdout` emits **pure JSONL records** so you can pipe directly into `datakeel` or `loadkeel`. When writing to a file, `stdout` emits compact `--format agent` telemetry (when non-TTY) or human output (in TTY). `stderr` receives structured JSON `ErrorEnvelope` on errors.
- **Exit codes:** `0` = success, `2` = config, missing env var, auth, rate-limit, HTTP, or IO error.

## Core Command Matrix

| Goal | Command |
| :--- | :--- |
| Validate manifest, env vars & watermarks offline | `fetchkeel run sources/canvas.toml --state .keel/state.json --dry-run --format agent` |
| Sample first 2 pages safely before full sync | `fetchkeel run sources/canvas.toml --stream courses --max-pages 2 -o /tmp/sample.jsonl --format agent` |
| Run full incremental extraction with watermarks | `fetchkeel run sources/populi.toml --state .keel/state.json --output-dir staging/ --format agent` |
| Stream straight into `loadkeel` via pipe | `fetchkeel run sources/populi.toml --stream students -o - \| loadkeel load - pg://staging.populi_students --flatten --key student_id --auto-create` |
| Ad-hoc paginated fetch (no manifest) | `fetchkeel get "https://api.example.com/v1/items" --bearer-env API_TOKEN --paginate link-header -o items.jsonl` |
| Inspect or reset saved watermark state | `fetchkeel state .keel/state.json --format agent` |

## Agent Decision Tree for API Extraction

1. **Always `--dry-run` first:** Verify all required `*_API_TOKEN` / `*_BASE_URL` env vars exist and resolved URLs look right before hitting production APIs.
2. **Sample with `--max-pages 1` + `datakeel infer`:** Fetch one page, then run `datakeel infer sample.jsonl -o contracts/<stream>.yaml` so you get a ground-truth contract in ~30 lines instead of reading raw JSON.
3. **Run incremental syncs with `--state .keel/state.json`:** State updates atomically only after a clean run. Pass `--full-refresh` when backfilling.
"#;

pub fn install_skill(target: &Path) -> Result<PathBuf> {
    let dest = if target
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("md"))
    {
        target.to_path_buf()
    } else {
        target.join("SKILL.md")
    };

    if let Some(parent) = dest.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| FetchKeelError::IoWrite {
            path: dest.clone(),
            source,
        })?;
    }

    fs::write(&dest, SKILL_MD).map_err(|source| FetchKeelError::IoWrite {
        path: dest.clone(),
        source,
    })?;

    Ok(dest)
}
