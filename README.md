# fetchkeel

**Blazing-fast, agent-first (DX + AX) Rust declarative REST API extractor with RFC 5988 `Link`-header, cursor, page, and offset pagination, HTTP 429 `Retry-After` backoff, and atomic incremental watermarks.**

The extraction leg of the **Keel Data Stack**:
- **`fetchkeel`** — Extracts paginated REST APIs (Canvas LMS, Populi SIS, Stripe, GitHub, etc.) into clean JSONL
- **[`datakeel`](https://github.com/robertguss/datakeel)** — Validates extracts, catches drift, splits clean vs. quarantined rows
- **[`loadkeel`](https://github.com/robertguss/loadkeel)** — Flattens nested JSON & bulk-loads into Postgres via `COPY` + atomic upserts
- **[`sqlkeel`](https://github.com/robertguss/sqlkeel)** — Statically verifies downstream SQL views, migrations, and column-level lineage

## Why `fetchkeel`?

Every API ETL project ends up rewriting the exact same fragile Python boilerplate: pagination loops (`Link` headers for Canvas, page/cursor tokens for Populi), `429 Retry-After` backoff, `X-Rate-Limit-Remaining` throttling, and incremental watermark tracking. `fetchkeel` replaces all of that with a single pure-Rust binary (`rustls`, zero OpenSSL deps) driven by a clean TOML/YAML manifest.

## Quickstart

```bash
export CANVAS_BASE_URL="https://your-school.instructure.com"
export CANVAS_API_TOKEN="..."

# 1. Dry-run to verify env vars, URLs, and saved watermarks (zero HTTP calls)
fetchkeel run examples/seminary-api/canvas.toml --state .keel/state.json --dry-run

# 2. Sample 2 pages to infer a datakeel contract
fetchkeel run examples/seminary-api/canvas.toml --stream courses --max-pages 2 -o /tmp/sample.jsonl
datakeel infer /tmp/sample.jsonl -o contracts/canvas_courses.yaml

# 3. Full incremental sync -> validate -> load straight into Postgres
fetchkeel run examples/seminary-api/populi.toml --stream students --state .keel/state.json -o /tmp/populi.jsonl
datakeel check contracts/populi_students.yaml /tmp/populi.jsonl --clean /tmp/clean.jsonl --quarantine /tmp/bad.jsonl --max-quarantine-rate 0.05
loadkeel load /tmp/clean.jsonl pg://staging.populi_students --flatten --key student_id --auto-create --evolve

# 4. Install the built-in AI Agent Skill
fetchkeel skill --install .agents/skills/fetchkeel
```

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).
