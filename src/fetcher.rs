use crate::config::{AuthKind, Manifest, PaginationKind, SourceConfig, StreamConfig};
use crate::error::{FetchKeelError, Result};
use crate::state::StateStore;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, LINK, RETRY_AFTER};
use reqwest::{Client, StatusCode};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use url::Url;

#[derive(Debug, Clone, Serialize)]
pub struct StreamRunReport {
    pub stream: String,
    pub output: String,
    pub dry_run: bool,
    pub resolved_url: String,
    pub pagination: PaginationKind,
    pub pages_fetched: usize,
    pub rows_fetched: usize,
    pub retries: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_watermark: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_watermark: Option<String>,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunReport {
    pub schema_version: &'static str,
    pub command: &'static str,
    pub ok: bool,
    pub source: String,
    pub dry_run: bool,
    pub total_pages: usize,
    pub total_rows: usize,
    pub total_retries: u32,
    pub streams: Vec<StreamRunReport>,
}

pub struct RunOptions {
    pub stream_filter: Option<String>,
    pub output_override: Option<String>,
    pub output_dir: Option<std::path::PathBuf>,
    pub state_path: Option<std::path::PathBuf>,
    pub full_refresh: bool,
    pub max_pages: Option<usize>,
    pub dry_run: bool,
}

pub async fn run_manifest(manifest: &Manifest, opts: &RunOptions) -> Result<(RunReport, bool)> {
    let base_url = manifest.resolve_base_url()?;
    let auth_secret = manifest.resolve_auth_secret()?;

    let selected_streams: Vec<&StreamConfig> = if let Some(ref name) = opts.stream_filter {
        let found: Vec<&StreamConfig> = manifest
            .streams
            .iter()
            .filter(|s| s.name == *name)
            .collect();
        if found.is_empty() {
            let available = manifest
                .streams
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(FetchKeelError::UnknownStream {
                stream: name.clone(),
                available,
            });
        }
        found
    } else {
        manifest.streams.iter().collect()
    };

    if opts.output_override.is_some() && selected_streams.len() > 1 {
        return Err(FetchKeelError::General(
            "`--output` can only be used when running a single stream (pass `--stream <name>` or use `--output-dir`)"
                .to_string(),
        ));
    }

    let mut state = match opts.state_path {
        Some(ref p) => StateStore::load_or_empty(p)?,
        None => StateStore::default(),
    };

    let client = build_client(&manifest.source)?;
    let mut reports = Vec::with_capacity(selected_streams.len());
    let mut wrote_to_stdout = false;

    for stream in selected_streams {
        let state_key = format!("{}.{}", manifest.source.name, stream.name);
        let prev_wm = if opts.full_refresh {
            None
        } else {
            state.watermark(&state_key).map(ToString::to_string)
        };

        let out_dest = resolve_output_dest(stream, opts);
        if out_dest == "-" && !opts.dry_run {
            wrote_to_stdout = true;
        }

        let initial_url = build_initial_url(&base_url, stream, prev_wm.as_deref())?;

        if opts.dry_run {
            reports.push(StreamRunReport {
                stream: stream.name.clone(),
                output: out_dest,
                dry_run: true,
                resolved_url: initial_url,
                pagination: stream.pagination,
                pages_fetched: 0,
                rows_fetched: 0,
                retries: 0,
                previous_watermark: prev_wm,
                new_watermark: None,
                elapsed_ms: 0,
            });
            continue;
        }

        let stream_report = fetch_stream(
            &client,
            &manifest.source,
            stream,
            &initial_url,
            auth_secret.as_deref(),
            prev_wm,
            &out_dest,
            opts.max_pages,
        )
        .await?;

        if let Some(ref sp) = opts.state_path {
            state.update(
                state_key,
                stream_report.new_watermark.clone(),
                stream_report.rows_fetched,
            );
            state.save_atomic(sp)?;
        }

        reports.push(stream_report);
    }

    let total_pages = reports.iter().map(|r| r.pages_fetched).sum();
    let total_rows = reports.iter().map(|r| r.rows_fetched).sum();
    let total_retries = reports.iter().map(|r| r.retries).sum();

    Ok((
        RunReport {
            schema_version: "1.0",
            command: "run",
            ok: true,
            source: manifest.source.name.clone(),
            dry_run: opts.dry_run,
            total_pages,
            total_rows,
            total_retries,
            streams: reports,
        },
        wrote_to_stdout,
    ))
}

fn resolve_output_dest(stream: &StreamConfig, opts: &RunOptions) -> String {
    if let Some(ref o) = opts.output_override {
        return o.clone();
    }
    if let Some(ref o) = stream.output {
        return o.clone();
    }
    if let Some(ref dir) = opts.output_dir {
        return dir.join(format!("{}.jsonl", stream.name)).display().to_string();
    }
    format!("{}.jsonl", stream.name)
}

fn build_client(source: &SourceConfig) -> Result<Client> {
    let mut default_headers = HeaderMap::new();
    for (k, v) in &source.headers {
        let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
            FetchKeelError::ConfigParse {
                path: source.name.clone(),
                detail: format!("invalid header name '{k}': {e}"),
            }
        })?;
        let val = HeaderValue::from_str(v).map_err(|e| FetchKeelError::ConfigParse {
            path: source.name.clone(),
            detail: format!("invalid header value for '{k}': {e}"),
        })?;
        default_headers.insert(name, val);
    }

    Client::builder()
        .timeout(Duration::from_secs(source.timeout_secs))
        .default_headers(default_headers)
        .user_agent(format!("fetchkeel/{}", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| FetchKeelError::General(format!("failed to build HTTP client: {e}")))
}

fn build_initial_url(
    base_url: &str,
    stream: &StreamConfig,
    prev_watermark: Option<&str>,
) -> Result<String> {
    let raw = if stream.path.starts_with("http://") || stream.path.starts_with("https://") {
        stream.path.clone()
    } else if stream.path.is_empty() {
        base_url.to_string()
    } else {
        format!(
            "{}/{}",
            base_url.trim_end_matches('/'),
            stream.path.trim_start_matches('/')
        )
    };

    let mut url = Url::parse(&raw).map_err(|e| FetchKeelError::InvalidUrl {
        url: raw.clone(),
        detail: e.to_string(),
    })?;

    {
        let mut qp = url.query_pairs_mut();
        for (k, v) in &stream.params {
            qp.append_pair(k, v);
        }
        if let (Some(wm), Some(wm_param)) = (prev_watermark, stream.watermark_param.as_deref()) {
            qp.append_pair(wm_param, wm);
        }
        match stream.pagination {
            PaginationKind::PageNumber => {
                qp.append_pair(&stream.page_param, "1");
                qp.append_pair(&stream.per_page_param, &stream.per_page.to_string());
            }
            PaginationKind::Offset => {
                qp.append_pair(&stream.offset_param, "0");
                qp.append_pair(&stream.limit_param, &stream.per_page.to_string());
            }
            PaginationKind::LinkHeader => {
                // Only add per_page if not already specified in stream.params
                if !stream.params.contains_key(&stream.per_page_param) && stream.per_page > 0 {
                    qp.append_pair(&stream.per_page_param, &stream.per_page.to_string());
                }
            }
            PaginationKind::Cursor | PaginationKind::None => {}
        }
    }

    Ok(url.to_string())
}

#[allow(clippy::too_many_arguments)]
pub async fn fetch_stream(
    client: &Client,
    source: &SourceConfig,
    stream: &StreamConfig,
    initial_url: &str,
    auth_secret: Option<&str>,
    prev_watermark: Option<String>,
    output_dest: &str,
    max_pages: Option<usize>,
) -> Result<StreamRunReport> {
    let started = Instant::now();
    let mut writer: Box<dyn Write> = if output_dest == "-" {
        Box::new(BufWriter::new(io::stdout()))
    } else {
        let p = Path::new(output_dest);
        if let Some(parent) = p.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|source| FetchKeelError::IoWrite {
                path: p.to_path_buf(),
                source,
            })?;
        }
        let file = File::create(p).map_err(|source| FetchKeelError::IoWrite {
            path: p.to_path_buf(),
            source,
        })?;
        Box::new(BufWriter::new(file))
    };

    let mut current_url = initial_url.to_string();
    let mut page_num: usize = 1;
    let mut offset: usize = 0;
    let mut pages_fetched: usize = 0;
    let mut rows_fetched: usize = 0;
    let mut total_retries: u32 = 0;
    let mut max_watermark: Option<String> = prev_watermark.clone();

    loop {
        if let Some(limit) = max_pages
            && pages_fetched >= limit
        {
            break;
        }

        let (headers, body, retries) =
            send_with_retry(client, source, &current_url, auth_secret).await?;
        total_retries += retries;
        pages_fetched += 1;

        let records = extract_records(&body, stream.data_path.as_deref(), &current_url)?;
        let page_count = records.len();

        for rec in &records {
            if let Some(ref wm_field) = stream.watermark_field
                && let Some(wm_val) = extract_scalar_at_path(rec, wm_field)
                && max_watermark.as_deref().is_none_or(|prev| wm_val.as_str() > prev)
            {
                max_watermark = Some(wm_val);
            }
            serde_json::to_writer(&mut writer, rec)
                .map_err(|e| FetchKeelError::General(format!("JSONL write error: {e}")))?;
            writer
                .write_all(b"\n")
                .map_err(|e| FetchKeelError::General(format!("output write error: {e}")))?;
            rows_fetched += 1;
        }

        // Respect proactive X-Rate-Limit-Remaining throttling if configured
        if let Some(min_rem) = source.rate_limit_min_remaining
            && let Some(rem) = parse_rate_limit_remaining(&headers)
            && rem <= min_rem
        {
            tokio::time::sleep(Duration::from_millis(source.rate_limit_delay_ms)).await;
        }

        // Compute next page URL according to pagination strategy
        let next_url: Option<String> = match stream.pagination {
            PaginationKind::None => None,
            PaginationKind::LinkHeader => headers
                .get_all(LINK)
                .iter()
                .filter_map(|hv| hv.to_str().ok())
                .find_map(parse_link_next),
            PaginationKind::PageNumber => {
                if page_count == 0 || page_count < stream.per_page {
                    None
                } else {
                    page_num += 1;
                    Some(set_query_param(
                        &current_url,
                        &stream.page_param,
                        &page_num.to_string(),
                    )?)
                }
            }
            PaginationKind::Offset => {
                if page_count == 0 || page_count < stream.per_page {
                    None
                } else {
                    offset += page_count;
                    Some(set_query_param(
                        &current_url,
                        &stream.offset_param,
                        &offset.to_string(),
                    )?)
                }
            }
            PaginationKind::Cursor => {
                let cpath = stream.cursor_path.as_deref().unwrap_or("next_cursor");
                extract_scalar_at_path(&body, cpath)
                    .filter(|c| !c.is_empty())
                    .map(|cursor| set_query_param(&current_url, &stream.cursor_param, &cursor))
                    .transpose()?
            }
        };

        match next_url {
            Some(next) if next != current_url => current_url = next,
            _ => break,
        }
    }

    writer
        .flush()
        .map_err(|e| FetchKeelError::General(format!("output flush error: {e}")))?;

    Ok(StreamRunReport {
        stream: stream.name.clone(),
        output: output_dest.to_string(),
        dry_run: false,
        resolved_url: initial_url.to_string(),
        pagination: stream.pagination,
        pages_fetched,
        rows_fetched,
        retries: total_retries,
        previous_watermark: prev_watermark,
        new_watermark: max_watermark,
        elapsed_ms: started.elapsed().as_millis() as u64,
    })
}

async fn send_with_retry(
    client: &Client,
    source: &SourceConfig,
    url: &str,
    auth_secret: Option<&str>,
) -> Result<(HeaderMap, JsonValue, u32)> {
    let mut attempt: u32 = 0;

    loop {
        let mut req_url = Url::parse(url).map_err(|e| FetchKeelError::InvalidUrl {
            url: url.to_string(),
            detail: e.to_string(),
        })?;

        let mut req = client.get(req_url.clone());
        if let Some(secret) = auth_secret {
            match source.auth {
                AuthKind::None => {}
                AuthKind::Bearer => {
                    req = req.bearer_auth(secret);
                }
                AuthKind::Header => {
                    let hname = source.auth_header.as_deref().unwrap_or("Authorization");
                    let hval = match source.auth_prefix.as_deref() {
                        Some(prefix) => format!("{prefix}{secret}"),
                        None => secret.to_string(),
                    };
                    req = req.header(hname, hval);
                }
                AuthKind::Query => {
                    let param = source.auth_query_param.as_deref().unwrap_or("api_key");
                    req_url.query_pairs_mut().append_pair(param, secret);
                    req = client.get(req_url);
                }
            }
        }

        let resp_res = req.send().await;
        match resp_res {
            Ok(resp) => {
                let status = resp.status();
                let headers = resp.headers().clone();

                if status.is_success() {
                    let text = resp.text().await.map_err(|e| FetchKeelError::HttpTransport {
                        url: url.to_string(),
                        detail: e.to_string(),
                    })?;
                    let json: JsonValue =
                        serde_json::from_str(&text).map_err(|e| FetchKeelError::JsonParse {
                            url: url.to_string(),
                            detail: e.to_string(),
                        })?;
                    return Ok((headers, json, attempt));
                }

                let retryable_status = status == StatusCode::TOO_MANY_REQUESTS
                    || status.is_server_error();
                if retryable_status && attempt < source.max_retries {
                    let delay = compute_retry_delay(&headers, source.base_backoff_ms, attempt);
                    attempt += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }

                let body = resp.text().await.unwrap_or_default();
                let preview: String = body.chars().take(240).collect();
                return Err(FetchKeelError::HttpStatus {
                    status: status.as_u16(),
                    url: url.to_string(),
                    body_preview: preview,
                });
            }
            Err(err) => {
                if attempt < source.max_retries {
                    let delay = compute_retry_delay(&HeaderMap::new(), source.base_backoff_ms, attempt);
                    attempt += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(FetchKeelError::HttpTransport {
                    url: url.to_string(),
                    detail: err.to_string(),
                });
            }
        }
    }
}

fn compute_retry_delay(headers: &HeaderMap, base_ms: u64, attempt: u32) -> Duration {
    if let Some(ra) = headers.get(RETRY_AFTER).and_then(|v| v.to_str().ok())
        && let Ok(secs) = ra.trim().parse::<u64>()
    {
        return Duration::from_secs(secs.min(60));
    }
    let exp = base_ms.saturating_mul(1u64 << attempt.min(6));
    Duration::from_millis(exp.min(30_000))
}

fn parse_rate_limit_remaining(headers: &HeaderMap) -> Option<f64> {
    headers
        .get("x-rate-limit-remaining")
        .or_else(|| headers.get("x-ratelimit-remaining"))
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<f64>().ok())
}

fn extract_records(body: &JsonValue, data_path: Option<&str>, url: &str) -> Result<Vec<JsonValue>> {
    let target = if let Some(path) = data_path {
        let mut cur = body;
        for seg in path.split('.') {
            cur = cur
                .get(seg)
                .ok_or_else(|| FetchKeelError::DataPathNotFound {
                    path: path.to_string(),
                    url: url.to_string(),
                })?;
        }
        cur
    } else {
        body
    };

    match target {
        JsonValue::Array(arr) => Ok(arr.clone()),
        JsonValue::Object(_) if data_path.is_none() => Ok(vec![target.clone()]),
        _ => Err(FetchKeelError::DataPathNotFound {
            path: data_path.unwrap_or("<root>").to_string(),
            url: url.to_string(),
        }),
    }
}

fn extract_scalar_at_path(val: &JsonValue, path: &str) -> Option<String> {
    let mut cur = val;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    match cur {
        JsonValue::String(s) if !s.is_empty() => Some(s.clone()),
        JsonValue::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn set_query_param(raw_url: &str, key: &str, value: &str) -> Result<String> {
    let mut url = Url::parse(raw_url).map_err(|e| FetchKeelError::InvalidUrl {
        url: raw_url.to_string(),
        detail: e.to_string(),
    })?;
    let pairs: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(k, _)| k != key)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    {
        let mut qp = url.query_pairs_mut();
        qp.clear();
        for (k, v) in pairs {
            qp.append_pair(&k, &v);
        }
        qp.append_pair(key, value);
    }
    Ok(url.to_string())
}

/// Parse RFC 5988 HTTP `Link` header (used by Canvas LMS, GitHub, Shopify, etc.) for `rel="next"`.
pub fn parse_link_next(header: &str) -> Option<String> {
    // Split carefully outside `<...>` angle brackets so commas inside URLs don't break parsing.
    let mut parts = Vec::new();
    let mut in_angle = false;
    let mut start = 0;
    for (idx, ch) in header.char_indices() {
        match ch {
            '<' => in_angle = true,
            '>' => in_angle = false,
            ',' if !in_angle => {
                parts.push(&header[start..idx]);
                start = idx + 1;
            }
            _ => {}
        }
    }
    parts.push(&header[start..]);

    for part in parts {
        let trimmed = part.trim();
        let Some(lt) = trimmed.find('<') else { continue };
        let Some(gt) = trimmed[lt + 1..].find('>') else {
            continue;
        };
        let url = trimmed[lt + 1..lt + 1 + gt].trim();
        let params = &trimmed[lt + 1 + gt + 1..];
        let is_next = params.split(';').any(|p| {
            let p = p.trim();
            if let Some((k, v)) = p.split_once('=') {
                let k = k.trim();
                let v = v.trim().trim_matches('"').trim_matches('\'');
                k.eq_ignore_ascii_case("rel")
                    && v.split_whitespace()
                        .any(|tok| tok.eq_ignore_ascii_case("next"))
            } else {
                false
            }
        });
        if is_next && !url.is_empty() {
            return Some(url.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canvas_rfc5988_link_header() {
        let hdr = r#"<https://school.instructure.com/api/v1/courses?page=1&per_page=100>; rel="current", <https://school.instructure.com/api/v1/courses?page=bookmark:WzEsMTAwXQ&per_page=100>; rel="next", <https://school.instructure.com/api/v1/courses?page=last&per_page=100>; rel="last""#;
        assert_eq!(
            parse_link_next(hdr).as_deref(),
            Some("https://school.instructure.com/api/v1/courses?page=bookmark:WzEsMTAwXQ&per_page=100")
        );
        assert_eq!(
            parse_link_next(r#"<https://example.com/p2>; rel="prev""#),
            None
        );
    }
}
