use crate::error::{FetchKeelError, Result};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Manifest {
    pub source: SourceConfig,
    #[serde(default)]
    pub streams: Vec<StreamConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthKind {
    #[default]
    None,
    Bearer,
    Header,
    Query,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PaginationKind {
    #[default]
    None,
    LinkHeader,
    PageNumber,
    Cursor,
    Offset,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    pub name: String,
    /// Literal base URL (e.g. "https://school.instructure.com/api/v1")
    pub base_url: Option<String>,
    /// Environment variable containing the base URL (e.g. "CANVAS_BASE_URL")
    pub base_url_env: Option<String>,
    #[serde(default)]
    pub auth: AuthKind,
    /// Environment variable holding the API token/key (NEVER hardcode secrets in config)
    pub auth_env: Option<String>,
    /// Custom header name for `auth = "header"` (default: "Authorization")
    pub auth_header: Option<String>,
    /// Optional prefix for header auth (e.g. "Token " or "Bearer ")
    pub auth_prefix: Option<String>,
    /// Query param name for `auth = "query"` (e.g. "api_key")
    pub auth_query_param: Option<String>,
    #[serde(default)]
    pub headers: IndexMap<String, String>,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_base_backoff_ms")]
    pub base_backoff_ms: u64,
    /// Optional throttle delay when X-Rate-Limit-Remaining falls below this threshold
    pub rate_limit_min_remaining: Option<f64>,
    #[serde(default = "default_rate_limit_delay_ms")]
    pub rate_limit_delay_ms: u64,
}

fn default_timeout_secs() -> u64 {
    30
}

fn default_max_retries() -> u32 {
    4
}

fn default_base_backoff_ms() -> u64 {
    250
}

fn default_rate_limit_delay_ms() -> u64 {
    500
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamConfig {
    pub name: String,
    /// Endpoint path appended to `base_url` (e.g. "/courses" or "/people")
    pub path: String,
    #[serde(default)]
    pub params: IndexMap<String, String>,
    /// Dot-separated path to JSON array inside response envelope (omit if root is an array)
    pub data_path: Option<String>,
    #[serde(default)]
    pub pagination: PaginationKind,
    #[serde(default = "default_page_param")]
    pub page_param: String,
    #[serde(default = "default_per_page_param")]
    pub per_page_param: String,
    #[serde(default = "default_per_page")]
    pub per_page: usize,
    /// Dot-separated path to next cursor in response body (for `pagination = "cursor"`)
    pub cursor_path: Option<String>,
    #[serde(default = "default_cursor_param")]
    pub cursor_param: String,
    #[serde(default = "default_offset_param")]
    pub offset_param: String,
    #[serde(default = "default_limit_param")]
    pub limit_param: String,
    /// Dot-separated path to boolean continuation flag in response (e.g. "has_more")
    pub has_more_path: Option<String>,
    /// When true, send page number inside the JSON request body (e.g. Populi API v2) instead of query string
    #[serde(default)]
    pub page_in_body: bool,
    /// Optional static JSON body merged into every request (e.g. Populi API v2 filters/expand)
    pub body: Option<serde_json::Value>,
    /// Field on each record used to track incremental watermark (e.g. "updated_at")
    pub watermark_field: Option<String>,
    /// Query parameter used to pass the saved watermark (e.g. "updated_since")
    pub watermark_param: Option<String>,
    /// Default output path for this stream (e.g. "data/populi_students.jsonl")
    pub output: Option<String>,
}

fn default_page_param() -> String {
    "page".to_string()
}

fn default_per_page_param() -> String {
    "per_page".to_string()
}

fn default_per_page() -> usize {
    100
}

fn default_cursor_param() -> String {
    "cursor".to_string()
}

fn default_offset_param() -> String {
    "offset".to_string()
}

fn default_limit_param() -> String {
    "limit".to_string()
}

impl Manifest {
    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path).map_err(|source| FetchKeelError::IoRead {
            path: path.to_path_buf(),
            source,
        })?;
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let src = path.display().to_string();

        let manifest: Self = match ext.as_str() {
            "toml" => toml::from_str(&content).map_err(|e| FetchKeelError::ConfigParse {
                path: src.clone(),
                detail: e.to_string(),
            })?,
            "yaml" | "yml" => {
                serde_yaml_ng::from_str(&content).map_err(|e| FetchKeelError::ConfigParse {
                    path: src.clone(),
                    detail: e.to_string(),
                })?
            }
            _ => {
                return Err(FetchKeelError::ConfigParse {
                    path: src,
                    detail: format!(
                        "unsupported manifest extension '.{ext}' (expected .toml, .yaml, or .yml)"
                    ),
                });
            }
        };

        if manifest.streams.is_empty() {
            return Err(FetchKeelError::ConfigParse {
                path: path.display().to_string(),
                detail: "manifest must define at least one stream in `[[streams]]`".to_string(),
            });
        }

        Ok(manifest)
    }

    pub fn resolve_base_url(&self) -> Result<String> {
        if let Some(ref var) = self.source.base_url_env {
            let val = std::env::var(var)
                .ok()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| FetchKeelError::MissingEnv { var: var.clone() })?;
            return Ok(val.trim_end_matches('/').to_string());
        }
        if let Some(ref u) = self.source.base_url {
            return Ok(u.trim_end_matches('/').to_string());
        }
        Err(FetchKeelError::ConfigParse {
            path: self.source.name.clone(),
            detail: "source must set either `base_url` or `base_url_env`".to_string(),
        })
    }

    pub fn resolve_auth_secret(&self) -> Result<Option<String>> {
        if self.source.auth == AuthKind::None {
            return Ok(None);
        }
        let var = self
            .source
            .auth_env
            .as_ref()
            .ok_or_else(|| FetchKeelError::ConfigParse {
                path: self.source.name.clone(),
                detail: "source with auth enabled must specify `auth_env`".to_string(),
            })?;
        let val = std::env::var(var)
            .ok()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| FetchKeelError::MissingEnv { var: var.clone() })?;
        Ok(Some(val))
    }
}
