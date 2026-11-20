use serde::Serialize;
use std::path::PathBuf;
use thiserror::Error;

pub const EXIT_OK: i32 = 0;
pub const EXIT_ERROR: i32 = 2;

#[derive(Debug, Error)]
pub enum FetchKeelError {
    #[error("Failed to read '{path}': {source}")]
    IoRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to write '{path}': {source}")]
    IoWrite {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to parse config '{path}': {detail}")]
    ConfigParse { path: String, detail: String },

    #[error("Missing required environment variable '{var}'")]
    MissingEnv { var: String },

    #[error("Stream '{stream}' not found in config (available: {available})")]
    UnknownStream { stream: String, available: String },

    #[error("Invalid URL '{url}': {detail}")]
    InvalidUrl { url: String, detail: String },

    #[error("HTTP request failed for '{url}': {detail}")]
    HttpTransport { url: String, detail: String },

    #[error("HTTP {status} from '{url}': {body_preview}")]
    HttpStatus {
        status: u16,
        url: String,
        body_preview: String,
    },

    #[error("JSON parse error from '{url}': {detail}")]
    JsonParse { url: String, detail: String },

    #[error("JSON data path '{path}' not found or not an array in response from '{url}'")]
    DataPathNotFound { path: String, url: String },

    #[error("{0}")]
    General(String),
}

impl FetchKeelError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::IoRead { .. } => "FK_IO_READ",
            Self::IoWrite { .. } => "FK_IO_WRITE",
            Self::ConfigParse { .. } => "FK_CONFIG_PARSE",
            Self::MissingEnv { .. } => "FK_MISSING_ENV",
            Self::UnknownStream { .. } => "FK_UNKNOWN_STREAM",
            Self::InvalidUrl { .. } => "FK_INVALID_URL",
            Self::HttpTransport { .. } => "FK_HTTP_TRANSPORT",
            Self::HttpStatus { status, .. } if *status == 429 => "FK_RATE_LIMIT_EXCEEDED",
            Self::HttpStatus { status, .. } if *status == 401 || *status == 403 => "FK_AUTH_ERROR",
            Self::HttpStatus { .. } => "FK_HTTP_STATUS",
            Self::JsonParse { .. } => "FK_JSON_PARSE",
            Self::DataPathNotFound { .. } => "FK_DATA_PATH_NOT_FOUND",
            Self::General(_) => "FK_ERROR",
        }
    }

    pub fn fix_hint(&self) -> Option<String> {
        match self {
            Self::MissingEnv { var } => Some(format!(
                "Export the secret/config variable before running: `export {var}=\"...\"` (never hardcode API tokens in config files)."
            )),
            Self::UnknownStream { available, .. } => Some(format!(
                "Pass one of the defined stream names via `--stream <name>`: {available}."
            )),
            Self::HttpStatus { status, .. } if *status == 401 || *status == 403 => Some(
                "Verify your API token env var is valid, unexpired, and has read permission for this endpoint."
                    .to_string(),
            ),
            Self::HttpStatus { status, .. } if *status == 429 => Some(
                "Increase `max_retries` or `rate_limit_delay_ms` in your config and retry."
                    .to_string(),
            ),
            Self::DataPathNotFound { path, .. } => Some(format!(
                "Verify `data_path = \"{path}\"` matches the API response JSON structure (or omit `data_path` if the root is a JSON array)."
            )),
            _ => None,
        }
    }

    pub fn retryable(&self) -> bool {
        match self {
            Self::HttpTransport { .. } => true,
            Self::HttpStatus { status, .. } => *status == 429 || *status >= 500,
            _ => false,
        }
    }

    pub fn to_envelope(&self) -> ErrorEnvelope {
        ErrorEnvelope {
            schema_version: "1.0",
            ok: false,
            error: ErrorBody {
                code: self.code(),
                message: self.to_string(),
                fix_hint: self.fix_hint(),
                retryable: self.retryable(),
            },
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorEnvelope {
    pub schema_version: &'static str,
    pub ok: bool,
    pub error: ErrorBody,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix_hint: Option<String>,
    pub retryable: bool,
}

pub type Result<T> = std::result::Result<T, FetchKeelError>;
