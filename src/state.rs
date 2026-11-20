use crate::error::{FetchKeelError, Result};
use chrono::Utc;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StateStore {
    pub schema_version: String,
    #[serde(default)]
    pub streams: IndexMap<String, StreamState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_watermark: Option<String>,
    pub last_run_at: String,
    pub last_rows_fetched: usize,
}

impl StateStore {
    pub fn load_or_empty(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self {
                schema_version: "1.0".to_string(),
                streams: IndexMap::new(),
            });
        }
        let raw = fs::read_to_string(path).map_err(|source| FetchKeelError::IoRead {
            path: path.to_path_buf(),
            source,
        })?;
        let mut store: Self =
            serde_json::from_str(&raw).map_err(|e| FetchKeelError::ConfigParse {
                path: path.display().to_string(),
                detail: format!("invalid state JSON: {e}"),
            })?;
        if store.schema_version.is_empty() {
            store.schema_version = "1.0".to_string();
        }
        Ok(store)
    }

    pub fn save_atomic(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|source| FetchKeelError::IoWrite {
                path: path.to_path_buf(),
                source,
            })?;
        }
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| FetchKeelError::General(format!("state JSON encode error: {e}")))?;
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, &json).map_err(|source| FetchKeelError::IoWrite {
            path: tmp.clone(),
            source,
        })?;
        fs::rename(&tmp, path).map_err(|source| FetchKeelError::IoWrite {
            path: path.to_path_buf(),
            source,
        })?;
        Ok(())
    }

    pub fn watermark(&self, key: &str) -> Option<&str> {
        self.streams
            .get(key)
            .and_then(|s| s.last_watermark.as_deref())
    }

    pub fn update(&mut self, key: String, watermark: Option<String>, rows_fetched: usize) {
        let prev = self
            .streams
            .get(&key)
            .and_then(|s| s.last_watermark.clone());
        self.streams.insert(
            key,
            StreamState {
                last_watermark: watermark.or(prev),
                last_run_at: Utc::now().to_rfc3339(),
                last_rows_fetched: rows_fetched,
            },
        );
    }
}
