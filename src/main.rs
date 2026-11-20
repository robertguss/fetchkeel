use clap::Parser;
use fetchkeel::cli::{Cli, Commands, PaginationArg};
use fetchkeel::config::{AuthKind, Manifest, PaginationKind, SourceConfig, StreamConfig};
use fetchkeel::error::{EXIT_ERROR, EXIT_OK, FetchKeelError, Result};
use fetchkeel::fetcher::{self, RunOptions};
use fetchkeel::output::{self, ResolvedFormat};
use fetchkeel::skill;
use fetchkeel::state::StateStore;
use indexmap::IndexMap;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let format = output::resolve_format(cli.format);
    let use_color = output::should_use_color(cli.no_color, format);

    match run(cli, format, use_color).await {
        Ok(code) => ExitCode::from(code as u8),
        Err(err) => {
            output::emit_error(&err, format, use_color);
            ExitCode::from(EXIT_ERROR as u8)
        }
    }
}

async fn run(cli: Cli, format: ResolvedFormat, use_color: bool) -> Result<i32> {
    match cli.command {
        Commands::Run {
            config,
            stream,
            output: output_override,
            output_dir,
            state,
            full_refresh,
            max_pages,
            dry_run,
        } => {
            let manifest = Manifest::load(&config)?;
            let (report, wrote_to_stdout) = fetcher::run_manifest(
                &manifest,
                &RunOptions {
                    stream_filter: stream,
                    output_override,
                    output_dir,
                    state_path: state,
                    full_refresh,
                    max_pages,
                    dry_run,
                },
            )
            .await?;

            // Keep stdout strictly pure JSONL when piping with `-o -`
            if !wrote_to_stdout {
                output::emit_run_report(&report, format, use_color)?;
            }
            Ok(EXIT_OK)
        }

        Commands::Get {
            url,
            bearer_env,
            headers,
            paginate,
            data_path,
            cursor_path,
            cursor_param,
            per_page,
            max_pages,
            output: out_dest,
        } => {
            let mut hdr_map = IndexMap::new();
            for raw in headers {
                let (k, v) = raw.split_once(':').ok_or_else(|| {
                    FetchKeelError::General(format!(
                        "Invalid header '{raw}': expected `Name: Value`"
                    ))
                })?;
                hdr_map.insert(k.trim().to_string(), v.trim().to_string());
            }

            let pagination = match paginate {
                PaginationArg::None => PaginationKind::None,
                PaginationArg::LinkHeader => PaginationKind::LinkHeader,
                PaginationArg::PageNumber => PaginationKind::PageNumber,
                PaginationArg::Cursor => PaginationKind::Cursor,
                PaginationArg::Offset => PaginationKind::Offset,
            };

            let manifest = Manifest {
                source: SourceConfig {
                    name: "adhoc".to_string(),
                    base_url: Some(url.clone()),
                    base_url_env: None,
                    auth: if bearer_env.is_some() {
                        AuthKind::Bearer
                    } else {
                        AuthKind::None
                    },
                    auth_env: bearer_env,
                    auth_header: None,
                    auth_prefix: None,
                    auth_query_param: None,
                    headers: hdr_map,
                    timeout_secs: 30,
                    max_retries: 4,
                    base_backoff_ms: 250,
                    rate_limit_min_remaining: None,
                    rate_limit_delay_ms: 500,
                },
                streams: vec![StreamConfig {
                    name: "get".to_string(),
                    path: String::new(),
                    params: IndexMap::new(),
                    data_path,
                    pagination,
                    page_param: "page".to_string(),
                    per_page_param: "per_page".to_string(),
                    per_page,
                    cursor_path,
                    cursor_param,
                    offset_param: "offset".to_string(),
                    limit_param: "limit".to_string(),
                    has_more_path: None,
                    page_in_body: false,
                    body: None,
                    watermark_field: None,
                    watermark_param: None,
                    output: Some(out_dest),
                }],
            };

            let (report, wrote_to_stdout) = fetcher::run_manifest(
                &manifest,
                &RunOptions {
                    stream_filter: Some("get".to_string()),
                    output_override: None,
                    output_dir: None,
                    state_path: None,
                    full_refresh: true,
                    max_pages,
                    dry_run: false,
                },
            )
            .await?;

            if !wrote_to_stdout {
                output::emit_run_report(&report, format, use_color)?;
            }
            Ok(EXIT_OK)
        }

        Commands::State { state_file, reset } => {
            let mut store = StateStore::load_or_empty(&state_file)?;
            if let Some(stream_key) = reset {
                store.streams.shift_remove(&stream_key);
                store.save_atomic(&state_file)?;
            }
            output::emit_state(&state_file, &store, format, use_color)?;
            Ok(EXIT_OK)
        }

        Commands::Skill { install } => {
            if let Some(dir) = install {
                let installed = skill::install_skill(&dir)?;
                output::emit_skill_installed(&installed, format, use_color)?;
            } else {
                print!("{}", skill::SKILL_MD);
            }
            Ok(EXIT_OK)
        }
    }
}
