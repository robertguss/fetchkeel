use crate::cli::OutputFormatArg;
use crate::error::{FetchKeelError, Result};
use crate::fetcher::RunReport;
use crate::state::StateStore;
use owo_colors::OwoColorize;
use std::io::IsTerminal;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedFormat {
    Human,
    Agent,
    Json,
}

pub fn resolve_format(arg: OutputFormatArg) -> ResolvedFormat {
    match arg {
        OutputFormatArg::Human => ResolvedFormat::Human,
        OutputFormatArg::Agent => ResolvedFormat::Agent,
        OutputFormatArg::Json => ResolvedFormat::Json,
        OutputFormatArg::Auto => {
            if std::io::stdout().is_terminal() {
                ResolvedFormat::Human
            } else {
                ResolvedFormat::Agent
            }
        }
    }
}

pub fn should_use_color(no_color: bool, format: ResolvedFormat) -> bool {
    if no_color || format != ResolvedFormat::Human {
        return false;
    }
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    std::io::stdout().is_terminal()
}

pub fn emit_run_report(report: &RunReport, format: ResolvedFormat, color: bool) -> Result<()> {
    match format {
        ResolvedFormat::Human => {
            let badge = if report.dry_run {
                if color {
                    " DRY RUN ".on_yellow().black().bold().to_string()
                } else {
                    "[DRY RUN]".to_string()
                }
            } else if color {
                " FETCHED ".on_green().black().bold().to_string()
            } else {
                "[FETCHED]".to_string()
            };
            println!(
                "{badge} {} — {} stream(s), {} page(s), {} row(s) ({} retries)",
                report.source,
                report.streams.len(),
                report.total_pages,
                report.total_rows,
                report.total_retries
            );
            for s in &report.streams {
                println!(
                    "\n  {} -> {} ({} page(s), {} row(s), {}ms)",
                    s.stream, s.output, s.pages_fetched, s.rows_fetched, s.elapsed_ms
                );
                if s.dry_run {
                    println!("    url: {}", s.resolved_url);
                }
                if s.previous_watermark.is_some() || s.new_watermark.is_some() {
                    println!(
                        "    watermark: {} -> {}",
                        s.previous_watermark.as_deref().unwrap_or("<none>"),
                        s.new_watermark.as_deref().unwrap_or("<none>")
                    );
                }
            }
            Ok(())
        }
        ResolvedFormat::Agent => {
            let summary = if report.dry_run {
                format!(
                    "DRY RUN: validated {} stream(s) for source '{}'.",
                    report.streams.len(),
                    report.source
                )
            } else {
                format!(
                    "FETCHED: {} row(s) across {} page(s) from '{}' ({} retries).",
                    report.total_rows, report.total_pages, report.source, report.total_retries
                )
            };
            let payload = serde_json::json!({
                "schema_version": report.schema_version,
                "command": report.command,
                "ok": report.ok,
                "dry_run": report.dry_run,
                "summary": summary,
                "source": report.source,
                "total_pages": report.total_pages,
                "total_rows": report.total_rows,
                "total_retries": report.total_retries,
                "streams": report.streams,
            });
            let json = serde_json::to_string(&payload)
                .map_err(|e| FetchKeelError::General(format!("JSON encode error: {e}")))?;
            println!("{json}");
            Ok(())
        }
        ResolvedFormat::Json => {
            let json = serde_json::to_string_pretty(report)
                .map_err(|e| FetchKeelError::General(format!("JSON encode error: {e}")))?;
            println!("{json}");
            Ok(())
        }
    }
}

pub fn emit_state(
    state_path: &Path,
    store: &StateStore,
    format: ResolvedFormat,
    color: bool,
) -> Result<()> {
    match format {
        ResolvedFormat::Human => {
            println!(
                "State ({}): {} stream(s)\n",
                state_path.display(),
                store.streams.len()
            );
            for (name, st) in &store.streams {
                let wm = st.last_watermark.as_deref().unwrap_or("<none>");
                if color {
                    println!(
                        "  {}  watermark={}  rows={}  last_run={}",
                        name.cyan().bold(),
                        wm,
                        st.last_rows_fetched,
                        st.last_run_at.dimmed()
                    );
                } else {
                    println!(
                        "  {name}  watermark={wm}  rows={}  last_run={}",
                        st.last_rows_fetched, st.last_run_at
                    );
                }
            }
            Ok(())
        }
        ResolvedFormat::Agent | ResolvedFormat::Json => {
            let s = serde_json::to_string_pretty(store)
                .map_err(|e| FetchKeelError::General(format!("JSON encode error: {e}")))?;
            println!("{s}");
            Ok(())
        }
    }
}

pub fn emit_skill_installed(
    installed_path: &Path,
    format: ResolvedFormat,
    color: bool,
) -> Result<()> {
    match format {
        ResolvedFormat::Human => {
            let badge = if color {
                " INSTALLED ".on_green().black().bold().to_string()
            } else {
                "[INSTALLED]".to_string()
            };
            println!(
                "{badge} Agent skill written to {}",
                installed_path.display()
            );
            Ok(())
        }
        ResolvedFormat::Agent | ResolvedFormat::Json => {
            let s = serde_json::to_string(&serde_json::json!({
                "schema_version": "1.0",
                "command": "skill",
                "ok": true,
                "installed_path": installed_path.display().to_string(),
            }))
            .map_err(|e| FetchKeelError::General(format!("JSON encode error: {e}")))?;
            println!("{s}");
            Ok(())
        }
    }
}

pub fn emit_error(err: &FetchKeelError, format: ResolvedFormat, color: bool) {
    match format {
        ResolvedFormat::Human => {
            let badge = if color {
                " ERROR ".on_red().white().bold().to_string()
            } else {
                "[ERROR]".to_string()
            };
            eprintln!("{badge} {}: {err}", err.code());
            if let Some(hint) = err.fix_hint() {
                eprintln!("  hint: {hint}");
            }
        }
        ResolvedFormat::Agent | ResolvedFormat::Json => {
            let env = err.to_envelope();
            if let Ok(json) = serde_json::to_string_pretty(&env) {
                eprintln!("{json}");
            } else {
                eprintln!("{{\"ok\":false,\"error\":{{\"code\":\"{}\"}}}}", err.code());
            }
        }
    }
}
