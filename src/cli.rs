use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "fetchkeel",
    version,
    about = "Blazing-fast, agent-first (DX + AX) Rust declarative REST API extractor with Link/cursor/page pagination, rate-limit backoff, and incremental watermarks.",
    long_about = None
)]
pub struct Cli {
    /// Output format: auto (human in TTY, agent when piped), human, agent, or json
    #[arg(long, global = true, value_enum, default_value_t = OutputFormatArg::Auto)]
    pub format: OutputFormatArg,

    /// Disable ANSI colors in human output
    #[arg(long, global = true)]
    pub no_color: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormatArg {
    Auto,
    Human,
    Agent,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PaginationArg {
    None,
    LinkHeader,
    PageNumber,
    Cursor,
    Offset,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Extract one or all streams defined in a fetchkeel.toml / .yaml manifest into JSONL
    Run {
        /// Path to source manifest (.toml, .yaml, or .yml)
        config: PathBuf,

        /// Run only a specific stream by name (omit to run all streams in manifest)
        #[arg(long)]
        stream: Option<String>,

        /// Output path override for a single stream (use '-' to stream JSONL to stdout for piping)
        #[arg(short, long)]
        output: Option<String>,

        /// Output directory when running multiple streams without per-stream `output` paths
        #[arg(long)]
        output_dir: Option<PathBuf>,

        /// Path to incremental watermark state JSON file (e.g. .keel/state.json)
        #[arg(long)]
        state: Option<PathBuf>,

        /// Ignore saved watermarks and perform a full extraction
        #[arg(long)]
        full_refresh: bool,

        /// Stop after N pages per stream (useful for safe agent sampling & schema inference)
        #[arg(long)]
        max_pages: Option<usize>,

        /// Validate config, check env vars, and print resolved URLs + watermarks without making HTTP calls
        #[arg(long)]
        dry_run: bool,
    },

    /// Ad-hoc paginated REST API fetch straight to JSONL (no config file required)
    Get {
        /// Target HTTP(S) URL
        url: String,

        /// Environment variable holding a Bearer token (sent as `Authorization: Bearer <token>`)
        #[arg(long)]
        bearer_env: Option<String>,

        /// Custom header in `Name: Value` format (repeatable)
        #[arg(short = 'H', long = "header")]
        headers: Vec<String>,

        /// Pagination strategy
        #[arg(long, value_enum, default_value_t = PaginationArg::None)]
        paginate: PaginationArg,

        /// Dot-separated JSON path to array of records (e.g. 'data.items')
        #[arg(long)]
        data_path: Option<String>,

        /// Cursor JSON path in response body (for --paginate cursor)
        #[arg(long)]
        cursor_path: Option<String>,

        /// Cursor query parameter name (default: 'cursor')
        #[arg(long, default_value = "cursor")]
        cursor_param: String,

        /// Page size for page-number / offset pagination (default: 100)
        #[arg(long, default_value_t = 100)]
        per_page: usize,

        /// Stop after N pages
        #[arg(long)]
        max_pages: Option<usize>,

        /// Output JSONL file path (default: '-' stdout)
        #[arg(short, long, default_value = "-")]
        output: String,
    },

    /// Inspect or reset saved incremental watermark state
    State {
        /// Path to state JSON file
        state_file: PathBuf,

        /// Reset (clear) saved watermark for a specific stream name
        #[arg(long)]
        reset: Option<String>,
    },

    /// Print or install the bundled SKILL.md agent manual (for Claude Code, Antigravity, Codex, Cursor)
    Skill {
        /// Install SKILL.md into a directory (e.g. .agents/skills/fetchkeel)
        #[arg(long)]
        install: Option<PathBuf>,
    },
}
