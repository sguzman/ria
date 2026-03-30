use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing::info;

use crate::config::{self, ConfigOverrides};
use crate::errors::{Error, Result};
use crate::telemetry;

#[derive(Debug, Parser)]
#[command(
    name = "ria",
    version,
    about = "A command line interface to Archive.org.",
    long_about = "A command line interface to Archive.org.\n\nDocumentation: https://archive.org/developers/internetarchive/cli.html",
    arg_required_else_help = true,
    disable_help_subcommand = true
)]
pub struct Cli {
    #[arg(short = 'c', long = "config-file", value_name = "FILE")]
    pub config_file: Option<PathBuf>,
    #[arg(short = 'l', long = "log")]
    pub log: bool,
    #[arg(short = 'd', long = "debug")]
    pub debug: bool,
    #[arg(short = 'i', long = "insecure")]
    pub insecure: bool,
    #[arg(short = 'H', long = "host")]
    pub host: Option<String>,
    #[arg(long = "user-agent-suffix", value_name = "STRING")]
    pub user_agent_suffix: Option<String>,
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Account,
    Configure,
    Copy,
    Delete,
    Download,
    Flag,
    List,
    Metadata,
    Move,
    Reviews,
    Search,
    Simplelists,
    Tasks,
    Upload,
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    let log_level = if cli.debug {
        Some("debug")
    } else if cli.log {
        Some("info")
    } else {
        None
    };

    telemetry::init(log_level)?;
    info!(?log_level, "telemetry initialized");

    let config_path = config::resolve_config_path(cli.config_file.clone());
    let mut config = config::load(config_path.as_deref())?;

    let overrides = ConfigOverrides {
        logging_level: log_level.map(str::to_string),
        insecure: cli.insecure.then_some(true),
        host: cli.host.clone(),
        user_agent_suffix: cli.user_agent_suffix.clone(),
    };

    config.apply_overrides(overrides);
    info!(?config_path, ?config, "config loaded");

    match cli.command {
        Some(command) => {
            info!(?command, "dispatching command");
            Ok(())
        }
        None => Err(Error::MissingCommand),
    }
}
