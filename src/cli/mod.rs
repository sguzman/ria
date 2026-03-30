use std::env;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing::info;

use crate::config::{self, ConfigOverrides};
use crate::errors::{Error, Result};
use crate::http;
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
    #[arg(long = "output", value_name = "FORMAT")]
    pub output: Option<String>,
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

    let env_path = env::var_os("RIA_CONFIG").map(PathBuf::from);
    let config_path = config::resolve_config_path(cli.config_file.clone());
    let mut config = config::load(config_path.as_deref())?;

    let search_paths = config::config_search_paths(cli.config_file.as_deref(), env_path.as_deref());
    config.apply_overrides(config::overrides_from_env());
    config.apply_overrides(overrides_from_cli(&cli, log_level));
    config::validate(&config)?;

    telemetry::init(&config, log_level)?;
    info!(?search_paths, ?config_path, ?config, "config loaded");

    let http_config = http::config_from_settings(&config);
    let _http_client = http::HttpClient::new(http_config)?;

    match cli.command {
        Some(command) => dispatch(command),
        None => Err(Error::MissingCommand),
    }
}

fn overrides_from_cli(cli: &Cli, log_level: Option<&str>) -> ConfigOverrides {
    ConfigOverrides {
        logging_level: log_level.map(str::to_string),
        insecure: cli.insecure.then_some(true),
        host: cli.host.clone(),
        user_agent_suffix: cli.user_agent_suffix.clone(),
        output_format: cli.output.clone(),
        ..ConfigOverrides::default()
    }
}

fn dispatch(command: Command) -> Result<()> {
    info!(?command, "dispatching command");
    match command {
        Command::Account => crate::domains::account::handle("account"),
        Command::Configure => crate::domains::account::handle("configure"),
        Command::Copy => crate::domains::transfer::handle("copy"),
        Command::Delete => crate::domains::transfer::handle("delete"),
        Command::Download => crate::domains::transfer::handle("download"),
        Command::Flag => crate::domains::account::handle("flag"),
        Command::List => crate::domains::metadata::handle("list"),
        Command::Metadata => crate::domains::metadata::handle("metadata"),
        Command::Move => crate::domains::transfer::handle("move"),
        Command::Reviews => crate::domains::account::handle("reviews"),
        Command::Search => crate::domains::metadata::handle("search"),
        Command::Simplelists => crate::domains::account::handle("simplelists"),
        Command::Tasks => crate::domains::account::handle("tasks"),
        Command::Upload => crate::domains::transfer::handle("upload"),
    }
}

#[cfg(test)]
mod tests {
    use super::Cli;

    #[test]
    fn parses_output_flag() {
        let cli = Cli::parse_from(["ria", "--output", "json", "upload"]);
        assert_eq!(cli.output.as_deref(), Some("json"));
    }

    #[test]
    fn parses_config_file_flag() {
        let cli = Cli::parse_from(["ria", "-c", "ria.toml", "list"]);
        assert_eq!(cli.config_file.as_deref().unwrap().to_str(), Some("ria.toml"));
    }
}
