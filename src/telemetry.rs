use crate::config::Config;
use crate::errors::{Error, Result};
use tracing_subscriber::EnvFilter;

pub fn init(config: &Config, cli_level: Option<&str>) -> Result<()> {
    let logging = config.logging.as_ref();
    if logging.and_then(|log| log.enabled) == Some(false) {
        return Ok(());
    }

    let filter = logging
        .and_then(|log| log.filter.as_deref())
        .or_else(|| logging.and_then(|log| log.level.as_deref()))
        .or(cli_level)
        .unwrap_or("warn");

    let env_filter = EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new(filter));
    let env_filter =
        env_filter.map_err(|err| Error::message(format!("invalid log filter: {err}")))?;

    let format = logging
        .and_then(|log| log.format.as_deref())
        .unwrap_or("pretty");
    let output = logging
        .and_then(|log| log.output.as_deref())
        .unwrap_or("stdout");
    let ansi = logging.and_then(|log| log.ansi).unwrap_or(true);
    let target = logging.and_then(|log| log.target).unwrap_or(true);
    let thread_ids = logging.and_then(|log| log.thread_ids).unwrap_or(true);
    let thread_names = logging.and_then(|log| log.thread_names).unwrap_or(true);

    let builder = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(target)
        .with_thread_ids(thread_ids)
        .with_thread_names(thread_names)
        .with_ansi(ansi);

    match (format, output) {
        ("json", "stderr") => builder.json().with_writer(std::io::stderr).init(),
        ("json", _) => builder.json().with_writer(std::io::stdout).init(),
        (_, "stderr") => builder.with_writer(std::io::stderr).init(),
        _ => builder.with_writer(std::io::stdout).init(),
    }

    Ok(())
}
