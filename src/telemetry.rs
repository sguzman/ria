use crate::errors::{Error, Result};
use tracing_subscriber::EnvFilter;

pub fn init(default_level: Option<&str>) -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env().or_else(|_| {
        let level = default_level.unwrap_or("warn");
        EnvFilter::try_new(level)
    });

    let env_filter = env_filter.map_err(|err| Error::message(format!("invalid log filter: {err}")))?;

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .init();

    Ok(())
}
