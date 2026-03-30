use tracing::warn;

use crate::cli::AppContext;
use crate::errors::{Error, Result};

#[derive(Debug, Default)]
pub struct TransferOptions {
    pub dry_run: bool,
}

pub fn handle(ctx: &AppContext, command: &str) -> Result<()> {
    warn!(%command, "command not implemented");
    let _ = ctx
        .output
        .write_error(&format!("ria: {command} not implemented"));
    Err(Error::not_implemented(command))
}
