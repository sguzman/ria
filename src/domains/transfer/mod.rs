use tracing::warn;

use crate::errors::{Error, Result};

#[derive(Debug, Default)]
pub struct TransferOptions {
    pub dry_run: bool,
}

pub fn handle(command: &str) -> Result<()> {
    warn!(%command, "command not implemented");
    Err(Error::not_implemented(command))
}
