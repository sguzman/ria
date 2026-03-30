use tracing::warn;

use crate::errors::{Error, Result};

#[derive(Debug, Default)]
pub struct SearchQuery {
    pub query: String,
}

pub fn handle(command: &str) -> Result<()> {
    warn!(%command, "command not implemented");
    Err(Error::not_implemented(command))
}
