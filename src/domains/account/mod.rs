use std::io::{self, Write};

use tracing::{info, instrument, warn};

use crate::cli::AppContext;
use crate::config;
use crate::errors::{Error, Result};

#[derive(Debug, Default)]
pub struct AuthStatus {
    pub username: Option<String>,
}

#[instrument(skip(ctx))]
pub fn configure(ctx: &AppContext) -> Result<()> {
    let destination = ctx
        .config_destination
        .as_ref()
        .ok_or_else(|| Error::message("unable to resolve config destination path"))?;
    let access_key = prompt("Access key")?;
    let secret_key = prompt("Secret key")?;
    if access_key.is_empty() || secret_key.is_empty() {
        return Err(Error::message("access key and secret key are required"));
    }

    let mut config = ctx.config.clone();
    let auth = config
        .auth
        .get_or_insert_with(crate::config::AuthConfig::default);
    auth.access_key = Some(access_key);
    auth.secret_key = Some(secret_key);

    config::save_to_path(&config, destination)?;
    info!(path = %destination.display(), "wrote config file");
    ctx.output
        .write_line(&format!(
            "Configured credentials in {}",
            destination.display()
        ))
        .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
    Ok(())
}

#[instrument(skip(ctx))]
pub fn account(ctx: &AppContext) -> Result<()> {
    let access_key = ctx
        .config
        .auth
        .as_ref()
        .and_then(|auth| auth.access_key.as_deref());
    let secret_key = ctx
        .config
        .auth
        .as_ref()
        .and_then(|auth| auth.secret_key.as_deref());

    match (access_key, secret_key) {
        (Some(access), Some(_)) => {
            ctx.output
                .write_line(&format!("Access key: {access}"))
                .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            ctx.output
                .write_line("Secret key: configured")
                .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            Ok(())
        }
        _ => {
            ctx.output
                .write_line("No credentials configured")
                .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            Err(Error::message(
                "no credentials configured (run `ria configure`)",
            ))
        }
    }
}

#[instrument(skip(ctx))]
pub fn reviews(ctx: &AppContext) -> Result<()> {
    not_implemented(ctx, "reviews")
}

#[instrument(skip(ctx))]
pub fn flag(ctx: &AppContext) -> Result<()> {
    not_implemented(ctx, "flag")
}

#[instrument(skip(ctx))]
pub fn simplelists(ctx: &AppContext) -> Result<()> {
    not_implemented(ctx, "simplelists")
}

#[instrument(skip(ctx))]
pub fn tasks(ctx: &AppContext) -> Result<()> {
    not_implemented(ctx, "tasks")
}

fn not_implemented(ctx: &AppContext, command: &str) -> Result<()> {
    warn!(%command, "command not implemented");
    let _ = ctx
        .output
        .write_error(&format!("ria: {command} not implemented"));
    Err(Error::not_implemented(command))
}

fn prompt(label: &str) -> Result<String> {
    let mut stdout = io::stdout();
    write!(stdout, "{label}: ")
        .and_then(|_| stdout.flush())
        .map_err(|err| Error::message(format!("failed to prompt: {err}")))?;
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|err| Error::message(format!("failed to read input: {err}")))?;
    Ok(input.trim().to_string())
}
