use std::io::{self, Write};

use tracing::{info, instrument, warn};

use crate::cli::{
    AppContext, FlagArgs, ReviewsArgs, SimplelistsArgs, TasksArgs,
};
use crate::config;
use crate::errors::{Error, Result};
use crate::output::OutputFormat;
use serde_json::Value;

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

#[instrument(skip(ctx, args))]
pub fn reviews(ctx: &AppContext, args: &ReviewsArgs) -> Result<()> {
    let auth = low_auth_header(&ctx.config)?;
    let url = format!(
        "{}/services/reviews.php",
        ctx.http.api_base().trim_end_matches('/')
    );
    let mut params = vec![("identifier".to_string(), args.identifier.clone())];

    if args.delete {
        let mut form = Vec::new();
        if let Some(value) = &args.username {
            form.push(("username".to_string(), value.clone()));
        }
        if let Some(value) = &args.screenname {
            form.push(("screenname".to_string(), value.clone()));
        }
        if let Some(value) = &args.itemname {
            form.push(("itemname".to_string(), value.clone()));
        }
        let response = ctx.http.delete_form(&url, &params, &form, &[auth_header(&auth)])?;
        return output_json_or_raw(ctx, &response);
    }

    if let (Some(title), Some(body)) = (&args.title, &args.body) {
        let mut payload = serde_json::json!({
            "title": title,
            "body": body,
        });
        if let Some(stars) = args.stars {
            payload["stars"] = Value::from(stars);
        }
        let response = ctx.http.post_json(&url, &params, &payload, &[auth_header(&auth)])?;
        return output_json_or_raw(ctx, &response);
    }

    params.push(("limit".to_string(), "100".to_string()));
    let response = ctx.http.get_with_params(&url, &params, &[auth_header(&auth)])?;
    output_json_or_raw(ctx, &response)
}

#[instrument(skip(ctx, args))]
pub fn flag(ctx: &AppContext, args: &FlagArgs) -> Result<()> {
    let url = format!(
        "{}/services/flags/admin.php",
        ctx.http.api_base().trim_end_matches('/')
    );
    let mut params = vec![("identifier".to_string(), args.identifier.clone())];
    if let Some(user) = &args.user {
        params.push(("user".to_string(), user.clone()));
    }
    let headers = vec![("Accept".to_string(), "text/json".to_string())];

    if let Some(category) = &args.add {
        params.push(("category".to_string(), category.clone()));
        let response = ctx.http.put_form(&url, &params, &[], &headers)?;
        return output_json_or_raw(ctx, &response);
    }
    if let Some(category) = &args.remove {
        params.push(("category".to_string(), category.clone()));
        let response = ctx.http.delete_form(&url, &params, &[], &headers)?;
        return output_json_or_raw(ctx, &response);
    }
    if args.list {
        let response = ctx.http.get_with_params(&url, &params, &headers)?;
        return output_json_or_raw(ctx, &response);
    }
    Err(Error::message("no flag operation specified"))
}

#[instrument(skip(ctx, args))]
pub fn simplelists(ctx: &AppContext, args: &SimplelistsArgs) -> Result<()> {
    if args.list_parents {
        let identifier = args
            .identifier
            .as_deref()
            .ok_or_else(|| Error::message("identifier is required for list-parents"))?;
        let url = format!(
            "{}/metadata/{}",
            ctx.http.api_base().trim_end_matches('/'),
            identifier
        );
        let response = ctx.http.get_with_params(&url, &[], &[])?;
        return output_json_or_raw(ctx, &response);
    }
    if args.list_children {
        let list_name = args.list_name.clone().unwrap_or_else(|| "catchall".to_string());
        let identifier = args.identifier.clone().unwrap_or_else(|| "*".to_string());
        let query = format!("simplelists__{list_name}:{identifier}");
        let search = crate::domains::metadata::SearchQuery {
            query,
            rows: 50,
            page: 1,
        };
        return crate::domains::metadata::search(ctx, &search, 1);
    }
    if let Some(parent) = args.set_parent.as_deref() {
        return submit_simplelist_patch(ctx, args, parent, "set");
    }
    if let Some(parent) = args.remove_parent.as_deref() {
        return submit_simplelist_patch(ctx, args, parent, "delete");
    }
    Err(Error::message("no simplelists operation specified"))
}

#[instrument(skip(ctx, args))]
pub fn tasks(ctx: &AppContext, args: &TasksArgs) -> Result<()> {
    let auth = low_auth_header(&ctx.config)?;
    let url = format!(
        "{}/services/tasks.php",
        ctx.http.api_base().trim_end_matches('/')
    );
    let mut params = Vec::new();
    if let Some(identifier) = &args.identifier {
        params.push(("identifier".to_string(), identifier.clone()));
    }
    params.push((
        "summary".to_string(),
        if args.summary { "1" } else { "0" }.to_string(),
    ));
    params.push((
        "history".to_string(),
        args.history.unwrap_or(true).to_string(),
    ));
    params.push((
        "catalog".to_string(),
        args.catalog.unwrap_or(true).to_string(),
    ));
    let response = ctx.http.get_with_params(&url, &params, &[auth_header(&auth)])?;
    output_json_or_raw(ctx, &response)
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

fn low_auth_header(config: &crate::config::Config) -> Result<String> {
    let access = config
        .auth
        .as_ref()
        .and_then(|auth| auth.access_key.as_deref())
        .ok_or_else(|| Error::message("missing access key for auth"))?;
    let secret = config
        .auth
        .as_ref()
        .and_then(|auth| auth.secret_key.as_deref())
        .ok_or_else(|| Error::message("missing secret key for auth"))?;
    Ok(format!("LOW {access}:{secret}"))
}

fn auth_header(value: &str) -> (String, String) {
    ("Authorization".to_string(), value.to_string())
}

fn output_json_or_raw(ctx: &AppContext, response: &str) -> Result<()> {
    match ctx.output.policy().format {
        OutputFormat::Raw => ctx.output.write_line(response)?,
        _ => {
            let json: Value = serde_json::from_str(response)
                .map_err(|err| Error::message(format!("failed to parse response: {err}")))?;
            ctx.output.write_json(&json)?;
        }
    }
    Ok(())
}

fn submit_simplelist_patch(
    ctx: &AppContext,
    args: &SimplelistsArgs,
    parent: &str,
    operation: &str,
) -> Result<()> {
    let identifier = args
        .identifier
        .as_deref()
        .ok_or_else(|| Error::message("identifier is required for simplelists update"))?;
    let list_name = args
        .list_name
        .as_deref()
        .ok_or_else(|| Error::message("list name is required"))?;
    let patch = serde_json::json!({
        "op": operation,
        "parent": parent,
        "list": list_name,
        "notes": args.notes,
    });
    let url = format!(
        "{}/metadata/{}",
        ctx.http.api_base().trim_end_matches('/'),
        identifier
    );
    let auth = low_auth_header(&ctx.config)?;
    let (access, secret) = split_low_auth(&auth)?;
    let form = vec![
        ("-patch".to_string(), patch.to_string()),
        ("-target".to_string(), "simplelists".to_string()),
        ("access".to_string(), access.to_string()),
        ("secret".to_string(), secret.to_string()),
    ];
    let response = ctx.http.post_form(&url, &form, &[])?;
    output_json_or_raw(ctx, &response)
}

fn split_low_auth(value: &str) -> Result<(&str, &str)> {
    let token = value
        .strip_prefix("LOW ")
        .ok_or_else(|| Error::message("invalid auth header"))?;
    let mut parts = token.splitn(2, ':');
    let access = parts
        .next()
        .ok_or_else(|| Error::message("invalid auth header"))?;
    let secret = parts
        .next()
        .ok_or_else(|| Error::message("invalid auth header"))?;
    Ok((access, secret))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_low_auth_header() {
        let (access, secret) = split_low_auth("LOW access:secret").expect("split");
        assert_eq!(access, "access");
        assert_eq!(secret, "secret");
    }
}
