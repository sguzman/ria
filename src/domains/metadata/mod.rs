use serde_json::Value;
use std::collections::HashSet;
use tracing::{info, instrument};
use url::Url;

use crate::cli::{AppContext, MetadataArgs};
use crate::errors::{Error, Result};
use crate::output::OutputFormat;
use crate::utils;

#[derive(Debug, Default)]
pub struct SearchQuery {
    pub query: String,
    pub rows: u32,
    pub page: u32,
}

#[instrument(skip(ctx))]
pub fn list(ctx: &AppContext, identifier: &str) -> Result<()> {
    validate_identifier(identifier)?;
    let url = metadata_url(ctx, identifier)?;
    let (raw, json) = fetch_json(ctx, &url)?;
    let files = parse_files(&json);
    output_list(ctx, &files, &json, &raw)
}

#[instrument(skip(ctx, args))]
pub fn metadata(ctx: &AppContext, args: &MetadataArgs) -> Result<()> {
    if args.upload_file.is_some()
        && (!args.set.is_empty() || args.metadata_file.is_some())
    {
        return Err(Error::message(
            "metadata updates and upload-file cannot be used together",
        ));
    }
    if let Some(_) = args.upload_file {
        return metadata_upload_file(ctx, args);
    }
    if args.set.is_empty() && args.metadata_file.is_none() {
        return metadata_get(ctx, &args.identifier);
    }
    metadata_update(ctx, args)
}

#[instrument(skip(ctx))]
pub fn search(ctx: &AppContext, query: &SearchQuery, pages: u32) -> Result<()> {
    if query.query.trim().is_empty() {
        return Err(Error::message("search query must not be empty"));
    }
    let pages = pages.max(1);
    let mut identifiers = Vec::new();
    let mut responses = Vec::new();
    let mut raw_pages = Vec::new();
    for offset in 0..pages {
        let page_query = SearchQuery {
            query: query.query.clone(),
            rows: query.rows,
            page: query.page + offset,
        };
        let url = search_url(ctx, &page_query)?;
        let (raw, json) = fetch_json(ctx, &url)?;
        identifiers.extend(parse_search_identifiers(&json));
        responses.push(json);
        raw_pages.push(raw);
    }
    let identifiers = dedupe_identifiers(identifiers);
    match ctx.output.policy().format {
        OutputFormat::Human => {
            for identifier in &identifiers {
                ctx.output.write_line(identifier)?;
            }
            Ok(())
        }
        OutputFormat::Json => {
            let payload = serde_json::json!({
                "pages": pages,
                "identifiers": identifiers,
                "responses": responses,
            });
            ctx.output.write_json(&payload)?;
            Ok(())
        }
        OutputFormat::Raw => {
            for raw in raw_pages {
                ctx.output.write_line(&raw)?;
            }
            Ok(())
        }
    }
}

#[instrument(skip(ctx))]
fn metadata_get(ctx: &AppContext, identifier: &str) -> Result<()> {
    validate_identifier(identifier)?;
    let url = metadata_url(ctx, identifier)?;
    let (raw, json) = fetch_json(ctx, &url)?;
    output_value(ctx, &json, &raw)
}

#[instrument(skip(ctx, args))]
fn metadata_update(ctx: &AppContext, args: &MetadataArgs) -> Result<()> {
    validate_identifier(&args.identifier)?;
    let updates = load_metadata_updates(args)?;
    if updates.is_empty() {
        return Err(Error::message("no metadata updates provided"));
    }

    let target = args.target.trim();
    if target.is_empty() {
        return Err(Error::message("metadata target must not be empty"));
    }

    let url = metadata_url(ctx, &args.identifier)?;
    let (_, current) = fetch_json(ctx, &url)?;
    let patch = build_metadata_patch(&current, target, &updates)?;
    let body = build_metadata_form(&patch, target, args.priority, &ctx.config)?;

    if args.dry_run {
        return output_metadata_dry_run(ctx, &patch, target, &body);
    }

    let response = ctx.http.post_form(&url, &body, &[])?;
    let json: Value = serde_json::from_str(&response)
        .map_err(|err| Error::message(format!("failed to parse metadata response: {err}")))?;
    output_value(ctx, &json, &response)
}

#[instrument(skip(ctx, args))]
fn metadata_upload_file(ctx: &AppContext, args: &MetadataArgs) -> Result<()> {
    validate_identifier(&args.identifier)?;
    let path = args
        .upload_file
        .as_ref()
        .ok_or_else(|| Error::message("upload file is required"))?;
    if !path.exists() {
        return Err(Error::message(format!(
            "upload file does not exist: {}",
            path.display()
        )));
    }
    let _ = path.file_name().and_then(|name| name.to_str()).ok_or_else(|| {
        Error::message(format!(
            "invalid upload file name: {}",
            path.display()
        ))
    })?;
    let upload_args = crate::cli::UploadArgs {
        identifier: args.identifier.clone(),
        paths: vec![path.clone()],
        metadata: None,
        dry_run: args.dry_run,
    };
    crate::domains::transfer::upload(ctx, &upload_args)
}

fn validate_identifier(identifier: &str) -> Result<()> {
    if utils::validate_identifier(identifier) {
        Ok(())
    } else {
        Err(Error::message(format!(
            "invalid identifier: {identifier}"
        )))
    }
}

#[instrument(skip(ctx))]
fn fetch_json(ctx: &AppContext, url: &str) -> Result<(String, Value)> {
    info!(%url, "requesting metadata");
    let raw = ctx.http.get_text(url)?;
    let json = serde_json::from_str(&raw)
        .map_err(|err| Error::message(format!("failed to parse JSON: {err}")))?;
    Ok((raw, json))
}

fn metadata_url(ctx: &AppContext, identifier: &str) -> Result<String> {
    let base = ctx.http.metadata_base().trim_end_matches('/');
    Ok(format!("{base}/{identifier}"))
}

fn load_metadata_updates(args: &MetadataArgs) -> Result<Vec<(String, Value)>> {
    let mut updates = Vec::new();
    if let Some(path) = args.metadata_file.as_deref() {
        let file_updates = load_metadata_file(path)?;
        for (key, value) in file_updates {
            updates.push((key, value));
        }
    }
    for entry in &args.set {
        let (key, value) = parse_kv(entry)?;
        updates.push((key, Value::String(value)));
    }
    Ok(updates)
}

fn parse_kv(input: &str) -> Result<(String, String)> {
    let mut parts = input.splitn(2, '=');
    let key = parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::message("metadata key must not be empty"))?;
    let value = parts
        .next()
        .map(str::to_string)
        .ok_or_else(|| Error::message("metadata value must not be empty"))?;
    Ok((key.to_string(), value))
}

fn load_metadata_file(path: &std::path::Path) -> Result<Vec<(String, Value)>> {
    let raw = std::fs::read_to_string(path)?;
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let value = match ext.as_str() {
        "json" => serde_json::from_str::<Value>(&raw)
            .map_err(|err| Error::message(format!("invalid JSON metadata: {err}")))?,
        "toml" => {
            let toml_value: toml::Value = toml::from_str(&raw)
                .map_err(|err| Error::message(format!("invalid TOML metadata: {err}")))?;
            serde_json::to_value(toml_value)
                .map_err(|err| Error::message(format!("invalid TOML metadata: {err}")))? 
        }
        _ => {
            return Err(Error::message(format!(
                "unsupported metadata file extension: {}",
                path.display()
            )))
        }
    };
    let obj = value.as_object().ok_or_else(|| {
        Error::message("metadata file must contain a JSON/TOML object")
    })?;
    Ok(obj
        .iter()
        .map(|(key, value)| (key.to_string(), value.clone()))
        .collect())
}

fn build_metadata_patch(
    current: &Value,
    target: &str,
    updates: &[(String, Value)],
) -> Result<Value> {
    let metadata_obj = match target {
        "metadata" => current
            .get("metadata")
            .and_then(|value| value.as_object())
            .ok_or_else(|| Error::message("metadata response missing metadata field"))?,
        _ => current
            .as_object()
            .ok_or_else(|| Error::message("metadata response missing fields"))?,
    };
    let mut patch = Vec::new();
    for (key, value) in updates {
        let path = format!("/{target}/{key}");
        let op = if metadata_obj.contains_key(key) { "replace" } else { "add" };
        patch.push(serde_json::json!({
            "op": op,
            "path": path,
            "value": value,
        }));
    }
    Ok(Value::Array(patch))
}

fn build_metadata_form(
    patch: &Value,
    target: &str,
    priority: Option<i32>,
    config: &crate::config::Config,
) -> Result<Vec<(String, String)>> {
    let access = config
        .auth
        .as_ref()
        .and_then(|auth| auth.access_key.as_deref())
        .ok_or_else(|| Error::message("missing access key for metadata update"))?;
    let secret = config
        .auth
        .as_ref()
        .and_then(|auth| auth.secret_key.as_deref())
        .ok_or_else(|| Error::message("missing secret key for metadata update"))?;
    let priority = priority.unwrap_or(-5);
    let patch_json = serde_json::to_string(patch)
        .map_err(|err| Error::message(format!("failed to serialize patch: {err}")))?;
    Ok(vec![
        ("-patch".to_string(), patch_json),
        ("-target".to_string(), target.to_string()),
        ("priority".to_string(), priority.to_string()),
        ("access".to_string(), access.to_string()),
        ("secret".to_string(), secret.to_string()),
    ])
}

fn output_metadata_dry_run(
    ctx: &AppContext,
    patch: &Value,
    target: &str,
    form: &[(String, String)],
) -> Result<()> {
    match ctx.output.policy().format {
        OutputFormat::Json => {
            let value = serde_json::json!({
                "dry_run": true,
                "target": target,
                "patch": patch,
                "form": form,
            });
            ctx.output.write_json(&value)?;
            Ok(())
        }
        _ => {
            ctx.output.write_line("Metadata dry-run")?;
            ctx.output
                .write_line(&format!("Target: {target}"))?;
            ctx.output
                .write_line(&format!("Patch operations: {}", patch.as_array().map(|v| v.len()).unwrap_or(0)))?;
            Ok(())
        }
    }
}

fn search_url(ctx: &AppContext, query: &SearchQuery) -> Result<String> {
    let mut url = Url::parse(ctx.http.api_base())
        .map_err(|err| Error::message(format!("invalid api base url: {err}")))?;
    url.set_path("advancedsearch.php");
    url.query_pairs_mut()
        .append_pair("q", query.query.trim())
        .append_pair("rows", &query.rows.to_string())
        .append_pair("page", &query.page.to_string())
        .append_pair("output", "json")
        .append_pair("fl[]", "identifier");
    Ok(url.to_string())
}

fn parse_files(value: &Value) -> Vec<String> {
    value
        .get("files")
        .and_then(|files| files.as_array())
        .map(|files| {
            files
                .iter()
                .filter_map(|entry| entry.get("name").and_then(|name| name.as_str()))
                .map(|name| name.to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn parse_search_identifiers(value: &Value) -> Vec<String> {
    value
        .get("response")
        .and_then(|response| response.get("docs"))
        .and_then(|docs| docs.as_array())
        .map(|docs| {
            docs.iter()
                .filter_map(|doc| doc.get("identifier").and_then(|id| id.as_str()))
                .map(|id| id.to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn dedupe_identifiers(items: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    for item in items {
        if seen.insert(item.clone()) {
            output.push(item);
        }
    }
    output
}

fn output_list(
    ctx: &AppContext,
    items: &[String],
    json: &Value,
    raw: &str,
) -> Result<()> {
    match ctx.output.policy().format {
        OutputFormat::Human => {
            for item in items {
                ctx.output.write_line(item)?;
            }
            Ok(())
        }
        OutputFormat::Json => {
            ctx.output.write_json(json)?;
            Ok(())
        }
        OutputFormat::Raw => {
            ctx.output.write_line(raw)?;
            Ok(())
        }
    }
}

fn output_value(ctx: &AppContext, json: &Value, raw: &str) -> Result<()> {
    match ctx.output.policy().format {
        OutputFormat::Human => {
            ctx.output.write_json(json)?;
            Ok(())
        }
        OutputFormat::Json => {
            ctx.output.write_json(json)?;
            Ok(())
        }
        OutputFormat::Raw => {
            ctx.output.write_line(raw)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use tempfile::TempDir;

    fn test_context() -> AppContext {
        let config = crate::config::Config::default();
        let http = crate::http::HttpClient::new(crate::http::config_from_settings(&config))
            .expect("http client");
        let output = crate::output::OutputWriter::new(crate::output::OutputPolicy::new(
            OutputFormat::Human,
        ));
        AppContext {
            config,
            http,
            output,
            config_path: None,
            config_destination: None,
        }
    }

    #[test]
    fn builds_search_url() {
        let query = SearchQuery {
            query: "collection:opensource".to_string(),
            rows: 5,
            page: 2,
        };
        let config = crate::config::Config::default();
        let http_config = crate::http::config_from_settings(&config);
        let http = crate::http::HttpClient::new(http_config).expect("http client");
        let ctx = AppContext {
            config,
            http,
            output: crate::output::OutputWriter::new(crate::output::OutputPolicy::new(
                OutputFormat::Human,
            )),
            config_path: None,
            config_destination: None,
        };
        let url = search_url(&ctx, &query).expect("search url");
        assert!(url.contains("advancedsearch.php"));
        assert!(url.contains("q=collection%3Aopensource"));
        assert!(url.contains("rows=5"));
        assert!(url.contains("page=2"));
        assert!(url.contains("fl%5B%5D=identifier"));
    }

    #[test]
    fn parses_files_from_metadata() {
        let value = serde_json::json!({
            "files": [
                {"name": "file1.txt"},
                {"name": "file2.txt"}
            ]
        });
        let files = parse_files(&value);
        assert_eq!(files, vec!["file1.txt", "file2.txt"]);
    }

    #[test]
    fn list_uses_mock_server() {
        let server = MockServer::start();
        let _mock = server.mock(|when, then| {
            when.method(GET)
                .path("/metadata/test-item");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"{"files":[{"name":"one.txt"},{"name":"two.txt"}]}"#);
        });

        let config = crate::config::Config {
            endpoints: Some(crate::config::EndpointsConfig {
                metadata_base: Some(server.url("/metadata")),
                api_base: Some(server.url("")),
                ..crate::config::EndpointsConfig::default()
            }),
            ..crate::config::Config::default()
        };
        let http_config = crate::http::config_from_settings(&config);
        let http = crate::http::HttpClient::new(http_config).expect("http client");
        let ctx = AppContext {
            config,
            http,
            output: crate::output::OutputWriter::new(crate::output::OutputPolicy::new(
                OutputFormat::Human,
            )),
            config_path: None,
            config_destination: None,
        };

        let result = list(&ctx, "test-item");
        assert!(result.is_ok());
    }

    #[test]
    fn list_reports_http_errors() {
        let server = MockServer::start();
        let _mock = server.mock(|when, then| {
            when.method(GET)
                .path("/metadata/bad-item");
            then.status(500)
                .header("content-type", "text/plain")
                .body("oops");
        });

        let config = crate::config::Config {
            endpoints: Some(crate::config::EndpointsConfig {
                metadata_base: Some(server.url("/metadata")),
                api_base: Some(server.url("")),
                ..crate::config::EndpointsConfig::default()
            }),
            ..crate::config::Config::default()
        };
        let http_config = crate::http::config_from_settings(&config);
        let http = crate::http::HttpClient::new(http_config).expect("http client");
        let ctx = AppContext {
            config,
            http,
            output: crate::output::OutputWriter::new(crate::output::OutputPolicy::new(
                OutputFormat::Human,
            )),
            config_path: None,
            config_destination: None,
        };

        let result = list(&ctx, "bad-item");
        assert!(result.is_err());
    }

    #[test]
    fn parses_metadata_kv_pairs() {
        let (key, value) = parse_kv("title=Example").expect("pair");
        assert_eq!(key, "title");
        assert_eq!(value, "Example");
    }

    #[test]
    fn loads_metadata_file_json() {
        let dir = TempDir::new().expect("temp");
        let path = dir.path().join("metadata.json");
        std::fs::write(&path, r#"{ "title": "Example" }"#).expect("write");
        let updates = load_metadata_file(&path).expect("updates");
        assert!(updates.iter().any(|(key, _)| key == "title"));
    }

    #[test]
    fn builds_metadata_patch_ops() {
        let current = serde_json::json!({ "metadata": { "title": "Old" } });
        let updates = vec![("title".to_string(), Value::String("New".into()))];
        let patch = build_metadata_patch(&current, "metadata", &updates).expect("patch");
        assert!(patch.as_array().unwrap()[0]["op"] == "replace");
    }

    #[test]
    fn metadata_upload_rejects_conflicts() {
        let ctx = test_context();
        let args = MetadataArgs {
            identifier: "example-item".into(),
            set: vec!["title=Example".into()],
            metadata_file: None,
            upload_file: Some(std::path::PathBuf::from("file.txt")),
            target: "metadata".into(),
            priority: None,
            dry_run: true,
        };
        let err = metadata(&ctx, &args).unwrap_err();
        assert!(err.to_string().contains("cannot be used together"));
    }

    #[test]
    fn metadata_upload_dry_run_succeeds() {
        let ctx = test_context();
        let dir = TempDir::new().expect("temp");
        let path = dir.path().join("metadata.json");
        std::fs::write(&path, r#"{ "title": "Example" }"#).expect("write");
        let args = MetadataArgs {
            identifier: "example-item".into(),
            set: Vec::new(),
            metadata_file: None,
            upload_file: Some(path),
            target: "metadata".into(),
            priority: None,
            dry_run: true,
        };
        let result = metadata(&ctx, &args);
        assert!(result.is_ok());
    }
}
