use serde_json::Value;
use tracing::{info, instrument};
use url::Url;

use crate::cli::AppContext;
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

#[instrument(skip(ctx))]
pub fn metadata(ctx: &AppContext, identifier: &str) -> Result<()> {
    validate_identifier(identifier)?;
    let url = metadata_url(ctx, identifier)?;
    let (raw, json) = fetch_json(ctx, &url)?;
    output_value(ctx, &json, &raw)
}

#[instrument(skip(ctx))]
pub fn search(ctx: &AppContext, query: &SearchQuery) -> Result<()> {
    if query.query.trim().is_empty() {
        return Err(Error::message("search query must not be empty"));
    }
    let url = search_url(ctx, query)?;
    let (raw, json) = fetch_json(ctx, &url)?;
    let identifiers = parse_search_identifiers(&json);
    output_list(ctx, &identifiers, &json, &raw)
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
}
