use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use tracing::{info, instrument, warn};
use url::Url;

use crate::cli::{AppContext, CopyArgs, DeleteArgs, DownloadArgs, MoveArgs, UploadArgs};
use crate::errors::{Error, Result};
use crate::output::OutputFormat;
use crate::utils::{self, GlobMatcher};

#[derive(Debug, Clone)]
struct TransferFile {
    name: String,
    size: Option<u64>,
}

#[derive(Debug, Clone)]
struct PlannedFile {
    name: String,
    url: String,
    size: Option<u64>,
}

#[derive(Debug, Clone)]
struct DownloadPlan {
    identifier: String,
    dest: PathBuf,
    files: Vec<PlannedFile>,
}

#[instrument(skip(ctx))]
pub fn download(ctx: &AppContext, args: &DownloadArgs) -> Result<()> {
    let plan = plan_download(ctx, args)?;
    emit_plan(ctx, &plan, args.dry_run)?;
    if args.dry_run {
        return Ok(());
    }
    execute_download(ctx, &plan)
}

#[instrument(skip(ctx))]
pub fn upload(ctx: &AppContext, _args: &UploadArgs) -> Result<()> {
    warn!("upload not implemented");
    let _ = ctx.output.write_error("ria: upload not implemented");
    Err(Error::not_implemented("upload"))
}

#[instrument(skip(ctx))]
pub fn delete(ctx: &AppContext, _args: &DeleteArgs) -> Result<()> {
    warn!("delete not implemented");
    let _ = ctx.output.write_error("ria: delete not implemented");
    Err(Error::not_implemented("delete"))
}

#[instrument(skip(ctx))]
pub fn copy(ctx: &AppContext, _args: &CopyArgs) -> Result<()> {
    warn!("copy not implemented");
    let _ = ctx.output.write_error("ria: copy not implemented");
    Err(Error::not_implemented("copy"))
}

#[instrument(skip(ctx))]
pub fn move_item(ctx: &AppContext, _args: &MoveArgs) -> Result<()> {
    warn!("move not implemented");
    let _ = ctx.output.write_error("ria: move not implemented");
    Err(Error::not_implemented("move"))
}

#[instrument(skip(ctx, args))]
fn plan_download(ctx: &AppContext, args: &DownloadArgs) -> Result<DownloadPlan> {
    validate_identifier(ctx, &args.identifier)?;

    let metadata = fetch_metadata(ctx, &args.identifier)?;
    let available = parse_metadata_files(&metadata)?;
    let available_map = available
        .into_iter()
        .map(|file| (file.name.clone(), file))
        .collect::<HashMap<_, _>>();

    let selection = select_files(ctx, args, &available_map)?;
    let mut planned = Vec::with_capacity(selection.len());
    for file in selection {
        validate_download_path(&file.name)?;
        planned.push(PlannedFile {
            name: file.name.clone(),
            url: build_file_url(ctx.http.s3_base(), &args.identifier, &file.name)?,
            size: file.size,
        });
    }
    planned.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(DownloadPlan {
        identifier: args.identifier.clone(),
        dest: args.dest.clone(),
        files: planned,
    })
}

#[instrument(skip(ctx, plan))]
fn execute_download(ctx: &AppContext, plan: &DownloadPlan) -> Result<()> {
    fs::create_dir_all(&plan.dest).map_err(|err| {
        Error::message(format!(
            "failed to create destination directory {}: {err}",
            plan.dest.display()
        ))
    })?;

    for file in &plan.files {
        let dest_path = plan.dest.join(Path::new(&file.name));
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                Error::message(format!(
                    "failed to create parent directory {}: {err}",
                    parent.display()
                ))
            })?;
        }

        info!(
            file = %file.name,
            url = %file.url,
            dest = %dest_path.display(),
            "downloading file"
        );

        let bytes = ctx.http.get_bytes(&file.url)?;
        fs::write(&dest_path, &bytes).map_err(|err| {
            Error::message(format!(
                "failed to write {}: {err}",
                dest_path.display()
            ))
        })?;
    }

    Ok(())
}

#[instrument(skip(ctx, plan))]
fn emit_plan(ctx: &AppContext, plan: &DownloadPlan, dry_run: bool) -> Result<()> {
    match ctx.output.policy().format {
        OutputFormat::Json => {
            let files = plan
                .files
                .iter()
                .map(|file| {
                    json!({
                        "name": file.name,
                        "url": file.url,
                        "size": file.size,
                    })
                })
                .collect::<Vec<_>>();
            let value = json!({
                "identifier": plan.identifier,
                "dest": plan.dest.display().to_string(),
                "dry_run": dry_run,
                "files": files,
            });
            ctx.output
                .write_json(&value)
                .map_err(|err| Error::message(format!("failed to write output: {err}")))
        }
        _ => {
            let header = if dry_run {
                format!(
                    "Download plan for {} -> {} (dry-run)",
                    plan.identifier,
                    plan.dest.display()
                )
            } else {
                format!(
                    "Downloading {} -> {}",
                    plan.identifier,
                    plan.dest.display()
                )
            };
            ctx.output
                .write_line(&header)
                .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            for file in &plan.files {
                let line = match file.size {
                    Some(size) => format!("{} ({size} bytes)", file.name),
                    None => file.name.clone(),
                };
                ctx.output
                    .write_line(&line)
                    .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            }
            Ok(())
        }
    }
}

fn validate_identifier(ctx: &AppContext, identifier: &str) -> Result<()> {
    let validate = ctx
        .config
        .input
        .as_ref()
        .and_then(|input| input.validate_identifiers)
        .unwrap_or(true);
    if validate && !utils::validate_identifier(identifier) {
        return Err(Error::message(format!(
            "invalid identifier: {identifier}"
        )));
    }
    Ok(())
}

fn fetch_metadata(ctx: &AppContext, identifier: &str) -> Result<Value> {
    let url = format!(
        "{}/{}",
        ctx.http.metadata_base().trim_end_matches('/'),
        identifier
    );
    ctx.http.get_json(&url)
}

fn parse_metadata_files(metadata: &Value) -> Result<Vec<TransferFile>> {
    let files = metadata
        .get("files")
        .and_then(|value| value.as_array())
        .ok_or_else(|| Error::message("metadata response missing files"))?;
    let mut results = Vec::new();
    for file in files {
        let name = match file.get("name").and_then(|value| value.as_str()) {
            Some(name) if !name.trim().is_empty() => name.to_string(),
            _ => continue,
        };
        let size = file
            .get("size")
            .and_then(|value| value.as_str())
            .and_then(|value| value.parse::<u64>().ok());
        results.push(TransferFile { name, size });
    }
    if results.is_empty() {
        return Err(Error::message("metadata response contained no files"));
    }
    Ok(results)
}

fn select_files(
    ctx: &AppContext,
    args: &DownloadArgs,
    available: &HashMap<String, TransferFile>,
) -> Result<Vec<TransferFile>> {
    let mut selected = HashSet::new();
    let mut results = Vec::new();

    let glob_pattern = args
        .glob
        .as_deref()
        .or_else(|| ctx.config.input.as_ref().and_then(|input| input.glob.as_deref()));
    let glob_matcher = match glob_pattern {
        Some(pattern) => Some(GlobMatcher::new(&[pattern.to_string()])?),
        None => None,
    };

    if !args.files.is_empty() {
        for name in &args.files {
            let file = available.get(name).ok_or_else(|| {
                Error::message(format!("requested file not found in metadata: {name}"))
            })?;
            if selected.insert(file.name.clone()) {
                results.push(file.clone());
            }
        }
    }

    if let Some(matcher) = &glob_matcher {
        for file in available.values() {
            if matcher.is_match(&file.name) && selected.insert(file.name.clone()) {
                results.push(file.clone());
            }
        }
    }

    if results.is_empty() {
        return Err(Error::message("no files matched selection rules"));
    }

    Ok(results)
}

fn validate_download_path(name: &str) -> Result<()> {
    let path = Path::new(name);
    if path.is_absolute() {
        return Err(Error::message(format!(
            "refusing to write absolute path: {name}"
        )));
    }
    for component in path.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(Error::message(format!(
                "refusing to write path with parent traversal: {name}"
            )));
        }
    }
    Ok(())
}

fn build_file_url(base: &str, identifier: &str, name: &str) -> Result<String> {
    let mut url = Url::parse(base)
        .map_err(|err| Error::message(format!("invalid s3 base url {base}: {err}")))?;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| Error::message("s3 base url cannot be a base"))?;
        for part in identifier.split('/') {
            if !part.is_empty() {
                segments.push(part);
            }
        }
        for part in name.split('/') {
            if !part.is_empty() {
                segments.push(part);
            }
        }
    }
    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn sample_metadata() -> Value {
        json!({
            "files": [
                { "name": "file-one.txt", "size": "12" },
                { "name": "nested/file-two.txt", "size": "24" },
                { "name": "image.jpg" }
            ]
        })
    }

    fn test_context() -> AppContext {
        let config = Config::default();
        let http = crate::http::HttpClient::new(crate::http::config_from_settings(&config))
            .expect("http client");
        let output = crate::output::OutputWriter::new(crate::output::policy_from_config(&config));
        AppContext {
            config,
            http,
            output,
        }
    }

    #[test]
    fn parses_metadata_files() {
        let files = parse_metadata_files(&sample_metadata()).expect("files");
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].name, "file-one.txt");
        assert_eq!(files[0].size, Some(12));
    }

    #[test]
    fn selects_files_by_glob() {
        let ctx = test_context();
        let mut available = HashMap::new();
        for file in parse_metadata_files(&sample_metadata()).expect("files") {
            available.insert(file.name.clone(), file);
        }
        let args = DownloadArgs {
            identifier: "example".into(),
            files: Vec::new(),
            glob: Some("*.txt".into()),
            dest: PathBuf::from("."),
            dry_run: true,
        };
        let selected = select_files(&ctx, &args, &available).expect("selected");
        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn selects_files_by_explicit_name() {
        let ctx = test_context();
        let mut available = HashMap::new();
        for file in parse_metadata_files(&sample_metadata()).expect("files") {
            available.insert(file.name.clone(), file);
        }
        let args = DownloadArgs {
            identifier: "example".into(),
            files: vec!["image.jpg".into()],
            glob: None,
            dest: PathBuf::from("."),
            dry_run: true,
        };
        let selected = select_files(&ctx, &args, &available).expect("selected");
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].name, "image.jpg");
    }

    #[test]
    fn errors_on_missing_explicit_file() {
        let ctx = test_context();
        let mut available = HashMap::new();
        for file in parse_metadata_files(&sample_metadata()).expect("files") {
            available.insert(file.name.clone(), file);
        }
        let args = DownloadArgs {
            identifier: "example".into(),
            files: vec!["missing.txt".into()],
            glob: None,
            dest: PathBuf::from("."),
            dry_run: true,
        };
        let err = select_files(&ctx, &args, &available).unwrap_err();
        assert!(err.to_string().contains("requested file not found"));
    }

    #[test]
    fn errors_on_empty_selection() {
        let ctx = test_context();
        let mut available = HashMap::new();
        for file in parse_metadata_files(&sample_metadata()).expect("files") {
            available.insert(file.name.clone(), file);
        }
        let args = DownloadArgs {
            identifier: "example".into(),
            files: Vec::new(),
            glob: Some("*.pdf".into()),
            dest: PathBuf::from("."),
            dry_run: true,
        };
        let err = select_files(&ctx, &args, &available).unwrap_err();
        assert!(err.to_string().contains("no files matched"));
    }

    #[test]
    fn validates_download_paths() {
        assert!(validate_download_path("ok/path.txt").is_ok());
        assert!(validate_download_path("../escape.txt").is_err());
        assert!(validate_download_path("/absolute.txt").is_err());
    }

    #[test]
    fn builds_file_url() {
        let url = build_file_url(
            "https://s3.us.archive.org",
            "sample-item",
            "nested/file.txt",
        )
        .expect("url");
        assert_eq!(
            url,
            "https://s3.us.archive.org/sample-item/nested/file.txt"
        );
    }
}
