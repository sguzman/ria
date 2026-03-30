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

#[derive(Debug, Clone)]
struct DeletePlan {
    identifier: String,
    files: Vec<TransferFile>,
}

#[derive(Debug, Clone)]
struct CopyPlan {
    source_identifier: String,
    dest_identifier: String,
    files: Vec<TransferFile>,
}

#[derive(Debug, Clone)]
struct MovePlan {
    source_identifier: String,
    dest_identifier: String,
    files: Vec<TransferFile>,
}

#[derive(Debug, Clone)]
struct UploadFile {
    source: PathBuf,
    dest: String,
    size: u64,
}

#[derive(Debug, Clone)]
struct UploadPlan {
    identifier: String,
    files: Vec<UploadFile>,
    metadata: Option<Value>,
    total_bytes: u64,
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
pub fn upload(ctx: &AppContext, args: &UploadArgs) -> Result<()> {
    let plan = plan_upload(ctx, args)?;
    emit_upload_plan(ctx, &plan, args.dry_run)?;
    if args.dry_run {
        return Ok(());
    }
    warn!("upload not implemented");
    let _ = ctx
        .output
        .write_error("ria: upload not implemented (use --dry-run to preview)");
    Err(Error::not_implemented("upload"))
}

#[instrument(skip(ctx))]
pub fn delete(ctx: &AppContext, _args: &DeleteArgs) -> Result<()> {
    let plan = plan_delete(ctx, _args)?;
    emit_delete_plan(ctx, &plan, _args.dry_run)?;
    if _args.dry_run {
        return Ok(());
    }
    warn!("delete not implemented");
    let _ = ctx
        .output
        .write_error("ria: delete not implemented (use --dry-run to preview)");
    Err(Error::not_implemented("delete"))
}

#[instrument(skip(ctx))]
pub fn copy(ctx: &AppContext, _args: &CopyArgs) -> Result<()> {
    let plan = plan_copy(ctx, _args)?;
    emit_copy_plan(ctx, &plan, _args.dry_run)?;
    if _args.dry_run {
        return Ok(());
    }
    warn!("copy not implemented");
    let _ = ctx
        .output
        .write_error("ria: copy not implemented (use --dry-run to preview)");
    Err(Error::not_implemented("copy"))
}

#[instrument(skip(ctx))]
pub fn move_item(ctx: &AppContext, _args: &MoveArgs) -> Result<()> {
    let plan = plan_move(ctx, _args)?;
    emit_move_plan(ctx, &plan, _args.dry_run)?;
    if _args.dry_run {
        return Ok(());
    }
    warn!("move not implemented");
    let _ = ctx
        .output
        .write_error("ria: move not implemented (use --dry-run to preview)");
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

    let selection = select_files(ctx, &args.files, args.glob.as_deref(), &available_map)?;
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

#[instrument(skip(ctx, args))]
fn plan_delete(ctx: &AppContext, args: &DeleteArgs) -> Result<DeletePlan> {
    validate_identifier(ctx, &args.identifier)?;
    let metadata = fetch_metadata(ctx, &args.identifier)?;
    let available = parse_metadata_files(&metadata)?;
    let available_map = available
        .into_iter()
        .map(|file| (file.name.clone(), file))
        .collect::<HashMap<_, _>>();
    let selection = select_files(ctx, &args.files, args.glob.as_deref(), &available_map)?;
    Ok(DeletePlan {
        identifier: args.identifier.clone(),
        files: selection,
    })
}

#[instrument(skip(ctx, plan))]
fn emit_delete_plan(ctx: &AppContext, plan: &DeletePlan, dry_run: bool) -> Result<()> {
    match ctx.output.policy().format {
        OutputFormat::Json => {
            let files = plan
                .files
                .iter()
                .map(|file| {
                    json!({
                        "name": file.name,
                        "size": file.size,
                    })
                })
                .collect::<Vec<_>>();
            let value = json!({
                "identifier": plan.identifier,
                "dry_run": dry_run,
                "files": files,
            });
            ctx.output
                .write_json(&value)
                .map_err(|err| Error::message(format!("failed to write output: {err}")))
        }
        _ => {
            let header = if dry_run {
                format!("Delete plan for {} (dry-run)", plan.identifier)
            } else {
                format!("Deleting from {}", plan.identifier)
            };
            ctx.output
                .write_line(&header)
                .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            for file in &plan.files {
                ctx.output
                    .write_line(&file.name)
                    .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            }
            Ok(())
        }
    }
}

#[instrument(skip(ctx, args))]
fn plan_copy(ctx: &AppContext, args: &CopyArgs) -> Result<CopyPlan> {
    validate_identifier(ctx, &args.source_identifier)?;
    validate_identifier(ctx, &args.dest_identifier)?;
    ensure_distinct_identifiers(&args.source_identifier, &args.dest_identifier, "copy")?;
    let metadata = fetch_metadata(ctx, &args.source_identifier)?;
    let available = parse_metadata_files(&metadata)?;
    let available_map = available
        .into_iter()
        .map(|file| (file.name.clone(), file))
        .collect::<HashMap<_, _>>();
    let selection = select_files(ctx, &args.files, args.glob.as_deref(), &available_map)?;
    Ok(CopyPlan {
        source_identifier: args.source_identifier.clone(),
        dest_identifier: args.dest_identifier.clone(),
        files: selection,
    })
}

#[instrument(skip(ctx, plan))]
fn emit_copy_plan(ctx: &AppContext, plan: &CopyPlan, dry_run: bool) -> Result<()> {
    match ctx.output.policy().format {
        OutputFormat::Json => {
            let files = plan
                .files
                .iter()
                .map(|file| {
                    json!({
                        "name": file.name,
                        "size": file.size,
                    })
                })
                .collect::<Vec<_>>();
            let value = json!({
                "source_identifier": plan.source_identifier,
                "dest_identifier": plan.dest_identifier,
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
                    "Copy plan {} -> {} (dry-run)",
                    plan.source_identifier, plan.dest_identifier
                )
            } else {
                format!(
                    "Copying {} -> {}",
                    plan.source_identifier, plan.dest_identifier
                )
            };
            ctx.output
                .write_line(&header)
                .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            for file in &plan.files {
                ctx.output
                    .write_line(&file.name)
                    .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            }
            Ok(())
        }
    }
}

#[instrument(skip(ctx, args))]
fn plan_move(ctx: &AppContext, args: &MoveArgs) -> Result<MovePlan> {
    validate_identifier(ctx, &args.source_identifier)?;
    validate_identifier(ctx, &args.dest_identifier)?;
    ensure_distinct_identifiers(&args.source_identifier, &args.dest_identifier, "move")?;
    let metadata = fetch_metadata(ctx, &args.source_identifier)?;
    let available = parse_metadata_files(&metadata)?;
    let available_map = available
        .into_iter()
        .map(|file| (file.name.clone(), file))
        .collect::<HashMap<_, _>>();
    let selection = select_files(ctx, &args.files, args.glob.as_deref(), &available_map)?;
    Ok(MovePlan {
        source_identifier: args.source_identifier.clone(),
        dest_identifier: args.dest_identifier.clone(),
        files: selection,
    })
}

#[instrument(skip(ctx, plan))]
fn emit_move_plan(ctx: &AppContext, plan: &MovePlan, dry_run: bool) -> Result<()> {
    match ctx.output.policy().format {
        OutputFormat::Json => {
            let files = plan
                .files
                .iter()
                .map(|file| {
                    json!({
                        "name": file.name,
                        "size": file.size,
                    })
                })
                .collect::<Vec<_>>();
            let value = json!({
                "source_identifier": plan.source_identifier,
                "dest_identifier": plan.dest_identifier,
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
                    "Move plan {} -> {} (dry-run)",
                    plan.source_identifier, plan.dest_identifier
                )
            } else {
                format!(
                    "Moving {} -> {}",
                    plan.source_identifier, plan.dest_identifier
                )
            };
            ctx.output
                .write_line(&header)
                .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            for file in &plan.files {
                ctx.output
                    .write_line(&file.name)
                    .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            }
            Ok(())
        }
    }
}

#[instrument(skip(ctx, args))]
fn plan_upload(ctx: &AppContext, args: &UploadArgs) -> Result<UploadPlan> {
    validate_identifier(ctx, &args.identifier)?;
    if args.paths.is_empty() {
        return Err(Error::message("no upload paths provided"));
    }

    let files = collect_upload_files(&args.paths)?;
    if files.is_empty() {
        return Err(Error::message("no upload files discovered"));
    }

    let metadata = match args.metadata.as_ref() {
        Some(path) => Some(load_metadata_sidecar(path)?),
        None => None,
    };

    let total_bytes = files.iter().map(|file| file.size).sum();
    Ok(UploadPlan {
        identifier: args.identifier.clone(),
        files,
        metadata,
        total_bytes,
    })
}

#[instrument(skip(ctx, plan))]
fn emit_upload_plan(ctx: &AppContext, plan: &UploadPlan, dry_run: bool) -> Result<()> {
    match ctx.output.policy().format {
        OutputFormat::Json => {
            let files = plan
                .files
                .iter()
                .map(|file| {
                    json!({
                        "source": file.source.display().to_string(),
                        "dest": file.dest,
                        "size": file.size,
                    })
                })
                .collect::<Vec<_>>();
            let value = json!({
                "identifier": plan.identifier,
                "dry_run": dry_run,
                "total_bytes": plan.total_bytes,
                "files": files,
                "metadata": plan.metadata,
            });
            ctx.output
                .write_json(&value)
                .map_err(|err| Error::message(format!("failed to write output: {err}")))
        }
        _ => {
            let header = if dry_run {
                format!("Upload plan for {} (dry-run)", plan.identifier)
            } else {
                format!("Uploading {}", plan.identifier)
            };
            ctx.output
                .write_line(&header)
                .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            ctx.output
                .write_line(&format!(
                    "Files: {} ({} bytes)",
                    plan.files.len(),
                    plan.total_bytes
                ))
                .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            for file in &plan.files {
                ctx.output
                    .write_line(&format!(
                        "{} -> {} ({} bytes)",
                        file.source.display(),
                        file.dest,
                        file.size
                    ))
                    .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            }
            if plan.metadata.is_some() {
                ctx.output
                    .write_line("Metadata: provided")
                    .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            }
            Ok(())
        }
    }
}

#[instrument]
fn collect_upload_files(paths: &[PathBuf]) -> Result<Vec<UploadFile>> {
    let mut files = Vec::new();
    let mut seen = HashSet::new();
    for path in paths {
        if contains_glob_pattern(path) {
            return Err(Error::message(format!(
                "glob patterns are not supported in upload paths: {}",
                path.display()
            )));
        }
        let metadata = fs::metadata(path).map_err(|err| {
            Error::message(format!("failed to read {}: {err}", path.display()))
        })?;
        if metadata.is_dir() {
            collect_dir_files(path, path, &mut files, &mut seen)?;
        } else if metadata.is_file() {
            let dest = file_name_string(path)?;
            validate_upload_dest(&dest)?;
            if !seen.insert(dest.clone()) {
                return Err(Error::message(format!(
                    "duplicate upload destination: {dest}"
                )));
            }
            files.push(UploadFile {
                source: path.clone(),
                dest,
                size: metadata.len(),
            });
        } else {
            return Err(Error::message(format!(
                "unsupported upload path type: {}",
                path.display()
            )));
        }
    }

    files.sort_by(|a, b| a.dest.cmp(&b.dest));
    Ok(files)
}

fn collect_dir_files(
    root: &Path,
    dir: &Path,
    files: &mut Vec<UploadFile>,
    seen: &mut HashSet<String>,
) -> Result<()> {
    for entry in fs::read_dir(dir)
        .map_err(|err| Error::message(format!("failed to read {}: {err}", dir.display())))?
    {
        let entry = entry
            .map_err(|err| Error::message(format!("failed to read {}: {err}", dir.display())))?;
        let path = entry.path();
        let metadata = entry.metadata().map_err(|err| {
            Error::message(format!("failed to read {}: {err}", path.display()))
        })?;
        if metadata.is_dir() {
            collect_dir_files(root, &path, files, seen)?;
        } else if metadata.is_file() {
            let dest = relative_dest(root, &path)?;
            validate_upload_dest(&dest)?;
            if !seen.insert(dest.clone()) {
                return Err(Error::message(format!(
                    "duplicate upload destination: {dest}"
                )));
            }
            files.push(UploadFile {
                source: path,
                dest,
                size: metadata.len(),
            });
        }
    }
    Ok(())
}

fn relative_dest(root: &Path, path: &Path) -> Result<String> {
    let rel = path.strip_prefix(root).map_err(|_| {
        Error::message(format!(
            "failed to compute upload path for {}",
            path.display()
        ))
    })?;
    let dest = rel
        .to_string_lossy()
        .replace('\\', "/")
        .trim_start_matches('/')
        .to_string();
    if dest.is_empty() {
        return Err(Error::message("empty upload destination path"));
    }
    Ok(dest)
}

fn file_name_string(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.to_string())
        .ok_or_else(|| {
            Error::message(format!(
                "invalid upload file name: {}",
                path.display()
            ))
        })
}

fn validate_upload_dest(dest: &str) -> Result<()> {
    let path = Path::new(dest);
    if path.is_absolute() {
        return Err(Error::message(format!(
            "refusing to upload absolute destination path: {dest}"
        )));
    }
    for component in path.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(Error::message(format!(
                "refusing to upload destination with parent traversal: {dest}"
            )));
        }
    }
    Ok(())
}

fn load_metadata_sidecar(path: &Path) -> Result<Value> {
    let contents = fs::read_to_string(path).map_err(|err| {
        Error::message(format!(
            "failed to read metadata file {}: {err}",
            path.display()
        ))
    })?;
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "json" => serde_json::from_str(&contents)
            .map_err(|err| Error::message(format!("invalid JSON metadata: {err}"))),
        "toml" => {
            let value: toml::Value = toml::from_str(&contents)
                .map_err(|err| Error::message(format!("invalid TOML metadata: {err}")))?;
            serde_json::to_value(value)
                .map_err(|err| Error::message(format!("invalid TOML metadata: {err}")))
        }
        _ => Err(Error::message(format!(
            "unsupported metadata file extension: {}",
            path.display()
        ))),
    }
}

fn contains_glob_pattern(path: &Path) -> bool {
    path.to_string_lossy()
        .chars()
        .any(|ch| matches!(ch, '*' | '?' | '[' | ']'))
}

fn ensure_distinct_identifiers(source: &str, dest: &str, action: &str) -> Result<()> {
    if source == dest {
        return Err(Error::message(format!(
            "{action} source and destination identifiers must differ"
        )));
    }
    Ok(())
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
    files: &[String],
    glob: Option<&str>,
    available: &HashMap<String, TransferFile>,
) -> Result<Vec<TransferFile>> {
    let mut selected = HashSet::new();
    let mut results = Vec::new();

    let glob_pattern = glob
        .or_else(|| ctx.config.input.as_ref().and_then(|input| input.glob.as_deref()));
    let glob_matcher = match glob_pattern {
        Some(pattern) => Some(GlobMatcher::new(&[pattern.to_string()])?),
        None => None,
    };

    if !files.is_empty() {
        for name in files {
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
    use tempfile::TempDir;

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
            config_path: None,
            config_destination: None,
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

    #[test]
    fn plans_delete_with_glob_selection() {
        let mut ctx = test_context();
        ctx.config.input = Some(crate::config::InputConfig {
            glob: Some("*.txt".into()),
            validate_identifiers: Some(true),
            read_stdin: None,
        });
        let args = DeleteArgs {
            identifier: "example".into(),
            files: Vec::new(),
            glob: None,
            dry_run: true,
        };
        let metadata = sample_metadata();
        let available = parse_metadata_files(&metadata).expect("files");
        let available_map = available
            .into_iter()
            .map(|file| (file.name.clone(), file))
            .collect::<HashMap<_, _>>();
        let selection = select_files(&ctx, &args.files, args.glob.as_deref(), &available_map)
            .expect("selection");
        assert_eq!(selection.len(), 2);
    }

    #[test]
    fn copy_requires_distinct_identifiers() {
        let err = ensure_distinct_identifiers("same", "same", "copy").unwrap_err();
        assert!(err
            .to_string()
            .contains("copy source and destination identifiers must differ"));
    }

    #[test]
    fn move_requires_distinct_identifiers() {
        let err = ensure_distinct_identifiers("same", "same", "move").unwrap_err();
        assert!(err
            .to_string()
            .contains("move source and destination identifiers must differ"));
    }

    #[test]
    fn loads_json_metadata_sidecar() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("metadata.json");
        fs::write(&path, r#"{ "title": "Example" }"#).expect("write");
        let value = load_metadata_sidecar(&path).expect("metadata");
        assert_eq!(value.get("title").and_then(|v| v.as_str()), Some("Example"));
    }

    #[test]
    fn loads_toml_metadata_sidecar() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("metadata.toml");
        fs::write(&path, "title = \"Example\"\n").expect("write");
        let value = load_metadata_sidecar(&path).expect("metadata");
        assert_eq!(value.get("title").and_then(|v| v.as_str()), Some("Example"));
    }

    #[test]
    fn collects_upload_files_from_directory() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path();
        fs::create_dir_all(root.join("nested")).expect("mkdir");
        fs::write(root.join("file.txt"), "data").expect("write");
        fs::write(root.join("nested/file2.txt"), "more").expect("write");

        let files = collect_upload_files(&[root.to_path_buf()]).expect("files");
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|file| file.dest == "file.txt"));
        assert!(files.iter().any(|file| file.dest == "nested/file2.txt"));
    }

    #[test]
    fn errors_on_duplicate_upload_dest() {
        let dir = TempDir::new().expect("temp dir");
        let first = dir.path().join("first.txt");
        let second_dir = dir.path().join("other");
        fs::create_dir_all(&second_dir).expect("mkdir");
        let second = second_dir.join("first.txt");
        fs::write(&first, "data").expect("write");
        fs::write(&second, "data").expect("write");

        let err = collect_upload_files(&[first, second]).unwrap_err();
        assert!(err.to_string().contains("duplicate upload destination"));
    }

    #[test]
    fn errors_on_unknown_metadata_extension() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("metadata.txt");
        fs::write(&path, "title=Example").expect("write");
        let err = load_metadata_sidecar(&path).unwrap_err();
        assert!(err.to_string().contains("unsupported metadata file extension"));
    }
}
