use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde_json::{json, Value};
use sha1::{Digest, Sha1};
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
    md5: Option<String>,
    sha1: Option<String>,
    formats: Vec<String>,
}

#[derive(Debug, Clone)]
struct PlannedFile {
    name: String,
    url: String,
    size: Option<u64>,
    md5: Option<String>,
    sha1: Option<String>,
}

#[derive(Debug, Clone)]
struct DownloadPlan {
    identifier: String,
    dest: PathBuf,
    files: Vec<PlannedFile>,
    total_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
struct DeletePlan {
    identifier: String,
    files: Vec<TransferFile>,
    cascade: bool,
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
    md5: String,
    sha1: String,
}

#[derive(Debug, Clone)]
struct UploadPlan {
    identifier: String,
    files: Vec<UploadFile>,
    metadata: Option<Value>,
    total_bytes: u64,
    total_files: usize,
}

#[derive(Debug, Clone, Copy)]
struct TransferPolicy {
    resume: bool,
    checksum_verify: bool,
    chunk_size_bytes: Option<u64>,
}

#[instrument(skip(ctx))]
pub fn download(ctx: &AppContext, args: &DownloadArgs) -> Result<()> {
    let policy = TransferPolicy::from_config(ctx);
    let plan = plan_download(ctx, args)?;
    emit_plan(ctx, &plan, args.dry_run, policy)?;
    if args.dry_run {
        return Ok(());
    }
    execute_download(ctx, &plan, policy)
}

#[instrument(skip(ctx))]
pub fn upload(ctx: &AppContext, args: &UploadArgs) -> Result<()> {
    let policy = TransferPolicy::from_config(ctx);
    if policy.chunk_size_bytes.is_some() {
        warn!("chunked uploads are not implemented; ignoring chunk_size_bytes");
    }
    let plan = plan_upload(ctx, args)?;
    emit_upload_plan(ctx, &plan, args.dry_run, policy)?;
    if args.dry_run {
        return Ok(());
    }
    execute_upload(ctx, &plan, policy)
}

#[instrument(skip(ctx))]
pub fn delete(ctx: &AppContext, _args: &DeleteArgs) -> Result<()> {
    let policy = TransferPolicy::from_config(ctx);
    let plan = plan_delete(ctx, _args)?;
    emit_delete_plan(ctx, &plan, _args.dry_run, policy)?;
    if _args.dry_run {
        return Ok(());
    }
    execute_delete(ctx, &plan, policy)
}

#[instrument(skip(ctx))]
pub fn copy(ctx: &AppContext, _args: &CopyArgs) -> Result<()> {
    let policy = TransferPolicy::from_config(ctx);
    let plan = plan_copy(ctx, _args)?;
    emit_copy_plan(ctx, &plan, _args.dry_run)?;
    if _args.dry_run {
        return Ok(());
    }
    execute_copy(ctx, &plan, policy)
}

#[instrument(skip(ctx))]
pub fn move_item(ctx: &AppContext, _args: &MoveArgs) -> Result<()> {
    let policy = TransferPolicy::from_config(ctx);
    let plan = plan_move(ctx, _args)?;
    emit_move_plan(ctx, &plan, _args.dry_run)?;
    if _args.dry_run {
        return Ok(());
    }
    execute_move(ctx, &plan, policy)
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
    let selection = filter_by_format(
        selection,
        &args.formats,
        &args.files,
        args.glob.as_deref(),
    )?;
    let mut planned = Vec::with_capacity(selection.len());
    for file in selection {
        validate_download_path(&file.name)?;
        planned.push(PlannedFile {
            name: file.name.clone(),
            url: build_file_url(ctx.http.s3_base(), &args.identifier, &file.name)?,
            size: file.size,
            md5: file.md5.clone(),
            sha1: file.sha1.clone(),
        });
    }
    planned.sort_by(|a, b| a.name.cmp(&b.name));

    let total_bytes = planned.iter().map(|file| file.size).sum::<Option<u64>>();

    Ok(DownloadPlan {
        identifier: args.identifier.clone(),
        dest: args.dest.clone(),
        files: planned,
        total_bytes,
    })
}

#[instrument(skip(ctx, plan))]
fn execute_download(ctx: &AppContext, plan: &DownloadPlan, policy: TransferPolicy) -> Result<()> {
    fs::create_dir_all(&plan.dest).map_err(|err| {
        Error::message(format!(
            "failed to create destination directory {}: {err}",
            plan.dest.display()
        ))
    })?;

    let mut aggregate = DownloadProgress::new(plan.total_bytes);
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

        if should_skip_existing(&dest_path, file.size, policy.resume)? {
            aggregate.on_skip(file.size);
            continue;
        }

        info!(
            file = %file.name,
            url = %file.url,
            dest = %dest_path.display(),
            "downloading file"
        );

        let bytes = ctx.http.get_bytes(&file.url)?;
        if policy.checksum_verify {
            verify_checksums(
                &file.name,
                &bytes,
                file.md5.as_deref(),
                file.sha1.as_deref(),
            )?;
        }
        fs::write(&dest_path, &bytes).map_err(|err| {
            Error::message(format!(
                "failed to write {}: {err}",
                dest_path.display()
            ))
        })?;

        aggregate.on_complete(file.size.or_else(|| Some(bytes.len() as u64)));
        if ctx.output.policy().verbose {
            let line = aggregate.format_line(&file.name);
            let _ = ctx.output.write_line(&line);
        }
    }

    Ok(())
}

#[instrument(skip(ctx, plan))]
fn emit_plan(
    ctx: &AppContext,
    plan: &DownloadPlan,
    dry_run: bool,
    policy: TransferPolicy,
) -> Result<()> {
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
                        "md5": file.md5,
                        "sha1": file.sha1,
                    })
                })
                .collect::<Vec<_>>();
            let value = json!({
                "identifier": plan.identifier,
                "dest": plan.dest.display().to_string(),
                "dry_run": dry_run,
                "resume": policy.resume,
                "checksum_verify": policy.checksum_verify,
                "chunk_size_bytes": policy.chunk_size_bytes,
                "total_bytes": plan.total_bytes,
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
            if let Some(total) = plan.total_bytes {
                ctx.output
                    .write_line(&format!("Total bytes: {total}"))
                    .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            }
            if ctx.output.policy().verbose {
                ctx.output
                    .write_line(&format!(
                        "Resume: {} | Checksum verify: {} | Chunk size: {}",
                        policy.resume,
                        policy.checksum_verify,
                        policy
                            .chunk_size_bytes
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "default".to_string())
                    ))
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
    let selection = filter_by_format(
        selection,
        &args.formats,
        &args.files,
        args.glob.as_deref(),
    )?;
    Ok(DeletePlan {
        identifier: args.identifier.clone(),
        files: selection,
        cascade: args.cascade,
    })
}

#[instrument(skip(ctx, plan))]
fn emit_delete_plan(
    ctx: &AppContext,
    plan: &DeletePlan,
    dry_run: bool,
    policy: TransferPolicy,
) -> Result<()> {
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
                "cascade": plan.cascade,
                "resume": policy.resume,
                "checksum_verify": policy.checksum_verify,
                "chunk_size_bytes": policy.chunk_size_bytes,
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
            if plan.cascade {
                ctx.output
                    .write_line("Cascade delete: enabled")
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
    let selection = filter_by_format(
        selection,
        &args.formats,
        &args.files,
        args.glob.as_deref(),
    )?;
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
    let selection = filter_by_format(
        selection,
        &args.formats,
        &args.files,
        args.glob.as_deref(),
    )?;
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
    let total_files = files.len();
    Ok(UploadPlan {
        identifier: args.identifier.clone(),
        files,
        metadata,
        total_bytes,
        total_files,
    })
}

#[instrument(skip(ctx, plan))]
fn emit_upload_plan(
    ctx: &AppContext,
    plan: &UploadPlan,
    dry_run: bool,
    policy: TransferPolicy,
) -> Result<()> {
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
                        "md5": file.md5,
                        "sha1": file.sha1,
                    })
                })
                .collect::<Vec<_>>();
            let value = json!({
                "identifier": plan.identifier,
                "dry_run": dry_run,
                "total_bytes": plan.total_bytes,
                "total_files": plan.total_files,
                "resume": policy.resume,
                "checksum_verify": policy.checksum_verify,
                "chunk_size_bytes": policy.chunk_size_bytes,
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
                        "{} -> {} ({} bytes) [md5:{} sha1:{}]",
                        file.source.display(),
                        file.dest,
                        file.size,
                        file.md5,
                        file.sha1
                    ))
                    .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            }
            if plan.metadata.is_some() {
                ctx.output
                    .write_line("Metadata: provided")
                    .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
            }
            if ctx.output.policy().verbose {
                ctx.output
                    .write_line(&format!(
                        "Resume: {} | Checksum verify: {} | Chunk size: {}",
                        policy.resume,
                        policy.checksum_verify,
                        policy
                            .chunk_size_bytes
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "default".to_string())
                    ))
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
            let (md5, sha1) = compute_hashes_from_path(path)?;
            files.push(UploadFile {
                source: path.clone(),
                dest,
                size: metadata.len(),
                md5,
                sha1,
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
            let (md5, sha1) = compute_hashes_from_path(&path)?;
            files.push(UploadFile {
                source: path,
                dest,
                size: metadata.len(),
                md5,
                sha1,
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

impl TransferPolicy {
    fn from_config(ctx: &AppContext) -> Self {
        let config = ctx.config.file_transfer.as_ref();
        Self {
            resume: config.and_then(|cfg| cfg.resume).unwrap_or(false),
            checksum_verify: config
                .and_then(|cfg| cfg.checksum_verify)
                .unwrap_or(false),
            chunk_size_bytes: config.and_then(|cfg| cfg.chunk_size_bytes),
        }
    }
}

#[derive(Debug)]
struct DownloadProgress {
    total_bytes: Option<u64>,
    completed_files: u64,
    completed_bytes: u64,
    skipped_files: u64,
}

impl DownloadProgress {
    fn new(total_bytes: Option<u64>) -> Self {
        Self {
            total_bytes,
            completed_files: 0,
            completed_bytes: 0,
            skipped_files: 0,
        }
    }

    fn on_complete(&mut self, size: Option<u64>) {
        self.completed_files += 1;
        if let Some(size) = size {
            self.completed_bytes = self.completed_bytes.saturating_add(size);
        }
    }

    fn on_skip(&mut self, size: Option<u64>) {
        self.skipped_files += 1;
        if let Some(size) = size {
            self.completed_bytes = self.completed_bytes.saturating_add(size);
        }
    }

    fn format_line(&self, name: &str) -> String {
        let mut line = format!(
            "Downloaded {name} (files: {} done, {} skipped)",
            self.completed_files, self.skipped_files
        );
        if let Some(total) = self.total_bytes {
            if total > 0 {
                let percent = (self.completed_bytes as f64 / total as f64) * 100.0;
                line.push_str(&format!(
                    " | bytes: {}/{} ({:.1}%)",
                    self.completed_bytes, total, percent
                ));
            }
        }
        line
    }
}

fn should_skip_existing(path: &Path, expected_size: Option<u64>, resume: bool) -> Result<bool> {
    if !resume {
        return Ok(false);
    }
    if let Ok(metadata) = fs::metadata(path) {
        if metadata.is_file() {
            if let Some(expected) = expected_size {
                if metadata.len() == expected {
                    return Ok(true);
                }
                return Err(Error::message(format!(
                    "resume enabled but existing file size differs for {}",
                    path.display()
                )));
            }
            return Err(Error::message(format!(
                "resume enabled but metadata size unknown for {}",
                path.display()
            )));
        }
    }
    Ok(false)
}

fn verify_checksums(
    name: &str,
    bytes: &[u8],
    md5_expected: Option<&str>,
    sha1_expected: Option<&str>,
) -> Result<()> {
    if md5_expected.is_none() && sha1_expected.is_none() {
        warn!(file = %name, "checksum verification enabled but metadata missing checksums");
        return Ok(());
    }

    if let Some(expected) = md5_expected {
        let computed = md5_hex(bytes);
        if !equals_ignore_case(&computed, expected) {
            return Err(Error::message(format!(
                "md5 checksum mismatch for {name}"
            )));
        }
    }

    if let Some(expected) = sha1_expected {
        let computed = sha1_hex(bytes);
        if !equals_ignore_case(&computed, expected) {
            return Err(Error::message(format!(
                "sha1 checksum mismatch for {name}"
            )));
        }
    }

    Ok(())
}

fn md5_hex(bytes: &[u8]) -> String {
    let digest = md5::compute(bytes);
    bytes_to_hex(&digest.0)
}

fn sha1_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    bytes_to_hex(&digest)
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn equals_ignore_case(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn compute_hashes(bytes: &[u8]) -> (String, String) {
    (md5_hex(bytes), sha1_hex(bytes))
}

fn compute_hashes_from_path(path: &Path) -> Result<(String, String)> {
    let mut file = fs::File::open(path).map_err(|err| {
        Error::message(format!("failed to read {}: {err}", path.display()))
    })?;
    let mut md5_ctx = md5::Context::new();
    let mut sha1_ctx = Sha1::new();
    let mut buffer = [0u8; 32 * 1024];
    loop {
        let read = std::io::Read::read(&mut file, &mut buffer)
            .map_err(|err| Error::message(format!("failed to read {}: {err}", path.display())))?;
        if read == 0 {
            break;
        }
        md5_ctx.consume(&buffer[..read]);
        sha1_ctx.update(&buffer[..read]);
    }
    let md5 = bytes_to_hex(&md5_ctx.finalize().0);
    let sha1 = bytes_to_hex(&sha1_ctx.finalize());
    Ok((md5, sha1))
}

fn execute_upload(ctx: &AppContext, plan: &UploadPlan, policy: TransferPolicy) -> Result<()> {
    let auth = build_low_auth_header(&ctx.config)?;
    let mut progress = UploadProgress::new(plan.total_bytes, plan.total_files);
    for file in &plan.files {
        let url = build_upload_url(ctx.http.s3_base(), &plan.identifier, &file.dest)?;
        let headers = build_upload_headers(&file.md5, &file.sha1, policy, &auth)?;
        if should_skip_upload(ctx, &url, file.size, policy.resume, &auth)? {
            progress.on_skip(file.size);
            if ctx.output.policy().verbose {
                let line = progress.format_line(&file.dest, std::time::Duration::from_secs(0));
                let _ = ctx.output.write_line(&line);
            }
            continue;
        }
        info!(
            file = %file.dest,
            url = %url,
            size = file.size,
            "uploading file"
        );

        let start = Instant::now();
        let body = fs::read(&file.source).map_err(|err| {
            Error::message(format!("failed to read {}: {err}", file.source.display()))
        })?;
        ctx.http
            .put_bytes(&url, &body, &headers)
            .map_err(|err| Error::message(format!("upload failed for {}: {err}", file.dest)))?;

        progress.on_complete(file.size);
        if ctx.output.policy().verbose {
            let line = progress.format_line(&file.dest, start.elapsed());
            let _ = ctx.output.write_line(&line);
        }
    }

    if ctx.output.policy().format == OutputFormat::Json {
        let value = json!({
            "identifier": plan.identifier,
            "uploaded_files": plan.total_files,
            "uploaded_bytes": progress.completed_bytes,
            "skipped_files": progress.skipped_files,
        });
        ctx.output
            .write_json(&value)
            .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
    } else {
        ctx.output
            .write_line(&format!(
                "Uploaded {} files ({} bytes, {} skipped)",
                plan.total_files, progress.completed_bytes, progress.skipped_files
            ))
            .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
    }

    Ok(())
}

fn build_upload_url(base: &str, identifier: &str, dest: &str) -> Result<String> {
    build_file_url(base, identifier, dest)
}

fn build_upload_headers(
    md5: &str,
    sha1: &str,
    policy: TransferPolicy,
    auth_header: &str,
) -> Result<Vec<(String, String)>> {
    let mut headers = Vec::new();
    headers.push(("Content-MD5".to_string(), md5.to_string()));
    headers.push(("X-Archive-SHA1".to_string(), sha1.to_string()));
    headers.push(("Authorization".to_string(), auth_header.to_string()));
    headers.push((
        "x-archive-auto-make-bucket".to_string(),
        "1".to_string(),
    ));
    headers.push((
        "x-archive-queue-derive".to_string(),
        if policy.chunk_size_bytes.is_some() {
            "0".to_string()
        } else {
            "1".to_string()
        },
    ));
    Ok(headers)
}

#[derive(Debug)]
struct UploadProgress {
    total_bytes: u64,
    total_files: usize,
    completed_files: usize,
    completed_bytes: u64,
    skipped_files: usize,
}

fn execute_delete(ctx: &AppContext, plan: &DeletePlan, _policy: TransferPolicy) -> Result<()> {
    let auth = build_low_auth_header(&ctx.config)?;
    for file in &plan.files {
        let url = build_file_url(ctx.http.s3_base(), &plan.identifier, &file.name)?;
        let headers = build_delete_headers(&auth, plan.cascade);
        info!(file = %file.name, url = %url, "deleting file");
        ctx.http
            .delete(&url, &headers)
            .map_err(|err| Error::message(format!("delete failed for {}: {err}", file.name)))?;
    }
    Ok(())
}

fn build_delete_headers(auth_header: &str, cascade: bool) -> Vec<(String, String)> {
    let mut headers = Vec::new();
    headers.push(("Authorization".to_string(), auth_header.to_string()));
    headers.push((
        "x-archive-cascade-delete".to_string(),
        if cascade { "1" } else { "0" }.to_string(),
    ));
    headers
}

fn build_low_auth_header(config: &crate::config::Config) -> Result<String> {
    let access = config
        .auth
        .as_ref()
        .and_then(|auth| auth.access_key.as_deref())
        .ok_or_else(|| Error::message("missing access key for S3 auth"))?;
    let secret = config
        .auth
        .as_ref()
        .and_then(|auth| auth.secret_key.as_deref())
        .ok_or_else(|| Error::message("missing secret key for S3 auth"))?;
    Ok(format!("LOW {access}:{secret}"))
}

impl UploadProgress {
    fn new(total_bytes: u64, total_files: usize) -> Self {
        Self {
            total_bytes,
            total_files,
            completed_files: 0,
            completed_bytes: 0,
            skipped_files: 0,
        }
    }

    fn on_complete(&mut self, size: u64) {
        self.completed_files += 1;
        self.completed_bytes = self.completed_bytes.saturating_add(size);
    }

    fn on_skip(&mut self, size: u64) {
        self.skipped_files += 1;
        self.completed_bytes = self.completed_bytes.saturating_add(size);
    }

    fn format_line(&self, name: &str, elapsed: std::time::Duration) -> String {
        let mut line = format!(
            "Uploaded {name} (files: {}/{} | skipped: {})",
            self.completed_files, self.total_files, self.skipped_files
        );
        if self.total_bytes > 0 {
            let percent = (self.completed_bytes as f64 / self.total_bytes as f64) * 100.0;
            line.push_str(&format!(
                " | bytes: {}/{} ({:.1}%)",
                self.completed_bytes, self.total_bytes, percent
            ));
        }
        if elapsed.as_secs_f64() > 0.0 {
            let rate = self.completed_bytes as f64 / elapsed.as_secs_f64();
            line.push_str(&format!(" | {:.1} B/s", rate));
        }
        line
    }
}

fn should_skip_upload(
    ctx: &AppContext,
    url: &str,
    expected_size: u64,
    resume: bool,
    auth_header: &str,
) -> Result<bool> {
    if !resume {
        return Ok(false);
    }
    let headers = vec![("Authorization".to_string(), auth_header.to_string())];
    let info = ctx.http.head_info(url, &headers)?;
    if info.status == 404 {
        return Ok(false);
    }
    match info.content_length {
        Some(remote_size) => {
            if remote_size == expected_size {
                Ok(true)
            } else {
                Err(Error::message(format!(
                    "resume enabled but remote size differs for {url}"
                )))
            }
        }
        None => Err(Error::message(format!(
            "resume enabled but remote size unknown for {url}"
        ))),
    }
}

fn execute_copy(ctx: &AppContext, plan: &CopyPlan, policy: TransferPolicy) -> Result<()> {
    let auth = build_low_auth_header(&ctx.config)?;
    let mut progress = CopyProgress::new(plan.files.len());
    for file in &plan.files {
        let source_url = build_file_url(ctx.http.s3_base(), &plan.source_identifier, &file.name)?;
        let dest_url = build_file_url(ctx.http.s3_base(), &plan.dest_identifier, &file.name)?;
        info!(
            file = %file.name,
            source = %source_url,
            dest = %dest_url,
            "copying file"
        );
        let bytes = ctx.http.get_bytes(&source_url)?;
        if policy.checksum_verify {
            verify_checksums(
                &file.name,
                &bytes,
                file.md5.as_deref(),
                file.sha1.as_deref(),
            )?;
        }
        let (md5, sha1) = compute_hashes(&bytes);
        let headers = build_upload_headers(&md5, &sha1, policy, &auth)?;
        ctx.http
            .put_bytes(&dest_url, &bytes, &headers)
            .map_err(|err| Error::message(format!("copy failed for {}: {err}", file.name)))?;

        progress.on_complete();
        if ctx.output.policy().verbose {
            let line = progress.format_line(&file.name);
            let _ = ctx.output.write_line(&line);
        }
    }
    emit_transfer_summary(ctx, "Copied", plan.files.len())?;
    Ok(())
}

fn execute_move(ctx: &AppContext, plan: &MovePlan, policy: TransferPolicy) -> Result<()> {
    let auth = build_low_auth_header(&ctx.config)?;
    let mut progress = CopyProgress::new(plan.files.len());
    for file in &plan.files {
        let source_url = build_file_url(ctx.http.s3_base(), &plan.source_identifier, &file.name)?;
        let dest_url = build_file_url(ctx.http.s3_base(), &plan.dest_identifier, &file.name)?;
        info!(
            file = %file.name,
            source = %source_url,
            dest = %dest_url,
            "moving file"
        );
        let bytes = ctx.http.get_bytes(&source_url)?;
        if policy.checksum_verify {
            verify_checksums(
                &file.name,
                &bytes,
                file.md5.as_deref(),
                file.sha1.as_deref(),
            )?;
        }
        let (md5, sha1) = compute_hashes(&bytes);
        let headers = build_upload_headers(&md5, &sha1, policy, &auth)?;
        ctx.http
            .put_bytes(&dest_url, &bytes, &headers)
            .map_err(|err| Error::message(format!("move upload failed for {}: {err}", file.name)))?;
        let delete_headers = build_delete_headers(&auth, false);
        ctx.http
            .delete(&source_url, &delete_headers)
            .map_err(|err| Error::message(format!("move delete failed for {}: {err}", file.name)))?;

        progress.on_complete();
        if ctx.output.policy().verbose {
            let line = progress.format_line(&file.name);
            let _ = ctx.output.write_line(&line);
        }
    }
    emit_transfer_summary(ctx, "Moved", plan.files.len())?;
    Ok(())
}

#[derive(Debug)]
struct CopyProgress {
    total_files: usize,
    completed_files: usize,
}

impl CopyProgress {
    fn new(total_files: usize) -> Self {
        Self {
            total_files,
            completed_files: 0,
        }
    }

    fn on_complete(&mut self) {
        self.completed_files += 1;
    }

    fn format_line(&self, name: &str) -> String {
        format!(
            "Transferred {name} (files: {}/{})",
            self.completed_files, self.total_files
        )
    }
}

fn emit_transfer_summary(ctx: &AppContext, label: &str, files: usize) -> Result<()> {
    if ctx.output.policy().format == OutputFormat::Json {
        let value = json!({
            "action": label.to_lowercase(),
            "files": files,
        });
        ctx.output
            .write_json(&value)
            .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
    } else {
        ctx.output
            .write_line(&format!("{label} {files} files"))
            .map_err(|err| Error::message(format!("failed to write output: {err}")))?;
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
        let md5 = file
            .get("md5")
            .and_then(|value| value.as_str())
            .map(str::to_string);
        let sha1 = file
            .get("sha1")
            .and_then(|value| value.as_str())
            .map(str::to_string);
    let formats = parse_formats(file);
    results.push(TransferFile {
        name,
        size,
        md5,
        sha1,
        formats,
    });
    }
    if results.is_empty() {
        return Err(Error::message("metadata response contained no files"));
    }
    Ok(results)
}

fn parse_formats(file: &Value) -> Vec<String> {
    let mut formats = Vec::new();
    match file.get("format") {
        Some(Value::String(value)) => {
            let value = value.trim();
            if !value.is_empty() {
                formats.push(value.to_string());
            }
        }
        Some(Value::Array(values)) => {
            for value in values {
                if let Some(value) = value.as_str() {
                    let value = value.trim();
                    if !value.is_empty() {
                        formats.push(value.to_string());
                    }
                }
            }
        }
        _ => {}
    }
    formats
}

fn filter_by_format(
    files: Vec<TransferFile>,
    formats: &[String],
    explicit_files: &[String],
    glob: Option<&str>,
) -> Result<Vec<TransferFile>> {
    if formats.is_empty() {
        return Ok(files);
    }

    let mut filtered = Vec::new();
    for file in files {
        if format_matches(&file, formats) {
            filtered.push(file);
        } else if explicit_files
            .iter()
            .any(|name| name == &file.name)
        {
            return Err(Error::message(format!(
                "requested file does not match format filter: {}",
                file.name
            )));
        }
    }

    if filtered.is_empty() {
        let hint = if let Some(pattern) = glob {
            format!(" for glob '{pattern}'")
        } else {
            String::new()
        };
        return Err(Error::message(format!(
            "no files matched format filters{hint}"
        )));
    }

    Ok(filtered)
}

fn format_matches(file: &TransferFile, formats: &[String]) -> bool {
    if formats.is_empty() {
        return true;
    }
    for format in &file.formats {
        for wanted in formats {
            if format.eq_ignore_ascii_case(wanted) {
                return true;
            }
        }
    }
    false
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
                { "name": "file-one.txt", "size": "12", "md5": "098f6bcd4621d373cade4e832627b4f6", "format": "Text" },
                { "name": "nested/file-two.txt", "size": "24", "sha1": "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3", "format": ["Text", "Source"] },
                { "name": "image.jpg", "format": "JPEG" }
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
        assert_eq!(
            files[0].md5.as_deref(),
            Some("098f6bcd4621d373cade4e832627b4f6")
        );
        assert_eq!(files[1].sha1.as_deref(), Some("a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"));
        assert!(files[0].formats.iter().any(|fmt| fmt == "Text"));
        assert!(files[1].formats.iter().any(|fmt| fmt == "Source"));
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
            formats: Vec::new(),
            glob: Some("*.txt".into()),
            dest: PathBuf::from("."),
            dry_run: true,
        };
        let selected =
            select_files(&ctx, &args.files, args.glob.as_deref(), &available).expect("selected");
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
            formats: Vec::new(),
            glob: None,
            dest: PathBuf::from("."),
            dry_run: true,
        };
        let selected =
            select_files(&ctx, &args.files, args.glob.as_deref(), &available).expect("selected");
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
            formats: Vec::new(),
            glob: None,
            dest: PathBuf::from("."),
            dry_run: true,
        };
        let err = select_files(&ctx, &args.files, args.glob.as_deref(), &available).unwrap_err();
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
            formats: vec!["Text".into()],
            glob: Some("*.pdf".into()),
            dest: PathBuf::from("."),
            dry_run: true,
        };
        let err = select_files(&ctx, &args.files, args.glob.as_deref(), &available).unwrap_err();
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
            formats: Vec::new(),
            glob: None,
            cascade: false,
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

    #[test]
    fn resume_skips_existing_when_size_matches() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("file.txt");
        fs::write(&path, "data").expect("write");
        let skip = should_skip_existing(&path, Some(4), true).expect("skip");
        assert!(skip);
    }

    #[test]
    fn resume_errors_on_size_mismatch() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("file.txt");
        fs::write(&path, "data").expect("write");
        let err = should_skip_existing(&path, Some(2), true).unwrap_err();
        assert!(err.to_string().contains("existing file size differs"));
    }

    #[test]
    fn progress_formats_with_totals() {
        let mut progress = DownloadProgress::new(Some(10));
        progress.on_complete(Some(4));
        let line = progress.format_line("file.txt");
        assert!(line.contains("40.0%"));
    }

    #[test]
    fn verifies_md5_checksum() {
        let bytes = b"test";
        verify_checksums(
            "file.txt",
            bytes,
            Some("098f6bcd4621d373cade4e832627b4f6"),
            None,
        )
        .expect("checksum");
    }

    #[test]
    fn verifies_sha1_checksum() {
        let bytes = b"test";
        verify_checksums(
            "file.txt",
            bytes,
            None,
            Some("a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"),
        )
        .expect("checksum");
    }

    #[test]
    fn fails_on_checksum_mismatch() {
        let bytes = b"test";
        let err = verify_checksums("file.txt", bytes, Some("deadbeef"), None).unwrap_err();
        assert!(err.to_string().contains("md5 checksum mismatch"));
    }

    #[test]
    fn computes_hashes_from_path() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("file.txt");
        fs::write(&path, "test").expect("write");
        let (md5, sha1) = compute_hashes_from_path(&path).expect("hashes");
        assert_eq!(md5, "098f6bcd4621d373cade4e832627b4f6");
        assert_eq!(sha1, "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3");
    }

    #[test]
    fn builds_low_auth_header() {
        let config = crate::config::Config {
            auth: Some(crate::config::AuthConfig {
                access_key: Some("access".into()),
                secret_key: Some("secret".into()),
            }),
            ..crate::config::Config::default()
        };
        let header = build_low_auth_header(&config).expect("auth");
        assert_eq!(header, "LOW access:secret");
    }

    #[test]
    fn build_delete_headers_sets_cascade() {
        let headers = build_delete_headers("LOW access:secret", true);
        assert!(headers
            .iter()
            .any(|(key, value)| key == "x-archive-cascade-delete" && value == "1"));
    }

    #[test]
    fn copy_progress_formats_line() {
        let mut progress = CopyProgress::new(2);
        progress.on_complete();
        let line = progress.format_line("file.txt");
        assert!(line.contains("files: 1/2"));
    }

    #[test]
    fn upload_progress_tracks_skips() {
        let mut progress = UploadProgress::new(10, 2);
        progress.on_skip(4);
        let line = progress.format_line("file.txt", std::time::Duration::from_secs(0));
        assert!(line.contains("skipped: 1"));
    }

    #[test]
    fn upload_summary_includes_skips() {
        let config = crate::config::Config::default();
        let http = crate::http::HttpClient::new(crate::http::config_from_settings(&config))
            .expect("http client");
        let output = crate::output::OutputWriter::new(crate::output::OutputPolicy::new(
            OutputFormat::Json,
        ));
        let ctx = AppContext {
            config,
            http,
            output,
            config_path: None,
            config_destination: None,
        };
        let plan = UploadPlan {
            identifier: "example".into(),
            files: Vec::new(),
            metadata: None,
            total_bytes: 0,
            total_files: 0,
        };
        let policy = TransferPolicy::from_config(&ctx);
        let result = emit_upload_plan(&ctx, &plan, true, policy);
        assert!(result.is_ok());
    }

    #[test]
    fn filters_by_format() {
        let ctx = test_context();
        let available = parse_metadata_files(&sample_metadata()).expect("files");
        let filtered = filter_by_format(available, &["JPEG".into()], &[], None).expect("filtered");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "image.jpg");
    }

    #[test]
    fn errors_on_format_mismatch_for_explicit_file() {
        let ctx = test_context();
        let available = parse_metadata_files(&sample_metadata()).expect("files");
        let err = filter_by_format(
            available,
            &["JPEG".into()],
            &["file-one.txt".into()],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not match format"));
    }
}
