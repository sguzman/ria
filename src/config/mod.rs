use directories::ProjectDirs;
use globset::Glob;
use serde::{Deserialize, Serialize};
use std::env;
use std::path::{Path, PathBuf};

use crate::errors::{Error, Result};

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct Config {
    pub logging: Option<LoggingConfig>,
    pub general: Option<GeneralConfig>,
    pub network: Option<NetworkConfig>,
    pub output: Option<OutputConfig>,
    pub input: Option<InputConfig>,
    pub tls: Option<TlsConfig>,
    pub endpoints: Option<EndpointsConfig>,
    pub auth: Option<AuthConfig>,
    pub file_transfer: Option<FileTransferConfig>,
    pub compatibility: Option<CompatibilityConfig>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct LoggingConfig {
    pub enabled: Option<bool>,
    pub level: Option<String>,
    pub filter: Option<String>,
    pub format: Option<String>,
    pub output: Option<String>,
    pub ansi: Option<bool>,
    pub target: Option<bool>,
    pub thread_ids: Option<bool>,
    pub thread_names: Option<bool>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct GeneralConfig {
    pub insecure: Option<bool>,
    pub host: Option<String>,
    pub user_agent_base: Option<String>,
    pub user_agent_suffix: Option<String>,
    pub user_agent_opt_out: Option<bool>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct NetworkConfig {
    pub timeout_secs: Option<u64>,
    pub connect_timeout_secs: Option<u64>,
    pub retry_max: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
    pub rate_limit_per_sec: Option<u32>,
    pub concurrency: Option<u32>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct OutputConfig {
    pub format: Option<String>,
    pub paging: Option<bool>,
    pub color: Option<bool>,
    pub quiet: Option<bool>,
    pub verbose: Option<bool>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct InputConfig {
    pub glob: Option<String>,
    pub validate_identifiers: Option<bool>,
    pub read_stdin: Option<bool>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct TlsConfig {
    pub verify: Option<bool>,
    pub ca_bundle: Option<String>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct EndpointsConfig {
    pub api_base: Option<String>,
    pub s3_base: Option<String>,
    pub metadata_base: Option<String>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct AuthConfig {
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct FileTransferConfig {
    pub chunk_size_bytes: Option<u64>,
    pub checksum_verify: Option<bool>,
    pub resume: Option<bool>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct CompatibilityConfig {
    pub python_user_agent: Option<bool>,
    pub legacy_metadata_format: Option<bool>,
    pub legacy_logging: Option<bool>,
}

#[derive(Debug, Default, Clone)]
pub struct ConfigOverrides {
    pub logging_level: Option<String>,
    pub logging_filter: Option<String>,
    pub logging_format: Option<String>,
    pub logging_output: Option<String>,
    pub logging_enabled: Option<bool>,
    pub logging_ansi: Option<bool>,
    pub logging_target: Option<bool>,
    pub logging_thread_ids: Option<bool>,
    pub logging_thread_names: Option<bool>,
    pub insecure: Option<bool>,
    pub host: Option<String>,
    pub user_agent_base: Option<String>,
    pub user_agent_suffix: Option<String>,
    pub user_agent_opt_out: Option<bool>,
    pub output_format: Option<String>,
    pub output_color: Option<bool>,
    pub output_paging: Option<bool>,
    pub output_quiet: Option<bool>,
    pub output_verbose: Option<bool>,
    pub tls_verify: Option<bool>,
    pub ca_bundle: Option<String>,
    pub api_base: Option<String>,
    pub s3_base: Option<String>,
    pub metadata_base: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub input_glob: Option<String>,
    pub input_validate_identifiers: Option<bool>,
    pub input_read_stdin: Option<bool>,
    pub transfer_chunk_size_bytes: Option<u64>,
    pub transfer_checksum_verify: Option<bool>,
    pub transfer_resume: Option<bool>,
    pub compat_python_user_agent: Option<bool>,
    pub compat_legacy_metadata_format: Option<bool>,
    pub compat_legacy_logging: Option<bool>,
}

pub fn resolve_config_path(cli_path: Option<PathBuf>) -> Option<PathBuf> {
    if cli_path.is_some() {
        return cli_path;
    }

    if let Some(path) = env::var_os("RIA_CONFIG").map(PathBuf::from) {
        return Some(path);
    }

    default_config_path().filter(|path| path.exists())
}

pub fn resolve_config_destination(cli_path: Option<PathBuf>) -> Option<PathBuf> {
    if cli_path.is_some() {
        return cli_path;
    }

    if let Some(path) = env::var_os("RIA_CONFIG").map(PathBuf::from) {
        return Some(path);
    }

    default_config_path()
}

pub fn load(config_path: Option<&Path>) -> Result<Config> {
    if let Some(path) = config_path {
        return load_from_path(path);
    }

    Ok(Config::default())
}

pub fn load_from_path(path: &Path) -> Result<Config> {
    let contents = std::fs::read_to_string(path)?;
    let config = toml::from_str(&contents)?;
    Ok(config)
}

pub fn save_to_path(config: &Config, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let contents = toml::to_string_pretty(config)
        .map_err(|err| Error::message(format!("failed to serialize config: {err}")))?;
    std::fs::write(path, contents)?;
    Ok(())
}

pub fn default_config_path() -> Option<PathBuf> {
    ProjectDirs::from("org", "archive", "ria")
        .map(|dirs| dirs.config_dir().join("ria.toml"))
}

pub fn config_search_paths(cli_path: Option<&Path>, env_path: Option<&Path>) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(path) = cli_path {
        paths.push(path.to_path_buf());
    }
    if let Some(path) = env_path {
        paths.push(path.to_path_buf());
    }
    if let Some(path) = default_config_path() {
        paths.push(path);
    }
    paths
}

pub fn overrides_from_env() -> ConfigOverrides {
    ConfigOverrides {
        logging_level: env::var("RIA_LOG_LEVEL").ok(),
        logging_filter: env::var("RIA_LOG_FILTER").ok(),
        logging_format: env::var("RIA_LOG_FORMAT").ok(),
        logging_output: env::var("RIA_LOG_OUTPUT").ok(),
        logging_enabled: parse_bool_env("RIA_LOG_ENABLED"),
        logging_ansi: parse_bool_env("RIA_LOG_ANSI"),
        logging_target: parse_bool_env("RIA_LOG_TARGET"),
        logging_thread_ids: parse_bool_env("RIA_LOG_THREAD_IDS"),
        logging_thread_names: parse_bool_env("RIA_LOG_THREAD_NAMES"),
        insecure: parse_bool_env("RIA_INSECURE"),
        host: env::var("RIA_HOST").ok(),
        user_agent_base: env::var("RIA_USER_AGENT_BASE").ok(),
        user_agent_suffix: env::var("RIA_USER_AGENT_SUFFIX").ok(),
        user_agent_opt_out: parse_bool_env("RIA_USER_AGENT_OPT_OUT"),
        output_format: env::var("RIA_OUTPUT").ok(),
        output_color: parse_bool_env("RIA_OUTPUT_COLOR"),
        output_paging: parse_bool_env("RIA_OUTPUT_PAGING"),
        output_quiet: parse_bool_env("RIA_QUIET"),
        output_verbose: parse_bool_env("RIA_VERBOSE"),
        tls_verify: parse_bool_env("RIA_TLS_VERIFY"),
        ca_bundle: env::var("RIA_CA_BUNDLE").ok(),
        api_base: env::var("RIA_API_BASE").ok(),
        s3_base: env::var("RIA_S3_BASE").ok(),
        metadata_base: env::var("RIA_METADATA_BASE").ok(),
        access_key: env::var("RIA_ACCESS_KEY").ok(),
        secret_key: env::var("RIA_SECRET_KEY").ok(),
        input_glob: env::var("RIA_INPUT_GLOB").ok(),
        input_validate_identifiers: parse_bool_env("RIA_VALIDATE_IDENTIFIERS"),
        input_read_stdin: parse_bool_env("RIA_READ_STDIN"),
        transfer_chunk_size_bytes: parse_u64_env("RIA_TRANSFER_CHUNK_SIZE_BYTES"),
        transfer_checksum_verify: parse_bool_env("RIA_TRANSFER_CHECKSUM_VERIFY"),
        transfer_resume: parse_bool_env("RIA_TRANSFER_RESUME"),
        compat_python_user_agent: parse_bool_env("RIA_COMPAT_PYTHON_USER_AGENT"),
        compat_legacy_metadata_format: parse_bool_env("RIA_COMPAT_LEGACY_METADATA_FORMAT"),
        compat_legacy_logging: parse_bool_env("RIA_COMPAT_LEGACY_LOGGING"),
    }
}

fn parse_bool_env(key: &str) -> Option<bool> {
    env::var(key).ok().and_then(|value| parse_bool(&value))
}

fn parse_u64_env(key: &str) -> Option<u64> {
    env::var(key).ok().and_then(|value| value.trim().parse().ok())
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

impl Config {
    pub fn apply_overrides(&mut self, overrides: ConfigOverrides) {
        if let Some(enabled) = overrides.logging_enabled {
            self.logging_mut().enabled = Some(enabled);
        }
        if let Some(level) = overrides.logging_level {
            self.logging_mut().level = Some(level);
        }
        if let Some(filter) = overrides.logging_filter {
            self.logging_mut().filter = Some(filter);
        }
        if let Some(format) = overrides.logging_format {
            self.logging_mut().format = Some(format);
        }
        if let Some(output) = overrides.logging_output {
            self.logging_mut().output = Some(output);
        }
        if let Some(ansi) = overrides.logging_ansi {
            self.logging_mut().ansi = Some(ansi);
        }
        if let Some(target) = overrides.logging_target {
            self.logging_mut().target = Some(target);
        }
        if let Some(thread_ids) = overrides.logging_thread_ids {
            self.logging_mut().thread_ids = Some(thread_ids);
        }
        if let Some(thread_names) = overrides.logging_thread_names {
            self.logging_mut().thread_names = Some(thread_names);
        }

        if let Some(insecure) = overrides.insecure {
            self.general_mut().insecure = Some(insecure);
        }
        if let Some(host) = overrides.host {
            self.general_mut().host = Some(host);
        }
        if let Some(user_agent_base) = overrides.user_agent_base {
            self.general_mut().user_agent_base = Some(user_agent_base);
        }
        if let Some(user_agent_suffix) = overrides.user_agent_suffix {
            self.general_mut().user_agent_suffix = Some(user_agent_suffix);
        }
        if let Some(user_agent_opt_out) = overrides.user_agent_opt_out {
            self.general_mut().user_agent_opt_out = Some(user_agent_opt_out);
        }

        if let Some(output_format) = overrides.output_format {
            self.output_mut().format = Some(output_format);
        }
        if let Some(output_color) = overrides.output_color {
            self.output_mut().color = Some(output_color);
        }
        if let Some(output_paging) = overrides.output_paging {
            self.output_mut().paging = Some(output_paging);
        }
        if let Some(output_quiet) = overrides.output_quiet {
            self.output_mut().quiet = Some(output_quiet);
        }
        if let Some(output_verbose) = overrides.output_verbose {
            self.output_mut().verbose = Some(output_verbose);
        }

        if let Some(tls_verify) = overrides.tls_verify {
            self.tls_mut().verify = Some(tls_verify);
        }
        if let Some(ca_bundle) = overrides.ca_bundle {
            self.tls_mut().ca_bundle = Some(ca_bundle);
        }

        if let Some(api_base) = overrides.api_base {
            self.endpoints_mut().api_base = Some(api_base);
        }
        if let Some(s3_base) = overrides.s3_base {
            self.endpoints_mut().s3_base = Some(s3_base);
        }
        if let Some(metadata_base) = overrides.metadata_base {
            self.endpoints_mut().metadata_base = Some(metadata_base);
        }

        if let Some(access_key) = overrides.access_key {
            self.auth_mut().access_key = Some(access_key);
        }
        if let Some(secret_key) = overrides.secret_key {
            self.auth_mut().secret_key = Some(secret_key);
        }

        if let Some(input_glob) = overrides.input_glob {
            self.input_mut().glob = Some(input_glob);
        }
        if let Some(input_validate_identifiers) = overrides.input_validate_identifiers {
            self.input_mut().validate_identifiers = Some(input_validate_identifiers);
        }
        if let Some(input_read_stdin) = overrides.input_read_stdin {
            self.input_mut().read_stdin = Some(input_read_stdin);
        }

        if let Some(chunk_size_bytes) = overrides.transfer_chunk_size_bytes {
            self.file_transfer_mut().chunk_size_bytes = Some(chunk_size_bytes);
        }
        if let Some(checksum_verify) = overrides.transfer_checksum_verify {
            self.file_transfer_mut().checksum_verify = Some(checksum_verify);
        }
        if let Some(resume) = overrides.transfer_resume {
            self.file_transfer_mut().resume = Some(resume);
        }

        if let Some(python_user_agent) = overrides.compat_python_user_agent {
            self.compatibility_mut().python_user_agent = Some(python_user_agent);
        }
        if let Some(legacy_metadata_format) = overrides.compat_legacy_metadata_format {
            self.compatibility_mut().legacy_metadata_format = Some(legacy_metadata_format);
        }
        if let Some(legacy_logging) = overrides.compat_legacy_logging {
            self.compatibility_mut().legacy_logging = Some(legacy_logging);
        }
    }

    fn logging_mut(&mut self) -> &mut LoggingConfig {
        self.logging.get_or_insert_with(LoggingConfig::default)
    }

    fn general_mut(&mut self) -> &mut GeneralConfig {
        self.general.get_or_insert_with(GeneralConfig::default)
    }

    fn output_mut(&mut self) -> &mut OutputConfig {
        self.output.get_or_insert_with(OutputConfig::default)
    }

    fn input_mut(&mut self) -> &mut InputConfig {
        self.input.get_or_insert_with(InputConfig::default)
    }

    fn tls_mut(&mut self) -> &mut TlsConfig {
        self.tls.get_or_insert_with(TlsConfig::default)
    }

    fn endpoints_mut(&mut self) -> &mut EndpointsConfig {
        self.endpoints.get_or_insert_with(EndpointsConfig::default)
    }

    fn auth_mut(&mut self) -> &mut AuthConfig {
        self.auth.get_or_insert_with(AuthConfig::default)
    }

    fn file_transfer_mut(&mut self) -> &mut FileTransferConfig {
        self.file_transfer.get_or_insert_with(FileTransferConfig::default)
    }

    fn compatibility_mut(&mut self) -> &mut CompatibilityConfig {
        self.compatibility
            .get_or_insert_with(CompatibilityConfig::default)
    }
}

pub fn validate(config: &Config) -> Result<()> {
    if let Some(network) = &config.network {
        if let Some(timeout) = network.timeout_secs {
            if timeout == 0 {
                return Err(Error::message(
                    "network.timeout_secs must be greater than zero",
                ));
            }
        }
        if let Some(connect) = network.connect_timeout_secs {
            if connect == 0 {
                return Err(Error::message(
                    "network.connect_timeout_secs must be greater than zero",
                ));
            }
        }
        if let Some(retry_max) = network.retry_max {
            if retry_max == 0 {
                return Err(Error::message("network.retry_max must be greater than zero"));
            }
        }
        if let Some(rate_limit) = network.rate_limit_per_sec {
            if rate_limit == 0 {
                return Err(Error::message(
                    "network.rate_limit_per_sec must be greater than zero",
                ));
            }
        }
        if let Some(concurrency) = network.concurrency {
            if concurrency == 0 {
                return Err(Error::message(
                    "network.concurrency must be greater than zero",
                ));
            }
        }
    }

    if let Some(output) = &config.output {
        if let Some(format) = output.format.as_deref() {
            if crate::output::OutputFormat::parse(format).is_none() {
                return Err(Error::message(format!("unknown output format: {format}")));
            }
        }
        if output.quiet == Some(true) && output.verbose == Some(true) {
            return Err(Error::message(
                "output.quiet and output.verbose cannot both be true",
            ));
        }
    }

    if let Some(logging) = &config.logging {
        if let Some(format) = logging.format.as_deref() {
            if !matches!(format, "pretty" | "json") {
                return Err(Error::message(format!(
                    "unknown logging.format value: {format}"
                )));
            }
        }
        if let Some(output) = logging.output.as_deref() {
            if !matches!(output, "stdout" | "stderr") {
                return Err(Error::message(format!(
                    "unknown logging.output value: {output}"
                )));
            }
        }
    }

    if let Some(endpoints) = &config.endpoints {
        validate_url("endpoints.api_base", endpoints.api_base.as_deref())?;
        validate_url("endpoints.s3_base", endpoints.s3_base.as_deref())?;
        validate_url("endpoints.metadata_base", endpoints.metadata_base.as_deref())?;
    }

    if let Some(general) = &config.general {
        validate_url("general.host", general.host.as_deref())?;
    }

    if let Some(tls) = &config.tls {
        if let Some(path) = tls.ca_bundle.as_deref() {
            if !Path::new(path).exists() {
                return Err(Error::message(format!("tls.ca_bundle not found: {path}")));
            }
        }
    }

    if let Some(input) = &config.input {
        if let Some(pattern) = input.glob.as_deref() {
            Glob::new(pattern)
                .map_err(|err| Error::message(format!("invalid input.glob: {err}")))?;
        }
    }

    if let Some(transfer) = &config.file_transfer {
        if let Some(chunk_size) = transfer.chunk_size_bytes {
            if chunk_size == 0 {
                return Err(Error::message(
                    "file_transfer.chunk_size_bytes must be greater than zero",
                ));
            }
        }
    }

    Ok(())
}

fn validate_url(field: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        url::Url::parse(value)
            .map(|_| ())
            .map_err(|err| Error::message(format!("{field} is not a valid URL: {err}")))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn resolves_env_path_first() {
        let temp = NamedTempFile::new().expect("tempfile");
        env::set_var("RIA_CONFIG", temp.path());
        let resolved = resolve_config_path(None);
        env::remove_var("RIA_CONFIG");
        assert_eq!(resolved.as_deref(), Some(temp.path()));
    }

    #[test]
    fn loads_toml_config() {
        let mut file = NamedTempFile::new().expect("tempfile");
        std::io::Write::write_all(
            &mut file,
            br#"
                [logging]
                level = "info"
            "#,
        )
        .expect("write");
        let config = load_from_path(file.path()).expect("load");
        assert_eq!(
            config
                .logging
                .as_ref()
                .and_then(|logging| logging.level.as_deref()),
            Some("info")
        );
    }

    #[test]
    fn saves_and_loads_config() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("ria.toml");
        let config = Config {
            auth: Some(AuthConfig {
                access_key: Some("access".to_string()),
                secret_key: Some("secret".to_string()),
            }),
            ..Config::default()
        };
        save_to_path(&config, &path).expect("save");
        let loaded = load_from_path(&path).expect("load");
        assert_eq!(
            loaded
                .auth
                .as_ref()
                .and_then(|auth| auth.access_key.as_deref()),
            Some("access")
        );
    }

    #[test]
    fn resolves_config_destination_without_existing_file() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("ria.toml");
        let resolved = resolve_config_destination(Some(path.clone()));
        assert_eq!(resolved.as_deref(), Some(path.as_path()));
    }

    #[test]
    fn rejects_unknown_output_format() {
        let config = Config {
            output: Some(OutputConfig {
                format: Some("wat".to_string()),
                ..OutputConfig::default()
            }),
            ..Config::default()
        };
        let result = validate(&config);
        assert!(result.is_err());
    }

    #[test]
    fn rejects_invalid_endpoint_url() {
        let config = Config {
            endpoints: Some(EndpointsConfig {
                api_base: Some("not-a-url".to_string()),
                ..EndpointsConfig::default()
            }),
            ..Config::default()
        };
        let result = validate(&config);
        assert!(result.is_err());
    }

    #[test]
    fn applies_overrides() {
        let mut config = Config::default();
        let overrides = ConfigOverrides {
            output_format: Some("json".to_string()),
            ..ConfigOverrides::default()
        };
        config.apply_overrides(overrides);
        assert_eq!(
            config
                .output
                .as_ref()
                .and_then(|output| output.format.as_deref()),
            Some("json")
        );
    }

    #[test]
    fn rejects_quiet_and_verbose() {
        let config = Config {
            output: Some(OutputConfig {
                quiet: Some(true),
                verbose: Some(true),
                ..OutputConfig::default()
            }),
            ..Config::default()
        };
        let result = validate(&config);
        assert!(result.is_err());
    }
}
