use directories::ProjectDirs;
use serde::Deserialize;
use std::env;
use std::path::{Path, PathBuf};

use crate::errors::Result;

#[derive(Debug, Default, Clone, Deserialize)]
pub struct Config {
    pub logging: Option<LoggingConfig>,
    pub general: Option<GeneralConfig>,
}

#[derive(Debug, Default, Clone, Deserialize)]
pub struct LoggingConfig {
    pub level: Option<String>,
}

#[derive(Debug, Default, Clone, Deserialize)]
pub struct GeneralConfig {
    pub insecure: Option<bool>,
    pub host: Option<String>,
    pub user_agent_suffix: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct ConfigOverrides {
    pub logging_level: Option<String>,
    pub insecure: Option<bool>,
    pub host: Option<String>,
    pub user_agent_suffix: Option<String>,
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

impl Config {
    pub fn apply_overrides(&mut self, overrides: ConfigOverrides) {
        if let Some(level) = overrides.logging_level {
            self.logging_mut().level = Some(level);
        }

        if let Some(insecure) = overrides.insecure {
            self.general_mut().insecure = Some(insecure);
        }

        if let Some(host) = overrides.host {
            self.general_mut().host = Some(host);
        }

        if let Some(user_agent_suffix) = overrides.user_agent_suffix {
            self.general_mut().user_agent_suffix = Some(user_agent_suffix);
        }
    }

    fn logging_mut(&mut self) -> &mut LoggingConfig {
        self.logging.get_or_insert_with(LoggingConfig::default)
    }

    fn general_mut(&mut self) -> &mut GeneralConfig {
        self.general.get_or_insert_with(GeneralConfig::default)
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
}
