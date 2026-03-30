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

    env::var_os("RIA_CONFIG").map(PathBuf::from)
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
