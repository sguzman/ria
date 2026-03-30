#[derive(Debug, Default)]
pub struct Config {
    pub logging_level: Option<String>,
}

pub fn load() -> Config {
    // Stub: load config from file/env/cli overrides.
    Config::default()
}
