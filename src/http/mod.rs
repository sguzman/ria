use crate::config::Config;

#[derive(Debug, Default, Clone)]
pub struct HttpClientConfig {
    pub base_url: Option<String>,
    pub insecure: bool,
    pub timeout_secs: Option<u64>,
    pub connect_timeout_secs: Option<u64>,
    pub retry_max: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
    pub rate_limit_per_sec: Option<u32>,
    pub concurrency: Option<u32>,
    pub user_agent_suffix: Option<String>,
}

#[derive(Debug, Default)]
pub struct HttpClient {
    pub config: HttpClientConfig,
}

impl HttpClient {
    pub fn new(config: HttpClientConfig) -> Self {
        Self { config }
    }
}

pub fn config_from_settings(settings: &Config) -> HttpClientConfig {
    let general = settings.general.as_ref();
    let network = settings.network.as_ref();

    HttpClientConfig {
        base_url: general.and_then(|config| config.host.clone()),
        insecure: general
            .and_then(|config| config.insecure)
            .unwrap_or(false),
        timeout_secs: network.and_then(|config| config.timeout_secs),
        connect_timeout_secs: network.and_then(|config| config.connect_timeout_secs),
        retry_max: network.and_then(|config| config.retry_max),
        retry_backoff_ms: network.and_then(|config| config.retry_backoff_ms),
        rate_limit_per_sec: network.and_then(|config| config.rate_limit_per_sec),
        concurrency: network.and_then(|config| config.concurrency),
        user_agent_suffix: general.and_then(|config| config.user_agent_suffix.clone()),
    }
}
