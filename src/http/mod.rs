use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use reqwest::blocking::{Client, ClientBuilder, Response};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT};
use reqwest::Certificate;
use serde_json::Value;
use tracing::{debug, info, instrument, warn};

use crate::config::Config;
use crate::errors::{Error, Result};

const DEFAULT_API_BASE: &str = "https://archive.org";
const DEFAULT_S3_BASE: &str = "https://s3.us.archive.org";
const DEFAULT_METADATA_BASE: &str = "https://archive.org/metadata";
const DEFAULT_RETRY_BACKOFF_MS: u64 = 250;

#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    pub api_base: String,
    pub s3_base: String,
    pub metadata_base: String,
    pub insecure: bool,
    pub tls_verify: bool,
    pub ca_bundle: Option<String>,
    pub timeout: Option<Duration>,
    pub connect_timeout: Option<Duration>,
    pub retry_max: u32,
    pub retry_backoff_ms: u64,
    pub rate_limit_per_sec: Option<u32>,
    pub concurrency: Option<u32>,
    pub user_agent: Option<String>,
}

#[derive(Debug)]
pub struct HttpClient {
    client: Client,
    config: HttpClientConfig,
    rate_limiter: RateLimiter,
    concurrency: ConcurrencyLimiter,
}

impl HttpClient {
    pub fn new(config: HttpClientConfig) -> Result<Self> {
        let client = build_client(&config)?;
        let rate_limiter = RateLimiter::new(config.rate_limit_per_sec);
        let concurrency = ConcurrencyLimiter::new(config.concurrency);
        Ok(Self {
            client,
            config,
            rate_limiter,
            concurrency,
        })
    }

    #[instrument(skip(self))]
    pub fn get(&self, url: &str) -> Result<Response> {
        let _permit = self.concurrency.acquire();
        self.rate_limiter.throttle();
        self.send_with_retry(|| self.client.get(url).send(), url)
    }

    pub fn get_text(&self, url: &str) -> Result<String> {
        let response = self.get(url)?;
        let status = response.status();
        let text = response
            .text()
            .map_err(|err| Error::message(format!("failed to read response body: {err}")))?;
        if !status.is_success() {
            return Err(Error::message(format!(
                "request failed with status {}: {}",
                status.as_u16(),
                truncate_body(&text)
            )));
        }
        Ok(text)
    }

    pub fn get_json(&self, url: &str) -> Result<Value> {
        let text = self.get_text(url)?;
        serde_json::from_str(&text)
            .map_err(|err| Error::message(format!("failed to parse JSON: {err}")))
    }

    pub fn api_base(&self) -> &str {
        &self.config.api_base
    }

    pub fn metadata_base(&self) -> &str {
        &self.config.metadata_base
    }

    fn send_with_retry<F>(&self, mut send: F, url: &str) -> Result<Response>
    where
        F: FnMut() -> reqwest::Result<Response>,
    {
        let attempts = self.config.retry_max.max(1);
        for attempt in 1..=attempts {
            let started = Instant::now();
            match send() {
                Ok(response) => {
                    if should_retry_status(response.status().as_u16()) && attempt < attempts {
                        warn!(
                            %url,
                            status = response.status().as_u16(),
                            attempt,
                            "retrying request after status"
                        );
                        self.backoff(attempt);
                        continue;
                    }
                    debug!(
                        %url,
                        status = response.status().as_u16(),
                        elapsed_ms = started.elapsed().as_millis(),
                        "request completed"
                    );
                    return Ok(response);
                }
                Err(err) => {
                    warn!(%url, attempt, error = %err, "request error");
                    if attempt >= attempts {
                        return Err(Error::message(format!(
                            "request failed after {attempts} attempts: {err}"
                        )));
                    }
                    self.backoff(attempt);
                }
            }
        }

        Err(Error::message("request retry loop exited unexpectedly"))
    }

    fn backoff(&self, attempt: u32) {
        let delay_ms = self
            .config
            .retry_backoff_ms
            .saturating_mul(attempt as u64)
            .max(DEFAULT_RETRY_BACKOFF_MS);
        std::thread::sleep(Duration::from_millis(delay_ms));
    }
}

pub fn config_from_settings(settings: &Config) -> HttpClientConfig {
    let general = settings.general.as_ref();
    let network = settings.network.as_ref();
    let endpoints = settings.endpoints.as_ref();
    let tls = settings.tls.as_ref();
    let auth = settings.auth.as_ref();
    let compat = settings.compatibility.as_ref();

    let api_base = endpoints
        .and_then(|config| config.api_base.clone())
        .or_else(|| general.and_then(|config| config.host.clone()))
        .unwrap_or_else(|| DEFAULT_API_BASE.to_string());
    let s3_base = endpoints
        .and_then(|config| config.s3_base.clone())
        .unwrap_or_else(|| DEFAULT_S3_BASE.to_string());
    let metadata_base = endpoints
        .and_then(|config| config.metadata_base.clone())
        .unwrap_or_else(|| DEFAULT_METADATA_BASE.to_string());

    let timeout = network
        .and_then(|config| config.timeout_secs)
        .map(Duration::from_secs);
    let connect_timeout = network
        .and_then(|config| config.connect_timeout_secs)
        .map(Duration::from_secs);

    let retry_max = network.and_then(|config| config.retry_max).unwrap_or(3);
    let retry_backoff_ms = network
        .and_then(|config| config.retry_backoff_ms)
        .unwrap_or(DEFAULT_RETRY_BACKOFF_MS);

    let user_agent = build_user_agent(general, auth, compat);

    HttpClientConfig {
        api_base,
        s3_base,
        metadata_base,
        insecure: general
            .and_then(|config| config.insecure)
            .unwrap_or(false),
        tls_verify: tls.and_then(|config| config.verify).unwrap_or(true),
        ca_bundle: tls.and_then(|config| config.ca_bundle.clone()),
        timeout,
        connect_timeout,
        retry_max,
        retry_backoff_ms,
        rate_limit_per_sec: network.and_then(|config| config.rate_limit_per_sec),
        concurrency: network.and_then(|config| config.concurrency),
        user_agent,
    }
}

fn build_user_agent(
    general: Option<&crate::config::GeneralConfig>,
    auth: Option<&crate::config::AuthConfig>,
    compat: Option<&crate::config::CompatibilityConfig>,
) -> Option<String> {
    let general = general?;
    if general.user_agent_opt_out.unwrap_or(false) {
        return None;
    }

    let base = general.user_agent_base.clone().unwrap_or_else(|| default_user_agent(auth, compat));

    if let Some(suffix) = general.user_agent_suffix.as_deref() {
        Some(format!("{base} {suffix}"))
    } else {
        Some(base)
    }
}

fn default_user_agent(
    auth: Option<&crate::config::AuthConfig>,
    compat: Option<&crate::config::CompatibilityConfig>,
) -> String {
    let version = env!("CARGO_PKG_VERSION");
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let lang = std::env::var("LANG")
        .ok()
        .and_then(|value| value.split('.').next().map(str::to_string))
        .unwrap_or_else(|| "C".to_string());
    let access_key = auth
        .and_then(|config| config.access_key.as_deref())
        .unwrap_or("anonymous");

    if compat.and_then(|config| config.python_user_agent) == Some(true) {
        format!(
            "ria/{version} ({os} {arch}; N; {lang}; {access_key})"
        )
    } else {
        format!("ria/{version} ({os} {arch})")
    }
}

fn build_client(config: &HttpClientConfig) -> Result<Client> {
    let mut builder = ClientBuilder::new();
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("*/*"));

    if let Some(timeout) = config.timeout {
        builder = builder.timeout(timeout);
    }
    if let Some(timeout) = config.connect_timeout {
        builder = builder.connect_timeout(timeout);
    }
    if let Some(user_agent) = &config.user_agent {
        if let Ok(header) = HeaderValue::from_str(user_agent) {
            headers.insert(reqwest::header::USER_AGENT, header);
        }
    }
    builder = builder.default_headers(headers);

    let accept_invalid = config.insecure || !config.tls_verify;
    if accept_invalid {
        builder = builder.danger_accept_invalid_certs(true);
        builder = builder.danger_accept_invalid_hostnames(true);
    }

    if let Some(path) = &config.ca_bundle {
        let pem = std::fs::read(path)?;
        let cert = Certificate::from_pem(&pem)
            .map_err(|err| Error::message(format!("invalid CA bundle: {err}")))?;
        builder = builder.add_root_certificate(cert);
    }

    let client = builder
        .build()
        .map_err(|err| Error::message(format!("failed to build HTTP client: {err}")))?;

    info!(
        api_base = %config.api_base,
        s3_base = %config.s3_base,
        metadata_base = %config.metadata_base,
        insecure = config.insecure,
        tls_verify = config.tls_verify,
        "http client configured"
    );

    Ok(client)
}

fn should_retry_status(status: u16) -> bool {
    status == 429 || (500..=599).contains(&status)
}

fn truncate_body(body: &str) -> String {
    const LIMIT: usize = 240;
    if body.len() <= LIMIT {
        body.to_string()
    } else {
        format!("{}...", &body[..LIMIT])
    }
}

#[derive(Debug)]
struct RateLimiter {
    min_interval: Option<Duration>,
    last_request: Mutex<Instant>,
}

impl RateLimiter {
    fn new(rate_limit_per_sec: Option<u32>) -> Self {
        let min_interval = rate_limit_per_sec
            .and_then(|rate| if rate > 0 { Some(rate) } else { None })
            .map(|rate| Duration::from_secs_f64(1.0 / rate as f64));

        Self {
            min_interval,
            last_request: Mutex::new(Instant::now()),
        }
    }

    fn throttle(&self) {
        let Some(interval) = self.min_interval else {
            return;
        };
        let mut last_request = self
            .last_request
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let elapsed = last_request.elapsed();
        if elapsed < interval {
            std::thread::sleep(interval - elapsed);
        }
        *last_request = Instant::now();
    }
}

#[derive(Debug, Clone)]
struct ConcurrencyLimiter {
    limit: Option<usize>,
    state: Arc<(Mutex<usize>, Condvar)>,
}

impl ConcurrencyLimiter {
    fn new(limit: Option<u32>) -> Self {
        Self {
            limit: limit.map(|value| value as usize),
            state: Arc::new((Mutex::new(0), Condvar::new())),
        }
    }

    fn acquire(&self) -> Permit {
        let Some(limit) = self.limit else {
            return Permit { state: None };
        };

        let (lock, cvar) = &*self.state;
        let mut active = lock.lock().unwrap_or_else(|err| err.into_inner());
        while *active >= limit {
            active = cvar.wait(active).unwrap_or_else(|err| err.into_inner());
        }
        *active += 1;
        Permit {
            state: Some(self.state.clone()),
        }
    }
}

#[derive(Debug)]
struct Permit {
    state: Option<Arc<(Mutex<usize>, Condvar)>>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        let (lock, cvar) = &*state;
        let mut active = lock.lock().unwrap_or_else(|err| err.into_inner());
        if *active > 0 {
            *active -= 1;
        }
        cvar.notify_one();
    }
}
