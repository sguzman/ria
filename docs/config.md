# Configuration

## Discovery Order

The configuration file is loaded in this order:

1. `--config-file` CLI flag
2. `RIA_CONFIG` environment variable
3. Platform config directory via `directories` (`org/archive/ria/ria.toml`)

## Environment Overrides

Environment variables override values from the config file. CLI flags override both.

- `RIA_CONFIG`: Path to config file
- `RIA_LOG_LEVEL`: Logging level (e.g. `info`, `debug`)
- `RIA_LOG_FILTER`: Tracing filter string
- `RIA_LOG_FORMAT`: `pretty` or `json`
- `RIA_LOG_OUTPUT`: `stdout` or `stderr`
- `RIA_LOG_ENABLED`: `true` or `false`
- `RIA_LOG_ANSI`: `true` or `false`
- `RIA_LOG_TARGET`: `true` or `false`
- `RIA_LOG_THREAD_IDS`: `true` or `false`
- `RIA_LOG_THREAD_NAMES`: `true` or `false`
- `RIA_HOST`: Base host override
- `RIA_INSECURE`: `true` or `false`
- `RIA_USER_AGENT_BASE`: Base User-Agent string
- `RIA_USER_AGENT_SUFFIX`: User-Agent suffix
- `RIA_USER_AGENT_OPT_OUT`: `true` or `false`
- `RIA_OUTPUT`: Output format (`human`, `json`, `raw`)
- `RIA_OUTPUT_COLOR`: `true` or `false`
- `RIA_OUTPUT_PAGING`: `true` or `false`
- `RIA_QUIET`: `true` or `false`
- `RIA_VERBOSE`: `true` or `false`
- `RIA_TLS_VERIFY`: `true` or `false`
- `RIA_CA_BUNDLE`: Path to CA bundle
- `RIA_API_BASE`: API base URL
- `RIA_S3_BASE`: S3 base URL
- `RIA_METADATA_BASE`: Metadata base URL
- `RIA_ACCESS_KEY`: Access key
- `RIA_SECRET_KEY`: Secret key
- `RIA_INPUT_GLOB`: Default glob pattern
- `RIA_VALIDATE_IDENTIFIERS`: `true` or `false`
- `RIA_READ_STDIN`: `true` or `false`
- `RIA_TRANSFER_CHUNK_SIZE_BYTES`: Upload chunk size in bytes
- `RIA_TRANSFER_CHECKSUM_VERIFY`: `true` or `false`
- `RIA_TRANSFER_RESUME`: `true` or `false`
- `RIA_COMPAT_PYTHON_USER_AGENT`: `true` or `false`
- `RIA_COMPAT_LEGACY_METADATA_FORMAT`: `true` or `false`
- `RIA_COMPAT_LEGACY_LOGGING`: `true` or `false`
