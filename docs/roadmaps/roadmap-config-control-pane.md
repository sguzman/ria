# Roadmap: TOML Config Control Pane

- [ ] Define config file name(s) and discovery order (CLI flag, env var, default paths).
- [ ] Specify a full TOML schema covering all CLI flags and defaults.
- [ ] Add logging policies: level, format, sinks, sampling, and per-module overrides.
- [ ] Add network policies: timeouts, retries, backoff strategy, rate limits, concurrency.
- [ ] Add TLS/security policies: verify certs, custom CA bundle, insecure toggle.
- [ ] Add host and endpoint overrides for all services (API, S3, metadata).
- [ ] Add user-agent policy: base string, suffix, and opt-out.
- [ ] Add auth policies: key source order, profile selection, token caching.
- [ ] Add file transfer policies: chunk size, checksum verification, resume behavior.
- [ ] Add output policies: format default, paging, color, quiet/verbose switches.
- [ ] Add input policies: glob rules, identifier validation modes, stdin handling.
- [ ] Add telemetry policies: tracing enablement, filters, and export targets.
- [ ] Add compatibility policies: Python parity toggles and legacy behavior switches.
- [ ] Document every field with examples and defaults.
- [ ] Add config validation with actionable error messages.
- [ ] Create a sample `ria.toml` for end users.
