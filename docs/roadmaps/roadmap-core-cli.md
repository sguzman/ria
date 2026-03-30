# Roadmap: Core CLI + Config + Logging

- [ ] Define CLI structure with `clap` (subcommands, global flags, help/usage parity).
- [ ] Implement global flags: `--version`, `--config-file`, `--log`, `--debug`, `--insecure`, `--host`, `--user-agent-suffix`.
- [ ] Create config loader (TOML/INI parity with Python) and merge with CLI overrides.
- [ ] Build HTTP client layer with defaults (base URLs, user-agent, TLS, timeouts).
- [ ] Add extensive `tracing` instrumentation across CLI entry, config load, and HTTP requests.
- [ ] Implement structured error types and user-friendly exit codes.
- [ ] Add `--help` and subcommand help text aligned with Python docs.
- [ ] Add minimal smoke tests for CLI parsing and config merging.
