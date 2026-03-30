# Roadmap: Core CLI + Config + Logging

- [x] Define CLI structure with `clap` (subcommands, global flags, help/usage parity).
- [x] Implement global flags: `--version`, `--config-file`, `--log`, `--debug`, `--insecure`, `--host`, `--user-agent-suffix`.
- [x] Create config loader (TOML/INI parity with Python) and merge with CLI overrides.
- [ ] Build HTTP client layer with defaults (base URLs, user-agent, TLS, timeouts).
- [x] Add extensive `tracing` instrumentation across CLI entry and config load.
- [ ] Add `tracing` instrumentation across HTTP requests.
- [x] Implement structured error types and user-friendly exit codes.
- [x] Add `--help` and subcommand help text aligned with Python docs.
- [ ] Add minimal smoke tests for CLI parsing and config merging.
