# Roadmap: Parity + Quality (Cross-Cutting)

- [x] Define config discovery order (CLI flag, env vars, default paths) and document it.
- [x] Implement environment variable overrides for auth/session and host options.
- [ ] Match Python user-agent construction and request headers.
- [x] Add signal handling parity (SIGINT, SIGPIPE) and consistent exit codes.
- [x] Implement stdin/stdout conventions for piping and streaming.
- [x] Add robust globbing and file/identifier validation utilities.
- [x] Implement retry/backoff, rate limiting, and concurrency controls.
- [ ] Add resumable downloads/uploads with checksum verification.
- [ ] Build golden CLI tests for argument parsing and output formatting.
- [ ] Add API mock fixtures and integration tests for error cases.
- [x] Write parity notes and CLI behavior compatibility docs.
- [x] Add default HTTP request headers (Accept/User-Agent).
