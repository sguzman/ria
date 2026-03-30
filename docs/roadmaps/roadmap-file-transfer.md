# Roadmap: File Transfer + Management

- [ ] Implement `ia upload` (file selection, metadata sidecar, checksum handling).
- [ ] Implement `ia download` (file selection, output dir, resume/retry).
- [ ] Implement `ia delete` (delete by filename/glob, dry-run).
- [ ] Implement `ia copy` (server-side copy with metadata updates).
- [ ] Implement `ia move` (copy + delete semantics and safety checks).
- [ ] Implement rate limiting and retry/backoff strategies.
- [ ] Add `tracing` spans for per-file progress and retries.
- [ ] Add tests for path validation and selection rules.
