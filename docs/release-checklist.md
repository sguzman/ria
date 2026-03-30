# Release Checklist

- [x] Confirm roadmaps show 100% completion.
- [x] Run `cargo build` and ensure no warnings.
- [x] Run `cargo test` for core suites if available.
- [ ] Verify config example in `docs/ria.toml` matches current schema.
- [x] Smoke test CLI: `ria --help`.
- [x] Smoke test CLI: `ria --version`.
- [ ] Smoke test CLI: `ria search "collection:test" --rows 1 --page 1`.
- [ ] Smoke test CLI: `ria list <identifier>`.
- [ ] Confirm upload/delete/copy/move require auth and fail gracefully without it.
- [ ] Check that metadata updates work in `--dry-run` mode.
- [x] Review `docs/parity.md` for any outdated notes.
- [ ] Tag release notes with date and version.
