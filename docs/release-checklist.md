# Release Checklist

1. Confirm roadmaps show 100% completion.
2. Run `cargo build` and ensure no warnings.
3. Run `cargo test` for core suites if available.
4. Verify config example in `docs/ria.toml` matches current schema.
5. Smoke test CLI:
   - `ria --help`
   - `ria search "collection:test" --rows 1 --page 1`
   - `ria list <identifier>`
6. Confirm upload/delete/copy/move require auth and fail gracefully without it.
7. Check that metadata updates work in `--dry-run` mode.
8. Review `docs/parity.md` for any outdated notes.
9. Tag release notes with date and version.
