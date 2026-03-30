# Parity Notes

This Rust CLI aims to mirror core behavior from the Python `internetarchive` CLI where feasible.

## Differences

- User-Agent defaults include `ria/<version>` and omit Python runtime version.
- Output formatting defaults to `human` unless overridden.
- Signal handling uses Rust signal hooks for SIGINT/SIGTERM and defaults SIGPIPE.

## Compatibility Toggles Implemented

- `compatibility.python_user_agent`: Emit Python-style User-Agent layout.

## Planned Compatibility Toggles

- `compatibility.legacy_metadata_format`: Preserve legacy metadata formatting when enabled.
- `compatibility.legacy_logging`: Preserve legacy logging behavior when enabled.
