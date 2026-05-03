# ria

`ria` is a Rust command-line interface to Archive.org.

## Intent

Port or reimagine Archive.org CLI workflows in Rust while preserving important config, logging, transfer, and compatibility behaviors.

## Ambition

The domain modules, parity docs, release checklist, and command surface all suggest an ambition to become a trustworthy Archive.org CLI with strong operational parity to established tooling.

## Current Status

Search, list, metadata, transfer, and related command groups are already present, along with config discovery, output policy, telemetry, and tests. The project looks early-version but real.

## Core Capabilities Or Focus Areas

- Archive.org-oriented command surface including search, list, metadata, upload, download, and related operations.
- Config file discovery plus environment and CLI overrides.
- Structured output and telemetry controls.
- Domain-split implementation across CLI, config, HTTP, output, and utility modules.
- Roadmap and parity docs that guide the port.

## Project Layout

- `docs/`: project documentation, reference material, and roadmap notes.
- `src/`: Rust source for the main crate or application entrypoint.
- `Cargo.toml`: crate or workspace manifest and the first place to check for package structure.

## Setup And Requirements

- Rust toolchain.
- Network access to Archive.org endpoints.
- Credentials/config for authenticated operations when needed.

## Build / Run / Test Commands

```bash
cargo build
cargo test
cargo run -- --help
```

## Notes, Limitations, Or Known Gaps

- Compatibility and operational parity are central concerns, not afterthoughts.
- Because this is an API-facing CLI, remote behavior can change independently of the codebase.

## Next Steps Or Roadmap Hints

- Expand parity validation against the incumbent workflow as edge cases emerge.
- Document any intentional behavior differences from the upstream CLI once they are stable.
