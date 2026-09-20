# Contributing to GlueSQL

Before opening an issue, check whether an existing issue already covers the same problem or proposal.

## Getting Started

Clone your fork of the repository. The Rust version and required `rustfmt` and `clippy` components are specified in [`rust-toolchain.toml`](rust-toolchain.toml) and installed automatically by rustup.

Run the `gluesql-core` tests to verify your development environment.

```sh
cargo test -p gluesql-core
```

## Repository Structure

| Path | Purpose |
| --- | --- |
| `core/` | Query planning, execution, data types, storage traits, and Query Builder |
| `macros/` | Procedural macros |
| `storages/` | Reference storage implementations |
| `test-suite/` | Shared SQL fixtures and storage conformance tests |
| `pkg/rust/` | Public `gluesql` crate and examples |
| `cli/` | Command-line interface |
| `docs/` | Documentation website |

## Making Code Changes

Keep each change focused and add a test that covers changed behavior. SQL behavior is primarily tested through the fixtures in `test-suite/fixtures/`; storage-specific behavior belongs in the affected storage crate.

Run the tests for each affected package.

```sh
cargo test -p PACKAGE_NAME
```

MongoDB, Redis, and remote Git storage tests require their corresponding external services. See [`.github/workflows/rust.yml`](.github/workflows/rust.yml) for their CI setup and test commands.

### Required Checks

After running the relevant tests, run:

```sh
cargo clippy --all-targets -- -D warnings
cargo fmt --all
```

Open a pull request only after the relevant tests and required checks succeed.

## Making Documentation Changes

When code changes affect user-facing behavior, update the relevant documentation, including `README.md` and `CONTRIBUTING.md` when applicable. Links to published documentation include version information, so ensure that each link points to the correct version.

## Pull Requests

- Explain the problem and the behavior changed by the pull request.
- Include tests for behavior changes.
- Update documentation or examples when user-facing behavior changes.
- Keep unrelated changes out of the pull request.
