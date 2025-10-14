# AGENTS.md
- After editing code, run `cargo clippy --all-targets -- -D warnings`.
- Then run `cargo fmt --all`.
- When importing items, group them under a single `use` statement whenever possible.
- Run tests related to your changes when available; running the entire test suite is not required.
- Commit only when the above steps succeed.
- Branch names may contain only lowercase a-z, dashes (-), and slashes (/).

## Personal Instructions
- Store collaborator-specific guidance in `AGENTS.local.md`. This file is gitignored and should remain local to each contributor.
