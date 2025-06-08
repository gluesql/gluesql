#!/usr/bin/env bash
set -euo pipefail

packages=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[].name' | grep -v -E '^(gluesql-js|gluesql-py|gluesql-test-suite)$')
mkdir -p coverage/tmp
export RUSTFLAGS="-Cinstrument-coverage"
export LLVM_PROFILE_FILE="gluesql-%p-%m.profraw"

for pkg in $packages; do
  echo "::group::Running tests for $pkg"
  GIT_REMOTE=${GIT_REMOTE:-} cargo test -p "$pkg" --all-features --verbose
  grcov . \
    --binary-path ./target/debug/ \
    -s . \
    -t lcov \
    --branch \
    --ignore-not-existing \
    --ignore "/*" \
    --ignore "pkg/rust/examples/*.rs" \
    --ignore "cli/src/{cli,helper,lib,main}.rs" \
    --excl-line "(#\\[derive\\()|(^\\s*.await[;,]?$)|(^test_case\\!\\([\\d\\w]+,)" \
    -o coverage/tmp/"$pkg".info
  find . -name '*.profraw' -delete
  echo "::endgroup::"
done

cat coverage/tmp/*.info > coverage/lcov.info
