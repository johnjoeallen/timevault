#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$repo_root/Cargo.toml"
cli_mod="$repo_root/src/cli/mod.rs"

current_revision="$(
  sed -n 's/^[[:space:]]*revision = "\([0-9][0-9]*\)"[[:space:]]*$/\1/p' "$manifest"
)"
current_build="$(
  sed -n 's/^pub(crate) const BUILD_NUMBER: u32 = \([0-9][0-9]*\);$/\1/p' "$cli_mod"
)"

if [[ -z "$current_revision" ]]; then
  echo "could not find package.metadata.deb revision in $manifest" >&2
  exit 1
fi

if [[ -z "$current_build" ]]; then
  echo "could not find BUILD_NUMBER in $cli_mod" >&2
  exit 1
fi

if [[ "$current_revision" != "$current_build" ]]; then
  echo "revision mismatch: Cargo.toml has $current_revision, BUILD_NUMBER has $current_build" >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required" >&2
  exit 1
fi

if ! command -v git >/dev/null 2>&1; then
  echo "git is required" >&2
  exit 1
fi

if ! cargo deb --version >/dev/null 2>&1; then
  echo "cargo-deb is required; install it with: cargo install cargo-deb" >&2
  exit 1
fi

if ! git -C "$repo_root" diff --quiet -- "$manifest" "$cli_mod" \
  || ! git -C "$repo_root" diff --cached --quiet -- "$manifest" "$cli_mod"; then
  echo "refusing to auto-commit build number bump because Cargo.toml or src/cli/mod.rs is dirty" >&2
  exit 1
fi

cargo build --release
cargo deb

echo "built package:"
ls -1 "$repo_root"/target/debian/*.deb

# Semantic version changes are made with source changes. Packaging builds only
# advance the build number, which is mirrored into the Debian revision.
next_build=$((current_build + 1))

perl -0pi -e "s/revision = \"\\Q$current_revision\\E\"/revision = \"$next_build\"/" "$manifest"
perl -0pi -e "s/pub\\(crate\\) const BUILD_NUMBER: u32 = \\Q$current_build\\E;/pub(crate) const BUILD_NUMBER: u32 = $next_build;/" "$cli_mod"

echo "bumped build number: $current_build -> $next_build"

git -C "$repo_root" add "$manifest" "$cli_mod"
git -C "$repo_root" commit -m "Bump build number to $next_build"
