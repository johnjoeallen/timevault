#!/bin/sh
# Fail if a command-line option shown by `timevault --help` is not mentioned in
# USER_GUIDE.md, so the user guide cannot silently fall behind the CLI.
set -eu

cd "$(dirname "$0")/.."
bin=${TIMEVAULT_BIN:-}
if [ -n "$bin" ]; then
  help_out=$("$bin" --help 2>/dev/null || true)
else
  help_out=$(cargo run -q --locked -- --help 2>/dev/null || true)
fi

flags=$(printf '%s\n' "$help_out" | grep -oE '(^|[[:space:]])--[a-z][a-z-]*' | tr -d ' \t' | sort -u)
[ -n "$flags" ] || { echo "no options found in --help output" >&2; exit 1; }

status=0
for flag in $flags; do
  case "$flag" in --help|--version) continue ;; esac
  if ! grep -qF -- "$flag" USER_GUIDE.md; then
    echo "not documented in USER_GUIDE.md: $flag" >&2
    status=1
  fi
done
exit $status
