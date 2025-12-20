#!/bin/sh
set -eu

help_out=$(timevault --help 2>/dev/null || true)

must_have() {
  echo "$help_out" | grep -q "$1" || {
    echo "missing in --help: $1" >&2
    exit 1
  }
}

must_have "disk enroll"
must_have "disk discover"
must_have "disk mount"
must_have "disk umount"
must_have "--disk-id"
must_have "--fs-uuid"
must_have "--device"
must_have "--job"
must_have "--dry-run"
must_have "--safe"
must_have "--print-order"
must_have "--rsync"
must_have "mount"
must_have "umount"
must_have "--version"

echo "compat smoke ok"
