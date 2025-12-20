# Compatibility Inventory (legacy main.rs)

This document captures the current TimeVault CLI/config behavior from the existing `src/main.rs` implementation. The rewrite must preserve all behavior unless explicitly noted.

## CLI commands and flags

### Commands
- `timevault` (default) → runs backup (same as `timevault backup`).
- `timevault backup` → runs backup.
- `timevault disk enroll ...` → enrolls a backup disk.
- `timevault disk discover` → lists candidate disks for enrollment.
- `timevault mount [--disk-id <id>]` → mounts an enrolled disk read-only for restore; prints mountpoint.
- `timevault umount` → unmounts the single user mount under `/run/timevault/user-mounts`.

### Flag aliases / modes
- `--disk-enroll` → same as `timevault disk enroll` (subcommand implied).
- `--disk-discover` → same as `timevault disk discover` (subcommand implied).
- `--backup` → no-op alias for backup.
- `--help` / `-h` → prints help (after banner).
- `--version` → prints banner, copyright, project URL, license.
- `--config <path>` → config file path (default `/etc/timevault.yaml`).
- `--job <name>` → select job(s) (repeatable).
- `--print-order` → print selected jobs and exit.
- `--dry-run` → no writes; still mounts; prints commands.
- `--safe` → suppress deletions (no rsync delete flags; no snapshot deletion).
- `--verbose` / `-v` → verbose logging.
- `--rsync <args...>` → remaining args forwarded to rsync.
- `--disk-id <id>` → selects enrolled disk for backup/mount; sets disk-id during enroll.

### Disk enrollment flags
- `--fs-uuid <uuid>` → filesystem UUID for enroll.
- `--device <path>` or `--block-id <path>` → block device path for enroll.
- `--label <label>` → optional label saved in config.
- `--mount-options <opts>` → per-disk mount options saved in config.
- `--force` → allow enroll if disk non-empty or identity file exists.

### Unknown options
- Unrecognized `-`/`--` options cause `unknown option <arg>` and exit code 2.

## Config schema (YAML)

Config file default: `/etc/timevault.yaml` (override with `--config`).

Top-level fields:
- `mountBase` (string, optional; default `/run/timevault/mounts`)
- `userMountBase` (string, optional; default `/run/timevault/user-mounts`)
- `backupDisks` (list; default empty)
  - `diskId` (string)
  - `fsUuid` (string)
  - `label` (string, optional; omitted when null)
  - `mountOptions` (string, optional; omitted when null)
- `excludes` (list of strings; optional)
- `jobs` (list)
  - `name` (string)
  - `source` (string)
  - `copies` (integer)
  - `run` (string: `auto|demand|off`, default `auto`)
  - `excludes` (list of strings; optional)

Job backup directory is `/<job name>` under the mounted disk root.

## Exit codes

Disk errors (via `exit_for_disk_error`):
- 10: no enrolled disk connected
- 11: multiple enrolled disks connected
- 12: identity mismatch
- 13: disk not empty
- 14: mount/unmount failure

Other exit behavior:
- 3: job already running (lock file exists with live PID)
- 2: config parse/validation errors, unknown options, missing job/disk, disk enroll errors
- 1: backup failed

## Mount behavior

- Backup mounts use mount base `/run/timevault/mounts/<fsUuid>` with options `rw,nodev,nosuid,noexec` unless per-disk `mountOptions` provided.
- Restore mounts use an ephemeral directory under `/run/timevault/user-mounts` and are read-only (`ro,nodev,nosuid,noexec`).
- `/run/timevault/*` base directories must be root-owned 0700; created or chmodded if necessary.
- Mounts are refused if device already mounted.

## Disk identity file (`/.timevault`)

Identity file (YAML):
- `version: 1`
- `diskId: <disk-id>`
- `fsUuid: <uuid>`
- `created: <UTC RFC3339 timestamp>`

Verification occurs after mounts for backup/restore. Missing identity file errors include path, expected diskId/fsUuid.

## Disk enrollment behavior

- Resolves UUID from `--fs-uuid`, `--device`, or auto-detects if only one UUID exists.
- Refuses if disk already mounted or if disk-id/fs-uuid already enrolled.
- Mounts to `/run/timevault/mounts/<fsUuid>` for enrollment.
- Requires empty disk root (allowed entries: `lost+found`), unless `--force`.
- Refuses if identity file exists unless `--force`.
- Always unmounts after enrollment.

## Disk discovery behavior

`timevault disk discover` lists devices under `/dev/disk/by-uuid` meeting any of:
- removable device
- mounted and empty
- has `.timevault` identity file
- enrolled in config

Discovery exclusions:
- swap devices (from `/proc/swaps`)
- FAT/vfat/msdos filesystems (via `/proc/self/mounts` or `blkid`)
- RAID members (based on `/sys/block/md*/slaves`)

Unmounted devices are temporarily mounted read-only for inspection (probe); probe failures are suppressed. Output includes:
- uuid, device path, mounted path (or `no`), enrolled yes/no, identity values (if present), empty/removable status, and reasons.

## Backup behavior

- Prints banner `TimeVault <version>` at start of all runs (including help/version).
- For `backup`: prints timestamp at start and end (`%d-%m-%Y %H:%M`).
- Job selection:
  - If no `--job`, runs jobs with `run: auto`.
  - If `--job` provided, runs only those jobs; `run: off` aborts.
- Per-job lock file: `/var/run/timevault.<job>.pid` (skipped in dry-run).
- Excludes file: `$HOME/tmp/timevault.excludes` (prints in dry-run).
- Backup day name: `YYYYMMDD` for **yesterday** (Local::now - 1 day).
- If `current` exists and `backup_dir` doesn’t: pre-seeds snapshot by hardlinking files (skips symlinks).
- rsync args: `rsync -ar --stats --exclude-from=<file>`, plus:
  - If not safe mode: `--delete-after --delete-excluded`
  - Extra args from `--rsync`.
- rsync is run via `nice -n 19 ionice -c 3 -n7` and retried up to 3 times; exit code 24 is treated as success.
- On success, updates `current` symlink to backup day unless `current` is a directory or safe/dry-run prevents removal.
- Rotation: keeps `copies` snapshots; deletes oldest directories (skips symlinks, skips non-dirs).

## External dependencies

- `rsync`, `mount`, `umount`, `sync`, `nice`, `ionice`, `blkid`.

## Legacy implementation

The legacy C/C++ implementations live under `legacy/` and must remain intact.

## Compatibility mapping (rewrite)

- `timevault disk add` is the canonical disk-enroll path in the rewrite; `timevault disk enroll` and `--disk-enroll` remain supported aliases.
- `timevault disk discover` and `--disk-discover` remain supported.
- `timevault mount` now supports `--mountpoint` and `--read-write` (additive; defaults unchanged).
- Identity files written by the rewrite include `fsType`. Existing identity files without `fsType` continue to work; when present, `fsType` is validated.
- Disk enrollment uses a temporary mountpoint under `/run/timevault/mounts/add-*` (legacy used `/run/timevault/mounts/<fsUuid>`). This is internal and does not affect backup mountpoints.
