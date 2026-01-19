# Timevault

Timevault is a safe, rsync-based backup tool built for people who want dependable, offline-friendly backups without a maze of services. It uses rsync under the hood, keeps clean snapshot rotations with a `current` pointer, and only mounts disks when it needs them.

## Why Timevault
- Safer by default: disks stay offline until a backup runs.
- Clear identity: disks are enrolled by filesystem UUID with a `.timevault` marker.
- Predictable snapshots: simple, date-stamped rotations and a stable `current` symlink.
- Minimal moving parts: YAML config, rsync, and system tools you already have.

## Features
- YAML config with job-based backups, run policies, and per-job disk allowlists.
- Disk enrollment by filesystem UUID with a `.timevault` identity file.
- Snapshot rotation with date-stamped directories and a `current` symlink.
- Safe and dry-run modes for validation before writing.
- Optional pristine package excludes cached at `~/.cache/timevault/pristine-cache.json`.
- Manual runs: target jobs with `--job`, select disks with `--disk-id`, or cascade with `--cascade`.
- Systemd service + timer support for unattended schedules.

## Documentation
- [User Guide](USER_GUIDE.md): configuration, commands, setup workflow, and systemd usage.
- [Packaging](PACKAGING.md): build and install instructions for `.deb` and local installs.
