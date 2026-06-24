# Timevault

Timevault is a safe, rsync-based backup tool built for people who want dependable, offline-friendly backups without a maze of services. It uses rsync under the hood, keeps clean snapshot rotations with a `current` pointer, only mounts disks when it needs them, and now supports local and remote jobs, wake-on-LAN, suspend inhibition, and email reporting.

## Why Timevault
- Safer by default: disks stay offline until a backup runs.
- Clear identity: disks are enrolled by filesystem UUID with a `.timevault` marker.
- Predictable snapshots: simple, date-stamped rotations and a stable `current` symlink.
- Minimal moving parts: YAML config, rsync, and system tools you already have.

## Features
- YAML config with job-based backups, run policies, per-job disk allowlists, per-job excludes, and disk states (`disabled`, `rotatedOut`).
- Local and SSH-style remote jobs, including optional wake-on-LAN and temporary suspend inhibition for remote hosts.
- Disk enrollment by filesystem UUID with a `.timevault` identity file.
- Snapshot rotation with date-stamped directories and a stable `current` symlink.
- Safe, dry-run, and print-order modes for validation before writing.
- Optional pristine package excludes with separate local and remote-host caches.
- Manual runs: target jobs with `--job`, select disks positionally or with `--disk-id`, or cascade with `--cascade`.
- Disk commands for registration, listing, mounting, checking, space reporting, rename, enable/disable, rotation control, inspect, and unregister.
- Optional HTML backup reports sent through `sendmail`.
- Systemd service + timer support for unattended schedules.

## Documentation
- [User Guide](USER_GUIDE.md): configuration, commands, setup workflow, and systemd usage.
- [Packaging](PACKAGING.md): build and install instructions for `.deb` and local installs.
