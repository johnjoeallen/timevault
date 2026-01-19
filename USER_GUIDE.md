# Timevault User Guide

## Philosophy
Timevault is built around a few principles:
- Safe by default: disks stay offline, mounts are short-lived, and destructive actions are explicit.
- Identifiable media: backup disks carry a `.timevault` identity tied to filesystem UUIDs.
- Repeatable results: snapshots are rotated consistently, and `current` points to the latest snapshot.
- Minimal moving parts: it is just rsync, mount, and a small config file.

## Other docs
- [README](README.md)
- [Packaging](PACKAGING.md)

## Configuration
Timevault reads a YAML config file (default `/etc/timevault.yaml`).

### Top-level options
- `mountBase`: Where Timevault mounts backup disks for backups. Default: `/run/timevault/mounts`.
- `userMountBase`: Where Timevault mounts disks for user inspection. Default: `/run/timevault/user-mounts`.
- `backupDisks`: List of enrolled backup disks (required for backups).
- `excludes`: Global exclude paths applied to all jobs.
- `options`: Optional defaults for CLI flags (currently `cascade`, `exclude-pristine`, `verbose`, `safe`, `rsync`). CLI flags still take precedence. `rsync` args from config are prepended to any CLI `--rsync` args.
- `jobs`: List of backup jobs.

### backupDisks entries
Each entry identifies a backup disk by UUID.
- `diskId`: Logical name used in CLI and job `diskIds`.
- `fsUuid`: Filesystem UUID (from `/dev/disk/by-uuid`).
- `label`: Optional human label.
- `mountOptions`: Optional mount options for backups. Default: `rw,nodev,nosuid,noexec`.

Example:
```yaml
backupDisks:
  - diskId: "primary"
    fsUuid: "REPLACE-WITH-UUID"
    label: "primary-backup"
    mountOptions: "rw,nodev,nosuid,noexec"
```

### Job entries
Each job defines a backup source and retention policy.
- `name`: Job name (used as the directory on the disk).
- `source`: Source path for rsync. Can be local or remote (`user@host:/path`).
- `copies`: Number of snapshots to keep (oldest beyond this are removed).
- `run`: Run policy (`auto`, `demand`, `off`).
- `excludes`: Job-specific exclude paths.
- `diskIds`: Optional list of disk IDs this job is allowed to run on.

Example:
```yaml
jobs:
  - name: "primary"
    source: "/"
    copies: 30
    run: "auto"
    excludes: []
    # Optional disk allowlist:
    # diskIds: ["primary"]
```

### Example config
```yaml
# job run policy: auto | demand | off
mountBase: "/run/timevault/mounts"
userMountBase: "/run/timevault/user-mounts"
backupDisks:
  - diskId: "primary"
    fsUuid: "REPLACE-WITH-UUID"
    label: "primary-backup"
    mountOptions: "rw,nodev,nosuid,noexec"
excludes:
  - "/backups"
  - "/proc"
  - "/var/run"
  - "/var/log"
  - "/var/tmp"
  - "/tmp"
  - "/mnt"
  - "/net"
  - "/dev"
  - "/initrd"
  - "/var/lib/imap/proc"
  - "/var/spool/postfix/active"
  - "/var/spool/postfix/defer"
  - "/var/spool/postfix/deferred"
  - "/var/spool/postfix/maildrop"
  - "/var/spool/squid"
  - "/var/cache"
  - "/sys"
  - "/data/docker"
  - "/data/mirrors"
  - "/data/swapfile"
  - "/data/venomsoft"
  - "/data/pub/backups"
  - "/media"
  - ".thumbnails"
  - "/run"
  - "/root/tmp"
options:
  cascade: false
  exclude-pristine: false
  verbose: false
  safe: false
  rsync:
    - "--one-file-system"

jobs:
  - name: "primary"
    source: "/"
    copies: 30
    run: "auto"
    excludes: []
  - name: "remote-primary"
    source: "root@example.com:/"
    copies: 30
    run: "auto"
    excludes: []
```

## Command-line options
Global options:
- `--config <path>`: Use a specific config file.
- `--job <name>`: Run only selected job(s). Repeat for multiple jobs.
- `--dry-run`: No writes or mounts; prints actions.
- `--safe`: Do not delete files; rsync without delete flags.
- `--verbose`: More detailed logging.
- `--print-order`: Print resolved job order and exit.
- `--exclude-pristine`: Exclude pristine package-managed files.
- `--exclude-pristine-only`: Generate pristine excludes and exit (no backup).
- `--rsync <args...>`: Pass remaining args to rsync.
- `--disk-id <id>`: Select a specific enrolled disk.
- `--cascade`: Run backups across all connected disks.
- `--version`: Show version and license info.
- `-h`: Show help.

## Commands

### Backup (default)
- `timevault` or `timevault backup`
- Runs all jobs with `run: auto` unless `--job` is specified.
- Uses the first connected disk unless `--disk-id` is set.
- With `--cascade`, uses the primary disk’s `current` as the source for other disks.

### Disk enroll
- `timevault disk enroll --disk-id <id> [--fs-uuid <uuid> | --device <path>] [--label <label>] [--mount-options <opts>] [--force]`
- `--fs-uuid` picks a disk by filesystem UUID; `--device` uses a block device path (alias `--block-id`).
- `--label` sets a human-friendly label in the config.
- `--mount-options` overrides the mount options stored for this disk (default `rw,nodev,nosuid,noexec`).
- If the disk already contains a `.timevault` identity, `--disk-id` is optional.
- `--force` reinitializes an existing identity or non-empty disk.

### Disk discover
- `timevault disk discover`
- Scans `/dev/disk/by-uuid` and prints candidate disks.

### Disk mount (restore)
- `timevault disk mount [--disk-id <id>]`
- Mounts an enrolled disk read-only and prints the mountpoint.

### Disk umount
- `timevault disk umount`
- Unmounts a restore mount.

### Disk inspect
- `timevault disk inspect [--disk-id <id>]`
- Mounts the disk read-only, opens a shell in the mountpoint, unmounts on exit.

### Disk unenroll
- `timevault disk unenroll [--disk-id <id> | --fs-uuid <uuid>]`
- Removes the disk from config (does not delete `.timevault`).

### Disk rename
- `timevault disk rename [--disk-id <id> | --fs-uuid <uuid>] --new-id <id>`
- Updates config and identity if the disk is connected.

## Job disk allowlists (`diskIds`)
- If `diskIds` is set for a job, it will only run on those disks.
- Without `--disk-id`, Timevault chooses the first connected disk in that list as the primary and cascades to the other connected disks in the list.
- With `--disk-id`, only jobs that include that disk in `diskIds` will run.

## Typical setup workflow
1) Create a test config in your home directory:
```bash
cp /usr/share/doc/timevault/timevault.example.yaml ~/timevault.test.yaml
```
2) Edit it for your environment:
```bash
${EDITOR:-vi} ~/timevault.test.yaml
```
3) Discover candidate disks:
```bash
sudo timevault disk discover
```
4) Enroll your backup disk(s) (requires a clean filesystem or an existing enrolled disk):
```bash
sudo timevault disk enroll --disk-id primary --fs-uuid <uuid>
```
5) Run a dry-run with the test config:
```bash
sudo timevault --config ~/timevault.test.yaml --dry-run --verbose
```
6) If the output looks correct, move the config into place:
```bash
sudo install -m 644 ~/timevault.test.yaml /etc/timevault.yaml
```
7) Run a real backup:
```bash
sudo timevault --verbose
```

## Systemd service and timer
Timevault ships with a systemd service and timer:
- `timevault.service`: Runs `timevault` (default backup).
- `timevault.timer`: Runs daily (default: 2am).

Enable and start:
```bash
sudo systemctl enable --now timevault.timer
```

Check status:
```bash
systemctl status timevault.timer
systemctl status timevault.service
```

Override schedule or config:
- Use `systemctl edit timevault.timer` to adjust the schedule.
- Use `systemctl edit timevault.service` to set `--config` or other flags.

### Passing options to the systemd service
Prefer `options` in `/etc/timevault.yaml` for routine flags (like `cascade`, `exclude-pristine`, or `verbose`). Use service overrides mainly for testing or one-off runs.
To pass options (like `--config`, `--safe`, or `--rsync`) directly, create a drop-in override for the service:
```bash
sudo systemctl edit timevault.service
```
In the editor, add an override that replaces `ExecStart`:
```ini
[Service]
ExecStart=
ExecStart=/usr/bin/timevault --config /etc/timevault.yaml --safe
```
Then reload systemd and restart the service:
```bash
sudo systemctl daemon-reload
sudo systemctl restart timevault.service
```
You can also set a different config for the timer run (the service is what the timer triggers).

### Pristine exclude cache
Pristine exclude caching uses:
- `~/.cache/timevault/pristine-cache.json`
Delete this file to force a full regeneration on the next run.
## Remote backups
Remote job sources require passwordless SSH (keys configured for the remote host), and the remote host must have rsync installed.

## Notes
- Backup disks must contain `/.timevault` and match the configured `diskId` and `fsUuid`.
- Snapshot structure is `<mount>/<job>/<YYYYMMDD>` with a `current` symlink.
- `--safe` and `--dry-run` are recommended when validating new configurations.
