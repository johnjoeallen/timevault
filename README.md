# Timevault

Timevault is a safe, rsync-based backup tool with snapshot rotation. It uses a YAML config file, enrolls backup disks by filesystem UUID, and supports dry-run/safe modes. Backup disks stay offline by default and are mounted only when needed.

## Features
- YAML config at `/etc/timevault.yaml` (override with `--config`)
- Enrolled backup disks by filesystem UUID
- Mounts only during backup or explicit restore mounts
- `.timevault` identity file required at the disk root
- Snapshot rotation with `current` symlink
- `--dry-run` and `--safe` modes
- `--job` selection for targeted runs
- `disk add`/`disk enroll`, `disk discover`, `mount`, `umount` commands for disk lifecycle

## Install (Cargo)

```bash
cargo build --release
sudo install -m 755 target/release/timevault /usr/bin/timevault
sudo install -m 644 timevault.yaml /etc/timevault.yaml
```

## Install (.deb)

```bash
cargo install cargo-deb
cargo build --release
cargo deb
sudo dpkg -i target/debian/*.deb
```

Systemd service + timer (2am daily) are included in the .deb:

```bash
sudo systemctl enable --now timevault.timer
```

## Config

Example `/etc/timevault.yaml`:

```yaml
mountBase: "/run/timevault/mounts"
userMountBase: "/run/timevault/user-mounts"
backupDisks:
  - diskId: "primary"
    fsUuid: "REPLACE-WITH-UUID"
    label: "primary-backup"
    mountOptions: "rw,nodev,nosuid,noexec"
excludes:
  - "/proc"
  - "/sys"
  - "/tmp"

jobs:
  - name: "primary"
    source: "/"
    copies: 30
    run: "auto"   # auto | demand | off
    excludes: []
```

Backup directory is `/<job name>` at the mounted backup disk root.

Run policy:
- `auto`: run when no explicit jobs are requested
- `demand`: only run when explicitly requested via `--job`
- `off`: never run, even if explicitly requested

## Usage

Run all `auto` jobs:

```bash
timevault
```

Run an explicit backup (same as default):

```bash
timevault backup
```

Enroll a disk:

```bash
timevault disk add --disk-id primary --fs-uuid <uuid>
```

Or:

```bash
timevault --disk-enroll --disk-id primary --fs-uuid <uuid>
```

Discover candidate disks:

```bash
timevault disk discover
```

Or:

```bash
timevault --disk-discover
```

Run specific jobs:

```bash
timevault --job primary --job secondary
```

Mount an enrolled disk for restore (read-only by default):

```bash
timevault mount
```

If multiple disks are connected, specify a disk:

```bash
timevault mount --disk-id primary
```

Mount to a specific location (optional):

```bash
timevault mount --mountpoint /mnt/timevault-restore
```

Mount read/write (optional):

```bash
timevault mount --read-write
```

Unmount a restore mount:

```bash
timevault umount --mountpoint /mnt/timevault-restore
```

Pass rsync options (everything after `--rsync` is forwarded to rsync):

```bash
timevault --rsync --delete --exclude='*.tmp'
```

Print resolved job order and exit with full job details:

```bash
timevault --print-order
```

Dry run (no writes, but mounts/umounts still run):

```bash
timevault --dry-run
```

Safe mode (no deletes, rsync without delete flags):

```bash
timevault --safe
```

Verbose logging:

```bash
timevault --verbose
```

Show version (includes license and project URL):

```bash
timevault --version
```

## Notes
- Backup disks are identified by UUID and must contain `/.timevault`.
- Backup disks are mounted on demand under `/run/timevault`.
- Requires `rsync`, `util-linux` (mount/umount), and `coreutils`.
