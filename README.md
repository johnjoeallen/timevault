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
- `disk enroll`, `disk discover`, `mount`, `umount` commands for disk lifecycle

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

jobs:
  - name: "primary"
    source: "/"
    copies: 30
    run: "auto"
    # Optional disk allowlist for this job:
    # diskIds: ["primary"]
    excludes: []
  - name: "remote-primary"
    source: "root@example.com:/"
    copies: 30
    run: "auto"
    excludes: []
```

Backup directory is `/<job name>` at the mounted backup disk root.

Run policy:
- `auto`: run when no explicit jobs are requested
- `demand`: only run when explicitly requested via `--job`
- `off`: never run, even if explicitly requested

Optional job disk allowlist:
- `diskIds`: restricts the job to specific enrolled disk IDs; without `--disk-id`, TimeVault picks the first connected disk in this list as the primary and cascades to the other connected disks in the list. With `--disk-id`, only jobs that include that disk in `diskIds` will run.

## Usage

Run all `auto` jobs:

```bash
timevault
```

Run an explicit backup (same as default):

```bash
timevault backup
```

Cascade a backup across all connected disks (uses the first connected disk's `current` snapshot for the others):

```bash
timevault --cascade
```

When `--disk-id` is provided, only jobs that list that disk in `diskIds` will run:

```bash
timevault --disk-id primary
```

Enroll a disk:

```bash
timevault disk enroll --disk-id primary --fs-uuid <uuid>
```

If the disk already has a `.timevault` identity, `--disk-id` is optional:

```bash
timevault disk enroll --fs-uuid <uuid>
```

Discover candidate disks:

```bash
timevault disk discover
```

Inspect an enrolled disk (mounts read-only, opens a shell in the mount, unmounts on exit):

```bash
timevault disk inspect --disk-id primary
```

Rename a disk (updates config, and identity if connected):

```bash
timevault disk rename --disk-id primary --new-id archive
```

If multiple disks share the same disk-id, use `--fs-uuid`:

```bash
timevault disk rename --fs-uuid <uuid> --new-id archive
```

Unenroll a disk (removes it from config):

```bash
timevault disk unenroll --fs-uuid <uuid>
```

Run specific jobs:

```bash
timevault --job primary --job secondary
```

Mount an enrolled disk for restore (read-only):

```bash
timevault disk mount
```

If multiple disks are connected, specify a disk:

```bash
timevault disk mount --disk-id primary
```

Unmount a restore mount:

```bash
timevault disk umount
```

Pass rsync options (everything after `--rsync` is forwarded to rsync):

```bash
timevault --rsync --delete --exclude='*.tmp'
```

Print resolved job order and exit with full job details:

```bash
timevault --print-order
```

Dry run (no writes and no mounts):

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
- When multiple enrolled disks are connected, `backup` uses the first disk in config order unless `--disk-id` is provided; `--cascade` runs across all connected disks.
- Remote job sources require passwordless SSH (e.g., keys configured for the remote host).
- Requires `rsync`, `util-linux` (mount/umount), and `coreutils`.
