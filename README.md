# Timevault

Timevault is a safe, rsync-based backup tool with mount validation and snapshot rotation. It uses a YAML config file, requires a `.timevault` marker on backup devices, and supports dry-run/safe modes.

## Features
- YAML config at `/etc/timevault.yaml` (override with `--config`)
- Mount verification via `/etc/fstab` and `/proc/mounts`
- `.timevault` marker required on the mount root
- Snapshot rotation with `current` symlink
- `--dry-run` and `--safe` modes
- `--job` selection for targeted runs
- `--init` / `--force-init` to initialize a mount

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
mount_prefix: "/mnt/backup/"
excludes:
  - "/proc"
  - "/sys"
  - "/tmp"

jobs:
  - name: "primary"
    source: "/"
    dest: "/mnt/backup/1/host"
    copies: 30
    mount: "/mnt/backup/1"
    run: "auto"   # auto | demand | off
    depends_on: [] # optional job dependencies
    excludes: []
```

Run policy:
- `auto`: run when no explicit jobs are requested
- `demand`: only run when explicitly requested via `--job`
- `off`: never run, even if explicitly requested

## Usage

Run all `auto` jobs:

```bash
timevault
```

Run specific jobs:

```bash
timevault --job primary --job secondary
```

Pass rsync options (everything after `--rsync` is forwarded to rsync):

```bash
timevault --rsync --delete --exclude='*.tmp'
```

Print resolved job order (including dependencies) and exit with full job details:

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

Initialize a timevault device:

```bash
timevault --init /mnt/backup/1
```

Force init on a non-empty mount:

```bash
timevault --force-init /mnt/backup/1
```

## Notes
- The mount point must exist in `/etc/fstab`.
- The mount root must contain `.timevault`.
- Requires `rsync`, `util-linux` (mount/umount), and `coreutils`.
