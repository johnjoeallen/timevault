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
- `mountBase`: Where Timevault mounts backup disks for backups. Default: `/run/timevault/mounts`. Each run mounts each disk at its own transient sub-directory (`<fsUuid>.<pid>.<timestamp>`), so two `timevault` processes can back up different jobs to the same disk at once; these are unmounted and removed when the run ends, and a stale one left by a hard kill is swept on the next run.
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
- `disabled`: Optional boolean. If `true`, Timevault will not use the disk for backups, including explicit `--disk-id` runs.
- `rotatedOut`: Optional boolean. If `true`, Timevault skips the disk for automatic backup selection, but explicit `--disk-id <id>` runs still work.

Example:
```yaml
backupDisks:
  - diskId: "primary"
    fsUuid: "REPLACE-WITH-UUID"
    label: "primary-backup"
    mountOptions: "rw,nodev,nosuid,noexec"
    # disabled: true
    # rotatedOut: true
```

### Job entries
Each job defines a backup source and retention policy.
- `name`: Job name (used as the directory on the disk).
- `description`: Optional human-readable label for reports and command output.
- `source`: Source path for rsync. Can be local or remote (`user@host:/path`).
- `copies`: Number of snapshots to keep (oldest beyond this are removed).
- `run`: Run policy (`auto`, `demand`, `off`).
- `excludes`: Job-specific exclude paths.
- `diskIds`: Optional list of disk IDs this job is allowed to run on.
- `remote`: Optional remote power behavior for SSH-style sources.

Optional job hooks are discovered by convention:
- `/etc/timevault/scripts/{jobname}.pre`: Runs before the job starts.
- `/etc/timevault/scripts/{jobname}.post`: Runs after rsync finishes.

Hooks are executed with `/bin/sh`.
During `--dry-run`, existing hooks are printed but not run.
Both hooks receive `TIMEVAULT_JOB_NAME`, `TIMEVAULT_JOB_SOURCE`, `TIMEVAULT_JOB_DESTINATION`, `TIMEVAULT_BACKUP_DAY`, and `TIMEVAULT_SCRIPT_PHASE`.
Post hooks also receive `TIMEVAULT_RSYNC_CODE`.
A non-zero pre-hook exit code skips that job and records it as failed.
A non-zero post-hook exit code records the job as failed.

For SSH-style remote sources (`user@host:/path`), Timevault also checks the remote host for the same hook names under `/etc/timevault/scripts`.
Remote pre-hooks run after local pre-hooks and before rsync.
Remote post-hooks run after rsync and before local post-hooks.
Remote hooks receive the same environment plus `TIMEVAULT_JOB_REMOTE_SOURCE`, containing the remote path portion of the source.
Remote hooks are best-effort discovered by SSH command execution; missing hook files are treated as success.

Remote power options are available for SSH-style sources:
- `remote.inhibitSuspend`: Legacy compatibility option. For configured SSH remote jobs, Timevault always prevents suspend while backup work runs and restores the prior state during cleanup.
- `remote.wol`: If `true`, send Wake-on-LAN only when the initial ping fails. Default: `false`.
- `remote.mac`: MAC address required when `remote.wol` is `true`.
- `remote.host`: Optional host name to ping. Defaults to the SSH host from `source`.
- `remote.broadcast`, `remote.port`, `remote.interface`, `remote.keepaliveSeconds`: Optional WOL delivery and keepalive settings; used only when `remote.wol` is `true`.
- `remote.probeTimeoutSeconds`: Total readiness budget for ping followed by an SSH `echo` probe. Default: `180`.
- `remote.minimumUptimeSeconds`: Uptime below this value after WOL is treated as a cold boot. Default: `600`. For a cold boot, Timevault checks recent persistent `systemd-logind` interactive-session records before deciding whether a backup is needed.
- `remote.minimumSessionSeconds`: Shortest `systemd-logind` session, in seconds, that counts as real use when deciding whether a cold boot should back up. Default: `300`. To count, a session must (a) belong to the same boot and line up in time with an `Accepted` SSH login by a person — i.e. any login except one from Timevault's own address (`$SSH_CONNECTION`) as the user Timevault connects as (`root`); an admin reaching the host as a different user, even from the Timevault box, still counts — (b) start inside the prior daily window, and (c) stay open at least this long (a session still open at reboot is measured up to that boot). This means greeter, `systemd --user` manager, `@reboot`/cron and Timevault's own rsync/hook/probe sessions never count, and briefly logging in to power the host off does not keep it in service. Sessions are keyed by `(boot id, session id)`, so a session number reused across reboots is not stitched into one long session. If Timevault cannot read `$SSH_CONNECTION`, it falls back to session length alone.
- `remote.ignoredSessionUsers`: `systemd-logind` session owners that never count as use, chiefly display-manager greeter accounts idling at the login screen. Unset applies a built-in list (`gdm`, `gdm3`, `Debian-gdm`, `sddm`, `lightdm`, `lxdm`, `xdm`, `kdm`, `slim`); an explicit list replaces it entirely, and `[]` disables the filter so any session owner counts.
- `remote.afterBackup`: Action after a successful backup: `return` (default), `none`, `suspend`, or `shutdown`.
- Ping- and SSH-readiness failures are always reported as `offline` and skipped. `remote.offlineIfUnreachable: true` also reports other remote-startup failures as `offline`. Default: `false`.

`remote.afterBackup` values:
- `return` (default): put the host back in the power state Timevault found it in — left running if it answered without Wake-on-LAN, suspended (`systemctl suspend`) if WOL resumed it from suspend, powered off (`systemctl poweroff`) if WOL cold-booted it. So Timevault never leaves a host more awake than it found it.
- `none`: take no action; leave the host running.
- `suspend`: always `systemctl suspend`.
- `shutdown`: always `systemctl poweroff`.

A cold boot with no qualifying prior activity skips the backup and powers the host off regardless of `afterBackup`.

Suspend ownership rule:

For each SSH-style backup job with `remote` options, Timevault probes the backup source host with ping and then SSH. If `remote.wol` is enabled, it sends WOL only when the initial ping fails. Both probes must succeed within `probeTimeoutSeconds` before the backup starts. Once SSH is up, Timevault reads the host's wall clock and `/proc/uptime` together; if the host's clock differs from the Timevault host's by more than a few seconds it prints a `warning:` line (a drifting backup-source clock skews rsync timestamps and the cold-boot window — usually an NTP problem). A low uptime is a cold boot, so it queries the remote persistent journal (as JSON) for logind's session create/remove events and for sshd `Accepted` logins, from 24 hours before that boot, excluding a 10-minute buffer at both ends. The boot instant and the window are computed from the host's *own* clock (`now − uptime`), so a skewed remote clock no longer drags boot-time sessions into the window. Each session is keyed by `(_BOOT_ID, session id)` — a session number reused across reboots is not stitched together. A session counts as "a person used the host" only if it starts in the window, lasts at least `minimumSessionSeconds` (an unclosed session is measured up to the boot), and lines up in time with an `Accepted` SSH login **from an address other than Timevault's own** ($SSH_CONNECTION). That excludes greeter, `systemd --user` manager, cron/`@reboot`, and Timevault's own rsync/hook/probe sessions. If any session qualifies the backup proceeds; otherwise Timevault skips it and powers the host off. Sessions owned by a display-manager greeter account (see `remote.ignoredSessionUsers`) are dropped up front. If Timevault cannot read `$SSH_CONNECTION`, it falls back to session length alone. The check needs the remote journal persisted across reboots (`Storage=persistent`, or a `/var/log/journal` directory) and retaining `sshd` records; if the journal holds no record at all for the prior-day window, Timevault prints a `warning:` and backs the host up without the cold-boot check rather than powering it off. (SSH-style remote jobs already require `root@` for rsync, and root can always read the journal.)
Cascade jobs copied from a remote job ignore wake and suspend handling after their source is rewritten to the primary disk's local snapshot path.

It runs on the backup source host:

```sh
systemctl is-enabled sleep.target suspend.target hibernate.target hybrid-sleep.target
```

Interpretation:
- If any target reports `masked`, suspend/hibernate is already disabled.
- If no target reports `masked`, suspend/hibernate is currently allowed/enabled.
- `static` is normal for these targets and is treated as allowed/enabled.

Behaviour:
- Immediately before backup work starts, Timevault checks suspend state and masks the targets if they are currently allowed.
- Cleanup always restores the prior suspend state: it unmasks only targets that Timevault masked for this job, and leaves pre-existing disabled state untouched.
- After a successful backup and suspend cleanup, `afterBackup` is the only post-backup policy: `return` (default) restores the found power state, `none` does nothing, `suspend` suspends the host, and `shutdown` powers it off.

Acceptance criteria:
- Suspend is disabled before each configured SSH remote backup and restored during cleanup, including error paths.
- Timevault never re-enables suspend unless it disabled suspend itself during that job.
- `remote.afterBackup` runs only after a successful backup.

Example:
```yaml
jobs:
  - name: "primary"
    description: "Primary filesystem"
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
    # disabled: true
    # rotatedOut: true
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
  # Optional HTML backup report email, sent from "Timevault".
  # report:
  #   emailTo: "admin@example.com"
  #   emailFrom: "timevault@example.com"
  #   sendmail: "/usr/sbin/sendmail"
  rsync:
    - "--one-file-system"

jobs:
  - name: "primary"
    description: "Primary filesystem"
    source: "/"
    copies: 30
    run: "auto"
    excludes: []
  - name: "remote-primary"
    description: "Remote primary filesystem"
    source: "root@example.com:/"
    copies: 30
    run: "auto"
    remote:
      wol: true
      mac: "aa:bb:cc:dd:ee:ff"
      host: "example.com"
      broadcast: "192.0.2.255"
      port: 9
      keepaliveSeconds: 60
      probeTimeoutSeconds: 180
      minimumUptimeSeconds: 600
      minimumSessionSeconds: 300
      ignoredSessionUsers: ["gdm", "sddm"]
      afterBackup: return
    excludes: []
```

## Command-line options
Global options:
- `--config <path>`: Use a specific config file.
- `--job <name>`: Run one named job. `--job all`, or omitting `--job` from a `backup` run, runs every job with `run: auto`; `all` is reserved and cannot be a job name. Repeat named jobs to select more than one.
- `--dry-run`: No writes or mounts; prints actions.
- `--safe`: Do not delete files; rsync without delete flags.
- `--verbose`: More detailed logging.
- `--print-order`: Print resolved job order and exit.
- `--exclude-pristine`: Exclude pristine package-managed files.
- `--exclude-pristine-only`: Generate pristine excludes and exit (no backup).
- `--min-session-seconds <n>`: Override `remote.minimumSessionSeconds` for every job this run, for the cold-boot session check. A testing aid — e.g. `--min-session-seconds 30` to make a short login count while validating wake behaviour.
- `--rsync <args...>`: Pass remaining args to rsync.
- `--disk-id <id>`: Select a specific enrolled disk by disk id.
- `--cascade`: Run backups across all connected disks.
- `--send-report`: Send the configured report email even during `--dry-run`.
- `--version`: Show version and license info.
- `-h`: Show help.

### Backup reports
Add `options.report.emailTo` to send a backup report after each backup run.
The HTML email is sent from `Timevault <emailFrom>` so Gmail displays the report body directly.
By default Timevault uses `/usr/sbin/sendmail -t`; set `options.report.sendmail` to override the command path.

## Commands

### Backup
- `timevault backup --job <name>` (or `timevault --job <name>`).
- `timevault backup --job all` runs all jobs with `run: auto`.
- `timevault backup` with no `--job` is the same as `timevault backup --job all`.
- Running Timevault with no command and no `--job` displays help, the same as `--help`.
- Uses the first connected disk unless `--disk-id` is set to a disk id or filesystem UUID.
- With `--cascade`, uses the primary disk’s `current` as the source for other disks. A job is cascaded only if its backup to the primary disk produced a snapshot this run; if the primary backup was skipped, failed, or offline, that job's cascade is skipped (with a note) rather than copying a stale snapshot.

### Wake test
- `timevault wake <job>`
- Requires `remote.wol: true`. Checks whether the configured host already responds to ping, then sends the configured WOL packet only when needed and waits for ping and SSH readiness.
- Does not run hooks, rsync, or remote suspend inhibition.
- Supports `--dry-run` and `--verbose`.

### Disk register
- `timevault disk register <id> [--fs-uuid <uuid> | --device <path>] [--label <label>] [--mount-options <opts>] [--force]`
- Alias: `timevault disk enroll [<id> | --disk-id <id>] ...`
- `--fs-uuid` picks a disk by filesystem UUID; `--device` uses a block device path (alias `--block-id`).
- `--label` sets a human-friendly label in the config.
- `--mount-options` overrides the mount options stored for this disk (default `rw,nodev,nosuid,noexec`).
- If the disk already contains a `.timevault` identity, `<id>` is optional.
- `--force` reinitializes an existing identity or non-empty disk.

### Disk ls
- `timevault disk ls [--short | --columns]`
- `timevault disk ls <disk>:/path`
- Alias: `timevault disk discover`
- `<disk>` may be either a disk id or filesystem UUID.
- Without a path, scans `/dev/disk/by-uuid`, prints candidate disks, and includes registered offline disks.
- Connected disk output includes the block device serial number when available.
- `--short` prints tab-separated `diskId`, UUID, status, registered state, enabled state, and serial.
- `--columns` prints the same disk summary as a columnar table.
- With `<disk>:/path`, mounts the enrolled disk read-only if needed and lists files under that path.

### Disk df
- `timevault disk df [<disk>]`
- Compatibility: `--disk-id <id>` or `--fs-uuid <uuid>` may be used instead of the positional selector.
- Shows enabled state, size, used space, free space, and percent used for enrolled disks; offline disks are listed with unknown usage.

### Disk check
- `timevault disk check [<disk>]`
- Compatibility: `--disk-id <id>` or `--fs-uuid <uuid>` may be used instead of the positional selector.
- Scans configured disks, reports whether they are online or offline, and verifies connected `.timevault` identities against config.
- Fails if a connected disk is missing its identity, has a mismatched `diskId`, or has a `.timevault` `fsUuid` that differs from the actual filesystem UUID.

### Disk du
- `timevault disk du [du options] <disk>:/path`
- Passes options through to the system `du` command after translating each `<disk>:/path` target to a verified read-only disk mount path.
- Example: `timevault disk du -sh primary:/snapshots`

### Disk mount (restore)
- `timevault disk mount [<disk>]`
- Compatibility: `--disk-id <id>` or `--fs-uuid <uuid>` may be used instead of the positional selector.
- Mounts an enrolled disk read-only and prints the mountpoint.

### Disk umount
- `timevault disk umount`
- Unmounts a restore mount.

### Disk enable / disable
- `timevault disk enable <disk>`
- `timevault disk disable <disk>`
- Compatibility: `--disk-id <id>` or `--fs-uuid <uuid>` may be used instead of the positional selector.
- `disable` keeps the disk enrolled but prevents all backup use, including explicit `--disk-id` runs.
- `enable` clears that state.

### Disk rotation
- `timevault disk rotate-in <disk>`
- `timevault disk rotate-out <disk>`
- Compatibility: `--disk-id <id>` or `--fs-uuid <uuid>` may be used instead of the positional selector.
- `rotate-out` keeps the disk enrolled but removes it from automatic backup selection and cascade planning.
- `rotate-in` returns the disk to automatic rotation.

### Disk inspect
- `timevault disk inspect [<disk>]`
- Compatibility: `--disk-id <id>` or `--fs-uuid <uuid>` may be used instead of the positional selector.
- Mounts the disk read-only, opens a shell in the mountpoint, unmounts on exit.

### Disk unregister
- `timevault disk unregister <disk>`
- Aliases: `timevault disk un-register <disk>`, `timevault disk unenroll <disk>`, `timevault disk de-register <disk>`
- Compatibility: `--disk-id <id>` or `--fs-uuid <uuid>` may be used instead of the positional selector.
- Removes the disk from config (does not delete `.timevault`).

### Disk rename
- `timevault disk rename <old-disk> <new-id>`
- Compatibility: `timevault disk rename [--disk-id <id> | --fs-uuid <uuid>] --new-id <id>`
- Updates config and identity if the disk is connected.

## Job disk allowlists (`diskIds`)
- If `diskIds` is set for a job, it will only run on those disks.
- Without `--disk-id`, Timevault chooses the first connected disk in that list as the primary and cascades to the other connected disks in the list.
- With `--disk-id`, only jobs that include that disk in `diskIds` will run.
- Disks with `disabled: true` are never used for backups.
- Disks with `rotatedOut: true` are skipped by automatic selection and cascade planning, but can still be targeted with `--disk-id`.

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
sudo timevault disk ls
```
4) Enroll your backup disk(s) (requires a clean filesystem or an existing enrolled disk):
```bash
sudo timevault disk register primary --fs-uuid <uuid>
```
5) Run a dry-run with the test config:
```bash
sudo timevault backup --job all --config ~/timevault.test.yaml --dry-run --verbose
```
6) If the output looks correct, move the config into place:
```bash
sudo install -m 644 ~/timevault.test.yaml /etc/timevault.yaml
```
7) Run a real backup:
```bash
sudo timevault backup --job all --verbose
```

## Systemd service and timer
Timevault ships with a systemd service and timer:
- `timevault.service`: Runs `timevault backup --job all`.
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
ExecStart=/usr/bin/timevault backup --job all --config /etc/timevault.yaml --safe
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
- `~/.cache/timevault/pristine-cache-<host>.json` for remote `host:/...` sources.
Delete the matching file to force a full regeneration on the next run.
## Remote backups
Remote job sources require passwordless SSH (keys configured for the remote host), and the remote host must have rsync installed.
When `--exclude-pristine` is enabled for a `host:/...` source, Timevault uses SSH to inspect the remote host's package database and file hashes, then stores that host's pristine cache separately on the local machine. Remote pristine analysis supports SSH-style rsync sources, not `rsync://` daemon sources.
Remote power options require `systemctl` over SSH on the remote host for suspend/shutdown handling and `ping` on the Timevault host for wake readiness checks.

## Notes
- Backup disks must contain `/.timevault` and match the configured `diskId` and `fsUuid`.
- Snapshot structure is `<mount>/<job>/<YYYYMMDD>` with a `current` symlink.
- `--safe` and `--dry-run` are recommended when validating new configurations.
- Two `timevault` processes may run different jobs concurrently, even against the same disk (each gets its own mount point and excludes file). A second run of the *same* job is refused with `job <name> is already running` (per-job lock at `/var/run/timevault.<job>.pid`, stale-pid aware).
