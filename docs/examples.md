# Operational examples

All examples use the real configuration keys and commands documented in the [User Guide](../USER_GUIDE.md). Disk UUIDs are placeholders: find yours with `sudo timevault disk ls`.

## 1. One Linux workstation, one backup disk

```yaml
backupDisks:
  - diskId: "backup1"
    fsUuid: "REPLACE-WITH-UUID"
excludes: ["/proc", "/sys", "/dev", "/run", "/tmp", "/var/tmp", "/var/cache", "/mnt", "/media"]
jobs:
  - name: "workstation"
    source: "/"
    copies: 30
    run: "auto"
```

```bash
sudo timevault disk register backup1 --fs-uuid <uuid>
sudo timevault backup --job all --dry-run --verbose
sudo systemctl enable --now timevault.timer
```

Result: one generation per night (about 30 days of history), with a single point of failure. See Strategy A.

## 2. Two disks alternating in one drive bay

Enroll both disks; there is nothing else to configure. Swap them on whatever day you choose.

```yaml
backupDisks:
  - diskId: "bay-a"
    fsUuid: "REPLACE-WITH-UUID-A"
  - diskId: "bay-b"
    fsUuid: "REPLACE-WITH-UUID-B"
jobs:
  - name: "home"
    source: "/home"
    copies: 20
    run: "auto"
```

Only the connected disk is backed up. Each disk keeps its own history; `copies: 20` on a weekly swap spans roughly 5-6 weeks per disk, with gaps (see [Retention](retention-capacity.md)). To see which disk is connected: `sudo timevault disk ls`.

## 3. Three disks, two connected, one offline

```yaml
options:
  cascade: true
backupDisks:
  - diskId: "disk-a"
    fsUuid: "REPLACE-WITH-UUID-A"
  - diskId: "disk-b"
    fsUuid: "REPLACE-WITH-UUID-B"
  - diskId: "disk-c"
    fsUuid: "REPLACE-WITH-UUID-C"
jobs:
  - name: "server"
    source: "/srv"
    copies: 20
    run: "auto"
```

Each week, replace one disk: A+B, then B+C, then C+A. The first eligible connected disk in config order is the primary and the other receives a cascade copy. The returning disk links against its own old generation, then gets the current contents from the primary.

## 4. Three disks, one connected, two offline

Same config as example 3 without `cascade`. Plug in one disk per week, rotating A, B, C. Each disk is about two weeks behind when it returns, and each run goes straight to the source. Nothing is copied between disks.

## 5. Home server backing up several machines, weekly rotation

```yaml
options:
  cascade: true
backupDisks:
  - diskId: "nas-1"
    fsUuid: "REPLACE-WITH-UUID-1"
  - diskId: "nas-2"
    fsUuid: "REPLACE-WITH-UUID-2"
jobs:
  - name: "laptop-anna"
    source: "root@anna-laptop.lan:/home"
    copies: 14
    run: "auto"
    remote:
      wol: true
      mac: "aa:bb:cc:dd:ee:01"
  - name: "desktop-ben"
    source: "root@ben-pc.lan:/home"
    copies: 14
    run: "auto"
  - name: "server-local"
    source: "/srv"
    copies: 30
    run: "auto"
```

Swap `nas-2` for an offsite disk each week. Both laptops and desktops are read once per day, whichever disks are connected.

## 6. Remote servers over SSH with cascade

See [Cascade](cascade.md) for a complete example. Requirements: the Timevault host reaches each remote host as `root` with SSH keys, and rsync is installed on the remote.

## 7. Four or more disks with staggered rotation

```yaml
options:
  cascade: true
backupDisks:
  - diskId: "d1"
    fsUuid: "REPLACE-1"
  - diskId: "d2"
    fsUuid: "REPLACE-2"
  - diskId: "d3"
    fsUuid: "REPLACE-3"
  - diskId: "d4"
    fsUuid: "REPLACE-4"
```

Keep two disks connected and swap one every week or two: d1+d2, d2+d3, d3+d4, d4+d1. Each swap keeps one disk that has recent generations. To keep a disk connected but stop writing to it, use `sudo timevault disk disable d3` and later `sudo timevault disk enable d3`.

## 8. Mixed schedules and retention

The shipped timer runs `timevault backup --job all` once a day, which runs jobs with `run: auto`. Give less frequent jobs `run: demand` and schedule them yourself.

```yaml
jobs:
  - name: "etc"
    source: "/etc"
    copies: 60          # 60 nightly generations
    run: "auto"
  - name: "databases"
    source: "/var/backups/db"
    copies: 14
    run: "auto"
  - name: "archive"
    source: "/srv/archive"
    copies: 12          # 12 weekly generations when run weekly
    run: "demand"       # skipped by --job all
```

```bash
# /etc/cron.d/timevault-archive
0 3 * * 0 root /usr/bin/timevault backup --job archive
```

Retention is per job and per disk: the three jobs keep different numbers of generations on the same disk.

## Useful commands

```bash
sudo timevault --job etc --print-order      # show how a job resolves
sudo timevault backup --job etc --dry-run --verbose
sudo timevault backup --job etc --recreate  # rebuild today's already-complete generation
sudo timevault disk df
sudo timevault disk check
```
