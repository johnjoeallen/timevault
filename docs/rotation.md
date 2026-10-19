# Disk rotation guide

Timevault does not require any particular rotation scheme. Each disk is an independent repository, so you can keep one disk connected, swap disks on a schedule, or run any mix. This page explains how disk selection actually works, then compares the common arrangements so you can pick one.

## How Timevault chooses disks

- **Connected disks** are enrolled disks whose filesystem UUID is present on the machine. Disks that are enrolled but not plugged in are ignored; nothing fails.
- **Eligible disks** are connected disks that are not `disabled: true`. (The deprecated `rotatedOut: true` flag also makes a disk ineligible for automatic runs.)
- **Order** is the order of `backupDisks` in the config file.
- **Primary disk, per job:** without `--disk-id`, each job's primary disk is the first eligible connected disk allowed for that job (see `diskIds` in the [User Guide](../USER_GUIDE.md)). There is no fixed primary: whichever eligible disk is first in config order and is plugged in becomes primary. Rotating the physical disks therefore moves the primary automatically.
- **Without `--cascade`, only the primary disk receives a backup.** Other connected disks are left untouched. To have two connected disks both receive each backup, enable cascade (`--cascade` or `options.cascade: true`).
- **Rotation is something you manage.** There is no rotation setting: you enroll N disks and connect some of them. A disk that is not connected is ignored.
- **`disabled: true`** (set with `timevault disk disable <disk>`) is a hard stop for backups, including cascade. Use it for a disk that stays connected but must not be written. A disabled disk can still be listed, mounted and inspected for restores.
- **`rotatedOut`** and `disk rotate-in` / `rotate-out` are deprecated and will be removed. They still work and print a warning; use `disable` instead.

### What "previous generation" means after rotation

For each job on each disk, the previous generation is the one that disk's own `current` points to. It may be a day old or a month old. When a disk is plugged back in, the next run links against its own latest generation and transfers only the difference between that generation and the source. There is no catch-up copy of the generations it missed, and no dependence on another disk's contents.

### The key limitation of rotation

A disk cannot back up data that was created **and** deleted entirely while it was disconnected. No index or catch-up process can recover something that no longer exists on the source. Another disk that was connected at the time may hold it, but a rotated-in disk does not acquire that disk's history. Choose a rotation that fits how quickly you need to be able to recover recently deleted files.

## Choosing a strategy

| Question | Points towards |
|---|---|
| Only one drive bay | B or E |
| Want an offline copy | B, D, E, F, G |
| Want two current copies at all times | C, D, F, G |
| Slow or metered remote sources | any cascade strategy: C, D, F, G |
| Need recent deletions recoverable from every connected disk | C, D |
| Simplest possible operation | A |

Always keep in mind that backup **frequency**, **retention** (`copies`) and **rotation interval** are three separate settings. See [Retention and capacity](retention-capacity.md).

## Strategy A: one permanently connected disk

```
machines --> Disk A
```

- **Configuration:** one disk enrolled; nothing else needed.
- **Advantages:** simplest; continuous history; no handling; cheapest.
- **Limitations:** one disk failure loses every generation; no offline copy; anything that damages the host or the connected disk can damage the backup.
- **Suitable for:** setups with another independent recovery path, or users who accept the risk. Many generations on one disk do **not** protect against that disk failing.

## Strategy B: two disks, one connected at a time

```
Week 1: A connected, B offline
Week 2: B connected, A offline
Week 3: A connected, B offline
```

- **Configuration:** enroll both disks. Swap them on a schedule you choose. No config change is needed at the swap.
- **Advantages:** one repository is always offline; works with one bay; each disk keeps its own history.
- **Limitations:** each day's backup lands on one disk only; the offline disk is up to a week behind; a file created and deleted while a disk was away exists only on the other disk; restoring an older generation may need a swap; there is no cascade with one disk connected.
- **On reconnect:** the disk links against its own latest generation (up to seven days old) and syncs directly from the original source.

## Strategy C: two permanently connected disks

```
source --> Disk A --> Disk B
```

- **Configuration:** enroll both, enable cascade.
- **Advantages:** two current repositories; remote sources are transferred once; independent histories; convenient local restores.
- **Limitations:** neither disk is offline; a single incident at the host can affect both; if the first stage fails, disk B does not get that job's backup for the day.

## Strategy D: three disks, two connected, one offline (recommended)

```
Week 1: A + B connected, C offline
Week 2: B + C connected, A offline
Week 3: C + A connected, B offline
Week 4: A + B connected, C offline
```

Only one disk changes at each swap, so one disk is always carried over from the previous period.

- **Configuration:** enroll all three in config order A, B, C and enable cascade.
- **Advantages:** two connected repositories receive every backup; one disk stays offline; the carried-over disk holds yesterday's generation; no permanent primary; wear is shared; returning disks resume from their own history; the original source is read once per job.
- **How a swap works:** going from A+B to B+C, disk B already has recent generations. Disk C has a generation that may be a week old; it receives the current backup through cascade and links against its own old generation. Nothing is copied between disks to bring C's history up to date.
- **Primary moves:** the primary is the first eligible connected disk in config order, so it can change at a swap. That is fine, because every disk's history is independent.
- **Limitations:** needs two bays; a failure of the first stage means the second disk misses that day's backup for the job.

## Strategy E: three disks, one connected, two offline

```
Week 1: A connected
Week 2: B connected
Week 3: C connected
Week 4: A connected
```

- **Advantages:** two independent offline repositories; one bay; good for offsite rotation.
- **Limitations:** each disk is about two weeks behind when reconnected; the calendar gaps in each history are large; a file created and deleted in a gap may not reach any disk; no cascade.

## Strategy F: four or more disks

Timevault assumes nothing about the number of disks, the rotation interval, which disk is primary, disk sizes or backup ages. Examples:

| Disks | Connected | Offline |
|---|---|---|
| 4 | 2 | 2 |
| 5 | 3 | 2 |
| 6 | 2 | 4 |

Rotate by the same rule as Strategy D: change one or two disks at a time and keep at least one disk connected across each swap, so recent generations stay reachable. With cascade enabled, every eligible connected disk after the primary is fed from the primary.

## Strategy G: permanent primary with rotating secondaries

```
Week 1: A + B
Week 2: A + C
Week 3: A + B
```

- **Advantages:** one continuous primary history; immediate restores; offline secondary copies.
- **Disadvantages:** the primary takes every backup (uneven wear and growth); first-stage failures affect all secondaries; the primary is always exposed to host-level incidents.
- **Compared with D:** G gives the best single history on A. D shares wear evenly and always keeps a carried-over recent disk, but no single disk holds an unbroken history. Neither is universally better. To keep A as the permanent primary, list it first in `backupDisks`.

## Practical notes

- Swapping disks needs no Timevault command. Run `timevault disk ls` to see which enrolled disks are connected.
- To keep a disk connected but out of backups, use `timevault disk disable <disk>` and `timevault disk enable <disk>` to bring it back.
- `timevault disk df` shows free space per enrolled disk. Capacities do not need to match.
