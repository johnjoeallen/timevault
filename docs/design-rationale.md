# Design rationale

Timevault's primary design principle is that it provides **independent, generational recovery repositories**. Everything else (storage savings, cascade, rotation) is allowed only if it strengthens that and does not weaken historical data protection. This page explains the choices and their costs.

## Independent disk repositories

Each disk holds complete generations and needs nothing from any other disk to be read, restored or extended.

- **Why:** a disk on a shelf or at another site must be usable on its own after the others are lost. Any dependency between disks would turn one failure into several.
- **Cost:** every disk stores its own copy of the data; hard links cannot cross filesystems anyway.

## No automatic synchronisation of history between rotating disks

When a disk returns, Timevault does not copy the generations it missed from another disk.

- **Why:** copying history would make disks depend on each other's contents, take a long time, require both to be present, and blur what each disk actually witnessed. It would also copy mistakes between disks.
- **Cost:** a rotated-in disk has gaps. A file created and deleted while a disk was away exists only on disks that were connected then. Documented in the [Rotation guide](rotation.md).

## "Previous generation" is the disk's own latest

The base for hard-linking is the generation `current` points to on that disk for that job, however old.

- **Why:** it is the only generation guaranteed to exist on that disk, and the data to compare against is local and fast. A long absence costs transfer of the differences, not a full backup.
- **Cost:** a disk away for a long time sees a larger single increment.

## Cascade for remote transfer efficiency

The original source is read once; other disks copy from the primary's finished generation.

- **Why:** remote links are the slow, expensive part, and the remote host stays awake for less time.
- **Star, not chain:** each downstream disk copies from the primary, so a failure on one disk does not block another. It does mean every disk depends on the first stage succeeding for that day.
- **No source fallback:** if the primary stage fails for a job, downstream disks skip it instead of silently going to the source. That keeps "what did cascade mean" unambiguous.

## Staging, then publish

A generation is built in a staging directory and renamed into place only after rsync succeeds.

- **Why:** a generation name always means a complete tree; a crash or failed run leaves no half-backup that looks like a recovery point. Data is flushed to disk before the completion marker is added, and the marker is added by rename, so a power loss cannot leave a marked generation whose data was never written.
- **Cost:** the in-progress tree needs real room until it is published.

## Completed generations are never modified

- **Why:** a restore from last month must still return last month. Hard links share inodes, so a permission or timestamp change applied in place to a shared file would rewrite that file in every generation that links it. Timevault builds into staging and stores a changed file anew.
- **Cost:** some changes that only touch metadata cost a full copy of that file.

## Historical pressure never deletes history

If a backup fails because the disk is full, Timevault deletes nothing.

- **Why:** the newest backup is worth less than the ability to recover from the older ones; deleting history to make a failing run succeed trades known-good data for a possibly-good one.
- **Cost:** users must free space deliberately. Retention runs only after a successful publish, so peak usage includes one generation more than `copies`.

## Preferring selective reuse over full copies

When move detection is added, it will create hard links only for specific matched files, not a full copy of the previous generation.

- **Why:** copying a full tree of links on every run is slow and touches every directory entry; reuse should cost in proportion to what moved.
- **Safety:** links will be offered to rsync as additional read-only references so rsync never writes through a shared inode.

See [PLAN.md](PLAN.md) for the staged work.
