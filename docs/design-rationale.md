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

## A completed day is skipped; only the current day can be recreated

A second run on a day that already has a completed generation does nothing unless `--recreate` is given, and `--recreate` can only rebuild the newest generation.

- **Why:** a completed generation is a recovery point. A later run that starts while the source is damaged, or a duplicate timer firing, must not be able to replace a good backup. Rebuilding a historical day would present today's data as that day's, destroying the one thing a historical generation is for.
- **Cost:** a job run several times a day keeps the state of the first run, and refreshing it needs an explicit flag. This reverses the behaviour of releases before 1.15.0.
- **Safe replacement:** the rebuild happens in staging and replaces the old generation by an atomic swap, so a failed rebuild leaves the old generation in place.

## Reusing moved files through a read-only reference

Moved and renamed files are offered to rsync as an extra `--link-dest` directory of hard links, instead of being pre-linked into the new generation or patched afterwards.

- **Why:** rsync never writes into a `--link-dest` reference. It links a file from it only when every preserved attribute matches the source, and otherwise writes a new file. A moved file whose permissions, owner or timestamp changed therefore cannot alter the old generation, which pre-populating the new generation with links could do, because hard-linked files share one inode's metadata.
- **Why a dry run first:** rsync's own comparison of the source against the previous generation is the cheapest complete list of what is new and what vanished, and it handles arbitrary file names. Pairing a *vanished* old path with a *new* path of the same size and modification time is strong evidence of a move; a file that still exists is a copy and is only shared after its content is compared.
- **Why bounded, and on by default:** the extra pass is real work, especially over SSH: about 0.25 s per 20,000 files locally and 0.37 s over loopback SSH on a quiet night. It is on because a single reorganisation saves far more (157 MiB of copying avoided in the benchmark), it cannot damage history, and it never fails a backup. A time limit and a change limit make it fall back to ordinary copying instead of slowing the backup, and `relocation: false` turns it off globally or per job.
- **Why rsync's checksum for remote confirmation:** it needs no code uploaded to, or run as root on, the remote host, and file names never pass through a shell.
- **Why remote files are hashed through rsync:** size and modification time never prove identical content, however unique the pairing, so a wrong pairing would silently store the wrong bytes. They only choose which files are worth reading. For a remote source the source's own rsync reports a SHA-1 for each candidate (`-n -c --checksum-choice=sha1 --out-format=%C...`), so nothing is installed or run as root there and file names never reach a shell. Reads are limited to new files that have an old file of the same size, capped by size and per run, so a huge file is never read unless you ask. When the source's rsync is too old to answer, the earlier, weaker tiers (folder moves trusted as a group, lone files confirmed pairwise) take over and the log says so. See [Remote content hashing](remote-hashing.md).
- **Cost:** an extra pass per run, the file index, and, for remote sources, a checksum read of each lone candidate up to `hashMaxSize`.

## A file index on each disk, not a central one

The index that lets Timevault recognise moved files is stored on the backup disk it describes.

- **Why:** a disk must stay usable on its own, and an index that lived on the host (or on another disk) would tie it to that machine or disk and go stale whenever disks are rotated. A disk-local index travels with the disk, describes exactly what that disk holds, and can be rebuilt from it.
- **Why it is an optimisation, not a dependency:** the generations are the truth. Treating the index as disposable means a crash, a corrupt file or a version change can never make backups unreadable, only slower once.
- **Why hashing is lazy:** hashing every file on every run would cost far more than the savings. Hashes are computed on demand, shared between hard links, and trusted only after the file is re-checked.
- **Cost:** extra disk space and a walk of each new generation per run, which is why `options.index` itself is off by default and the index is kept only while a feature that uses it, such as moved-file detection, is on.

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
