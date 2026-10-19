# Architecture overview

This page describes how Timevault stores and publishes backups **as the current release behaves**. Features that are still planned are listed at the end and are not available yet.

## The model in one paragraph

Each backup disk is an independent repository. Inside it, every job has its own directory containing one complete, browsable directory tree per backup day (a *generation*). Unchanged files in a new generation are hard links to the same file in the previous generation, so each generation costs only the space of what changed. Nothing on one disk depends on any other disk.

## Layout on a disk

```
<disk root>/
  .timevault                  disk identity (YAML file; id + filesystem UUID)
  <job>/
    20261017/                 a completed generation: a full tree of the source
    20261018/
    20261019/
    current -> 20261019       symlink to the latest completed generation
    .timevault-staging/       in-progress generation (exists only during a run)
    .timevault-replaced-<day>/  transient, only during a same-day replacement
```

- A generation is named after the **backup day**, which is the calendar day *before* the run starts. A run at 02:00 on 20 October writes `20261019`.
- `current` is only moved after a generation has been published successfully.
- A disk can hold several jobs. Each job keeps its own generations, its own `current` and its own retention.

## How a generation is built

1. **Lock the job.** A per-job lock (`/var/run/timevault.<job>.pid`) stops two runs of the *same* job. Different jobs can run at the same time (see [Parallel jobs](#parallel-jobs)).
2. **Prepare the source.** Remote jobs wake and inhibit suspend on the remote host if configured, then pre-hooks run.
3. **Pick the link-dest base.** The base is the directory `current` points to *on this disk, for this job*. If there is none, the run is a full initial backup.
4. **Prepare staging.** A leftover `.timevault-staging` is discarded when there is a base (its files could be hard links into completed generations). For an initial backup it is kept and rsync resumes into it.
5. **Run rsync** into `.timevault-staging` with `-ar --stats`, `--exclude-from`, `--link-dest=<base>`, and, unless `--safe`, `--delete-after --delete-excluded`. It retries up to three times. Exit code 0, or 24 (files vanished during the run), counts as success.
6. **Publish.** On success the staging directory is renamed to `<job>/<backup day>`. The rename is atomic on one filesystem, so an incomplete generation is never visible under a generation name.
7. **Retention.** Only after publishing, the oldest generations beyond `copies` are deleted. The generation `current` points to is never deleted.
8. **Move `current`** to the new generation, then run post-hooks.

If rsync fails, nothing is published, `current` does not move, and nothing is pruned. Completed generations are never written to.

```mermaid
flowchart TD
    S[Source: local path or user@host:/path] --> L{Job lock free?}
    L -- no --> X[Refuse: job already running]
    L -- yes --> B[link-dest base = this disk's current for this job]
    B --> ST[.timevault-staging]
    ST --> R[rsync --link-dest=base]
    R -- exit 0 or 24 --> P[Rename staging to job/YYYYMMDD]
    R -- failure --> F[Nothing published, current unchanged, nothing pruned]
    P --> RT[Prune oldest beyond copies]
    RT --> C[Move current symlink]
    C --> CAS{Cascade enabled?}
    CAS -- yes --> D[Other connected disks copy from primary job/current]
    CAS -- no --> E[Done]
```

## Hard-link sharing

`rsync --link-dest` compares each source file with the file at the *same relative path* in the base generation. If size, modification time and the attributes rsync preserves (`-a`: permissions, owner, group, times) all match, the new generation gets a hard link to the existing file instead of a copy.

Consequences:

- Unchanged files cost no extra data blocks (they still cost directory entries).
- A changed file is written as a new file. Older generations keep their old copy.
- Completed generations are never modified, even when a permission or timestamp changes at the source: that file is simply stored again.
- A file that is **moved or renamed** is stored again, because the path differs. See [Retention and capacity](retention-capacity.md).
- Hard links cannot cross filesystems, so every disk stores its own copy of the data.

## Previous generation: per disk and per job

The "previous" generation is always the one `current` points to on **the disk being written, for the job being written**. It is not "yesterday", and it is not the newest generation on any other disk. A disk that was disconnected for three weeks uses its own three-week-old generation as the base for the next run. Rsync then transfers only what differs from that base. A long absence does not force a full backup.

## Cascade

With `--cascade` (or `options.cascade: true`), a job is first backed up to its *primary* disk from the original source. Every other eligible connected disk then copies that job's `current` from the primary instead of going back to the original source. Each of those disks still uses its own `current` as the link-dest base. See [Cascade](cascade.md).

## Disk identity

A disk is enrolled by filesystem UUID and carries a `.timevault` identity file. Before use, Timevault mounts the disk and checks the identity against the config. Disks are mounted only for the duration of a run, at a private per-process mount point.

## Parallel jobs

Two `timevault` processes may run **different** jobs at the same time, even against the same disk: locks are per job and mounts are per process. Make sure parallel jobs do not have overlapping source or destination directories; Timevault does not check this.

## Not available yet

These are planned and are **not** part of the current behaviour: a disk-local content index, detection of moved or renamed files, an explicit way to recreate the current day's generation, and automatic crash recovery beyond the same-day replacement repair. The plan is in [PLAN.md](PLAN.md).
