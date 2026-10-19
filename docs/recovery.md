# Recovery guide

Every generation is an ordinary directory tree, so recovery needs no Timevault-specific tooling beyond mounting the disk. A single backup disk is enough to restore from; it never depends on another disk.

## Restore files from a connected disk

1. See which enrolled disks are connected:
   ```bash
   sudo timevault disk ls
   ```
2. List the generations for a job without mounting by hand:
   ```bash
   sudo timevault disk ls usb-a:/web1
   ```
   You will see one directory per retained day (`20261017`, `20261018`, ...) and the `current` symlink.
3. Mount the disk read-only and note the mountpoint it prints:
   ```bash
   sudo timevault disk mount usb-a
   ```
4. Copy what you need. Every generation is a full tree:
   ```bash
   cp -a <mountpoint>/web1/20261017/etc/nginx /tmp/restore/
   # or, for a directory tree:
   rsync -a <mountpoint>/web1/20261017/home/alice/ /home/alice/
   ```
5. Unmount:
   ```bash
   sudo timevault disk umount
   ```

For an interactive look, `timevault disk inspect usb-a` mounts the disk read-only, opens a shell in the mountpoint and unmounts on exit.

## Restore from an offline disk

Plug the disk in and follow the steps above. An enrolled disk is recognised by its filesystem UUID, so it does not matter which port or bay it uses, and Timevault does not need any other disk present.

## Find which disk holds a generation

With rotation, different disks hold different days. Connect the disks you have and list each:

```bash
sudo timevault disk ls usb-a:/web1
sudo timevault disk ls usb-b:/web1
```

A disk holds only the days it was connected for a run of that job, up to the job's `copies`. See [Retention and capacity](retention-capacity.md) for how far back each disk reaches.

## After a failed backup

A failed run never publishes anything, never moves `current` and never prunes.

1. Read the failure reason in the run output or the emailed report. It includes the job, the disk, the rsync exit code and rsync's message.
2. Fix the cause (unreachable host, full disk, failed hook, permission error).
3. Run the job again: `sudo timevault backup --job <name>`. If the day was in fact published, the run reports `already complete`. To rebuild that day's generation anyway, add `--recreate` (see [Skipping completed days and recreating](recreate.md)).

A leftover `.timevault-staging` directory is removed on the next run when the job has a previous generation. For an initial backup it is kept and rsync resumes into it. With `--safe`, it is left in place and the run resumes into it.

Cascade note: if the primary stage failed for a job, downstream disks skip that job. Run it again once the primary succeeds.

## Replace a failed backup disk

1. Remove the failed disk from the config: `sudo timevault disk unregister <old-id>`.
2. Enroll the replacement: `sudo timevault disk register <new-id> --fs-uuid <uuid>`. Registering writes the `.timevault` identity file and requires a clean filesystem (or `--force`).
3. Run the backup. The new disk has no history, so each job starts with a full initial backup from the source (or from the primary's `current` if cascade is on).

The replacement starts with a single generation. Older generations live only on your other disks.

## Resume after a disk was offline for weeks

Just connect it and run a normal backup. The disk links against its own latest generation, however old. Only the differences since then are written. It will not contain the days it was away. See the limitation in the [Rotation guide](rotation.md).

## Out-of-space condition

Timevault never deletes completed generations to make room. See [Out of space](retention-capacity.md#out-of-space) for the safe procedure.

## Missing or corrupt file index

The file index (`.timevault-index/file-index.sqlite`, kept while moved-file detection is on, which it is by default, or when `options.index: true` is set) is never needed to restore: mount the disk and copy from the generations as usual.

If the index is missing, stale, or from an interrupted run, no action is needed. The next backup rebuilds it from the generations on that disk (a log line says `file index: rebuilt from the previous generation`). If the database is corrupt or not a database at all, Timevault moves it aside as `file-index.sqlite.corrupt-<n>` (only the newest is kept), creates a new one and rebuilds. You can also delete the whole `.timevault-index` directory yourself; nothing else depends on it. If the index belongs to a newer Timevault than the one running, it is left untouched, a warning is printed and the backup runs without it.

## Check that a disk is healthy

`timevault disk check [<disk>]` verifies that connected disks have a `.timevault` identity matching the config. `timevault disk df` reports free space.

## Interrupted runs

No manual repair is needed. At the start of each run for a job, Timevault resolves whatever an interruption left behind before it does anything else:

| Interrupted at | State left behind | What the next run does |
|---|---|---|
| During rsync | partial `.timevault-staging` | discards it (a first backup resumes it) |
| While looking for moved files | `.timevault-reloc/` (and `.timevault-reloc.list`) of hard links | removes it; it holds only links, so no data is touched |
| After rsync, before publishing | complete but unpublished `.timevault-staging` | discards it and builds again |
| During a `--recreate` (same-day replacement) | `.timevault-publishing`, possibly the old generation under `.timevault-staging` or `.timevault-replaced-<day>` | keeps whichever generation was fully in place, removes the other, deletes the note |
| After publishing, before `current` moves | `current` points at the previous generation | points `current` at the newest finished generation |
| During retention | some old generations already deleted | deletes the rest on the next successful run |

In every case completed generations are left exactly as they were. A generation under its final `YYYYMMDD` name was published successfully by a current release; a partial directory left by a very old release may still be present and counts toward `copies` until it is replaced.

With `--safe`, nothing is deleted: displaced directories stay as `.timevault-replaced-<day>` until you remove them.
