# Skipping completed days and recreating the current generation

## Default: a completed day is skipped

A generation is named after its backup day (the calendar day before the run starts). From version 1.15.0, if the backup day already has a **completed** generation on a disk, Timevault does not run the job for that disk again. The job is reported as `already complete` and nothing happens: no remote host is woken, no hooks run, rsync does not start, `current` and retention are left alone.

This protects a good backup from being replaced by a later run that happened to start at a bad moment, such as a manual run while the source was half-broken.

**This changes earlier behaviour.** Before 1.15.0 a second run on the same day built a new generation and replaced the first. If you run a job more than once a day (a timer plus a manual run, or an hourly schedule), the later runs are now skipped, and the generation holds the state at the first run of the day. To refresh it, pass `--recreate`.

What counts as completed:

- a generation published by 1.14.4 or later, which carries a `.timevault-complete` file, or
- a generation that `current` points at (`current` only moves after a successful run, so this covers generations from older releases).

A directory with the day's name that is neither (a partial left by a very old release) is not a recovery point and is replaced as before.

A skipped day still has its interruption cleanup done first: if a previous run was interrupted after publishing but before `current` moved, `current` is repaired, so a cascade reads the generation that actually exists.

## Recreating the current generation

```bash
sudo timevault backup --job etc --recreate        # alias: --refresh
sudo timevault backup --job all --recreate --cascade
```

`--recreate` (alias `--refresh`) rebuilds the **current backup day's** generation even though it is complete. It takes no date argument, so there is no way to name a historical day.

### Restrictions

- **Only the current day.** The target is always today's backup day. If a newer generation than the target exists on the disk, the job fails with `refusing to recreate <day>: newer generation <x> exists`, and nothing is changed.
- **Historical generations are immutable.** They cannot be recreated. A past generation records what the source looked like on that day; rebuilding it later would silently present today's data as that day's, and the original state could not be recovered.
- **A day that does not exist yet** is simply created, as in a normal run.
- The flag applies to every disk in the run. With `--cascade`, each downstream disk also rebuilds its current day from the primary's new generation.

### What recreation does

1. A fresh staging directory is built. The existing generation is not touched.
2. rsync links unchanged files against two references: the **previous** generation (the newest completed one older than the target), and the existing target as an extra, read-only source of reusable files. Files that did not change since the previous generation are shared with it; files added or changed on the target day and unchanged since are reused from the old target instead of being stored again.
3. The result is flushed, stamped with `.timevault-complete`, and swapped with the existing generation in a single step (`renameat2` with `RENAME_EXCHANGE`) where the filesystem supports it, otherwise by setting the old one aside and renaming staging into place.
4. Only after the swap succeeds is the old generation removed.
5. Retention and `current` are handled as in a normal run. Moved-file detection also runs, against the previous generation, as in any other run.

### Guarantees

- If the rebuild fails (rsync error, out of space, an error before publishing), the existing generation is untouched, `current` does not move and nothing is pruned.
- If the machine loses power during the swap, the next run resolves it deterministically: either the old generation is kept, or the new one is, and never neither. See the interrupted-runs table in the [Recovery guide](recovery.md).
- Because the old generation is only a reference for hard links, the new generation shares data with it but never writes into it. When the old one is removed, the shared data stays referenced by the new one.
- With `--cascade`, downstream disks only read the primary's generation after it has been published successfully. If the primary's recreation fails, the job is skipped on the other disks and they keep their existing generations.

### Space

Rebuilding needs room for the new generation's changed data until the old one is removed. Files unchanged since the previous generation, or since the old target, are hard-linked and cost nothing extra. A recreate on a nearly full disk can fail; if it does, nothing is lost.

## Related

- [Architecture overview](architecture.md) for the generation lifecycle.
- [Cascade guide](cascade.md) for how `already complete` jobs behave downstream.
- [Recovery guide](recovery.md) for interrupted runs.
