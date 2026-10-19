# Implementation log: generational backup improvements

Running record of changes, decisions and findings for the work described in [PLAN.md](PLAN.md). Newest entries go at the bottom of each section. Update this file in the same commit as the change it describes.

## Decisions

| # | Decision | Date | Notes |
|---|---|---|---|
| D1 | A completed generation for the day is skipped unless `--recreate` is passed | 2026-10-19 | Behaviour change; ships in Phase 3 with a release note. Runs report `AlreadyComplete`. |
| D2 | Recreate flag is `backup --recreate`, alias `--refresh`; `--force` is not used | 2026-10-19 | `--force` already means something else in `disk register`. |
| D3 | Cascade stays a star (all other disks copy from the primary's `current`) | 2026-10-19 | Meets the cascade success-dependency rules; a chain would let one failure block later disks. Docs describe the star. |
| D4 | Relocation uses a shadow `--link-dest` directory, not pre-linking into staging | 2026-10-19 | rsync never writes to `--link-dest` references, so shared historical inodes cannot be mutated. |
| D5 | Remote hash tier extends the existing pristine helper pattern | 2026-10-19 | Backups already depend on root-to-root SSH keys. Helper input must be NUL-delimited, regular files only, no symlink following. |
| D6 | Different jobs may run in parallel; users keep their directories non-overlapping | 2026-10-19 | Per-job locks already allow it. New shared state (index, caches) must be parallel-safe. |
| D7 | Index is `rusqlite` with `bundled`; hash cache keyed `(dev, ino, size, mtime_ns)`, never `ctime` | 2026-10-19 | Hard-linking bumps `ctime`. |
| D8 | Index lives at `<disk>/.timevault-index/file-index.sqlite` | 2026-10-19 | `<disk>/.timevault` is the identity file, not a directory, so the originally planned path was impossible. |
| D9 | User docs describe only shipped behaviour; planned features are listed under "Not available yet" | 2026-10-19 | Checked in CI by `scripts/check-docs.sh` and `scripts/check-docs-yaml.py`. |
| D10 | A `disabled` disk never receives a cascade; `rotatedOut` is not filtered in the cascade planner | 2026-10-19 | A disk that is unplugged is rotated out by being absent. The `rotatedOut` flag only removes a connected disk from automatic primary selection. |
| D11 | `rotatedOut` and `disk rotate-in/rotate-out` are deprecated: still work, print a warning, removed at the next major version | 2026-10-19 | There is no rotation concept in Timevault: users connect or disconnect enrolled disks. To stop writing to a connected disk use `disable`, which still allows mount, inspect, ls and df. |
| D12 | Publication uses a completion marker (`.timevault-complete`, content = per-run token), written by rename after a `syncfs` of the staging filesystem | 2026-10-19 | The marker is atomic with publish and replaced by rename, never rewritten in place, because a cascade source tree can carry its own marker that rsync hard-links from an older generation. |
| D13 | Same-day replacement uses `renameat2(RENAME_EXCHANGE)` journalled by `.timevault-publishing`; fall back to set-aside where unsupported | 2026-10-19 | Recovery decides by whether the intent's token is now in the final directory. |

## Findings about the existing code

- The project is Rust, not Java; there is no `WalkDir` and no SQLite dependency yet.
- Staging, atomic publish and same-day replacement recovery already exist in `src/backup/generation.rs`.
- Cascade is a star. Downstream job failures are reported without aborting other disks; only mount and lock errors abort.
- The previous generation is already the job's own `current` symlink on each disk, so rotation gaps work today.
- `current` is updated by remove-then-symlink, which is not atomic (fixed in Phase 2).
- The primary disk is the first connected disk in config order.
- `run_backup_job` writes its excludes file to `$HOME/tmp/timevault.<job>.excludes`, which blocks safe full-run tests.
- `cargo fmt --check` already failed on `main` (formatting drift in `generation.rs` and a few test lines in `mod.rs`).
- `run_pristine_only` does take the per-job lock (an early concern that it did not was wrong).
- `<disk>/.timevault` is a YAML identity file at the disk root (`src/disk/identity.rs`).
- Without `--cascade`, only each job's primary disk receives a backup; a second connected disk gets nothing.
- Each job's primary is the first eligible connected disk allowed for it, in `backupDisks` order (not `diskIds` order).
- rsync exit 24 maps to `Success` (`status_for_rsync_code`); the `Partial` status is currently unused by that path.
- Bug (fixed in 1.14.2): with `--disk-id <id> --cascade`, cascade targets came from all connected disks and were not filtered for `disabled`. Decision D10 below.
- Same-size, same-second edits are invisible to rsync's quick check; tests must change file length or mtime.

## Change log

### Phase 0: testing seams (no behaviour change)

- `465c5d6`: `run_backup_for_day` and `backup_day_now` (`src/backup/mod.rs`); pure `plan_cascade` and `cascade_job_eligible` (`src/cli/commands/backup.rs`); rotation fixtures and tests for previous-generation selection per disk and per job, 41-day gaps, a disk with no history, independent histories under A/B rotation, and cascade planning (star, rotation, allowlists, status gating). Added `docs/PLAN.md`. No version bump (refactor and tests only).
- `84caaf1`: thread-local test lock directory (`util::paths::test_lock_dir`) so unit tests can take job locks without `/var/run`; test for the second-holder refusal and release on drop.
- `b189a61`: plan updated with parallel-job safety requirements.
- `7a6cb1c` (1.14.1, fix): pristine helper files made parallel-safe. Remote script and cache input use per-run unique names and are removed after the run; the local pristine cache is written to a temp file and renamed. Tests: unique names, cache replacement leaves no temp files, concurrent writers never expose a partial file (verified to fail against the old `fs::write`). The cache file is now created `0600`.

Deviations from the plan: the harness is in-crate test fixtures, not a `tests/` directory (private `sync_generation`, `/var/run` locks). Unrelated formatting drift in `generation.rs` was left alone to keep commits focused.

### Phase 1: docs foundation (docs only; no version bump)

- Added `docs/architecture.md` (layout, generation lifecycle, Mermaid workflow, hard-link behaviour, per-disk previous generation, parallel jobs), `rotation.md` (how disks are chosen; Strategies A to G; choosing guide), `cascade.md` (star topology, success dependencies, SSH examples), `retention-capacity.md` (frequency vs `copies` vs rotation, coverage calculations, capacity behaviour, out-of-space procedure), `recovery.md`, `examples.md` (eight scenarios), `design-rationale.md`.
- Linked from `README.md` and `USER_GUIDE.md`; corrected the `diskIds` primary-selection sentence to say `backupDisks` order. `USER_GUIDE.md` and the seven pages are now in the deb assets (`USER_GUIDE.md` was linked from the README but not shipped before).
- `scripts/check-docs.sh` fails if a flag in `--help` is missing from `USER_GUIDE.md`; `scripts/check-docs-yaml.py` runs every complete YAML example through the real config loader. Both run in CI. Both were checked to fail on a bad input.
- Docs deliberately omit the index, relocation, recreate and index-rebuild procedures until they ship.
- Coverage figures in the retention page (for example, 20 generations on a daily, two-of-three disk rotation spans about 30 days) were computed by hand from the generations-per-day rate; they are illustrative.
- Not verified by running: restore commands (`disk mount`, `disk ls <disk>:/path`) were written from the CLI help and User Guide, not executed against a real disk.

### Fix after Phase 1 (1.14.2)

- `plan_cascade` skips `disabled` disks, so `--disk-id <id> --cascade` can no longer write to a disabled disk. Tests: disabled disks are excluded; `rotatedOut` is not filtered by the planner (automatic runs already filter ineligible disks before planning). `docs/cascade.md` states the rule.

### Deprecation of `rotatedOut` (1.14.3)

- Config entries with `rotatedOut` and the `disk rotate-in` / `rotate-out` commands print a deprecation warning pointing to `disk disable`; behaviour is unchanged. `--help` marks both commands deprecated. README, User Guide and the new docs no longer present rotation as a Timevault setting.
- Tests: warning text, planner does not filter on `rotatedOut`, and a disabled disk can still be selected for mounting (`select_disk_from_connected`).
- Removal is left to the next major version, which is managed manually.

### Phase 2: publication hardening (1.14.4, fix)

- `src/backup/generation.rs`: completion marker and `finalize_staging` (flush, then stamp by rename); atomic `update_current` (temp symlink renamed over `current`, honouring dry-run, safe mode and a real-directory `current`); `repair_current` (missing, dangling or behind the newest marked generation); same-day replacement through `renameat2(RENAME_EXCHANGE)` with the `.timevault-publishing` intent and the set-aside fallback; `recover_exchange` resolves an interrupted swap by checking which directory holds the intent's token; `prepare_staging` always discards a marked staging directory; internal names extended. Test-only `failpoint` module simulates a crash at a named step.
- `src/backup/mod.rs`: `sync_generation` repairs `current` first, finalizes before publishing and exposes `before-publish` / `after-publish` / `mid-prune` fail points; `run_backup_job` uses `update_current`; out-of-space rsync failures now say the disk is out of space, nothing was published and no completed generation was deleted.
- `src/cli/commands/backup.rs`: per-job cascade summary line (`N of M disks complete (disk: status, ...)`, `not run` for disks never reached), logged after a cascade and when it aborts.
- 19 new tests. Crash tests cover: during rsync (partial staging holding hard links into a completed generation), before publish, after publish before `current` moves, between the temp link and its rename, before the exchange, after the exchange, after the set-aside, the aside fallback (normal and crashed), mid-retention, a marked staging with no base, and a marker that rsync hard-linked from an earlier generation. Each asserts completed generations are bit-identical (mode, size, mtime, inode, contents). The exchange tests ran here (not skipped) and skip themselves if a filesystem lacks `RENAME_EXCHANGE`. Disabling the `current` repair makes the matching test fail.
- Docs: `architecture.md` (marker, flush, exchange, repair), `recovery.md` (interrupted-runs table), `retention-capacity.md`, `cascade.md`, `design-rationale.md`, and the "Snapshot generations" section of `USER_GUIDE.md`.

Notes and limits:
- `syncfs` flushes the whole staging filesystem, including other parallel jobs' dirty data; accepted as the price of never publishing unflushed data.
- Pre-publish validation is limited to: rsync exit code, staging is a directory, data flushed, marker written. Deeper tree validation is not done.
- Generations from releases before the marker are never chosen by `repair_current`; a repository with no marked generation and a bad `current` still gets a full backup, as before.
- With `--safe`, a leftover staging directory is still resumed into (existing behaviour), which can write through hard links into completed generations; not changed here.
- The cascade summary is in the run output only, not in the emailed report.
- Mid-rsync kills were simulated by a pre-built partial staging tree, not by killing a real rsync.

### Phase 3 onwards

Not started.

## Open items

- Test override for the `$HOME/tmp` excludes location, needed for full `run_backup_for_day` tests.
- Formatting drift on `main` (`cargo fmt --check` fails); a separate formatting commit is needed before CI is clean.
- Verify the pristine helper's handling of unusual file names and file types before reusing it in Phase 5.
