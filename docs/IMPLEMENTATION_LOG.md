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

### Phase 2 onwards

Not started.

## Open items

- Test override for the `$HOME/tmp` excludes location, needed for full `run_backup_for_day` tests.
- Formatting drift on `main` (`cargo fmt --check` fails); a separate formatting commit is needed before CI is clean.
- Verify the pristine helper's handling of unusual file names and file types before reusing it in Phase 5.
