# Implementation summary

This summarises the work described in [PLAN.md](PLAN.md): staged publication, current-target recreation, a disk-local file index, moved-file reuse, rotation and cascade validation, and the user documentation. The detailed record of each step, including every decision and what was found along the way, is [IMPLEMENTATION_LOG.md](IMPLEMENTATION_LOG.md). The work is on branch `worktree-generational-phase0`, version 1.14.0 to 1.19.0.

## Outcome in one paragraph

TimeVault still provides independent, generational recovery repositories. Each backup disk is self-contained; completed generations are never written to; a failed or interrupted run leaves history exactly as it was; a disk that was away for weeks resumes from its own history; and a reorganised source no longer stores its data twice. The architecture was extended, not replaced: `rsync --link-dest`, hard-linked generations, the `current` symlink, per-disk and per-job `copies`, and the star-shaped cascade all remain.

## What was delivered

| Release | Change |
|---|---|
| 1.14.1 | Parallel jobs made safe for the shared pristine cache and the remote helper files |
| 1.14.2 | A disabled disk never receives a cascade |
| 1.14.3 | `rotatedOut` and `disk rotate-in/rotate-out` deprecated (still work, warn) |
| 1.14.4 | Publication hardening: completion marker, atomic `current`, `RENAME_EXCHANGE` replacement, deterministic crash recovery, clearer out-of-space failures, per-job cascade summary |
| 1.15.0 | A completed day is skipped; `--recreate` (alias `--refresh`) rebuilds only the current day |
| 1.16.0 | Disk-local SQLite file index with lazy, validated hashing |
| 1.17.0 | Moved and renamed file reuse through a read-only shadow `--link-dest` |
| 1.18.0 | Verification refinements: size cap and folder-move evidence |
| 1.18.1 | Rotation and cascade validated end to end; `copies: 1` retention fixed |
| 1.19.0 | Moved-file detection on by default |

Documentation: a `docs/` set (architecture, rotation strategies A to G, cascade, retention and capacity, recovery, recreation, moved-file reuse, operational examples, design rationale), plus updates to the README and User Guide, shipped in the Debian package. Four CI checks keep it honest (see [Verification](#verification)).

## Key decisions and their trade-offs

| Decision | Why | Cost |
|---|---|---|
| A completed day is skipped; only `--recreate` rebuilds, and only the current day | A later run can no longer replace a good backup; history cannot be rewritten | Users who ran a job several times a day now keep the first run's state unless they pass `--recreate` |
| Cascade stays a star (every other disk copies from the primary) | One failing disk never blocks another; matches the existing design | Every disk depends on the first stage succeeding for that day; a mount error stops later disks |
| Completion marker written after a `syncfs` and added by rename; `current` replaced by renaming a temp link; same-day replacement by `RENAME_EXCHANGE` with an intent file | Crash recovery is deterministic and needs no journal beyond a tiny note | A `.timevault-complete` file in every generation; the flush also writes out other jobs' dirty data on that filesystem |
| The file index is per disk, built only from that disk, and never required | A disk stays usable on its own; corruption or loss costs time, never data | About 0.5 KB of disk per file and one walk of each new generation per run |
| Moved files are offered to rsync as an extra read-only `--link-dest` of hard links | rsync never writes through a reference, so shared inodes cannot be modified, whatever changed at the source | An extra read-only pass per run |
| Remote confirmation uses rsync's own checksum, not a script run as root on the remote | No remote code or scratch files, no shell parsing of file names | rsync's checksum is not SHA-256; only unique-move pairings are confirmed |
| Folder moves trusted as a group; lone files checked; content comparison capped by size | Bounds remote disk I/O while avoiding reading whole moved folders | A trust decision, not a proof: a look-alike inside a moved folder, or a huge file above the cap, is accepted unless `verify: hash` is set. Superseded in design by remote hashing through rsync |
| Retention protects the generation just published | Fixes `copies: 1` keeping two; safe because `current` is repaired after a crash | None found |
| Moved-file detection on by default | Measured cost is small next to the saving on any reorganisation | A `.timevault-index` appears on every disk; `relocation: false` opts out |

## Verification

| Check | Result |
|---|---|
| Unit and integration tests | 309 pass (157 at the start), including the full-disk test, which re-runs the test binary in a mount namespace for its inner half; 4 further tests are ignored benchmarks |
| Formatting | `cargo fmt --check` clean (it was failing on the original code) |
| Static analysis | `cargo clippy --all-targets`: no warnings in code written for this work; 13 warnings remain in code that predates it and were left alone |
| Stability | repeated full test runs with no failures; one genuine intermittent failure was found in the first index implementation and fixed (see the log) |
| Mutation checks | disabling content verification, the staleness check, cascade eligibility, the `current` repair, the skip and the index commit each makes a specific test fail |
| Docs: CLI options | `scripts/check-docs.sh` – every option in `--help` is in the User Guide |
| Docs: examples | `scripts/check-docs-yaml.py` – every complete YAML example loads with the real config loader |
| Docs: commands | `scripts/check-docs-commands.py` – every `timevault ...` command and option shown exists |
| Docs: links | `scripts/check-docs-links.py` – every link and `#anchor` resolves |
| Packaging | `cargo deb` builds; the package contains the new documents; `scripts/compat-smoke.sh` passes (a pre-existing bug in that script was fixed) |
| Real conditions | a genuine `ENOSPC` on a 3 MiB filesystem; real SSH to this machine for a remote source and a cascade |

How to run everything is in [testing.md](testing.md).

### Benchmarks (20,000 files of about 4 KiB, release build, warm cache)

| Change before the second run | Plain | With moved-file detection |
|---|---|---|
| Local source, nothing | 0.30 s, 0.8 MiB | 0.55 s, 5.3 MiB |
| Local source, every folder moved | 0.36 s, 157 MiB | 1.24 s, 10.6 MiB |
| Local source, every file renamed | 0.37 s, 157 MiB | 1.26 s, 9.6 MiB |
| Loopback SSH, nothing | 0.66 s | 1.03 s |
| Loopback SSH, every folder moved | 0.77 s, 157 MiB | 2.56 s, 5.8 MiB |
| Loopback SSH, every file renamed | 0.76 s, 157 MiB | 1.31 s, 162 MiB |

The first mass-rename implementation took 55.7 s and saved nothing on uniform data; the benchmark found it and it was rewritten to hash each size-and-time group once.

## Acceptance criteria

| Criterion (plan section 17) | Status | Evidence |
|---|---|---|
| Existing configurations remain compatible | Met, with documented behaviour changes | All config keys are optional; `rotatedOut` still works; legacy generations are used as they are (`links_against_legacy_cp_al_generation`); changes are listed in the User Guide |
| Any single disk is independently restorable | Met | Generations are plain trees; restores in the tests use no index (`a_deleted_index_never_blocks_a_backup_and_is_rebuilt`, `a_corrupt_index_never_blocks_a_backup_or_a_restore`) |
| A rotated-in disk resumes after any offline interval | Met | `a_disk_returning_after_weeks_resumes_without_a_full_backup`; strategies B, D, E |
| Previous generation comes from that disk's own history | Met | `previous_generation_*` tests; `strategy_d_*` checks inode links to the disk's own old generation |
| Cascade avoids repeated remote transfers | Met by design; verified for correctness, not measured in bytes | Star topology; `remote_sources_over_real_ssh_cascade_and_reuse_moved_files` |
| Every destination uses its own local reference and index | Met | `strategy_c_*`, `a_cascade_copy_reuses_moved_files_from_its_own_previous_generation` |
| Relocated and renamed files reuse data without corrupting history | Met for every content-verified pairing. The default still trusts folder moves and files above `hashMaxSize` without reading them (a trust decision, not a proof), and `verify: size-mtime` trusts everything | Relocation tests, including look-alike files and a permission change on a moved file; `docs/remote-hashing.md` designs the way to prove the rest |
| No completed generation is ever modified | Met | Bit-identical snapshots (mode, size, mtime, inode, contents) asserted after every crash point and every relocation scenario |
| A failed backup cannot consume history through emergency pruning | Met | `a_full_disk_fails_the_run_and_deletes_nothing` with `copies: 1`; retention runs only after a successful publish |
| Only the current target can be recreated | Met | `--recreate` takes no date; `recreate_refuses_a_day_older_than_the_newest_generation` |
| A failed recreation cannot destroy the existing target | Met | `failed_recreate_leaves_the_existing_generation_intact`, `crash_during_recreate_keeps_a_complete_generation` |
| A missing index cannot prevent restoration | Met | As above; the index is rebuilt silently |
| Documentation accurately explains schedules, rotation, capacity and recovery | Met, and checked mechanically | The `docs/` set and four CI checks |
| Users can choose between one-, two- and multi-disk arrangements | Met | `docs/rotation.md`, strategies A to G with a chooser table and verified examples |

## Remaining limitations

- **Remote pairing is not fully content-proven.** Size and modification time are not proof of identity. For a remote source the default confirms lone files by checksum but trusts folder moves and files above `hashMaxSize`, and cannot pair thousands of renamed files that share a size and mtime. rsync can supply the hashes needed to fix all of this without a remote script (probed, not implemented): see [remote-hashing.md](remote-hashing.md). A local source is always proven by content.
- **SSH overhead is measured on loopback only.** There was no real network latency or bandwidth limit; the figures show protocol and process cost. Cold caches, spinning disks and trees of millions of files were not measured.
- **Index cost scales with all files, not changed files.** Recording a generation and advancing the index touch every row. Keying rows by generation and recording only changes would fix that. Extended attributes and ACLs are not captured in the index (rsync decides whether they match).
- **A mount or lock error stops the run;** disks after it are not attempted. This is the existing behaviour, documented and now tested; no automatic fallback was added.
- **Peak usage is `copies + 1` generations** during a run, by design.
- **Moved-file statistics are logged, not in the email report.**
- **`--safe` mode still resumes into a leftover staging directory,** which can write through hard links into completed generations. That behaviour predates this work and was not changed.
- **Not run on real removable hardware.** All tests use directories, a tmpfs and loopback SSH.
- **13 clippy warnings** in older code remain.
- **Removal of `rotatedOut` and its commands** is left for the next major version, which is managed manually.

## What users will notice after upgrading

1. A second run on the same day is skipped (`already complete`); use `--recreate` to rebuild.
2. A `.timevault-complete` file in each new generation and a `.timevault-index/` directory on each disk.
3. One extra read-only rsync pass per job per run, and less data copied after any reorganisation.
4. A deprecation warning wherever `rotatedOut` or `disk rotate-in/rotate-out` is used.
5. `copies: 1` now keeps exactly one generation.
