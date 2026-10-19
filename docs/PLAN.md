# Plan: relocation-aware reuse, disk-local index, safe publication, current-target recreation, rotation docs

Status: approved plan, no phase started. Evolves the existing architecture; it does not replace it.

Invariants (from the design brief): completed generations are immutable; each disk is an independent, self-contained repository; the index is an optimisation, never required for restore; retention never deletes completed generations to make room; cascade never consumes an incomplete upstream generation.

## 1. Architecture assessment

| Brief assumes | Repository reality |
|---|---|
| Java, with a `WalkDir` utility | Rust (1.14.0). No walk crate and no SQLite crate. `sha2` and `libc` are already dependencies. Phase 4 needs `rusqlite`; Phase 5 needs `walkdir` or `std::fs`. |
| Staging and atomic publish are to be built | Already exist. `src/backup/generation.rs` builds in `.timevault-staging`, publishes with `rename`, and recovers an interrupted same-day swap (`.timevault-replaced-<day>`). Phase 2 is hardening. |
| Cascade is a chain, A → B → C | Cascade is a star. `run_jobs_for_primary` (`src/cli/commands/backup.rs`) makes every other connected disk consume the primary's `current` for that job. A job cascades only if the primary status is `Success` or `Partial`. Downstream job failures are reported without aborting other disks; only mount/lock errors abort the run. |
| Re-running skips a completed target | Today a same-day re-run replaces the generation. `backup_day` is yesterday's date (`run_backup`, `src/backup/mod.rs`). |
| No fixed primary disk | The primary is the first connected disk in config order. Rotation moves it naturally. |
| Previous generation chosen per disk and job | Already true: `link_dest_base` follows each job's own `current` symlink on each disk. Rotation gaps already work. |

Further findings:
- The `current` update is not atomic (remove symlink, then recreate). A crash between the two leaves no base, so the next run does a full copy.
- Retention runs only after a successful publish, so a failed or ENOSPC run deletes nothing. Peak usage is `copies + 1` generations, by design.
- `generation.rs` documents that rsync updates attribute-only changes on an existing destination file in place. Pre-populating staging with hard links would mutate historical inodes. Hence the shadow `--link-dest` design below.
- Tests are unit tests in `src/backup/mod.rs` using real rsync on tempdirs. There is no `tests/` directory. `run_backup` calls `Local::now()` internally and cascade planning is tangled with mounting, so rotation and date-gap scenarios are untestable as written.
- Docs: `USER_GUIDE.md`, `README.md`, `PACKAGING.md`. No `docs/` directory, no mkdocs. The deb ships README and PACKAGING but not the user guide.
- Remote execution already exists: `--exclude-pristine` uploads a hash helper to the remote (`/root/tmp/timevault-pristine-hash.sh`), runs it over SSH, and keeps a per-host mtime-keyed hash cache. Backups already rely on root-to-root SSH keys.

## 2. Design

**Completion and crash state.**
- The filesystem is authoritative. A `.timevault-complete` marker is written into staging before the atomic rename.
- `current` is updated atomically (temp symlink plus `rename`). If `current` is missing or dangling it is derived from the newest marked generation.
- Legacy generations without a marker: the `current` target is treated as complete; other unmarked directories are treated as complete but unverified.
- The index holds a "committed generation" row. If it disagrees with `current`, the index is rebuilt. No separate journal.

**Safe replacement.** Use `renameat2(RENAME_EXCHANGE)` through `libc`. The old version lands at the staging name and is removed afterwards. Fall back to the existing aside-rename protocol on `EINVAL`.

**Relocation via a shadow `--link-dest` directory** (replaces pre-populating staging):
1. Dry-run pass: `rsync -n --out-format` with the real excludes and the previous generation as `--link-dest`. It lists paths with size and mtime. It produces no hashes. Skipped when there is no previous generation.
2. Filter to new paths: regular files with no counterpart at that path in the previous generation and at least the minimum size.
3. Match against the previous generation using the disk-local index:
   - Stage 2: same filename, different directory, same size and mtime.
   - Stage 3: different filename, same size and mtime. Ties are broken by hashing previous-generation candidates, and the source too when it is cheap to read (local or cascade).
   - Stage 4: no match; rsync copies the file.
4. Create `.timevault-reloc/` in the job directory and hard-link each matched old file at its new relative path. A failed link skips that file.
5. Run the real rsync with two `--link-dest` references: the previous generation and the shadow directory. rsync links only when every preserved attribute matches and otherwise copies. It never edits a reference file, so shared inodes stay unchanged.
6. Delete `.timevault-reloc/`, then validate, publish, update the index, apply retention. A crash leaves a leftover that staging recovery removes.

**Verification tiers.**
- Remote sources default to size and mtime only, the same trust level as rsync's own quick check.
- Optional remote hash tier: extend the pristine helper pattern. A sibling helper reads NUL-delimited candidate paths from stdin and returns `path, size, mtime, sha256`. It reuses the existing upload/SSH code and has a per-host source-side cache keyed by path, size and mtime. Same requirements as `--exclude-pristine` remote analysis (SSH-style sources, root-to-root keys).
- The helper runs as root on the remote, so it must: take paths from stdin and never interpolate them into a command line; refuse non-regular files and not follow symlinks; stay inside the job's source root; apply a per-file size or time limit; fall back to the size/mtime tier on any helper failure.
- Local and cascade sources: source hashing is a local read, so full hash verification is the default.
- Rejected: `rsync --fuzzy` (matches within one directory only) and detect-renamed (needs a patched rsync).

**Hash cache validity.** Hard-linking bumps an inode's `ctime`, so `ctime` cannot be a cache key. Use `(dev, ino, size, mtime_ns)`, rely on immutability of completed generations, and add a periodic sampled re-hash.

**Index.** `rusqlite` with the `bundled` feature (no new runtime package dependency for the deb). Location: `<disk>/.timevault/file-index.sqlite`, relative paths only. Only the "previous" and "current" generations are active. Hashing is lazy (SHA-256 via `sha2`). Hard links sharing `(dev, ino)` share a record. A missing, corrupt or stale index is rebuilt silently from the disk's own generations. Restore never touches it.

## 3. Decisions (resolved)

1. **Skip by default.** A completed generation for the day is skipped and reported as `AlreadyComplete` unless the recreate flag is passed. Users who run a job more than once a day will see later runs skipped; this is called out in release notes and the "Backup" section of `USER_GUIDE.md`.
2. **Flag name.** `backup --recreate`, with `--refresh` accepted as an alias. `--force` is not used (it already means something else in `disk register`).
3. **Cascade stays a star.** Every other connected disk copies from the primary's `current`. Docs describe it accurately instead of as a chain.

## 4. Phases

Definition of done for every phase:
- `cargo fmt --check`, `cargo check --locked`, `cargo test --locked` pass.
- The phase's data-integrity tests pass.
- Docs ship in the same commit and describe only shipped behaviour.
- Version bump per `AGENTS.md` (features: minor; fixes: patch; build number reset to 0).

### Phase 0: testing seams (no behaviour change; patch)
- Inject `backup_day` or a clock into `run_backup`.
- Extract pure cascade planning out of `run_jobs_for_primary`.
- Add a `tests/` harness that fakes N disk directories with controlled generation dates, using real rsync.
- Tests: existing suite stays green; previous-generation selection per disk and job with gaps of 1, 7 and 40 days and a job never run on a disk.
- Docs: none.

### Phase 1: docs foundation (docs only)
- Create `docs/` with `architecture.md` (Mermaid workflow, cascade star), `rotation.md` (Strategies A to G, selection guidance, first-connected-is-primary truth, `disk rotate-in/out`), `cascade.md` (SSH multi-machine examples, success dependencies, failed upstream), `retention-capacity.md` (frequency vs `copies` vs rotation interval, illustrative coverage math, offline-created-and-deleted limitation, hard-link space behaviour, disk-full recovery that deletes nothing), `recovery.md`, `examples.md` (real YAML and CLI only), `design-rationale.md`.
- Link from `USER_GUIDE.md` and `README.md`; add `docs/` to the deb assets.
- Mark honestly what is current: renames are re-copied, there is no index, no recreate. Leave out "rebuild index" and "recreate" until shipped.
- Add a script check that every global CLI flag in `args.rs` appears in `USER_GUIDE.md`.
- Architecture review addition: inventory existing remote-execution paths (`run_remote_job_script`, the pristine helper, SSH helpers): transport, privileges, failure modes, cache format, and handling of unusual file names and file types.
- Validation: read each document against the code; every command or option in an example must exist.

### Phase 2: publication hardening (fix; patch)
- Completion marker; atomic `current` update with repair; pre-publish validation; clear ENOSPC reporting stating nothing was pruned; `RENAME_EXCHANGE` with fallback; structured per-disk and per-job cascade status including partial cascade success.
- Crash-injection tests: during rsync, after rsync before publish, mid-exchange, after publish before `current` update, during retention. Completed generations must be bit-identical before and after.
- Docs: `architecture.md`, `recovery.md`, and the "Snapshot generations" section of `USER_GUIDE.md`.

### Phase 3: current-target recreation (feature; minor)
- `backup --recreate` (alias `--refresh`), current `backup_day` target only; historical targets are immutable and cannot be named.
- Normal runs skip a completed target (`AlreadyComplete` status, so cascade still feeds disks that lack that generation).
- Recreate builds fresh staging against the previous completed generation, optionally with the existing target as an extra read-only `--link-dest`, validates, exchanges, and retires the old copy only after success.
- Cascade: downstream disks consume the primary's target only after the replacement is committed.
- Tests: failed recreate leaves the old target intact; crash mid-exchange leaves one of the two versions, never neither; skip behaviour; cascade during a recreate never reads half-finished data.
- Docs: new `docs/recreate.md`; release note and "Backup" section for the behaviour change.

### Phase 4: disk-local SQLite index (feature; minor)
- Implement the index, lifecycle (load or rebuild, populate for staging, validate, publish, advance roles, retire old rows) and rebuild-from-disk.
- Tests: lifecycle, hash-cache invalidation, corrupt file falls back to rebuild, mismatch with `current` triggers rebuild, restore with the index deleted.
- Docs: index sections of `architecture.md` and `design-rationale.md`; "rebuild a missing index" in `recovery.md`.

### Phase 5: relocation-aware reuse (feature; minor)
- Implement the pipeline in section 2.
- Config: relocation on/off, minimum file size, verification tier, and bail-out limits (time limit on the dry-run pass, maximum new-path candidates). Hitting a limit abandons relocation for that run, logs why, and lets plain rsync copy everything.
- Cost: one extra traversal per remote job. The benchmark gate decides the default; if it costs more than it saves on common workloads, relocation ships off by default for remote sources and on for local and cascade sources.
- Cascade: another disk's hashes may speed matching, but the destination re-verifies against its own files and never depends on that disk's index.
- Tests: bulk directory move, mass rename, rename plus move (check inode count and disk usage); shadow-directory test where a relocated file with changed permissions, owner or mtime is copied and earlier generations are untouched; bail-out falls back and the backup completes; dry-run vs real-run consistency when a file changes between passes; same size and mtime with different content caught by the hash tier; ENOSPC during linking, failed hard link, shadow cleanup after a crash; helper tests (arbitrary candidate list, cache hit and miss, remote failure falls back to size/mtime).
- Docs: remove the "renamed files are copied again" caveat; document the pipeline, tiers, bail-out settings and benchmark results; `USER_GUIDE.md` job reference; note shared remote requirements with `--exclude-pristine`.

### Phase 6: rotation and cascade validation (tests, plus fixes found)
- Integration scenarios for Strategies A, B, C, D, E, a 4-disk and a 5-disk setup, and G: disk returning after 7+ days and after several weeks; multiple jobs with different last-success dates; downstream disk cannot consume a failed upstream generation; disk fills while earlier generations stay intact; remote-source cascade (local stand-in, or loopback SSH gated by an environment variable).
- Opt-in benchmark script (not in CI) against plain `--link-dest`: duration, bytes transferred, disk I/O, added space, hash CPU, SQLite overhead, bulk move, mass rename, and a remote job with nothing moved (pure overhead of the extra traversal). Record results honestly, including workloads where relocation does not help.
- Docs: reconcile rotation and cascade guides with tested behaviour; add limitations found.

### Phase 7: final pass and summary
- Full build and tests, `cargo fmt --check`, `cargo check --locked`, `cargo test --locked`, `cargo clippy` if clean on the baseline.
- Re-read every document against the shipped CLI and config.
- Write the implementation summary: decisions, trade-offs, test results, remaining limitations.

## 5. Risks

- The extra dry-run traversal per remote job is the main cost; mitigated by the bail-out and the benchmark gate.
- Skip-by-default is the one user-visible behaviour change; everything else is additive.
- Peak usage of `copies + 1` generations stays by design, because pruning after publish is what keeps history safe under disk pressure.
- The remote helper runs as root; its input handling (NUL-delimited stdin, regular files only, no symlink following, source-root confinement, limits) is a security requirement, not an optimisation.
