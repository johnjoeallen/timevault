# Testing

This page is for people working on Timevault. It describes the layers of tests.

## Everyday tests

```bash
cargo fmt --check
cargo test --locked
scripts/check-docs.sh          # every CLI option is in USER_GUIDE.md
python3 scripts/check-docs-yaml.py   # every complete YAML example loads
```

`cargo test` runs real `rsync` against temporary directories. Nothing needs root and nothing touches your home directory: job locks and per-run scratch files go to a per-test temporary directory (`util::paths::test_lock_dir`), and `/var/run` is never used.

### Layers

| Layer | Where | What it proves |
|---|---|---|
| Generation lifecycle | `src/backup/mod.rs` | staging, publication, retention, `current`, recreate and skip behaviour; crash injection with fail points at every publication step; completed generations stay bit-identical |
| Rotation and cascade | `src/cli/commands/backup.rs` | the real automatic-run orchestration over simulated disks and dates, through a `DiskHost` seam |
| Real full disk | `backup::tests::a_full_disk_fails_the_run_and_deletes_nothing` | a genuine `ENOSPC`: the run fails, nothing is deleted, the next run recovers |

### The rotation scenarios

`Lab` in `src/cli/commands/backup.rs` provides disks as directories, a chosen backup day and a chosen set of connected disks per day, and runs the real `run_automatic_backups` code. The scenarios cover every strategy in `docs/rotation.md`:

- A: one disk, retention within `copies`.
- B: two disks swapped weekly, each with gaps, each resuming from its own history.
- C: two connected disks with cascade.
- D: three disks, two connected, rolling weekly; primary moves with the swap; no history copied into gaps.
- E: three disks, one connected, rolling weekly.
- F: four disks with two connected, five with three, six with two; every day ends up on exactly the connected number of disks.
- G: a permanent primary with rotating secondaries.

Also covered: a disk returning after 40 days, several jobs with different last-success dates, a failed primary stage not being copied downstream, a failing downstream disk not stopping the others, a mount error stopping the run (later disks are not attempted), a disk that missed a day being filled from an already-complete primary, and disabled disks being left out.

### The full-disk test

The test re-runs the test binary inside an unprivileged mount namespace (`unshare -r -m`) with a 3 MiB tmpfs, so the filesystem really fills. It skips itself, with a message, where user namespaces are unavailable. It also checks that with `copies: 1`, the only completed generation survives the failed run.
