# Reusing moved and renamed files

## The problem

`rsync --link-dest` decides whether a file is unchanged by looking at the file with the **same relative path** in the previous generation. If you move a folder or rename files, the new paths have no counterpart, so rsync stores the content again even though identical data is already on the disk in the older generations. Reorganising a large collection can therefore consume as much space as the collection itself, and on a nearly full disk the next backup can fail.

Timevault detects such files and reuses the existing data. It is **on by default** (from version 1.19.0). To turn it off:

```yaml
options:
  relocation: false
```

### What changes when you upgrade

- a `.timevault/` directory appears on each backup disk. On the first run after the upgrade, the previous generation of every job is walked once to build it.
- Every run does one extra read-only rsync pass against the previous generation, and one walk of the new generation to keep the index current. On a quiet night this is the main cost: roughly 0.25 s per 20,000 files locally and 0.37 s over SSH (see [Cost and benefit](#cost-and-benefit)).
- The index uses about half a kilobyte of disk per file of the indexed generation (about 50 MB per 100,000 files).
- Initial backups, and runs on a disk with no previous generation for the job, are unaffected: there is nothing to take data from.

Good reasons to turn it off for a job or globally: trees of millions of files that never reorganise, a very slow remote link, or a disk with no room for the index.

```yaml
options:
  relocation: true
```

## What it does

```mermaid
flowchart TD
    A[Dry run of the real rsync command against the previous generation] --> B[New files and vanished files]
    B --> C{Pair new files with old ones}
    C -->|local source| D[Compare content hashes]
    C -->|remote source| E[Pair by size and mtime]
    E -->|folder move| G
    E -->|lone file| F[Confirm with rsync checksum]
    D --> G[Hard-link matches into .timevault-reloc]
    F --> G
    G --> H[Real rsync with the shadow directory as an extra --link-dest]
    H --> I[Remove .timevault-reloc]
```

1. **Find what changed.** Timevault runs the real rsync command a second time as a *dry run* against the previous generation. Nothing is written. It reports the files that are new at the source and the files that vanished from it. The pass reads the source tree once, so for a remote source it costs an extra file-list exchange over SSH.
2. **Recognise renamed folders.** If a directory of new files and a directory of vanished files hold exactly the same files (the same path below the directory, size and modification time, at least three of them), the folder was renamed or moved, and its files are paired without reading any content. This is the same evidence rsync itself accepts for a file at an unchanged path, so it is trusted in `verify: auto` and `size-mtime`, for a local source and for a remote one whose checksums are unavailable (`verify: hash` still demands content proof; a remote source that reports checksums is still matched by them, which reads only the candidate files). It is exact: one added, missing or touched file, or the same layout appearing twice, means the folder is not treated as a rename and its files go through the steps below. The outermost renamed folder is used, and the report says how many were matched this way.
3. **Pair the rest.** A new file is paired with an old file when they have the same size and modification time (moving or renaming a file keeps its modification time) and, where possible, the same content. The disk-local [file index](architecture.md#disk-local-file-index) supplies the old files' details, which is why relocation switches the index on.
4. **Offer the data to rsync safely.** For each pairing, Timevault makes a hard link at the *new* path, pointing to the old file, inside a scratch directory `<job>/.timevault-reloc`. That directory is passed to rsync as one more `--link-dest`.
5. **Clean up.** The scratch directory holds only links, and is removed when the run ends (or at the start of the next run if the machine crashed).

### Why this is safe

rsync only ever *reads* a `--link-dest` directory. It hard-links a file from it into the new generation when every attribute rsync preserves (permissions, owner, group, time) matches the source, and otherwise writes a new file. It never writes through the reference. So even if the moved file's permissions, owner, timestamp or contents differ from the old copy, no file shared with a completed generation is modified. Tests move files while also changing their permissions and confirm the old generation keeps its original mode and inode.

If anything goes wrong, nothing is lost: the worst case is that rsync copies the file as it always did.

## How sure Timevault is: verification

A pairing is made on size and modification time (moving or renaming a file keeps its modification time). How much more evidence is required depends on the source and on `verify`:

| Source | `verify: auto` (default) | `verify: hash` | `verify: size-mtime` |
|---|---|---|---|
| **Local** (including a cascade source on another disk) | Content compared by SHA-256, up to `hashMaxSize`. Also finds duplicates of files that still exist. A file above the limit is not read; it is accepted only as a unique move. | Same, but a file above the limit is skipped: only content-checked files are offered. | No content comparison; unique moves only. |
| **Remote** (`user@host:/path`) | Content compared by the checksum the source's rsync reports (SHA-1), up to `hashMaxSize`, for every new file whose size matches an old file. Also finds duplicates of files that still exist. A file above the limit is not read and is **not** offered. | Same. | No content comparison; unique moves only. |

- A **move** means the old path no longer exists at the source. A file that still exists at its old path is a **copy**, and a copy is only ever linked after its content has been compared.
- If several candidates look alike (many files with the same size and modification time), they are told apart by content, for local and remote sources alike, so the work grows with the number of files, not with the square of the number that look alike.
- **Remote sources are proven by content.** Size and modification time are never proof of identical content, however unique the pairing, so they only choose which files are worth reading: a new file is read only if some old file has the same size. Timevault asks the source's own rsync to print the SHA-1 of those files (`rsync -n -c --checksum-choice=sha1 --out-format=%C|%l|%B|%n`, a read-only run against an empty directory), asks the local rsync to do the same for the old files, and pairs a new file with an old one only when size, checksum and permission bits all agree. A look-alike inside a moved folder is therefore rejected, and a folder move is no longer taken on trust. Nothing is uploaded or run as root on the source, file names never pass through a shell, and only the checksums cross the network. The source host reads each candidate file once on its own disk. See [Remote content hashing](remote-hashing.md).
- **Requirement: rsync 3.5.0 or newer.** The local rsync's version is checked. The source's version cannot be read, so the source is judged by whether it answers with checksums. If either end is too old, or the request fails, Timevault says so in the log and the report and falls back to the weaker evidence: a folder move of three or more files is trusted as a group, a lone file within `hashMaxSize` is confirmed by rsync's pairwise checksum comparison, a lone file above the limit is accepted as a unique move (skipped under `verify: hash`), and look-alike groups are left to rsync. That fallback is a trust decision, not a proof.
- **Reads are limited to the suspects.** Only new files that have an old file of the same size are read, only up to `hashMaxSize` each (default 2 GiB; `0` removes the limit), and no more than 32 GiB of new files per run. The old files are read once, locally. Each is read again every run; the checksums are not cached yet.
- **Huge files** are never read by default, so they are not offered. Raise or remove `hashMaxSize` to have them compared, or choose `verify: size-mtime`, which trusts a size and modification time that match to the byte and the second.
- The weakest setting, `size-mtime`, is open to a mix-up: if a file vanishes and a *different* file with the same size and modification time appears in the same run, they can be confused. It is there for sources you cannot afford to read from.

Things that are not compared by Timevault itself (extended attributes, ACLs) are compared by rsync when it decides whether to link, so a mismatch there leads to a copy, not to wrong data.

## Configuration

```yaml
options:
  relocation: false           # turn it off everywhere (it is on unless you say so)
```

or, to tune it:

```yaml
options:
  relocation:
    enabled: true             # default true when this block is present
    minSize: 65536            # smallest file considered, in bytes (default 64 KiB)
    maxChanges: 200000        # give up if more files than this are new or vanished
    timeoutSeconds: 3600      # give up if planning takes longer than this
    verify: auto              # auto | hash | size-mtime
    hashMaxSize: 2147483648   # largest file whose content is compared (default 2 GiB, 0 = no limit)
jobs:
  - name: "photos"
    source: "/srv/photos"
    copies: 20
    relocation: true          # per-job override of the global setting (here, explicitly on)
  - name: "web1"
    source: "root@web1.example.com:/"
    copies: 14
    relocation: false
```

- `minSize` keeps the cost down: small files are cheap to copy. Lower it if your reorganisations involve many small files.
- `hashMaxSize` bounds how much is read to compare content. Raise it (or set `0`) if you want even huge files proven.
- Reaching `maxChanges` or `timeoutSeconds` abandons the pass for that run with a log line, and rsync copies everything normally. A large reorganisation therefore degrades to the old behaviour instead of making the backup slower.
- A job's own `relocation:` overrides the global setting in either direction.
- Relocation needs the file index and keeps it on for the disk while it is enabled. See [the index cost](architecture.md#disk-local-file-index).

## What you will see

```
photos: reusing 1840 moved, renamed or duplicate files (9437184000 bytes) already in the previous generation
photos: moved-file detection skipped: more than 200000 new or vanished files
photos: moved-file detection stopped at its time limit; the rest is copied normally
photos: moved-file detection skipped: the file index is unavailable
photos: the source's checksums could not be used (rsync 3.5.0 or newer is needed to hash remote files ...); weaker evidence was used instead
```

The result for each job is also in the emailed and Markdown report, under **Moved-file reuse**: how many files were reused (moved, renamed, duplicate), how much data that saved, how many look-alikes were rejected, how much was read at the source to compare, and anything that was skipped or fell back.

Relocation never fails a backup. Any problem becomes a "skipped" line and the backup proceeds as before.

## Cost and benefit

Measured on a local filesystem with a warm cache, 20,000 files of about 4 KiB each, release build. "Next run" is the second backup; "extra disk" is the space the second generation added.

| Change before the next run | Without relocation | With relocation |
|---|---|---|
| Nothing | 0.31 s, 0.8 MiB | 0.48 s, 4.9 MiB (includes the index) |
| Every folder moved | 0.37 s, 157 MiB | 1.17 s, 10.0 MiB |
| Every file renamed (identical sizes and times) | 0.37 s, 157 MiB | 1.18 s, 9.2 MiB |

Reading the table:

- Relocation costs time on every run, roughly 12 microseconds per file here for the index and the dry-run pass, and about 40 microseconds per moved file for hashing. (These figures are from before the folder-move and size-limit rules; the local numbers are unaffected by them.) With few, large files the saving in copying and disk space dwarfs this; for a tree that never reorganises it is overhead you pay for nothing.
- The extra dry-run pass over **SSH** is measured below. Loopback SSH has no network latency or bandwidth limit, so those figures show the protocol and process cost only; a real link adds the same time to the dry run and to the normal run, since both exchange a file list.
- `timeoutSeconds` bounds the worst case, and `relocation: false` (globally or per job) opts a slow remote job out.
- The benchmark is reproducible: `cargo test --release bench_relocation -- --ignored --nocapture`.

### Over SSH

The same workload with the source reached over real SSH to the same machine:

| Change before the next run | Without relocation | With relocation |
|---|---|---|
| Nothing | 0.66 s, 0.8 MiB | 1.06 s, 5.3 MiB |
| Every folder moved | 0.77 s, 157 MiB | 3.11 s, 5.8 MiB |
| Every file renamed (identical sizes and times) | 0.75 s, 157 MiB | 3.99 s, 5.2 MiB |

- On a quiet night the extra dry-run pass added about 0.4 s for 20,000 files, roughly **60 %** more run time here. That is less than a doubling in this measurement.
- A moved folder is the best case: 157 MiB of copying avoided, at the price of about 2.3 s. Every moved file is read once on the source host (80 MB here) and once locally, and each is proven by its checksum.
- The last row is the hardest case: 20,000 renamed files with identical size and modification time cannot be told apart by anything but content. Before remote hashing (1.19.0) nothing was paired and all 157 MiB were copied; now each is paired with its own original by checksum, for about 3.2 s more.
- The figures are one machine's loopback; treat them as an estimate of the protocol overhead, and measure on your own link with `TIMEVAULT_TEST_SSH=1 cargo test --release bench_relocation_over_ssh -- --ignored --nocapture --test-threads=1`. On a real link the checksums are small, but the source host's disk and CPU do the reading.
- The figures are one machine's loopback; treat them as an estimate of the protocol overhead, and measure on your own link with `TIMEVAULT_TEST_SSH=1 cargo test --release bench_relocation_over_ssh -- --ignored --nocapture --test-threads=1`.

## When it helps and when it does not

Helps: moving or renaming photo, video, music or document collections; reorganising a project tree; renaming many files in place; copying a large file to a second location (duplicates are linked when the source is local).

Does not help: files that are edited as well as moved (the new content must be stored); files below `minSize`; moves between jobs (each job's generations are separate); a file whose permissions or owner changed as it moved (rsync stores it anew; the old generation is untouched).

## Related

- [Retention and capacity](retention-capacity.md): how moves affect disk use.
- [Architecture: file index](architecture.md#disk-local-file-index).
- [Design rationale](design-rationale.md).
