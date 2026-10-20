# Remote content hashing for moved-file detection

**Status: implemented in 1.20.0.** This note records the problem, what was tested, the design as built, the decisions taken and what is left. It extends [Reusing moved and renamed files](relocation.md).

## The problem, stated precisely

To reuse a moved or renamed file, Timevault must decide that a new file at the source has the same content as an old file in the previous generation. From rsync's file list it knows names, sizes and modification times to the second. That is **not enough to prove identity**, for any set of vanished and new files:

- Two unrelated files can share a size and a modification time, and a tool or a person can set an mtime on purpose.
- A unique pairing (exactly one vanished file and exactly one new file with that size and mtime) makes a mix-up unlikely, not impossible.
- A wrong pairing is a silent failure: rsync sees a matching size and mtime, skips the file, and the backup holds the wrong bytes.

So proving a pairing, for lone files, folders, ambiguous groups or copies alike, needs the **content hash of the remote file**. With a local source Timevault reads the file itself. With a remote source something has to read it on the remote host and report a hash.

## What 1.19.0 did without remote hashes (now the fallback)

| Case | Treatment | Proof? |
|---|---|---|
| Lone pairing within `hashMaxSize` | Confirmed by `rsync -c` between the source file and the linked old file | Yes (pairwise) |
| Folder move, three or more files from one old folder to one new folder | Trusted as a pattern, nothing read | No |
| File above `hashMaxSize` | Accepted as a unique move | No |
| `verify: size-mtime` | Everything trusted | No |
| Ambiguous groups (many files with the same size and mtime) | Not paired | n/a |
| Copies (old file still exists) | Not detected for remote sources | n/a |

These tiers remain as the fallback when the source's rsync cannot report checksums (see [Requirements](#requirements-and-how-to-check-a-remote-host)). Pairwise `rsync -c` is a yes/no answer for one proposed pair. It cannot choose among candidates, so ambiguous groups would need on the order of N x N guesses, and it cannot find copies at all.

## Finding: rsync can report remote hashes, so no script is needed

The question was whether a script on the remote host is unavoidable. It is not. With `-c` (`--checksum`), the sending rsync computes the whole-file checksum of every file in the list, and `--out-format` can print it with `%C`, **even when the destination has no counterpart**. Combined with `--files-from` (NUL-separated) and a dry run (`-n`), that is a read-only "hash these files" request answered by the rsync already required on the remote.

Probed with rsync 3.5.0 (checksum algorithms: xxh128, xxh3, xxh64, md5, md4, sha1):

| Probe | Result |
|---|---|
| `rsync -n -c --from0 --files-from=LIST --out-format='%C\|%l\|%n' SRC/ EMPTYDIR/` | A checksum for every listed regular file; blank for directories |
| `--checksum-choice=md5` (alias `--cc`) | Values identical to `md5sum` of the same files |
| `--cc=sha1` | Values identical to `sha1sum` |
| `--cc=xxh128` | Works; the default when nothing is chosen |
| Same command through **real SSH** (remote sender) | Identical values |
| File names with a leading dash and spaces, a newline, quotes and `$(touch pwned)`, and a non-UTF-8 byte | All hashed correctly; names come back with rsync's `\#NNN` escapes, which Timevault already decodes; nothing was executed |
| `%B` | Reports the remote permission bits (for example `rw-------`) |
| `%U`, `%G` | Not meaningful here (printed `0` and `DEFAULT`), so ownership is left to rsync's own check at link time |

The command is read-only, touches nothing on the remote, and the list of files is data, not a command line.

## Design as built

1. **Choose candidates.** From the dry run, take the new files whose size equals that of at least one old file (vanished or still present) and is within `hashMaxSize`. Everything else cannot match and is never read. A total of 32 GiB of new files is read per run at most; the rest are left to rsync.
2. **Hash the new files at the source.** One rsync run (`-n -c --checksum-choice=sha1 --from0 --files-from=LIST --out-format='%C|%l|%B|%n'`) against an empty directory. Each line gives a SHA-1, the size and the permission bits. A file whose size differs from the dry run's (it changed meanwhile) is dropped.
3. **Hash the old candidates locally with the same algorithm**, by the same rsync run over the previous generation, for the old files of the sizes that remain.
4. **Match by size and checksum.** Linear in the number of files, however many share a size and mtime. Many-to-many is fine: a copy and its original both map to the same old data. Among equal old files the order of preference is a vanished file of the same name, any vanished file, a file of the same name, then the first path. The permission bits must equal the old file's, so a permission change is copied instead of linked.
5. **Offer through the scratch directory as before.** Only content-proven pairings are linked, and rsync still compares everything it preserves when it decides whether to link.
6. **Fall back safely.** The local rsync's version is checked (`rsync --version`, 3.5.0 or newer). The source's version cannot be read over rsync's connection, so it is judged by whether it answers: an error, or no checksum in the output, means "unavailable". The reason is logged and shown in the report, and the earlier tiers are used for that job and that run.

### What changed

| Case | Before (1.19.0) | Now |
|---|---|---|
| Lone pairing | Confirmed by pairwise `rsync -c` | Content-proven by checksum, like a local source |
| Folder move | Trusted as a group | Every member read and proven |
| File above `hashMaxSize` | Accepted as a unique move | Not offered unless the limit is raised, or `verify: size-mtime` is chosen |
| Ambiguous groups, such as 20,000 uniform files renamed | Left to rsync | Recovered, with 40,000 hashes instead of 20,000 x 20,000 guesses |
| Copies of files that still exist | Not detected | Detected |
| `verify: size-mtime` | Trusts everything | The only setting that trusts size and mtime |

Measured over loopback SSH (20,000 files of about 4 KiB, release build): a mass rename of identical-size files now copies 5.2 MiB instead of 157 MiB, in 3.99 s against 0.75 s for a plain run that copies everything; a moved folder takes 3.11 s (it was 2.56 s when trusted as a group) and also copies 5.8 MiB. The extra time is the source host reading about 80 MB once and the local disk reading the same again.

## Requirements and how to check a remote host

- **rsync 3.5.0 or newer on both ends.** This is the version the probes above were run with, on both the local and the remote side, so it is the minimum that has been shown to work. `--checksum-choice` only arrived in 3.2.0 and nothing older than 3.5.0 was tested for `%C`, so the minimum is deliberately not lowered on a guess; it can be lowered once an older release has been tested. The local version is enforced. The remote version is not read, because rsync does not tell a client the version of its peer; the remote is accepted if it answers with checksums. Many distribution packages are older, so check the remote host (`rsync --version`).
- Quick check from the Timevault host, against any readable file on the remote:

```
rsync -n -c --cc=sha1 --out-format='%C|%n' user@host:/etc/hostname /tmp/
```

A 40-character hex value before the `|` (the SHA-1 of that file) means it works; the same command was run locally and over SSH and matched `sha1sum`. An error such as `unknown checksum name`, or an empty value, means the remote rsync is older than the minimum, and Timevault will use the weaker tiers for that source.

## Alternatives considered

| Alternative | Why not |
|---|---|
| Trust size and mtime when the pairing is unique | Not proof; a look-alike is accepted silently |
| Sample the first and last bytes of a file | rsync cannot read byte ranges, so it needs a remote command; weak on templated and structured files |
| An `ssh ... sha256sum` helper script run as root | Works on any rsync version, but adds remote code, scratch files and shell parsing of names. It remains an option for remotes with an old rsync, if that ever matters |
| Pairwise `rsync -c` in rounds | Quadratic on ambiguous groups; cannot find copies |

## Decisions

1. **Algorithm: SHA-1**, the strongest of the algorithms in rsync 3.5.0 that is also easy to verify with `sha1sum`. It is used to tell files apart, not to defend against a hostile source; reads are disk-bound whichever algorithm is used. A failure of the request falls back to the earlier tiers, so there is no per-source algorithm choice.
2. **Budget: `hashMaxSize` per file (existing) and 32 GiB of new files per run (fixed), plus `timeoutSeconds`.** Not configurable yet.
3. **Old remote rsync: keep the earlier tiers** and say so in the log and the report. Requiring `verify: size-mtime` to be chosen explicitly was rejected as a regression for users with an old remote.
4. **Folder moves: every member is hashed.** A sample would be cheaper but is not proof.
5. **Caching: not done.** The old files are hashed with rsync each run, because rsync's SHA-1 is not the index's SHA-256 and the index key would need the algorithm. Storing the checksum in the index (the key already includes the algorithm name) would remove the local reads on later runs.

## Left to do

- Cache the rsync checksums of old files in the file index.
- Make the per-run byte budget configurable.
- Measure over a real network, on spinning disks and on trees of millions of files.
- Lower the minimum rsync version after testing 3.2.x.
