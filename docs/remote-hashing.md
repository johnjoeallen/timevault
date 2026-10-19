# Remote content hashing for moved-file detection

**Status: designed and probed, not implemented.** This note records the problem, what was tested, the proposed design and the open decisions. It extends [Reusing moved and renamed files](relocation.md).

## The problem, stated precisely

To reuse a moved or renamed file, Timevault must decide that a new file at the source has the same content as an old file in the previous generation. From rsync's file list it knows names, sizes and modification times to the second. That is **not enough to prove identity**, for any set of vanished and new files:

- Two unrelated files can share a size and a modification time, and a tool or a person can set an mtime on purpose.
- A unique pairing (exactly one vanished file and exactly one new file with that size and mtime) makes a mix-up unlikely, not impossible.
- A wrong pairing is a silent failure: rsync sees a matching size and mtime, skips the file, and the backup holds the wrong bytes.

So proving a pairing, for lone files, folders, ambiguous groups or copies alike, needs the **content hash of the remote file**. With a local source Timevault reads the file itself. With a remote source something has to read it on the remote host and report a hash.

## What 1.19.0 does without remote hashes

| Case | Treatment | Proof? |
|---|---|---|
| Lone pairing within `hashMaxSize` | Confirmed by `rsync -c` between the source file and the linked old file | Yes (pairwise) |
| Folder move, three or more files from one old folder to one new folder | Trusted as a pattern, nothing read | No |
| File above `hashMaxSize` | Accepted as a unique move | No |
| `verify: size-mtime` | Everything trusted | No |
| Ambiguous groups (many files with the same size and mtime) | Not paired | n/a |
| Copies (old file still exists) | Not detected for remote sources | n/a |

Pairwise `rsync -c` is a yes/no answer for one proposed pair. It cannot choose among candidates, so ambiguous groups would need on the order of N x N guesses, and it cannot find copies at all.

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

## Proposed design

1. **Choose candidates.** From the dry run, take the new files whose size equals that of at least one old file (vanished or still present). Everything else cannot match and is never read. Apply `minSize` and `hashMaxSize` as today, plus a total byte budget.
2. **Hash the new files remotely.** One rsync run of the form above, with an explicit algorithm (`sha1` if both ends support it, otherwise `md5`). Parse `%C`, size and `%B` per file; ignore files without a checksum.
3. **Hash the old candidates locally with the same algorithm.** The same rsync form run over the previous generation, or a stored value in the file index under an algorithm name such as `rsync-sha1` (the index key already includes the algorithm). The algorithm must match, so rsync does both sides.
4. **Match by size and hash.** Linear in the number of files, however many share a size and mtime. Many-to-many is fine: a copy and its original both map to the same old data. Prefer the same name, then the first path.
5. **Check attributes before offering.** Remote permission bits against the old file's mode; rsync still compares everything it preserves when it decides whether to link.
6. **Offer through the scratch directory as today.** Only content-proven pairings are linked.
7. **Fall back safely.** Probe once per run with a tiny request. If the remote rsync rejects `--cc`, or the output has no checksum, use the current tiers.

### What it would change

| Case | After |
|---|---|
| Lone pairing | Content-proven by hash, like a local source |
| Folder move | Every member proven (reads the moved files on the remote disk), or the cap decides |
| File above `hashMaxSize` | Not offered unless the cap is raised, or the explicit `size-mtime` setting is chosen |
| Ambiguous groups, such as 20,000 uniform files renamed | Recovered: 40,000 hashes instead of 20,000 x 20,000 guesses |
| Copies of files that still exist | Detected |
| `verify: size-mtime` | The only setting that trusts size and mtime |

Cost: the remote host reads each candidate file once (its disk, not the network; only checksums cross the link), plus the local read of the old file once, which the index can cache. A 20,000-file uniform rename of 4 KiB files is about 80 MB of remote reads.

## Requirements and how to check a remote host

- A recent rsync on **both** ends. `--checksum-choice` arrived in rsync 3.2.0; the exact minimum for `%C` still has to be confirmed when this is implemented, because the probes above used 3.5.0 on both ends. Older rsync versions may refuse the option or print no usable checksum, and many distribution packages are older, so check before relying on it.
- Quick check from the Timevault host, against any readable file on the remote:

```
rsync -n -c --cc=sha1 --out-format='%C|%n' user@host:/etc/hostname /tmp/
```

A 40-character hex value before the `|` (the SHA-1 of that file) means it works; the same command was run locally and over SSH and matched `sha1sum`. An error such as `unknown checksum name`, or an empty value, means the remote rsync is too old.

## Alternatives considered

| Alternative | Why not |
|---|---|
| Trust size and mtime when the pairing is unique | Not proof; a look-alike is accepted silently |
| Sample the first and last bytes of a file | rsync cannot read byte ranges, so it needs a remote command; weak on templated and structured files |
| An `ssh ... sha256sum` helper script run as root | Works on any rsync version, but adds remote code, scratch files and shell parsing of names. It remains the fallback for remotes with an old rsync, if that ever matters |
| Pairwise `rsync -c` in rounds | Quadratic on ambiguous groups; cannot find copies |

## Open decisions

1. **Default algorithm.** `sha1` where available (stronger), `md5` otherwise. `xxh128` is faster but not cryptographic; reads are disk-bound either way.
2. **Byte budget and timeout.** A total number of bytes to hash per run, in addition to `hashMaxSize` per file and the planning time limit.
3. **Behaviour when the remote rsync is too old.** Keep today's tiers (recommended), or require `verify: size-mtime` to be chosen explicitly.
4. **Folder moves.** Hash every member (proof, more remote reads) or a sample (cheaper, still not proof).
5. **Caching.** Store the rsync checksum of old files in the file index so they are read once.

## Implementation plan

1. Capability probe and the hash request/parse (`%C|%l|%B|%n`), with the hostile-file-name tests used for the existing confirmation step.
2. Local hashing of old candidates through the same mechanism, cached in the index.
3. Matching by size and hash for remote sources, replacing the unverified tiers.
4. Documentation: update the trust tables, remove the "folder move" and "huge file" exceptions, and record the measured cost.
