//! Reuse of files that were moved or renamed.
//!
//! `rsync --link-dest` matches files by relative path, so a moved or renamed
//! file is stored again even though identical data is already in the previous
//! generation. This module finds such files before the real rsync run and
//! offers them to it through a *shadow directory*: a scratch directory next to
//! the generations that holds hard links to the old files at their new paths.
//! The shadow directory is passed to rsync as an additional `--link-dest`.
//!
//! That is what makes the approach safe. rsync only ever reads a `--link-dest`
//! reference: it hard-links a reference file into the new generation when every
//! attribute it preserves matches, and otherwise writes a new file. It never
//! writes through the reference, so no inode shared with a completed
//! generation is modified, whatever happens to ownership, permissions,
//! timestamps or contents at the source.
//!
//! Finding candidates costs one extra, read-only pass: a dry run of the real
//! rsync command against the *previous generation*, which reports exactly what
//! is new at the source (`>f+++++++++`) and what vanished from it
//! (`*deleting`). The pass is bounded by a time limit and a limit on the number
//! of changes; exceeding either abandons relocation for the run, and rsync
//! copies everything as it always did.
//!
//! Evidence, strongest first:
//! - **Content verified.** When the source is local (including a cascade source
//!   on another disk) the new file is hashed and compared with the old file's
//!   hash from the disk-local index.
//! - **A move.** Without hashing (remote sources), a new file is paired with a
//!   file that vanished in the same run only if size and modification time
//!   match and the pairing is unique in both directions. Moving or renaming
//!   preserves the modification time. A file that still exists at its old path
//!   is a copy, not a move, and is never linked without verifying content.

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::io::{self, BufRead, BufReader, Read};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::backup::generation::RELOC_NAME;
use crate::backup::index::{sha256_file, DiskIndex, FileRecord, IndexError, Role};
use crate::backup::rsync::normalize_rsync_source;
use crate::config::model::RelocationOption;

pub const DEFAULT_MIN_SIZE: u64 = 64 * 1024;
pub const DEFAULT_MAX_CHANGES: usize = 200_000;
pub const DEFAULT_TIMEOUT_SECS: u64 = 900;
/// Content is compared for files up to this size (2 GiB); larger files are
/// not read. `0` removes the limit.
pub const DEFAULT_HASH_MAX_SIZE: u64 = 2 * 1024 * 1024 * 1024;
/// Pairings that map one old directory to one new directory count as a folder
/// move once there are this many of them.
pub const GROUP_MIN: usize = 3;
/// Largest group of same-size, same-mtime files whose contents are compared.
/// Bigger groups are left to rsync; the time limit bounds the work anyway.
const MAX_GROUP: usize = 50_000;

/// How a candidate pairing is checked before it is offered to rsync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verify {
    /// Compare content when the source is local; otherwise rely on a unique
    /// move (size, modification time, and the old path vanished).
    Auto,
    /// Always compare content. A remote source cannot be hashed yet, so it is
    /// treated like `Auto`.
    Hash,
    /// Never compare content. Weakest; only moves are matched.
    SizeMtime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelocationConfig {
    pub enabled: bool,
    pub min_size: u64,
    pub max_changes: usize,
    pub timeout_secs: u64,
    pub verify: Verify,
    /// Largest file whose content is compared; `0` means no limit.
    pub hash_max_size: u64,
}

impl RelocationConfig {
    /// Whether a file of `size` bytes may have its content compared.
    pub fn may_hash(&self, size: u64) -> bool {
        self.hash_max_size == 0 || size <= self.hash_max_size
    }
}

impl Default for RelocationConfig {
    fn default() -> Self {
        RelocationConfig {
            enabled: false,
            min_size: DEFAULT_MIN_SIZE,
            max_changes: DEFAULT_MAX_CHANGES,
            timeout_secs: DEFAULT_TIMEOUT_SECS,
            verify: Verify::Auto,
            hash_max_size: DEFAULT_HASH_MAX_SIZE,
        }
    }
}

impl RelocationConfig {
    /// Resolve `options.relocation`.
    pub fn from_option(option: Option<&RelocationOption>) -> Result<Self, String> {
        let mut cfg = RelocationConfig::default();
        match option {
            None => {}
            Some(RelocationOption::Flag(enabled)) => cfg.enabled = *enabled,
            Some(RelocationOption::Settings(settings)) => {
                cfg.enabled = settings.enabled.unwrap_or(true);
                if let Some(min_size) = settings.min_size {
                    cfg.min_size = min_size;
                }
                if let Some(max_changes) = settings.max_changes {
                    cfg.max_changes = max_changes;
                }
                if let Some(timeout) = settings.timeout_seconds {
                    cfg.timeout_secs = timeout;
                }
                if let Some(limit) = settings.hash_max_size {
                    cfg.hash_max_size = limit;
                }
                if let Some(verify) = settings.verify.as_deref() {
                    cfg.verify = match verify {
                        "auto" => Verify::Auto,
                        "hash" => Verify::Hash,
                        "size-mtime" => Verify::SizeMtime,
                        other => {
                            let message = format!(
                                "options.relocation.verify must be auto, hash or size-mtime, not {}",
                                other
                            );
                            return Err(message);
                        }
                    };
                }
            }
        }
        Ok(cfg)
    }

    /// Apply a job's own `relocation: true|false`.
    pub fn for_job(mut self, job: Option<bool>) -> Self {
        if let Some(enabled) = job {
            self.enabled = enabled;
        }
        self
    }
}

/// What a relocation pass found and did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    /// New regular files rsync would store (at or above the minimum size).
    pub new_files: usize,
    /// Old files that no longer exist at the source.
    pub vanished_files: usize,
    /// Files paired with an old file under the same name (moved).
    pub moved: usize,
    /// Files paired with an old file under a different name (renamed).
    pub renamed: usize,
    /// Files paired with an old file that still exists (copies).
    pub copies: usize,
    /// Pairings confirmed by comparing content.
    pub verified: usize,
    /// Candidates left to rsync because the pairing was not unique.
    pub ambiguous: usize,
    /// Unverified pairings accepted because they form a folder move.
    pub group_supported: usize,
    /// Files above `hashMaxSize` that were not read (offered only on the
    /// strength of a unique move, or skipped under `verify: hash`).
    pub skipped_large: usize,
    /// Candidates dropped because content or attributes did not match.
    pub rejected: usize,
    /// Links created in the shadow directory.
    pub linked: usize,
    pub link_failures: usize,
    /// Bytes of file data offered for reuse.
    pub bytes_reused: u64,
    /// Matching stopped early because the time limit passed.
    pub hit_deadline: bool,
}

impl Stats {
    pub fn matched(&self) -> usize {
        self.moved + self.renamed + self.copies
    }
}

#[derive(Debug)]
pub enum Outcome {
    /// Planning finished. `shadow` is the directory to pass to rsync as an
    /// additional `--link-dest`, or `None` if nothing was worth offering.
    Prepared {
        shadow: Option<PathBuf>,
        stats: Stats,
    },
    /// Relocation was abandoned for this run; rsync proceeds as usual.
    Skipped(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewFile {
    pub path: Vec<u8>,
    pub size: u64,
    /// Modification time, seconds since the epoch (rsync reports seconds).
    pub mtime: i64,
}

#[derive(Debug, Default)]
pub struct Changes {
    pub new: Vec<NewFile>,
    pub deleted: Vec<Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq)]
enum Line {
    New(NewFile),
    Deleted(Vec<u8>),
    Other,
}

/// Undo rsync's `\#NNN` octal escaping of unusual bytes in file names.
pub fn unescape_name(name: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(name.len());
    let mut i = 0;
    while i < name.len() {
        if name[i] == b'\\'
            && i + 4 < name.len()
            && name[i + 1] == b'#'
            && name[i + 2..i + 5].iter().all(|b| (b'0'..=b'7').contains(b))
        {
            let value = (name[i + 2] - b'0') as u32 * 64
                + (name[i + 3] - b'0') as u32 * 8
                + (name[i + 4] - b'0') as u32;
            if let Ok(byte) = u8::try_from(value) {
                out.push(byte);
                i += 5;
                continue;
            }
        }
        out.push(name[i]);
        i += 1;
    }
    out
}

fn parse_mtime(text: &str) -> Option<i64> {
    chrono::NaiveDateTime::parse_from_str(text.trim(), "%Y/%m/%d-%H:%M:%S")
        .ok()
        .map(|dt| dt.and_utc().timestamp())
}

/// Parse one `--out-format=%i|%l|%M|%n` line of the diff pass.
fn parse_line(line: &[u8]) -> Line {
    let mut fields = line.splitn(4, |b| *b == b'|');
    let (Some(item), Some(size), Some(mtime), Some(name)) =
        (fields.next(), fields.next(), fields.next(), fields.next())
    else {
        return Line::Other;
    };
    if item.starts_with(b"*deleting") {
        return Line::Deleted(unescape_name(name));
    }
    // A regular file that does not exist at all in the reference.
    if item.len() == 11 && item.starts_with(b">f") && item[2..].iter().all(|b| *b == b'+') {
        let size = std::str::from_utf8(size)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok());
        let mtime = std::str::from_utf8(mtime).ok().and_then(parse_mtime);
        if let (Some(size), Some(mtime)) = (size, mtime) {
            return Line::New(NewFile {
                path: unescape_name(name),
                size,
                mtime,
            });
        }
    }
    Line::Other
}

fn safe_relative(path: &[u8]) -> bool {
    !path.is_empty()
        && Path::new(OsStr::from_bytes(path))
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
}

fn file_name_bytes(path: &[u8]) -> Vec<u8> {
    Path::new(OsStr::from_bytes(path))
        .file_name()
        .map(|n| n.as_bytes().to_vec())
        .unwrap_or_default()
}

/// A read-only rsync run, started at low priority.
fn rsync_command(args: &[&OsStr]) -> Command {
    let mut cmd = Command::new("nice");
    cmd.args(["-n", "19", "ionice", "-c", "3", "-n7", "rsync"])
        .args(args)
        // rsync prints modification times in its own time zone.
        .env("TZ", "UTC")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

/// Run `cmd`, passing each output line to `on_line` as it arrives. Stops the
/// process and returns the reason if `on_line` fails, the time limit passes,
/// or rsync exits with an error (exit code 24, files vanished, is normal).
fn stream_lines(
    mut cmd: Command,
    deadline: Instant,
    mut on_line: impl FnMut(&[u8]) -> Result<(), String>,
) -> Result<(), String> {
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("could not start rsync: {}", e))?;
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let reader = std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        loop {
            let mut line = Vec::new();
            match reader.read_until(b'\n', &mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    while matches!(line.last(), Some(b'\n') | Some(b'\r')) {
                        line.pop();
                    }
                    if tx.send(line).is_err() {
                        break;
                    }
                }
            }
        }
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = BufReader::new(stderr).take(8192).read_to_string(&mut text);
        text
    });

    let abandon = |child: &mut std::process::Child, reason: String| -> String {
        let _ = child.kill();
        let _ = child.wait();
        reason
    };
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(line) => {
                if let Err(reason) = on_line(&line) {
                    return Err(abandon(&mut child, reason));
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(abandon(&mut child, "the time limit passed".to_string()));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    let _ = reader.join();
    let status = child
        .wait()
        .map_err(|e| format!("waiting for rsync failed: {}", e))?;
    let stderr_text = stderr_reader.join().unwrap_or_default();
    match status.code() {
        Some(0) | Some(24) => Ok(()),
        code => Err(format!(
            "rsync exited with code {}: {}",
            code.map_or("signal".to_string(), |c| c.to_string()),
            stderr_text.trim()
        )),
    }
}

/// Run the real rsync command as a dry run against the previous generation to
/// learn what is new at the source and what vanished from it.
///
/// Returns `Err(reason)` when it fails or exceeds the change or time limit.
pub fn collect_changes(
    source: &str,
    excludes_file: &Path,
    rsync_extra: &[String],
    prev_root: &Path,
    max_changes: usize,
    deadline: Instant,
) -> Result<Changes, String> {
    let exclude_from = format!("--exclude-from={}", excludes_file.display());
    let source = normalize_rsync_source(source);
    let target = format!("{}/", prev_root.display());
    let fixed = [
        "-ar",
        "-n",
        "--delete",
        "--delete-excluded",
        exclude_from.as_str(),
    ];
    let mut args: Vec<&OsStr> = fixed.iter().map(|a| OsStr::new(*a)).collect();
    args.extend(rsync_extra.iter().map(OsStr::new));
    // Last, so it wins over anything in the user's rsync options.
    args.push(OsStr::new("--out-format=%i|%l|%M|%n"));
    args.push(OsStr::new(&source));
    args.push(OsStr::new(&target));

    let mut changes = Changes::default();
    stream_lines(rsync_command(&args), deadline, |line| {
        match parse_line(line) {
            Line::New(file) => changes.new.push(file),
            Line::Deleted(path) => changes.deleted.push(path),
            Line::Other => {}
        }
        if changes.new.len() + changes.deleted.len() > max_changes {
            return Err(format!("more than {} new or vanished files", max_changes));
        }
        Ok(())
    })
    .map_err(|reason| {
        if reason.starts_with("rsync exited") {
            format!("the dry run failed: {}", reason)
        } else {
            reason
        }
    })?;
    Ok(changes)
}

/// How rsync's own checksum comparison judged one pairing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// Same content and same preserved attributes: rsync would link it.
    Usable,
    /// Different content, or different attributes rsync would not link across.
    Rejected,
}

/// Classify one `-ii --out-format=%i|...` line from a checksum comparison.
fn parse_verdict(line: &[u8]) -> Option<(Vec<u8>, Verdict)> {
    let mut fields = line.splitn(4, |b| *b == b'|');
    let (item, _size, _mtime, name) = (
        fields.next()?,
        fields.next()?,
        fields.next()?,
        fields.next()?,
    );
    if item.len() != 11 || item[1] != b'f' {
        return None; // directories, symlinks and noise
    }
    // Positions: 0 update type, 1 file type, 2 checksum, 3 size, 4 time,
    // 5 permissions, 6 owner, 7 group.
    let differs = item[0] == b'>' || item[2] == b'c' || item[3] == b's';
    let attrs = matches!(item[5], b'p') || matches!(item[6], b'o') || matches!(item[7], b'g');
    let verdict = if differs || attrs {
        Verdict::Rejected
    } else {
        Verdict::Usable
    };
    Some((unescape_name(name), verdict))
}

/// Ask rsync to compare whole-file checksums between the source and the
/// shadow directory for `paths`, in a read-only run. The shadow files are hard
/// links to the old files, so this compares the source with the old data.
/// Used for remote sources, where the checksum is computed on the remote host
/// by the same rsync that is already required: nothing is uploaded or run as
/// root, and file names never pass through a shell.
fn verify_with_rsync(
    source: &str,
    shadow: &Path,
    paths: &[Vec<u8>],
    list_file: &Path,
    deadline: Instant,
) -> Result<HashMap<Vec<u8>, Verdict>, String> {
    let mut list = Vec::new();
    for path in paths {
        list.extend_from_slice(path);
        list.push(0);
    }
    fs::write(list_file, &list)
        .map_err(|e| format!("could not write the candidate list: {}", e))?;
    let files_from = format!("--files-from={}", list_file.display());
    let source = normalize_rsync_source(source);
    let target = format!("{}/", shadow.display());
    let fixed = [
        "-n",
        "-c",
        "-ii",
        "-l",
        "-p",
        "-t",
        "-g",
        "-o",
        "--from0",
        files_from.as_str(),
        "--out-format=%i|%l|%M|%n",
        source.as_str(),
        target.as_str(),
    ];
    let args: Vec<&OsStr> = fixed.iter().map(|a| OsStr::new(*a)).collect();
    let mut verdicts = HashMap::new();
    let result = stream_lines(rsync_command(&args), deadline, |line| {
        if let Some((name, verdict)) = parse_verdict(line) {
            verdicts.insert(name, verdict);
        }
        Ok(())
    });
    let _ = fs::remove_file(list_file);
    result.map(|()| verdicts)
}

/// A pairing of a new path with the old file whose data it can reuse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pair {
    pub new: PathBuf,
    pub old: PathBuf,
    pub size: u64,
    /// Whether the content was compared (as opposed to a unique move
    /// accepted on size and modification time).
    pub verified: bool,
}

pub struct MatchInput<'a> {
    pub cfg: &'a RelocationConfig,
    /// The source directory when it can be read locally (hashing allowed).
    pub local_root: Option<&'a Path>,
    /// The previous generation the index describes.
    pub prev_root: &'a Path,
    pub deadline: Instant,
}

fn mtime_secs(mtime_ns: i64) -> i64 {
    mtime_ns.div_euclid(1_000_000_000)
}

/// What is known about a new file at a local source: its content hash and mode.
struct SourceDigest {
    hash: String,
    mode: u32,
}

/// Hash a new file at a local source, making sure it did not change while it
/// was read and still has the size and modification time rsync reported.
fn source_digest(input: &MatchInput<'_>, new: &NewFile) -> Option<SourceDigest> {
    let root = input.local_root?;
    let path = root.join(OsStr::from_bytes(&new.path));
    let before = fs::symlink_metadata(&path).ok()?;
    if !before.is_file() || before.len() != new.size || before.mtime() != new.mtime {
        return None;
    }
    let hash = sha256_file(&path).ok()?;
    let after = fs::symlink_metadata(&path).ok()?;
    if after.len() != before.len()
        || after.mtime() != before.mtime()
        || after.mtime_nsec() != before.mtime_nsec()
    {
        return None;
    }
    Some(SourceDigest {
        hash,
        mode: before.mode(),
    })
}

/// Hash every record of a group of old files once, in a single transaction,
/// and index them by hash. Entries are positions in `records`. A file that
/// changed since it was indexed is left out. Stops early at the deadline.
fn hash_group(
    input: &MatchInput<'_>,
    index: &mut DiskIndex,
    records: &[&FileRecord],
    hit_deadline: &mut bool,
) -> Result<HashMap<String, Vec<usize>>, IndexError> {
    index.batch(|index| {
        let mut by_hash: HashMap<String, Vec<usize>> = HashMap::new();
        for (position, record) in records.iter().enumerate() {
            if Instant::now() > input.deadline {
                *hit_deadline = true;
                break;
            }
            match index.ensure_hash(Role::Previous, &record.path, input.prev_root) {
                Ok(hash) => by_hash.entry(hash).or_default().push(position),
                Err(IndexError::Stale(_)) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(by_hash)
    })
}

/// Pair new files with reusable old ones. See the module documentation for
/// the evidence each kind of pairing relies on.
///
/// With a local source, files are grouped by size and modification time, each
/// group is hashed once, and pairs are made by equal content, so the work is
/// linear however many files share a size. Without one, only unique moves are
/// paired.
pub fn match_changes(
    input: &MatchInput<'_>,
    index: &mut DiskIndex,
    changes: &Changes,
) -> Result<(Vec<Pair>, Stats), IndexError> {
    let cfg = input.cfg;
    let hash_ok = input.local_root.is_some() && cfg.verify != Verify::SizeMtime;
    let mut stats = Stats::default();

    let mut gone: Vec<FileRecord> = Vec::new();
    for path in &changes.deleted {
        let rel = Path::new(OsStr::from_bytes(path));
        if let Some(record) = index.find_by_path(Role::Previous, rel)? {
            if record.kind == 'f' && record.size >= cfg.min_size {
                gone.push(record);
            }
        }
    }
    stats.vanished_files = gone.len();

    let mut news: Vec<&NewFile> = changes
        .new
        .iter()
        .filter(|n| n.size >= cfg.min_size && safe_relative(&n.path))
        .collect();
    news.sort_by(|a, b| a.path.cmp(&b.path));
    stats.new_files = news.len();

    let mut gone_by_name: HashMap<(Vec<u8>, u64, i64), Vec<usize>> = HashMap::new();
    let mut gone_by_key: HashMap<(u64, i64), Vec<usize>> = HashMap::new();
    for (i, record) in gone.iter().enumerate() {
        let secs = mtime_secs(record.mtime_ns);
        gone_by_name
            .entry((record.name.clone(), record.size, secs))
            .or_default()
            .push(i);
        gone_by_key.entry((record.size, secs)).or_default().push(i);
    }
    let mut new_by_name: HashMap<(Vec<u8>, u64, i64), usize> = HashMap::new();
    let mut new_by_key: HashMap<(u64, i64), usize> = HashMap::new();
    for n in &news {
        *new_by_name
            .entry((file_name_bytes(&n.path), n.size, n.mtime))
            .or_default() += 1;
        *new_by_key.entry((n.size, n.mtime)).or_default() += 1;
    }

    let mut pairs: Vec<Pair> = Vec::new();
    let mut claimed: HashSet<usize> = HashSet::new();
    let mut paired_new: HashSet<usize> = HashSet::new();
    let mut digests: HashMap<usize, Option<SourceDigest>> = HashMap::new();
    let mut gone_groups: HashMap<(u64, i64), HashMap<String, Vec<usize>>> = HashMap::new();

    // Moves and renames: the old path vanished from the source.
    for (ni, n) in news.iter().enumerate() {
        if Instant::now() > input.deadline {
            stats.hit_deadline = true;
            break;
        }
        let name = file_name_bytes(&n.path);
        let key = (n.size, n.mtime);
        let Some(all) = gone_by_key.get(&key) else {
            continue;
        };
        // Content is compared when the source is readable here and the file
        // is not too large; a file above the limit is trusted as a unique move
        // only in the default tier, and skipped when content proof is demanded.
        let use_hash = hash_ok && cfg.may_hash(n.size);
        let allow_unique = !use_hash && (!hash_ok || cfg.verify != Verify::Hash);
        if !use_hash && !allow_unique {
            stats.skipped_large += 1;
            continue;
        }
        if hash_ok && !use_hash {
            stats.skipped_large += 1;
        }
        let chosen: Option<usize> = if use_hash {
            if all.len() > MAX_GROUP {
                stats.ambiguous += 1;
                continue;
            }
            let Some(digest) = digests
                .entry(ni)
                .or_insert_with(|| source_digest(input, n))
                .as_ref()
            else {
                stats.rejected += 1;
                continue;
            };
            if !gone_groups.contains_key(&key) {
                let records: Vec<&FileRecord> = all.iter().map(|&i| &gone[i]).collect();
                let group = hash_group(input, index, &records, &mut stats.hit_deadline)?;
                gone_groups.insert(key, group);
            }
            let found = gone_groups
                .get(&key)
                .and_then(|group| group.get(&digest.hash))
                .and_then(|positions| {
                    positions
                        .iter()
                        .map(|&p| all[p])
                        .filter(|i| !claimed.contains(i) && gone[*i].mode == digest.mode)
                        // Prefer the file with the same name, then the first path.
                        .min_by_key(|i| (gone[*i].name != name, gone[*i].path.clone()))
                });
            if found.is_some() {
                stats.verified += 1;
            } else {
                stats.rejected += 1;
            }
            found
        } else {
            // No way to read the content: only a unique move is trusted.
            let unclaimed: Vec<usize> = all
                .iter()
                .copied()
                .filter(|i| !claimed.contains(i))
                .collect();
            let same_name: Vec<usize> = gone_by_name
                .get(&(name.clone(), n.size, n.mtime))
                .map(|v| v.iter().copied().filter(|i| !claimed.contains(i)).collect())
                .unwrap_or_default();
            let unique = if !same_name.is_empty() {
                (same_name.len() == 1
                    && new_by_name.get(&(name.clone(), n.size, n.mtime)).copied() == Some(1))
                .then(|| same_name[0])
            } else {
                (unclaimed.len() == 1 && new_by_key.get(&key).copied() == Some(1))
                    .then(|| unclaimed[0])
            };
            if unique.is_none() && !unclaimed.is_empty() {
                stats.ambiguous += 1;
            }
            unique
        };
        if let Some(i) = chosen {
            claimed.insert(i);
            paired_new.insert(ni);
            if gone[i].name == name {
                stats.moved += 1;
            } else {
                stats.renamed += 1;
            }
            pairs.push(Pair {
                new: PathBuf::from(OsStr::from_bytes(&n.path)),
                old: gone[i].path.clone(),
                size: n.size,
                verified: use_hash,
            });
        }
    }

    // Copies: the old file still exists, so only identical content justifies
    // sharing it, which needs a local source to read.
    if hash_ok {
        let mut kept_groups: HashMap<(u64, i64), (Vec<FileRecord>, HashMap<String, Vec<usize>>)> =
            HashMap::new();
        for (ni, n) in news.iter().enumerate() {
            if paired_new.contains(&ni) || !cfg.may_hash(n.size) {
                continue;
            }
            if Instant::now() > input.deadline {
                stats.hit_deadline = true;
                break;
            }
            let key = (n.size, n.mtime);
            if !kept_groups.contains_key(&key) {
                let records: Vec<FileRecord> = index
                    .find_by_size(Role::Previous, n.size)?
                    .into_iter()
                    .filter(|r| r.kind == 'f' && mtime_secs(r.mtime_ns) == n.mtime)
                    .collect();
                let group = if records.is_empty() || records.len() > MAX_GROUP {
                    HashMap::new()
                } else {
                    let refs: Vec<&FileRecord> = records.iter().collect();
                    hash_group(input, index, &refs, &mut stats.hit_deadline)?
                };
                kept_groups.insert(key, (records, group));
            }
            let (records, group) = &kept_groups[&key];
            if group.is_empty() {
                continue;
            }
            let Some(digest) = digests
                .entry(ni)
                .or_insert_with(|| source_digest(input, n))
                .as_ref()
            else {
                continue;
            };
            let found = group.get(&digest.hash).and_then(|positions| {
                positions
                    .iter()
                    .map(|&p| &records[p])
                    .filter(|r| {
                        r.mode == digest.mode && r.path.as_os_str().as_bytes() != n.path.as_slice()
                    })
                    .min_by_key(|r| r.path.clone())
            });
            if let Some(record) = found {
                stats.copies += 1;
                stats.verified += 1;
                pairs.push(Pair {
                    new: PathBuf::from(OsStr::from_bytes(&n.path)),
                    old: record.path.clone(),
                    size: n.size,
                    verified: true,
                });
            }
        }
    }
    Ok((pairs, stats))
}

/// Create the shadow directory under `dest` with a hard link at each new path
/// to the old file in the previous generation. A link that cannot be made is
/// skipped. Returns the directory when at least one link exists.
pub fn build_shadow(
    dest: &Path,
    prev_root: &Path,
    pairs: &[Pair],
    stats: &mut Stats,
) -> io::Result<Option<PathBuf>> {
    let shadow = dest.join(RELOC_NAME);
    match fs::remove_dir_all(&shadow) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    for pair in pairs {
        let target = shadow.join(&pair.new);
        let linked = target
            .parent()
            .map_or(Ok(()), fs::create_dir_all)
            .and_then(|()| fs::hard_link(prev_root.join(&pair.old), &target));
        match linked {
            Ok(()) => {
                stats.linked += 1;
                stats.bytes_reused += pair.size;
            }
            Err(_) => stats.link_failures += 1,
        }
    }
    Ok((stats.linked > 0).then_some(shadow))
}

/// Remove the shadow directory. It holds only hard links, so this never
/// touches data that other generations still reference.
pub fn cleanup(dest: &Path) {
    match fs::remove_dir_all(dest.join(RELOC_NAME)) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => crate::pnote!("warning: could not remove {}: {}", RELOC_NAME, err),
    }
}

/// The directory part of a relative path (empty for a top-level file).
fn parent_of(path: &Path) -> PathBuf {
    path.parent().map(Path::to_path_buf).unwrap_or_default()
}

/// Decide, for pairings made without reading content, which to trust as they
/// are and which to confirm with rsync's checksum.
///
/// - A folder move (at least [`GROUP_MIN`] pairings mapping the same old
///   directory to the same new directory) is strong collective evidence, so
///   those pairings are trusted in the default tier.
/// - A lone pairing is confirmed by checksum when the file is within
///   `hashMaxSize`.
/// - A file above the limit is accepted as a unique move in the default tier
///   (a byte-exact size on a huge file is strong evidence) and skipped under
///   `verify: hash`, which only offers content-checked files.
/// - `verify: size-mtime` trusts everything.
///
/// Returns the pairings to offer and the new paths among them to confirm.
fn select_unverified(
    cfg: &RelocationConfig,
    pairs: Vec<Pair>,
    stats: &mut Stats,
) -> (Vec<Pair>, HashSet<PathBuf>) {
    let mut group_sizes: HashMap<(PathBuf, PathBuf), usize> = HashMap::new();
    for pair in pairs.iter().filter(|pair| !pair.verified) {
        *group_sizes
            .entry((parent_of(&pair.old), parent_of(&pair.new)))
            .or_default() += 1;
    }
    let mut kept = Vec::with_capacity(pairs.len());
    let mut confirm = HashSet::new();
    for pair in pairs {
        if pair.verified || cfg.verify == Verify::SizeMtime {
            kept.push(pair);
            continue;
        }
        let grouped = group_sizes
            .get(&(parent_of(&pair.old), parent_of(&pair.new)))
            .copied()
            .unwrap_or(0)
            >= GROUP_MIN;
        if cfg.verify == Verify::Auto && grouped {
            stats.group_supported += 1;
            kept.push(pair);
        } else if cfg.may_hash(pair.size) {
            confirm.insert(pair.new.clone());
            kept.push(pair);
        } else if cfg.verify == Verify::Auto {
            stats.skipped_large += 1;
            kept.push(pair);
        } else {
            stats.skipped_large += 1;
        }
    }
    (kept, confirm)
}

/// Build the shadow directory for `pairs`, then keep only the links whose
/// content rsync confirms (by whole-file checksum) is the same as the source.
fn confirm_remote(
    p: &Prepare<'_>,
    pairs: &[Pair],
    confirm: &HashSet<PathBuf>,
    deadline: Instant,
    stats: &mut Stats,
) -> Result<Option<PathBuf>, String> {
    let shadow = build_shadow(p.dest, p.prev_root, pairs, stats)
        .map_err(|e| format!("could not build {}: {}", RELOC_NAME, e))?;
    let Some(shadow) = shadow else {
        return Ok(None);
    };
    let linked: Vec<&Pair> = pairs
        .iter()
        .filter(|pair| confirm.contains(&pair.new) && shadow.join(&pair.new).exists())
        .collect();
    let paths: Vec<Vec<u8>> = linked
        .iter()
        .map(|pair| pair.new.as_os_str().as_bytes().to_vec())
        .collect();
    let list_file = p.dest.join(format!("{}.list", RELOC_NAME));
    let verdicts = verify_with_rsync(p.source, &shadow, &paths, &list_file, deadline)?;
    for pair in linked {
        if verdicts.get(pair.new.as_os_str().as_bytes()) == Some(&Verdict::Usable) {
            stats.verified += 1;
            continue;
        }
        // Unconfirmed: take the link back out so rsync copies the file.
        let _ = fs::remove_file(shadow.join(&pair.new));
        stats.linked -= 1;
        stats.bytes_reused -= pair.size;
        stats.rejected += 1;
    }
    if stats.linked == 0 {
        cleanup(p.dest);
        return Ok(None);
    }
    Ok(Some(shadow))
}

pub struct Prepare<'a> {
    pub cfg: &'a RelocationConfig,
    pub source: &'a str,
    /// The source directory when it can be read locally.
    pub local_root: Option<&'a Path>,
    pub excludes_file: &'a Path,
    pub rsync_extra: &'a [String],
    /// The previous generation (the first `--link-dest`), which the index
    /// must already describe.
    pub prev_root: &'a Path,
    /// The job directory.
    pub dest: &'a Path,
}

#[cfg(test)]
thread_local! {
    static HOOK: std::cell::RefCell<Option<Box<dyn FnMut()>>> = const { std::cell::RefCell::new(None) };
}

/// Test hook run after planning and before rsync, to change the source in
/// between and check that the final result is still correct.
#[cfg(test)]
pub fn set_test_hook(hook: impl FnMut() + 'static) -> TestHookGuard {
    HOOK.with(|h| *h.borrow_mut() = Some(Box::new(hook)));
    TestHookGuard
}

#[cfg(test)]
pub struct TestHookGuard;

#[cfg(test)]
impl Drop for TestHookGuard {
    fn drop(&mut self) {
        HOOK.with(|h| *h.borrow_mut() = None);
    }
}

fn run_test_hook() {
    #[cfg(test)]
    HOOK.with(|h| {
        if let Some(hook) = h.borrow_mut().as_mut() {
            hook();
        }
    });
}

/// Plan relocations for the next run and build the shadow directory.
///
/// Never fails the backup: every problem becomes [`Outcome::Skipped`], and an
/// index error is returned for the caller to switch the index off.
pub fn prepare(p: &Prepare<'_>, index: &mut DiskIndex) -> Result<Outcome, IndexError> {
    let deadline = Instant::now() + Duration::from_secs(p.cfg.timeout_secs);
    let changes = match collect_changes(
        p.source,
        p.excludes_file,
        p.rsync_extra,
        p.prev_root,
        p.cfg.max_changes,
        deadline,
    ) {
        Ok(changes) => changes,
        Err(reason) => return Ok(Outcome::Skipped(reason)),
    };
    let input = MatchInput {
        cfg: p.cfg,
        local_root: p.local_root,
        prev_root: p.prev_root,
        deadline,
    };
    let (pairs, mut stats) = match_changes(&input, index, &changes)?;
    // Pairings made on size and modification time alone (a remote source
    // cannot be read here) are either trusted as a folder move, confirmed by
    // rsync's own checksum, or, for files above the size limit, accepted as a
    // unique move or skipped, depending on `verify`.
    let (pairs, confirm) = if p.local_root.is_none() {
        select_unverified(p.cfg, pairs, &mut stats)
    } else {
        (pairs, HashSet::new())
    };
    let shadow = if confirm.is_empty() {
        match build_shadow(p.dest, p.prev_root, &pairs, &mut stats) {
            Ok(shadow) => shadow,
            Err(err) => {
                return Ok(Outcome::Skipped(format!(
                    "could not build {}: {}",
                    RELOC_NAME, err
                )))
            }
        }
    } else {
        match confirm_remote(p, &pairs, &confirm, deadline, &mut stats) {
            Ok(shadow) => shadow,
            Err(reason) => {
                cleanup(p.dest);
                return Ok(Outcome::Skipped(format!(
                    "could not confirm moved files: {}",
                    reason
                )));
            }
        }
    };
    run_test_hook();
    Ok(Outcome::Prepared { shadow, stats })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::generation::COMPLETE_MARKER;
    use crate::config::model::RelocationSettings;
    use std::os::unix::fs::PermissionsExt;

    fn write(path: &Path, text: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }

    fn set_mtime(path: &Path, stamp: &str) {
        let status = Command::new("touch")
            .args(["-d", stamp])
            .arg(path)
            .status()
            .unwrap();
        assert!(status.success());
    }

    fn ino(path: &Path) -> u64 {
        fs::metadata(path).unwrap().ino()
    }

    fn settings(verify: Option<&str>) -> RelocationConfig {
        RelocationConfig::from_option(Some(&RelocationOption::Settings(RelocationSettings {
            min_size: Some(1),
            verify: verify.map(str::to_string),
            ..Default::default()
        })))
        .unwrap()
    }

    /// A source tree and its previous generation (a faithful copy), with the
    /// disk-local index describing that generation.
    struct World {
        _tmp: tempfile::TempDir,
        source: PathBuf,
        dest: PathBuf,
        prev: PathBuf,
        excludes: PathBuf,
        index: DiskIndex,
    }

    fn world(files: &[(&str, &str)]) -> World {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        let dest = tmp.path().join("disk/job");
        let prev = dest.join("20260101");
        fs::create_dir_all(&source).unwrap();
        for (rel, text) in files {
            write(&source.join(rel), text);
            set_mtime(&source.join(rel), "2020-05-05 10:00:00");
        }
        // The previous generation is a faithful copy: same modes and mtimes.
        fs::create_dir_all(&dest).unwrap();
        let status = Command::new("cp")
            .args(["-a", "-T"])
            .arg(&source)
            .arg(&prev)
            .status()
            .unwrap();
        assert!(status.success());
        write(&prev.join(COMPLETE_MARKER), "token-1");
        let excludes = tmp.path().join("excludes");
        fs::write(&excludes, "").unwrap();
        let mut index = DiskIndex::open(&tmp.path().join("disk"), "job").unwrap();
        index.ensure_previous(Some(&prev)).unwrap();
        World {
            _tmp: tmp,
            source,
            dest,
            prev,
            excludes,
            index,
        }
    }

    impl World {
        fn plan(&mut self, cfg: &RelocationConfig, local: bool) -> Outcome {
            let source = self.source.to_string_lossy().to_string();
            let request = Prepare {
                cfg,
                source: &source,
                local_root: local.then_some(self.source.as_path()),
                excludes_file: &self.excludes,
                rsync_extra: &[],
                prev_root: &self.prev,
                dest: &self.dest,
            };
            prepare(&request, &mut self.index).unwrap()
        }

        fn planned(&mut self, cfg: &RelocationConfig, local: bool) -> (Option<PathBuf>, Stats) {
            match self.plan(cfg, local) {
                Outcome::Prepared { shadow, stats } => (shadow, stats),
                Outcome::Skipped(reason) => panic!("skipped: {reason}"),
            }
        }
    }

    // ---- parsing ----

    #[test]
    fn rsync_name_escapes_are_undone() {
        assert_eq!(unescape_name(b"plain.txt"), b"plain.txt");
        assert_eq!(unescape_name(b"lat\\#351.txt"), b"lat\xe9.txt");
        assert_eq!(unescape_name(b"a\\#012b"), b"a\nb");
        // Not an escape: left as written.
        assert_eq!(unescape_name(b"back\\slash"), b"back\\slash");
        assert_eq!(unescape_name(b"cut\\#35"), b"cut\\#35");
        assert_eq!(unescape_name(b"bad\\#999"), b"bad\\#999");
    }

    #[test]
    fn diff_lines_are_classified() {
        assert_eq!(
            parse_line(b">f+++++++++|1234|2026/10/19-21:55:24|dir/new.txt"),
            Line::New(NewFile {
                path: b"dir/new.txt".to_vec(),
                size: 1234,
                mtime: 1_792_446_924,
            })
        );
        assert_eq!(
            parse_line(b"*deleting  |0|1970/01/01-00:00:00|gone/old.txt"),
            Line::Deleted(b"gone/old.txt".to_vec())
        );
        // A name containing the separator survives.
        assert_eq!(
            parse_line(b">f+++++++++|1|2026/10/19-21:55:24|odd|name"),
            Line::New(NewFile {
                path: b"odd|name".to_vec(),
                size: 1,
                mtime: 1_792_446_924,
            })
        );
        // Changed files, directories, symlinks and noise are not "new files".
        assert_eq!(
            parse_line(b">f.s.......|5|2026/10/19-21:55:24|chg.txt"),
            Line::Other
        );
        assert_eq!(
            parse_line(b"cd+++++++++|4096|2026/10/19-21:55:24|B/"),
            Line::Other
        );
        assert_eq!(
            parse_line(b"cL+++++++++|3|2026/10/19-21:55:24|ln"),
            Line::Other
        );
        assert_eq!(parse_line(b"sending incremental file list"), Line::Other);
        assert_eq!(
            parse_line(b">f+++++++++|x|2026/10/19-21:55:24|bad"),
            Line::Other
        );
    }

    #[test]
    fn only_plain_relative_paths_are_trusted() {
        assert!(safe_relative(b"a/b.txt"));
        assert!(!safe_relative(b""));
        assert!(!safe_relative(b"../escape"));
        assert!(!safe_relative(b"/absolute"));
        assert!(!safe_relative(b"a/../../b"));
    }

    // ---- configuration ----

    #[test]
    fn relocation_config_resolves_flags_settings_and_job_overrides() {
        let off = RelocationConfig::from_option(None).unwrap();
        assert!(!off.enabled);
        assert_eq!(off.min_size, DEFAULT_MIN_SIZE);

        let on = RelocationConfig::from_option(Some(&RelocationOption::Flag(true))).unwrap();
        assert!(on.enabled);
        assert_eq!(on.verify, Verify::Auto);

        let tuned =
            RelocationConfig::from_option(Some(&RelocationOption::Settings(RelocationSettings {
                min_size: Some(10),
                max_changes: Some(5),
                timeout_seconds: Some(7),
                verify: Some("hash".into()),
                hash_max_size: Some(123),
                ..Default::default()
            })))
            .unwrap();
        assert!(tuned.enabled, "a settings block enables by default");
        assert_eq!(tuned.hash_max_size, 123);
        assert_eq!(
            RelocationConfig::default().hash_max_size,
            DEFAULT_HASH_MAX_SIZE
        );
        let unlimited = RelocationConfig {
            hash_max_size: 0,
            ..tuned
        };
        assert!(unlimited.may_hash(u64::MAX));
        assert!(tuned.may_hash(123) && !tuned.may_hash(124));
        assert_eq!(
            (
                tuned.min_size,
                tuned.max_changes,
                tuned.timeout_secs,
                tuned.verify
            ),
            (10, 5, 7, Verify::Hash)
        );
        let disabled =
            RelocationConfig::from_option(Some(&RelocationOption::Settings(RelocationSettings {
                enabled: Some(false),
                ..Default::default()
            })))
            .unwrap();
        assert!(!disabled.enabled);

        assert!(
            RelocationConfig::from_option(Some(&RelocationOption::Settings(RelocationSettings {
                verify: Some("bogus".into()),
                ..Default::default()
            })))
            .is_err()
        );

        assert!(off.for_job(Some(true)).enabled);
        assert!(!on.for_job(Some(false)).enabled);
        assert!(on.for_job(None).enabled);
    }

    #[test]
    fn relocation_yaml_accepts_a_flag_or_a_settings_block() {
        let flag: crate::config::model::ConfigOptions =
            serde_yaml::from_str("relocation: true").unwrap();
        assert_eq!(flag.relocation, Some(RelocationOption::Flag(true)));
        let block: crate::config::model::ConfigOptions =
            serde_yaml::from_str("relocation:\n  minSize: 4096\n  verify: hash\n").unwrap();
        let Some(RelocationOption::Settings(settings)) = block.relocation else {
            panic!("expected settings");
        };
        assert_eq!(
            (settings.min_size, settings.verify.as_deref()),
            (Some(4096), Some("hash"))
        );
        assert!(serde_yaml::from_str::<crate::config::model::ConfigOptions>(
            "relocation:\n  nonsense: 1\n"
        )
        .is_err());
    }

    // ---- matching against real rsync output ----

    #[test]
    fn a_moved_directory_is_found_and_linked_by_name() {
        let mut w = world(&[
            ("old/a.bin", "alpha-data"),
            ("old/b.bin", "bravo-data!"),
            ("keep.bin", "keep"),
        ]);
        fs::create_dir_all(w.source.join("new")).unwrap();
        fs::rename(w.source.join("old/a.bin"), w.source.join("new/a.bin")).unwrap();
        fs::rename(w.source.join("old/b.bin"), w.source.join("new/b.bin")).unwrap();
        fs::remove_dir(w.source.join("old")).unwrap();

        let (shadow, stats) = w.planned(&settings(None), true);

        let shadow = shadow.expect("shadow");
        assert_eq!(
            (stats.moved, stats.renamed, stats.copies, stats.linked),
            (2, 0, 0, 2)
        );
        assert_eq!(
            ino(&shadow.join("new/a.bin")),
            ino(&w.prev.join("old/a.bin"))
        );
        assert_eq!(
            ino(&shadow.join("new/b.bin")),
            ino(&w.prev.join("old/b.bin"))
        );
        // Unchanged files are left to rsync's own path matching.
        assert!(!shadow.join("keep.bin").exists());
        assert_eq!(stats.bytes_reused, 10 + 11);
    }

    #[test]
    fn a_renamed_file_is_found_by_size_and_mtime() {
        let mut w = world(&[("a.bin", "alpha-data"), ("b.bin", "other-content")]);
        fs::rename(w.source.join("a.bin"), w.source.join("renamed.bin")).unwrap();

        let (shadow, stats) = w.planned(&settings(None), true);

        assert_eq!((stats.moved, stats.renamed, stats.linked), (0, 1, 1));
        assert_eq!(
            ino(&shadow.unwrap().join("renamed.bin")),
            ino(&w.prev.join("a.bin"))
        );
    }

    #[test]
    fn a_file_both_moved_and_renamed_is_found() {
        let mut w = world(&[("x/a.bin", "alpha-data")]);
        fs::create_dir_all(w.source.join("y/z")).unwrap();
        fs::rename(w.source.join("x/a.bin"), w.source.join("y/z/c.bin")).unwrap();

        let (shadow, stats) = w.planned(&settings(None), true);

        assert_eq!(stats.renamed, 1);
        assert_eq!(
            ino(&shadow.unwrap().join("y/z/c.bin")),
            ino(&w.prev.join("x/a.bin"))
        );
    }

    #[test]
    fn an_isolated_remote_move_is_confirmed_by_checksum_by_default() {
        let mut w = world(&[("a.bin", "alpha-data"), ("b.bin", "bravo-data!")]);
        fs::rename(w.source.join("a.bin"), w.source.join("moved-a.bin")).unwrap();

        let (shadow, stats) = w.planned(&settings(None), false);

        assert_eq!(
            (stats.renamed, stats.verified, stats.linked),
            (1, 1, 1),
            "{stats:?}"
        );
        assert_eq!(stats.group_supported, 0);
        assert!(shadow.unwrap().join("moved-a.bin").exists());
    }

    #[test]
    fn size_mtime_trusts_a_unique_remote_move_without_reading_anything() {
        let mut w = world(&[("a.bin", "alpha-data"), ("b.bin", "bravo-data!")]);
        fs::rename(w.source.join("a.bin"), w.source.join("moved-a.bin")).unwrap();

        let (shadow, stats) = w.planned(&settings(Some("size-mtime")), false);

        assert_eq!(
            (stats.renamed, stats.verified, stats.linked),
            (1, 0, 1),
            "{stats:?}"
        );
        assert!(shadow.unwrap().join("moved-a.bin").exists());
    }

    #[test]
    fn a_remote_folder_move_is_trusted_as_a_group_without_reading_content() {
        let names: Vec<String> = (0..4).map(|i| format!("album/track{i}.flac")).collect();
        let texts: Vec<String> = (0..4).map(|i| format!("audio-data-{i}")).collect();
        let files: Vec<(&str, &str)> = names
            .iter()
            .zip(&texts)
            .map(|(n, t)| (n.as_str(), t.as_str()))
            .collect();
        let mut w = world(&files);
        fs::rename(w.source.join("album"), w.source.join("Music Archive")).unwrap();

        let (shadow, stats) = w.planned(&settings(None), false);

        // Four files from one old folder to one new folder: collective
        // evidence, so no checksum is needed.
        assert_eq!(
            (stats.linked, stats.group_supported, stats.verified),
            (4, 4, 0),
            "{stats:?}"
        );
        let shadow = shadow.unwrap();
        for i in 0..4 {
            assert_eq!(
                ino(&shadow.join(format!("Music Archive/track{i}.flac"))),
                ino(&w.prev.join(format!("album/track{i}.flac")))
            );
        }
    }

    #[test]
    fn two_files_are_not_enough_to_call_it_a_folder_move() {
        let mut w = world(&[
            ("album/a.flac", "audio-data-a"),
            ("album/b.flac", "audio-data-b"),
        ]);
        fs::rename(w.source.join("album"), w.source.join("renamed")).unwrap();

        let (_, stats) = w.planned(&settings(None), false);

        assert_eq!(
            (stats.group_supported, stats.verified, stats.linked),
            (0, 2, 2),
            "{stats:?}"
        );
    }

    #[test]
    fn folder_membership_is_by_old_and_new_directory_together() {
        let pairs: Vec<Pair> = [
            ("a/1", "x/1"),
            ("a/2", "x/2"),
            ("a/3", "y/3"), // same old folder, different new folder
            ("b/4", "x/4"), // same new folder, different old folder
        ]
        .iter()
        .map(|(old, new)| Pair {
            new: PathBuf::from(new),
            old: PathBuf::from(old),
            size: 10,
            verified: false,
        })
        .collect();
        let mut stats = Stats::default();

        let (kept, confirm) = select_unverified(&RelocationConfig::default(), pairs, &mut stats);

        // No (old folder, new folder) combination reaches three files.
        assert_eq!(kept.len(), 4);
        assert_eq!(confirm.len(), 4);
        assert_eq!(stats.group_supported, 0);
    }

    #[test]
    fn selection_follows_the_verify_mode_and_the_size_limit() {
        let mk = |old: &str, new: &str, size: u64| Pair {
            new: PathBuf::from(new),
            old: PathBuf::from(old),
            size,
            verified: false,
        };
        let pairs = || {
            vec![
                mk("a/small", "b/small", 100),
                mk("a/huge", "c/huge", 10_000),
            ]
        };
        let limited = |verify| RelocationConfig {
            hash_max_size: 1000,
            verify,
            ..RelocationConfig::default()
        };

        // Default: confirm the small file, trust the huge unique move.
        let mut stats = Stats::default();
        let (kept, confirm) = select_unverified(&limited(Verify::Auto), pairs(), &mut stats);
        assert_eq!(kept.len(), 2);
        assert_eq!(confirm, HashSet::from([PathBuf::from("b/small")]));
        assert_eq!(stats.skipped_large, 1);

        // Verify: hash is verified-or-nothing, so the huge file is dropped.
        let mut stats = Stats::default();
        let (kept, confirm) = select_unverified(&limited(Verify::Hash), pairs(), &mut stats);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].new, PathBuf::from("b/small"));
        assert_eq!(confirm.len(), 1);
        assert_eq!(stats.skipped_large, 1);

        // size-mtime trusts both and confirms nothing.
        let mut stats = Stats::default();
        let (kept, confirm) = select_unverified(&limited(Verify::SizeMtime), pairs(), &mut stats);
        assert_eq!((kept.len(), confirm.len()), (2, 0));

        // A limit of 0 means no limit: both are confirmed.
        let mut stats = Stats::default();
        let unlimited = RelocationConfig {
            hash_max_size: 0,
            ..RelocationConfig::default()
        };
        let (_, confirm) = select_unverified(&unlimited, pairs(), &mut stats);
        assert_eq!(confirm.len(), 2);
    }

    #[test]
    fn a_huge_remote_file_is_trusted_as_a_unique_move_but_not_under_verify_hash() {
        let mut w = world(&[("big.bin", "this file counts as huge here")]);
        fs::rename(w.source.join("big.bin"), w.source.join("renamed.bin")).unwrap();
        let tiny_limit = |verify: &str| {
            let mut cfg = settings(Some(verify));
            cfg.hash_max_size = 5;
            cfg
        };

        let (shadow, stats) = w.planned(&tiny_limit("auto"), false);
        assert_eq!(
            (stats.linked, stats.verified, stats.skipped_large),
            (1, 0, 1),
            "{stats:?}"
        );
        assert!(shadow.is_some());

        let (shadow, stats) = w.planned(&tiny_limit("hash"), false);
        assert!(shadow.is_none());
        assert_eq!((stats.linked, stats.skipped_large), (0, 1), "{stats:?}");
    }

    #[test]
    fn a_huge_local_file_is_not_read_but_may_still_be_a_unique_move() {
        let mut w = world(&[("big.bin", "this file counts as huge here")]);
        fs::rename(w.source.join("big.bin"), w.source.join("renamed.bin")).unwrap();
        let tiny_limit = |verify: &str| {
            let mut cfg = settings(Some(verify));
            cfg.hash_max_size = 5;
            cfg
        };

        let (_, stats) = w.planned(&tiny_limit("auto"), true);
        assert_eq!((stats.linked, stats.verified), (1, 0), "{stats:?}");

        let (shadow, stats) = w.planned(&tiny_limit("hash"), true);
        assert!(shadow.is_none());
        assert_eq!(stats.linked, 0);
    }

    #[test]
    fn ambiguous_pairings_are_left_to_rsync_unless_content_decides() {
        // Two vanished and two new files with identical size and mtime.
        let mut w = world(&[("a.bin", "same-size-AA"), ("b.bin", "same-size-BB")]);
        fs::rename(w.source.join("a.bin"), w.source.join("n1.bin")).unwrap();
        fs::rename(w.source.join("b.bin"), w.source.join("n2.bin")).unwrap();

        // Without a way to compare content nothing is guessed.
        let (shadow, stats) = w.planned(&settings(None), false);
        assert!(shadow.is_none());
        assert_eq!((stats.linked, stats.ambiguous), (0, 2));

        // With a local source, content pairs each file with its own original.
        let (shadow, stats) = w.planned(&settings(None), true);
        let shadow = shadow.expect("shadow");
        assert_eq!((stats.linked, stats.verified), (2, 2));
        assert_eq!(ino(&shadow.join("n1.bin")), ino(&w.prev.join("a.bin")));
        assert_eq!(ino(&shadow.join("n2.bin")), ino(&w.prev.join("b.bin")));
    }

    #[test]
    fn many_files_with_identical_size_and_mtime_are_all_matched_by_content() {
        // Generated data: every file the same size and modification time, so
        // size and mtime say nothing about which file is which.
        let names: Vec<String> = (0..300).map(|i| format!("dir/f{i:04}.dat")).collect();
        let texts: Vec<String> = (0..300).map(|i| format!("content-{i:04}")).collect();
        let files: Vec<(&str, &str)> = names
            .iter()
            .zip(&texts)
            .map(|(n, t)| (n.as_str(), t.as_str()))
            .collect();
        let mut w = world(&files);
        for i in 0..300 {
            fs::rename(
                w.source.join(format!("dir/f{i:04}.dat")),
                w.source.join(format!("dir/renamed-{:04}.dat", 299 - i)),
            )
            .unwrap();
        }

        let (shadow, stats) = w.planned(&settings(None), true);

        let shadow = shadow.expect("shadow");
        assert_eq!(
            (stats.linked, stats.rejected, stats.ambiguous),
            (300, 0, 0),
            "{stats:?}"
        );
        // Each new name links the old file with the same content, not merely
        // one of the same size.
        for i in 0..300 {
            assert_eq!(
                ino(&shadow.join(format!("dir/renamed-{:04}.dat", 299 - i))),
                ino(&w.prev.join(format!("dir/f{i:04}.dat")))
            );
        }
        // Without content to compare, none of them can be told apart.
        let (shadow, stats) = w.planned(&settings(None), false);
        assert!(shadow.is_none());
        assert_eq!(stats.linked, 0);
    }

    #[test]
    fn a_new_file_with_different_content_is_never_taken_for_a_moved_one() {
        let mut w = world(&[("old/a.bin", "alpha-data")]);
        // The original vanished; a different file of the same size and mtime
        // appears under the same name elsewhere.
        fs::remove_file(w.source.join("old/a.bin")).unwrap();
        write(&w.source.join("new/a.bin"), "alpha-DATA");
        set_mtime(&w.source.join("new/a.bin"), "2020-05-05 10:00:00");

        let (shadow, stats) = w.planned(&settings(None), true);

        assert!(shadow.is_none());
        assert_eq!((stats.linked, stats.rejected), (0, 1));
    }

    #[test]
    fn identical_content_that_still_exists_elsewhere_is_shared_only_when_verified() {
        let mut w = world(&[("orig.bin", "duplicate-me")]);
        write(&w.source.join("copy/dup.bin"), "duplicate-me");
        set_mtime(&w.source.join("copy/dup.bin"), "2020-05-05 10:00:00");
        // Same permissions as the original, as `cp -p` would give.
        fs::set_permissions(
            w.source.join("copy/dup.bin"),
            fs::metadata(w.source.join("orig.bin"))
                .unwrap()
                .permissions(),
        )
        .unwrap();

        // Without hashing, a file whose original still exists is just a new file.
        let (shadow, stats) = w.planned(&settings(None), false);
        assert!(shadow.is_none());
        assert_eq!(stats.copies, 0);
        let (shadow, _) = w.planned(&settings(Some("size-mtime")), true);
        assert!(shadow.is_none());

        // Verified by content, it shares the existing data.
        let (shadow, stats) = w.planned(&settings(None), true);
        assert_eq!((stats.copies, stats.verified), (1, 1));
        assert_eq!(
            ino(&shadow.unwrap().join("copy/dup.bin")),
            ino(&w.prev.join("orig.bin"))
        );
    }

    #[test]
    fn a_same_size_same_mtime_copy_with_other_content_is_rejected() {
        let mut w = world(&[("orig.bin", "aaaaaaaaaaaa")]);
        write(&w.source.join("other/x.bin"), "bbbbbbbbbbbb");
        set_mtime(&w.source.join("other/x.bin"), "2020-05-05 10:00:00");
        fs::set_permissions(
            w.source.join("other/x.bin"),
            fs::metadata(w.source.join("orig.bin"))
                .unwrap()
                .permissions(),
        )
        .unwrap();

        let (shadow, stats) = w.planned(&settings(None), true);

        assert!(shadow.is_none());
        assert_eq!(stats.linked, 0);
    }

    #[test]
    fn a_moved_file_whose_mode_changed_is_not_offered() {
        let mut w = world(&[("a.bin", "alpha-data")]);
        fs::rename(w.source.join("a.bin"), w.source.join("b.bin")).unwrap();
        fs::set_permissions(w.source.join("b.bin"), fs::Permissions::from_mode(0o600)).unwrap();

        let (shadow, stats) = w.planned(&settings(None), true);

        assert!(shadow.is_none());
        assert_eq!((stats.linked, stats.rejected), (0, 1));
    }

    #[test]
    fn small_files_below_the_minimum_size_are_ignored() {
        let mut w = world(&[("tiny.bin", "t"), ("big.bin", "this one is large enough")]);
        fs::rename(w.source.join("tiny.bin"), w.source.join("t2.bin")).unwrap();
        fs::rename(w.source.join("big.bin"), w.source.join("b2.bin")).unwrap();
        let cfg = RelocationConfig {
            min_size: 10,
            ..settings(None)
        };

        let (shadow, stats) = w.planned(&cfg, true);

        assert_eq!(stats.linked, 1);
        let shadow = shadow.unwrap();
        assert!(shadow.join("b2.bin").exists());
        assert!(!shadow.join("t2.bin").exists());
    }

    #[test]
    fn non_utf8_names_are_moved_and_linked() {
        let odd = OsStr::from_bytes(b"caf\xe9.bin");
        let odd_new = OsStr::from_bytes(b"cr\xe8me.bin");
        let mut w = world(&[]);
        fs::write(w.prev.join(odd), "latin1-data").unwrap();
        set_mtime(&w.prev.join(odd), "2020-05-05 10:00:00");
        fs::write(w.source.join(odd_new), "latin1-data").unwrap();
        set_mtime(&w.source.join(odd_new), "2020-05-05 10:00:00");
        fs::set_permissions(
            w.source.join(odd_new),
            fs::metadata(w.prev.join(odd)).unwrap().permissions(),
        )
        .unwrap();
        w.index.ensure_previous(None).unwrap();
        w.index.ensure_previous(Some(&w.prev)).unwrap();

        let (shadow, stats) = w.planned(&settings(None), true);

        assert_eq!(stats.renamed, 1, "{stats:?}");
        assert_eq!(ino(&shadow.unwrap().join(odd_new)), ino(&w.prev.join(odd)));
    }

    // ---- limits and failure handling ----

    #[test]
    fn too_many_changes_abandons_the_pass() {
        let mut w = world(&[("a.bin", "aaaa"), ("b.bin", "bbbb"), ("c.bin", "cccc")]);
        for name in ["a", "b", "c"] {
            fs::rename(
                w.source.join(format!("{name}.bin")),
                w.source.join(format!("{name}2.bin")),
            )
            .unwrap();
        }
        let cfg = RelocationConfig {
            max_changes: 2,
            ..settings(None)
        };

        let Outcome::Skipped(reason) = w.plan(&cfg, true) else {
            panic!("expected the pass to be abandoned");
        };

        assert!(reason.contains("more than 2"), "{reason}");
        assert!(!w.dest.join(RELOC_NAME).exists());
    }

    #[test]
    fn an_expired_time_limit_abandons_the_pass() {
        let w = world(&[("a.bin", "aaaa")]);
        let past = Instant::now() - Duration::from_secs(1);

        let result = collect_changes(
            &w.source.to_string_lossy(),
            &w.excludes,
            &[],
            &w.prev,
            1000,
            past,
        );

        assert!(result.unwrap_err().contains("time limit"));
    }

    #[test]
    fn a_failing_dry_run_abandons_the_pass() {
        let w = world(&[("a.bin", "aaaa")]);
        let missing = w.source.join("does-not-exist");

        let result = collect_changes(
            &missing.to_string_lossy(),
            &w.excludes,
            &[],
            &w.prev,
            1000,
            Instant::now() + Duration::from_secs(30),
        );

        assert!(result.unwrap_err().contains("exited with code"));
    }

    #[test]
    fn links_that_cannot_be_made_are_skipped_not_fatal() {
        let w = world(&[("a.bin", "aaaa")]);
        let pairs = vec![
            Pair {
                new: PathBuf::from("n/ok.bin"),
                old: PathBuf::from("a.bin"),
                size: 4,
                verified: false,
            },
            Pair {
                new: PathBuf::from("n/bad.bin"),
                old: PathBuf::from("no-such-old-file"),
                size: 9,
                verified: false,
            },
        ];
        let mut stats = Stats::default();

        let shadow = build_shadow(&w.dest, &w.prev, &pairs, &mut stats)
            .unwrap()
            .unwrap();

        assert_eq!(
            (stats.linked, stats.link_failures, stats.bytes_reused),
            (1, 1, 4)
        );
        assert!(shadow.join("n/ok.bin").exists());
        assert!(!shadow.join("n/bad.bin").exists());
    }

    #[test]
    fn nothing_linked_means_no_shadow_directory() {
        let w = world(&[("a.bin", "aaaa")]);
        let mut stats = Stats::default();
        assert!(build_shadow(&w.dest, &w.prev, &[], &mut stats)
            .unwrap()
            .is_none());
    }

    #[test]
    fn cleanup_removes_only_links() {
        let w = world(&[("a.bin", "aaaa")]);
        let pairs = vec![Pair {
            new: PathBuf::from("n/a2.bin"),
            old: PathBuf::from("a.bin"),
            size: 4,
            verified: false,
        }];
        let mut stats = Stats::default();
        build_shadow(&w.dest, &w.prev, &pairs, &mut stats).unwrap();
        assert!(w.dest.join(RELOC_NAME).exists());

        cleanup(&w.dest);

        assert!(!w.dest.join(RELOC_NAME).exists());
        assert_eq!(fs::read_to_string(w.prev.join("a.bin")).unwrap(), "aaaa");
        assert_eq!(fs::metadata(w.prev.join("a.bin")).unwrap().nlink(), 1);
    }

    // ---- remote sources: confirmation by rsync's own checksum ----

    #[test]
    fn checksum_comparison_lines_are_classified() {
        let usable = parse_verdict(b".f         |5|2026/10/19-21:55:24|dir/a.bin").unwrap();
        assert_eq!(usable, (b"dir/a.bin".to_vec(), Verdict::Usable));
        // Equal content with only a time difference is still usable.
        assert_eq!(
            parse_verdict(b".f..t......|5|2026/10/19-21:55:24|a")
                .unwrap()
                .1,
            Verdict::Usable
        );
        // Different content, size or attributes rsync will not link across.
        for line in [
            &b">fc........|5|2026/10/19-21:55:24|a"[..],
            b">f.s.......|5|2026/10/19-21:55:24|a",
            b".f...p.....|5|2026/10/19-21:55:24|a",
            b".f....o....|5|2026/10/19-21:55:24|a",
            b".f.....g...|5|2026/10/19-21:55:24|a",
        ] {
            assert_eq!(
                parse_verdict(line).unwrap().1,
                Verdict::Rejected,
                "{:?}",
                line
            );
        }
        // Not files.
        assert!(parse_verdict(b".d..t......|4096|2026/10/19-21:55:24|dir/").is_none());
        assert!(parse_verdict(b"noise").is_none());
    }

    fn hash_mode() -> RelocationConfig {
        settings(Some("hash"))
    }

    #[test]
    fn a_remote_move_confirmed_by_checksum_is_linked() {
        let mut w = world(&[("a.bin", "alpha-data")]);
        fs::create_dir_all(w.source.join("n")).unwrap();
        fs::rename(w.source.join("a.bin"), w.source.join("n/a.bin")).unwrap();

        // `local = false` makes the source behave like a remote one.
        let (shadow, stats) = w.planned(&hash_mode(), false);

        assert_eq!(
            (stats.linked, stats.verified, stats.rejected),
            (1, 1, 0),
            "{stats:?}"
        );
        assert_eq!(
            ino(&shadow.unwrap().join("n/a.bin")),
            ino(&w.prev.join("a.bin"))
        );
        assert!(!w.dest.join(format!("{}.list", RELOC_NAME)).exists());
    }

    #[test]
    fn a_remote_look_alike_is_rejected_by_default_and_only_size_mtime_is_fooled() {
        let mut w = world(&[("old/a.bin", "alpha-data")]);
        fs::remove_dir_all(w.source.join("old")).unwrap();
        write(&w.source.join("new/a.bin"), "alpha-DATA");
        set_mtime(&w.source.join("new/a.bin"), "2020-05-05 10:00:00");

        // An isolated pairing is confirmed by checksum, which catches it.
        let (shadow, stats) = w.planned(&settings(None), false);
        assert!(shadow.is_none());
        assert_eq!((stats.linked, stats.rejected), (0, 1), "{stats:?}");
        let (shadow, stats) = w.planned(&hash_mode(), false);
        assert!(shadow.is_none());
        assert_eq!((stats.linked, stats.rejected), (0, 1), "{stats:?}");
        assert!(!w.dest.join(RELOC_NAME).exists());

        // The explicit weakest tier trusts size and modification time alone.
        let (shadow, stats) = w.planned(&settings(Some("size-mtime")), false);
        assert_eq!(stats.linked, 1);
        assert!(shadow.is_some());
    }

    #[test]
    fn only_the_confirmed_pairings_stay_in_the_shadow_directory() {
        let mut w = world(&[
            ("a/one.bin", "content-one"),
            ("a/two.bin", "content-two"),
            ("a/three.bin", "content-3.."),
        ]);
        fs::remove_dir_all(w.source.join("a")).unwrap();
        for (name, text) in [
            ("one", "content-one"),
            ("two", "content-two"),
            ("three", "CONTENT-3.."),
        ] {
            write(&w.source.join(format!("b/{name}.bin")), text);
            set_mtime(
                &w.source.join(format!("b/{name}.bin")),
                "2020-05-05 10:00:00",
            );
        }

        let (shadow, stats) = w.planned(&hash_mode(), false);

        let shadow = shadow.expect("shadow");
        assert_eq!(
            (stats.linked, stats.rejected, stats.verified),
            (2, 1, 2),
            "{stats:?}"
        );
        assert!(shadow.join("b/one.bin").exists());
        assert!(shadow.join("b/two.bin").exists());
        assert!(!shadow.join("b/three.bin").exists());
        assert_eq!(stats.bytes_reused, 22);
    }

    #[test]
    fn a_remote_move_with_changed_permissions_is_not_offered() {
        let mut w = world(&[("a.bin", "alpha-data")]);
        fs::rename(w.source.join("a.bin"), w.source.join("b.bin")).unwrap();
        fs::set_permissions(w.source.join("b.bin"), fs::Permissions::from_mode(0o600)).unwrap();

        let (shadow, stats) = w.planned(&hash_mode(), false);

        assert!(shadow.is_none());
        assert_eq!((stats.linked, stats.rejected), (0, 1));
    }

    #[test]
    fn hostile_file_names_never_reach_a_shell() {
        // Spaces, a leading dash, a newline, quotes, glob characters and a
        // non-UTF-8 byte: the names go to rsync through --files-from.
        let names: [&[u8]; 4] = [
            b"-dash and space.bin",
            b"line\nbreak.bin",
            b"quo'te \"x\" $(touch pwned) *.bin",
            b"caf\xe9; rm -rf.bin",
        ];
        let mut w = world(&[]);
        for (i, name) in names.iter().enumerate() {
            let old = w.prev.join(OsStr::from_bytes(name));
            fs::write(&old, format!("content-{i}")).unwrap();
            set_mtime(&old, "2020-05-05 10:00:00");
            let new = w.source.join("moved").join(OsStr::from_bytes(name));
            fs::create_dir_all(new.parent().unwrap()).unwrap();
            fs::write(&new, format!("content-{i}")).unwrap();
            set_mtime(&new, "2020-05-05 10:00:00");
            fs::set_permissions(&new, fs::metadata(&old).unwrap().permissions()).unwrap();
        }
        w.index.ensure_previous(None).unwrap();
        w.index.ensure_previous(Some(&w.prev)).unwrap();

        let (shadow, stats) = w.planned(&hash_mode(), false);

        let shadow = shadow.expect("shadow");
        assert_eq!((stats.linked, stats.rejected), (4, 0), "{stats:?}");
        for name in names {
            assert_eq!(
                ino(&shadow.join("moved").join(OsStr::from_bytes(name))),
                ino(&w.prev.join(OsStr::from_bytes(name)))
            );
        }
        assert!(!w.source.join("pwned").exists());
        assert!(!Path::new("pwned").exists());
    }

    #[test]
    fn a_failing_checksum_run_abandons_relocation() {
        let w = world(&[("a.bin", "aaaa")]);
        let missing = w.source.join("does-not-exist");

        let result = verify_with_rsync(
            &missing.to_string_lossy(),
            &w.prev,
            &[b"a.bin".to_vec()],
            &w.dest.join("list"),
            Instant::now() + Duration::from_secs(30),
        );

        assert!(result.unwrap_err().contains("rsync exited"));
        assert!(!w.dest.join("list").exists());
    }
}
