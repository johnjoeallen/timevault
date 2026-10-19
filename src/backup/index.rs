//! Disk-local file index.
//!
//! Each backup disk keeps its own SQLite database at
//! `<disk>/.timevault-index/file-index.sqlite`. It describes, per job, the
//! files of two generations: the **previous** one (the last completed
//! generation, used as the reference) and the **current** one (the generation
//! being built). Paths are relative to the generation, never absolute, so the
//! database is valid wherever the disk is mounted.
//!
//! The index is an optimisation. The generations on the disk are the source of
//! truth: a missing, stale or corrupt index is rebuilt from them, and nothing
//! about restoring data depends on it. Every operation made on the backup path
//! goes through [`Session`], which turns any index failure into a warning so
//! an index problem can never fail a backup.
//!
//! Contents are hashed lazily and only on request ([`DiskIndex::ensure_hash`]).
//! A stored hash is keyed by `(inode, size, mtime_ns)`, so hard-linked copies
//! share one hash, and it is only reused after the file's current metadata is
//! checked against the record. `ctime` is deliberately not used: hard-linking
//! a file into a new generation changes its `ctime`.

use std::fs;
use std::io::{self, Read};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, ErrorCode, OptionalExtension};
use sha2::{Digest, Sha256};

use crate::backup::generation::{marker_token, COMPLETE_MARKER};
use crate::types::RunMode;

pub const INDEX_DIR: &str = ".timevault-index";
pub const INDEX_FILE: &str = "file-index.sqlite";
pub const SCHEMA_VERSION: i64 = 1;
pub const HASH_ALGO: &str = "sha256";
pub const HASH_VERSION: i64 = 1;

/// Whether the index is maintained when `options.index` is not set.
pub const INDEX_DEFAULT: bool = false;

const BUSY_TIMEOUT: Duration = Duration::from_secs(60);
const MARKER_TMP: &str = ".timevault-complete.new";

pub fn index_path(disk_root: &Path) -> PathBuf {
    disk_root.join(INDEX_DIR).join(INDEX_FILE)
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io: {0}")]
    Io(#[from] io::Error),
    /// The recorded file no longer matches what is on disk.
    #[error("{0}")]
    Stale(String),
    /// The database was written by a newer, incompatible version.
    #[error("{0}")]
    Unsupported(String),
}

impl IndexError {
    fn is_busy(&self) -> bool {
        matches!(
            self,
            IndexError::Sqlite(rusqlite::Error::SqliteFailure(failure, _))
                if matches!(failure.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
        )
    }

    fn is_corrupt(&self) -> bool {
        matches!(
            self,
            IndexError::Sqlite(rusqlite::Error::SqliteFailure(failure, _))
                if matches!(failure.code, ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase)
        )
    }
}

type IResult<T> = Result<T, IndexError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Previous,
    Current,
}

impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Role::Previous => "previous",
            Role::Current => "current",
        }
    }
}

/// One indexed file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRecord {
    /// Path relative to the generation root.
    pub path: PathBuf,
    pub name: Vec<u8>,
    /// `'f'` regular file, `'l'` symlink.
    pub kind: char,
    pub size: u64,
    pub mtime_ns: i64,
    pub dev: u64,
    pub ino: u64,
    pub nlink: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

/// What `ensure_previous` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreviousState {
    /// There is no previous generation (initial backup).
    None,
    /// The stored index already describes the reference generation.
    Reused(u64),
    /// The index was (re)built from the generation on disk.
    Rebuilt(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationRow {
    pub generation: String,
    pub marker_token: String,
    pub file_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Stats {
    pub previous_files: u64,
    pub current_files: u64,
    pub hashes: u64,
}

pub struct DiskIndex {
    conn: Connection,
    job: String,
    /// Contents hashed by this handle (for tests and diagnostics).
    pub hashes_computed: u64,
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Move a bad database (and its WAL files) out of the way so a fresh one is
/// created. Only the newest quarantined copy is kept.
pub fn quarantine(disk_root: &Path) -> io::Result<()> {
    let path = index_path(disk_root);
    let dir = disk_root.join(INDEX_DIR);
    if let Ok(entries) = fs::read_dir(&dir) {
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().contains(".corrupt-") {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    if path.exists() {
        fs::rename(&path, dir.join(format!("{}.corrupt-{}", INDEX_FILE, stamp)))?;
    }
    for suffix in ["-wal", "-shm"] {
        let side = dir.join(format!("{}{}", INDEX_FILE, suffix));
        match fs::remove_file(&side) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn setup(conn: &Connection) -> IResult<()> {
    conn.busy_timeout(BUSY_TIMEOUT)?;
    // WAL lets parallel jobs share one disk's database; if the filesystem
    // refuses it the default journal still works, just serialises more.
    let _: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS meta(
             key TEXT PRIMARY KEY, value TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS generations(
             job TEXT NOT NULL, role TEXT NOT NULL, generation TEXT NOT NULL,
             marker_token TEXT NOT NULL, file_count INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, PRIMARY KEY(job, role));
         CREATE TABLE IF NOT EXISTS files(
             job TEXT NOT NULL, role TEXT NOT NULL, path BLOB NOT NULL,
             name BLOB NOT NULL, kind TEXT NOT NULL, size INTEGER NOT NULL,
             mtime_ns INTEGER NOT NULL, dev INTEGER NOT NULL, ino INTEGER NOT NULL,
             nlink INTEGER NOT NULL, mode INTEGER NOT NULL, uid INTEGER NOT NULL,
             gid INTEGER NOT NULL, PRIMARY KEY(job, role, path)) WITHOUT ROWID;
         CREATE INDEX IF NOT EXISTS files_name_size ON files(job, role, name, size);
         CREATE INDEX IF NOT EXISTS files_size ON files(job, role, size);
         CREATE INDEX IF NOT EXISTS files_inode ON files(ino, size, mtime_ns);
         CREATE TABLE IF NOT EXISTS hashes(
             ino INTEGER NOT NULL, size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL,
             algo TEXT NOT NULL, version INTEGER NOT NULL, hash TEXT NOT NULL,
             hashed_at INTEGER NOT NULL, source TEXT NOT NULL,
             PRIMARY KEY(ino, size, mtime_ns, algo, version)) WITHOUT ROWID;
         CREATE INDEX IF NOT EXISTS hashes_lookup ON hashes(size, hash);",
    )?;
    let version: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    match version {
        None => {
            conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?1)",
                params![SCHEMA_VERSION.to_string()],
            )?;
        }
        Some(found) => {
            let found: i64 = found.parse().unwrap_or(i64::MAX);
            if found > SCHEMA_VERSION {
                return Err(IndexError::Unsupported(format!(
                    "index schema version {} is newer than this release understands ({})",
                    found, SCHEMA_VERSION
                )));
            }
        }
    }
    Ok(())
}

struct Scanned {
    rel: Vec<u8>,
    name: Vec<u8>,
    kind: char,
    size: i64,
    mtime_ns: i64,
    dev: i64,
    ino: i64,
    nlink: i64,
    mode: i64,
    uid: i64,
    gid: i64,
}

/// Walk a generation, calling `visit` for each regular file and symlink.
/// Directories are traversed; the top-level completion marker is skipped
/// (it is Timevault's, not the source's). Entries that vanish mid-scan are
/// skipped.
fn scan_tree(root: &Path, mut visit: impl FnMut(&Scanned) -> IResult<()>) -> IResult<u64> {
    let mut count = 0;
    let mut stack = vec![PathBuf::new()];
    while let Some(rel_dir) = stack.pop() {
        let entries = match fs::read_dir(root.join(&rel_dir)) {
            Ok(entries) => entries,
            Err(err)
                if err.kind() == io::ErrorKind::NotFound && !rel_dir.as_os_str().is_empty() =>
            {
                continue
            }
            Err(err) => return Err(err.into()),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            if rel_dir.as_os_str().is_empty() && (name == COMPLETE_MARKER || name == MARKER_TMP) {
                continue;
            }
            let meta = match fs::symlink_metadata(entry.path()) {
                Ok(meta) => meta,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            let rel = rel_dir.join(&name);
            let kind = if meta.is_dir() {
                stack.push(rel);
                continue;
            } else if meta.file_type().is_symlink() {
                'l'
            } else if meta.is_file() {
                'f'
            } else {
                continue;
            };
            visit(&Scanned {
                rel: rel.as_os_str().as_bytes().to_vec(),
                name: name.as_bytes().to_vec(),
                kind,
                size: meta.len() as i64,
                mtime_ns: meta.mtime() * 1_000_000_000 + meta.mtime_nsec(),
                dev: meta.dev() as i64,
                ino: meta.ino() as i64,
                nlink: meta.nlink() as i64,
                mode: meta.mode() as i64,
                uid: meta.uid() as i64,
                gid: meta.gid() as i64,
            })?;
            count += 1;
        }
    }
    Ok(count)
}

fn record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileRecord> {
    let path: Vec<u8> = row.get(0)?;
    let kind: String = row.get(2)?;
    Ok(FileRecord {
        path: PathBuf::from(std::ffi::OsStr::from_bytes(&path)),
        name: row.get(1)?,
        kind: kind.chars().next().unwrap_or('f'),
        size: row.get::<_, i64>(3)? as u64,
        mtime_ns: row.get(4)?,
        dev: row.get::<_, i64>(5)? as u64,
        ino: row.get::<_, i64>(6)? as u64,
        nlink: row.get::<_, i64>(7)? as u64,
        mode: row.get::<_, i64>(8)? as u32,
        uid: row.get::<_, i64>(9)? as u32,
        gid: row.get::<_, i64>(10)? as u32,
    })
}

const RECORD_COLUMNS: &str = "path, name, kind, size, mtime_ns, dev, ino, nlink, mode, uid, gid";

impl DiskIndex {
    /// Open (creating if needed) the index for `job` on the disk at
    /// `disk_root`. A corrupt database is set aside and replaced.
    pub fn open(disk_root: &Path, job: &str) -> IResult<Self> {
        fs::create_dir_all(disk_root.join(INDEX_DIR))?;
        let path = index_path(disk_root);
        let conn = match Self::connect(&path) {
            Ok(conn) => conn,
            Err(err) if err.is_corrupt() => {
                quarantine(disk_root)?;
                Self::connect(&path)?
            }
            Err(err) => return Err(err),
        };
        Ok(DiskIndex {
            conn,
            job: job.to_string(),
            hashes_computed: 0,
        })
    }

    /// Open and prepare the database. Switching a fresh database to WAL can
    /// report "busy" at once, without waiting on the busy timeout, when other
    /// jobs are creating or opening the same file at that moment, so setup is
    /// retried until it succeeds or the busy timeout has passed.
    fn connect(path: &Path) -> IResult<Connection> {
        let deadline = Instant::now() + BUSY_TIMEOUT;
        loop {
            let conn = Connection::open(path)?;
            match setup(&conn) {
                Ok(()) => return Ok(conn),
                Err(err) if err.is_busy() && Instant::now() < deadline => {
                    drop(conn);
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn generation_row(&self, role: Role) -> IResult<Option<GenerationRow>> {
        Ok(self
            .conn
            .query_row(
                "SELECT generation, marker_token, file_count FROM generations
                 WHERE job = ?1 AND role = ?2",
                params![self.job, role.as_str()],
                |row| {
                    Ok(GenerationRow {
                        generation: row.get(0)?,
                        marker_token: row.get(1)?,
                        file_count: row.get::<_, i64>(2)? as u64,
                    })
                },
            )
            .optional()?)
    }

    pub fn count_files(&self, role: Role) -> IResult<u64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM files WHERE job = ?1 AND role = ?2",
            params![self.job, role.as_str()],
            |row| row.get::<_, i64>(0),
        )? as u64)
    }

    pub fn stats(&self) -> IResult<Stats> {
        Ok(Stats {
            previous_files: self.count_files(Role::Previous)?,
            current_files: self.count_files(Role::Current)?,
            hashes: self
                .conn
                .query_row("SELECT COUNT(*) FROM hashes", [], |row| {
                    row.get::<_, i64>(0)
                })? as u64,
        })
    }

    /// Replace `role`'s rows for this job with a scan of `root`.
    fn load(&mut self, role: Role, root: &Path, generation: &str, token: &str) -> IResult<u64> {
        let job = self.job.clone();
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute(
            "DELETE FROM files WHERE job = ?1 AND role = ?2",
            params![job, role.as_str()],
        )?;
        let count = {
            let mut insert = tx.prepare(
                "INSERT OR REPLACE INTO files(job, role, path, name, kind, size, mtime_ns,
                     dev, ino, nlink, mode, uid, gid)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            )?;
            scan_tree(root, |f| {
                insert.execute(params![
                    job,
                    role.as_str(),
                    f.rel,
                    f.name,
                    f.kind.to_string(),
                    f.size,
                    f.mtime_ns,
                    f.dev,
                    f.ino,
                    f.nlink,
                    f.mode,
                    f.uid,
                    f.gid
                ])?;
                Ok(())
            })?
        };
        tx.execute(
            "INSERT OR REPLACE INTO generations(job, role, generation, marker_token,
                 file_count, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                job,
                role.as_str(),
                generation,
                token,
                count as i64,
                now_secs()
            ],
        )?;
        tx.commit()?;
        Ok(count)
    }

    /// Make sure the "previous" rows describe `base`, the generation used as
    /// the reference for the next backup. A stored index is trusted only if it
    /// names the same generation with the same completion token and still has
    /// the file count it recorded; otherwise it is rebuilt from the disk.
    pub fn ensure_previous(&mut self, base: Option<&Path>) -> IResult<PreviousState> {
        let Some(base) = base else {
            let tx = self
                .conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            tx.execute(
                "DELETE FROM files WHERE job = ?1 AND role = 'previous'",
                params![self.job],
            )?;
            tx.execute(
                "DELETE FROM generations WHERE job = ?1 AND role = 'previous'",
                params![self.job],
            )?;
            tx.commit()?;
            return Ok(PreviousState::None);
        };
        let name = base
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        let token = marker_token(base).unwrap_or_default();
        if let Some(row) = self.generation_row(Role::Previous)? {
            if row.generation == name
                && row.marker_token == token
                && self.count_files(Role::Previous)? == row.file_count
            {
                return Ok(PreviousState::Reused(row.file_count));
            }
        }
        let count = self.load(Role::Previous, base, &name, &token)?;
        Ok(PreviousState::Rebuilt(count))
    }

    /// Discard any "current" rows left by an earlier, unfinished run.
    pub fn begin_current(&mut self) -> IResult<()> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute(
            "DELETE FROM files WHERE job = ?1 AND role = 'current'",
            params![self.job],
        )?;
        tx.execute(
            "DELETE FROM generations WHERE job = ?1 AND role = 'current'",
            params![self.job],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Record the finished, not yet published, generation as "current".
    pub fn record_current(
        &mut self,
        staging: &Path,
        generation: &str,
        token: &str,
    ) -> IResult<u64> {
        self.load(Role::Current, staging, generation, token)
    }

    /// The generation was published: "current" becomes "previous" and the old
    /// "previous" rows are dropped, in one transaction.
    pub fn commit_current(&mut self) -> IResult<()> {
        if self.generation_row(Role::Current)?.is_none() {
            return Err(IndexError::Stale("no current generation recorded".into()));
        }
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute(
            "DELETE FROM files WHERE job = ?1 AND role = 'previous'",
            params![self.job],
        )?;
        tx.execute(
            "UPDATE files SET role = 'previous' WHERE job = ?1 AND role = 'current'",
            params![self.job],
        )?;
        tx.execute(
            "DELETE FROM generations WHERE job = ?1 AND role = 'previous'",
            params![self.job],
        )?;
        tx.execute(
            "UPDATE generations SET role = 'previous' WHERE job = ?1 AND role = 'current'",
            params![self.job],
        )?;
        // Hashes of files no job's index refers to any more are dead weight.
        tx.execute(
            "DELETE FROM hashes WHERE NOT EXISTS (
                 SELECT 1 FROM files f
                 WHERE f.ino = hashes.ino AND f.size = hashes.size
                   AND f.mtime_ns = hashes.mtime_ns)",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }

    // ---- lookups -----------------------------------------------------------

    pub fn find_by_path(&self, role: Role, path: &Path) -> IResult<Option<FileRecord>> {
        Ok(self
            .conn
            .query_row(
                &format!(
                    "SELECT {RECORD_COLUMNS} FROM files WHERE job = ?1 AND role = ?2 AND path = ?3"
                ),
                params![self.job, role.as_str(), path.as_os_str().as_bytes()],
                record_from_row,
            )
            .optional()?)
    }

    /// Every indexed file of `role`, ordered by path.
    pub fn list_files(&self, role: Role) -> IResult<Vec<FileRecord>> {
        self.query_records(
            &format!(
                "SELECT {RECORD_COLUMNS} FROM files WHERE job = ?1 AND role = ?2 ORDER BY path"
            ),
            params![self.job, role.as_str()],
        )
    }

    /// Files with this name and size, in any directory.
    pub fn find_by_name_size(
        &self,
        role: Role,
        name: &[u8],
        size: u64,
    ) -> IResult<Vec<FileRecord>> {
        self.query_records(
            &format!(
                "SELECT {RECORD_COLUMNS} FROM files
                 WHERE job = ?1 AND role = ?2 AND name = ?3 AND size = ?4 AND kind = 'f'
                 ORDER BY path"
            ),
            params![self.job, role.as_str(), name, size as i64],
        )
    }

    /// Regular files of this size (candidates for content comparison).
    pub fn find_by_size(&self, role: Role, size: u64) -> IResult<Vec<FileRecord>> {
        self.query_records(
            &format!(
                "SELECT {RECORD_COLUMNS} FROM files
                 WHERE job = ?1 AND role = ?2 AND size = ?3 AND kind = 'f' ORDER BY path"
            ),
            params![self.job, role.as_str(), size as i64],
        )
    }

    /// Files with this size and an already-computed hash, whatever their name.
    pub fn find_by_hash(&self, role: Role, size: u64, hash: &str) -> IResult<Vec<FileRecord>> {
        self.query_records(
            "SELECT f.path, f.name, f.kind, f.size, f.mtime_ns, f.dev, f.ino, f.nlink,
                    f.mode, f.uid, f.gid
             FROM files f JOIN hashes h
               ON h.ino = f.ino AND h.size = f.size AND h.mtime_ns = f.mtime_ns
             WHERE f.job = ?1 AND f.role = ?2 AND f.size = ?3 AND f.kind = 'f'
               AND h.algo = ?4 AND h.version = ?5 AND h.hash = ?6
             ORDER BY f.path",
            params![
                self.job,
                role.as_str(),
                size as i64,
                HASH_ALGO,
                HASH_VERSION,
                hash
            ],
        )
    }

    fn query_records(&self, sql: &str, params: impl rusqlite::Params) -> IResult<Vec<FileRecord>> {
        let mut statement = self.conn.prepare(sql)?;
        let rows = statement.query_map(params, record_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Sets of paths that share an inode within `role` (hard links).
    pub fn hard_link_groups(&self, role: Role) -> IResult<Vec<Vec<PathBuf>>> {
        let mut statement = self.conn.prepare(
            "SELECT ino, path FROM files WHERE job = ?1 AND role = ?2 AND kind = 'f'
               AND ino IN (SELECT ino FROM files WHERE job = ?1 AND role = ?2 AND kind = 'f'
                           GROUP BY ino HAVING COUNT(*) > 1)
             ORDER BY ino, path",
        )?;
        let rows = statement.query_map(params![self.job, role.as_str()], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;
        let mut groups: Vec<Vec<PathBuf>> = Vec::new();
        let mut last = None;
        for row in rows {
            let (ino, path) = row?;
            let path = PathBuf::from(std::ffi::OsStr::from_bytes(&path));
            if last == Some(ino) {
                groups.last_mut().expect("group").push(path);
            } else {
                groups.push(vec![path]);
                last = Some(ino);
            }
        }
        Ok(groups)
    }

    pub fn same_inode(&self, role: Role, a: &Path, b: &Path) -> IResult<bool> {
        match (self.find_by_path(role, a)?, self.find_by_path(role, b)?) {
            (Some(a), Some(b)) => Ok(a.ino == b.ino),
            _ => Ok(false),
        }
    }

    /// Run `f` inside one write transaction, so many small writes (such as
    /// storing a batch of hashes) cost one commit. The transaction is rolled
    /// back if `f` fails.
    pub fn batch<T>(&mut self, f: impl FnOnce(&mut DiskIndex) -> IResult<T>) -> IResult<T> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        match f(self) {
            Ok(value) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(value)
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    // ---- hashing -----------------------------------------------------------

    /// The SHA-256 of a regular file in `generation_root`, computed on first
    /// request and cached.
    ///
    /// The cached value is used only after the file's current size, mtime and
    /// inode are checked against the index record; a file that has changed
    /// (or changes while it is being read) yields [`IndexError::Stale`] rather
    /// than a hash of unknown provenance.
    pub fn ensure_hash(
        &mut self,
        role: Role,
        rel: &Path,
        generation_root: &Path,
    ) -> IResult<String> {
        let record = self
            .find_by_path(role, rel)?
            .ok_or_else(|| IndexError::Stale(format!("{} is not indexed", rel.display())))?;
        if record.kind != 'f' {
            return Err(IndexError::Stale(format!(
                "{} is not a regular file",
                rel.display()
            )));
        }
        let full = generation_root.join(rel);
        check_unchanged(&full, &record)?;
        if let Some(hash) = self.cached_hash(&record)? {
            return Ok(hash);
        }
        let hash = sha256_file(&full)?;
        check_unchanged(&full, &record)?;
        self.hashes_computed += 1;
        self.conn.execute(
            "INSERT OR REPLACE INTO hashes(ino, size, mtime_ns, algo, version, hash,
                 hashed_at, source) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'local')",
            params![
                record.ino as i64,
                record.size as i64,
                record.mtime_ns,
                HASH_ALGO,
                HASH_VERSION,
                hash,
                now_secs()
            ],
        )?;
        Ok(hash)
    }

    fn cached_hash(&self, record: &FileRecord) -> IResult<Option<String>> {
        Ok(self
            .conn
            .query_row(
                "SELECT hash FROM hashes WHERE ino = ?1 AND size = ?2 AND mtime_ns = ?3
                   AND algo = ?4 AND version = ?5",
                params![
                    record.ino as i64,
                    record.size as i64,
                    record.mtime_ns,
                    HASH_ALGO,
                    HASH_VERSION
                ],
                |row| row.get(0),
            )
            .optional()?)
    }
}

/// The lowercase hex SHA-256 of a file's contents, streamed.
pub fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 128 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn check_unchanged(path: &Path, record: &FileRecord) -> IResult<()> {
    let meta = fs::symlink_metadata(path)?;
    let mtime_ns = meta.mtime() * 1_000_000_000 + meta.mtime_nsec();
    if meta.len() != record.size || mtime_ns != record.mtime_ns || meta.ino() != record.ino {
        return Err(IndexError::Stale(format!(
            "{} no longer matches the index",
            path.display()
        )));
    }
    Ok(())
}

/// The index as used on the backup path. Every step is best effort: a failure
/// is reported as a warning and switches the index off for the rest of the
/// run, and a corrupt database is set aside so the next run rebuilds it. An
/// index problem never fails a backup.
pub struct Session {
    index: Option<DiskIndex>,
    disk_root: PathBuf,
}

impl Session {
    /// Open the index for `job` on the disk holding `dest` (`<disk>/<job>`).
    /// Disabled, dry-run and unopenable cases give an inert session.
    pub fn start(enabled: bool, dest: &Path, job: &str, run_mode: RunMode) -> Session {
        let disk_root = dest.parent().map(Path::to_path_buf).unwrap_or_default();
        let mut session = Session {
            index: None,
            disk_root: disk_root.clone(),
        };
        if !enabled || run_mode.dry_run || disk_root.as_os_str().is_empty() {
            return session;
        }
        let opened = DiskIndex::open(&disk_root, job);
        session.index = session.handle("open", opened);
        session
    }

    pub fn is_active(&self) -> bool {
        self.index.is_some()
    }

    /// The open index, for lookups by moved-file detection. Callers must treat
    /// an error from it as "no index": turn it into a skip, never a failure.
    pub fn index_mut(&mut self) -> Option<&mut DiskIndex> {
        self.index.as_mut()
    }

    /// Switch the index off for the rest of the run after a failed lookup.
    pub fn disable(&mut self, what: &str, err: IndexError) {
        self.handle::<()>(what, Err(err));
    }

    fn handle<T>(&mut self, what: &str, result: IResult<T>) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(err) => {
                crate::pnote!(
                    "warning: file index {} failed: {}; continuing without it",
                    what,
                    err
                );
                if err.is_corrupt() {
                    if let Err(err) = quarantine(&self.disk_root) {
                        crate::pnote!("warning: could not set aside the file index: {}", err);
                    }
                }
                self.index = None;
                None
            }
        }
    }

    /// Bring the "previous" rows in line with the reference generation and
    /// clear leftovers of an unfinished run.
    pub fn prepare(&mut self, base: Option<&Path>) {
        let Some(mut index) = self.index.take() else {
            return;
        };
        let result = index
            .ensure_previous(base)
            .and_then(|state| index.begin_current().map(|()| state));
        self.index = Some(index);
        if let Some(state) = self.handle("prepare", result) {
            match state {
                PreviousState::Rebuilt(count) => {
                    crate::pnote!(
                        "file index: rebuilt from the previous generation ({} files)",
                        count
                    );
                }
                PreviousState::Reused(_) | PreviousState::None => {}
            }
        }
    }

    /// Record the finished generation, before it is published.
    pub fn record(&mut self, staging: &Path, generation: &str, token: &str) {
        let Some(mut index) = self.index.take() else {
            return;
        };
        let result = index.record_current(staging, generation, token);
        self.index = Some(index);
        self.handle("record", result);
    }

    /// The generation was published: advance the roles.
    pub fn commit(&mut self) {
        let Some(mut index) = self.index.take() else {
            return;
        };
        let result = index.commit_current();
        self.index = Some(index);
        self.handle("commit", result);
    }

    /// The run failed: drop the unfinished rows, leave "previous" alone.
    pub fn abandon(&mut self) {
        let Some(mut index) = self.index.take() else {
            return;
        };
        let result = index.begin_current();
        self.index = Some(index);
        self.handle("abandon", result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;
    use std::os::unix::fs::symlink;

    fn write(path: &Path, text: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }

    fn set_mtime(path: &Path, stamp: &str) {
        let status = std::process::Command::new("touch")
            .args(["-d", stamp])
            .arg(path)
            .status()
            .unwrap();
        assert!(status.success());
    }

    /// A disk root holding one job's generation with a few files.
    struct Fixture {
        _tmp: tempfile::TempDir,
        disk: PathBuf,
        generation: PathBuf,
    }

    fn fixture() -> Fixture {
        let tmp = tempfile::tempdir().unwrap();
        let disk = tmp.path().join("disk");
        let generation = disk.join("job/20260101");
        write(&generation.join("a.txt"), "alpha");
        write(&generation.join("dir/b.txt"), "bravo!");
        write(&generation.join("dir/sub/c.txt"), "alpha");
        write(&generation.join(COMPLETE_MARKER), "token-1");
        symlink("a.txt", generation.join("link")).unwrap();
        Fixture {
            _tmp: tmp,
            disk,
            generation,
        }
    }

    fn paths(records: &[FileRecord]) -> Vec<String> {
        records
            .iter()
            .map(|r| r.path.to_string_lossy().to_string())
            .collect()
    }

    #[test]
    fn open_creates_the_database_under_the_disk_root() {
        let f = fixture();
        let _index = DiskIndex::open(&f.disk, "job").unwrap();
        assert!(index_path(&f.disk).is_file());
        assert!(index_path(&f.disk).starts_with(f.disk.join(INDEX_DIR)));
    }

    #[test]
    fn previous_is_built_from_the_generation_with_relative_paths_only() {
        let f = fixture();
        let mut index = DiskIndex::open(&f.disk, "job").unwrap();

        let state = index.ensure_previous(Some(&f.generation)).unwrap();

        // a.txt, dir/b.txt, dir/sub/c.txt and the symlink; the marker is not data.
        assert_eq!(state, PreviousState::Rebuilt(4));
        let a = index
            .find_by_path(Role::Previous, Path::new("a.txt"))
            .unwrap()
            .unwrap();
        assert_eq!((a.kind, a.size, a.name.as_slice()), ('f', 5, &b"a.txt"[..]));
        let link = index
            .find_by_path(Role::Previous, Path::new("link"))
            .unwrap()
            .unwrap();
        assert_eq!(link.kind, 'l');
        assert!(index
            .find_by_path(Role::Previous, Path::new(COMPLETE_MARKER))
            .unwrap()
            .is_none());

        // Nothing absolute was stored, so the disk can be mounted anywhere.
        let absolute: i64 = index
            .conn
            .query_row(
                "SELECT COUNT(*) FROM files WHERE substr(path, 1, 1) = x'2f'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(absolute, 0);
        let marker = f.generation.to_string_lossy().to_string();
        let leaked: i64 = index
            .conn
            .query_row(
                "SELECT COUNT(*) FROM files WHERE instr(CAST(path AS TEXT), ?1) > 0",
                [marker],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(leaked, 0);
    }

    #[test]
    fn file_names_that_are_not_utf8_round_trip() {
        let f = fixture();
        let odd = OsStr::from_bytes(b"caf\xe9.txt");
        fs::write(f.generation.join(odd), "latin1").unwrap();
        let mut index = DiskIndex::open(&f.disk, "job").unwrap();
        index.ensure_previous(Some(&f.generation)).unwrap();

        let found = index
            .find_by_path(Role::Previous, Path::new(odd))
            .unwrap()
            .unwrap();

        assert_eq!(found.name, b"caf\xe9.txt");
        assert_eq!(found.size, 6);
    }

    #[test]
    fn lookups_by_name_size_and_content() {
        let f = fixture();
        fs::hard_link(
            f.generation.join("dir/b.txt"),
            f.generation.join("b-link.txt"),
        )
        .unwrap();
        let mut index = DiskIndex::open(&f.disk, "job").unwrap();
        index.ensure_previous(Some(&f.generation)).unwrap();

        // Same filename and size, regardless of directory.
        let same_name = index
            .find_by_name_size(Role::Previous, b"b.txt", 6)
            .unwrap();
        assert_eq!(paths(&same_name), vec!["dir/b.txt"]);
        assert!(index
            .find_by_name_size(Role::Previous, b"b.txt", 7)
            .unwrap()
            .is_empty());

        // Size alone gives the candidates for content comparison.
        let by_size = index.find_by_size(Role::Previous, 5).unwrap();
        assert_eq!(paths(&by_size), vec!["a.txt", "dir/sub/c.txt"]);

        // Equivalent content under a different name needs hashes, which are
        // computed only on request.
        assert!(index
            .find_by_hash(Role::Previous, 5, "x")
            .unwrap()
            .is_empty());
        let hash = index
            .ensure_hash(Role::Previous, Path::new("a.txt"), &f.generation)
            .unwrap();
        let other = index
            .ensure_hash(Role::Previous, Path::new("dir/sub/c.txt"), &f.generation)
            .unwrap();
        assert_eq!(hash, other);
        assert_eq!(
            paths(&index.find_by_hash(Role::Previous, 5, &hash).unwrap()),
            vec!["a.txt", "dir/sub/c.txt"]
        );

        // Hard links are recognised without hashing anything.
        let groups = index.hard_link_groups(Role::Previous).unwrap();
        assert_eq!(
            groups,
            vec![vec![
                PathBuf::from("b-link.txt"),
                PathBuf::from("dir/b.txt")
            ]]
        );
        assert!(index
            .same_inode(
                Role::Previous,
                Path::new("dir/b.txt"),
                Path::new("b-link.txt")
            )
            .unwrap());
        assert!(!index
            .same_inode(Role::Previous, Path::new("a.txt"), Path::new("dir/b.txt"))
            .unwrap());
    }

    #[test]
    fn hashing_is_lazy_and_shared_between_hard_links() {
        let f = fixture();
        fs::hard_link(f.generation.join("a.txt"), f.generation.join("a-link.txt")).unwrap();
        let mut index = DiskIndex::open(&f.disk, "job").unwrap();
        index.ensure_previous(Some(&f.generation)).unwrap();
        assert_eq!(index.stats().unwrap().hashes, 0);
        assert_eq!(index.hashes_computed, 0);

        let first = index
            .ensure_hash(Role::Previous, Path::new("a.txt"), &f.generation)
            .unwrap();
        let second = index
            .ensure_hash(Role::Previous, Path::new("a-link.txt"), &f.generation)
            .unwrap();
        let again = index
            .ensure_hash(Role::Previous, Path::new("a.txt"), &f.generation)
            .unwrap();

        assert_eq!(first, second);
        assert_eq!(first, again);
        // One inode, one hash computation, one stored row.
        assert_eq!(index.hashes_computed, 1);
        assert_eq!(index.stats().unwrap().hashes, 1);
        // The value is the real SHA-256 of "alpha".
        let expected = format!("{:x}", Sha256::digest(b"alpha"));
        assert_eq!(first, expected);
    }

    #[test]
    fn a_changed_file_is_never_given_a_cached_or_fresh_hash() {
        let f = fixture();
        let mut index = DiskIndex::open(&f.disk, "job").unwrap();
        index.ensure_previous(Some(&f.generation)).unwrap();
        index
            .ensure_hash(Role::Previous, Path::new("a.txt"), &f.generation)
            .unwrap();

        // Same size, new content, new mtime: size and mtime would not be proof
        // of identity, so the record is checked and found stale.
        fs::write(f.generation.join("a.txt"), "ALPHA").unwrap();
        set_mtime(&f.generation.join("a.txt"), "2031-01-01 00:00:00");
        let err = index
            .ensure_hash(Role::Previous, Path::new("a.txt"), &f.generation)
            .unwrap_err();
        assert!(matches!(err, IndexError::Stale(_)), "{err}");

        // A cached hash is refused too when only the mtime has moved.
        index
            .ensure_hash(Role::Previous, Path::new("dir/b.txt"), &f.generation)
            .unwrap();
        set_mtime(&f.generation.join("dir/b.txt"), "2031-01-01 00:00:00");
        let err = index
            .ensure_hash(Role::Previous, Path::new("dir/b.txt"), &f.generation)
            .unwrap_err();
        assert!(matches!(err, IndexError::Stale(_)), "{err}");

        // Rebuilding picks up the new state and hashing works again.
        index.ensure_previous(None).unwrap();
        let mut index = DiskIndex::open(&f.disk, "job").unwrap();
        index.ensure_previous(Some(&f.generation)).unwrap();
        assert!(index
            .ensure_hash(Role::Previous, Path::new("a.txt"), &f.generation)
            .is_ok());
    }

    #[test]
    fn a_missing_file_or_replaced_inode_is_stale() {
        let f = fixture();
        let mut index = DiskIndex::open(&f.disk, "job").unwrap();
        index.ensure_previous(Some(&f.generation)).unwrap();

        fs::remove_file(f.generation.join("a.txt")).unwrap();
        assert!(index
            .ensure_hash(Role::Previous, Path::new("a.txt"), &f.generation)
            .is_err());

        // A new file at the same path is a different inode.
        write(&f.generation.join("dir/b.txt.new"), "bravo!");
        fs::rename(
            f.generation.join("dir/b.txt.new"),
            f.generation.join("dir/b.txt"),
        )
        .unwrap();
        set_mtime(&f.generation.join("dir/b.txt"), "2001-01-01 00:00:00");
        let err = index
            .ensure_hash(Role::Previous, Path::new("dir/b.txt"), &f.generation)
            .unwrap_err();
        assert!(matches!(err, IndexError::Stale(_)));
    }

    fn staged_generation(root: &Path, name: &str, token: &str, files: &[(&str, &str)]) -> PathBuf {
        let dir = root.join(name);
        for (rel, text) in files {
            write(&dir.join(rel), text);
        }
        write(&dir.join(COMPLETE_MARKER), token);
        dir
    }

    #[test]
    fn publishing_advances_current_to_previous_and_retires_the_old_rows() {
        let f = fixture();
        let mut index = DiskIndex::open(&f.disk, "job").unwrap();
        index.ensure_previous(Some(&f.generation)).unwrap();
        index.begin_current().unwrap();

        let next = staged_generation(
            &f.disk.join("job"),
            "20260102",
            "token-2",
            &[("new.txt", "fresh")],
        );
        let count = index.record_current(&next, "20260102", "token-2").unwrap();
        assert_eq!(count, 1);
        // Recording does not disturb the reference.
        assert_eq!(
            index
                .generation_row(Role::Previous)
                .unwrap()
                .unwrap()
                .generation,
            "20260101"
        );
        assert_eq!(index.stats().unwrap().current_files, 1);

        index.commit_current().unwrap();

        let previous = index.generation_row(Role::Previous).unwrap().unwrap();
        assert_eq!(
            (
                previous.generation.as_str(),
                previous.marker_token.as_str(),
                previous.file_count
            ),
            ("20260102", "token-2", 1)
        );
        assert!(index.generation_row(Role::Current).unwrap().is_none());
        assert_eq!(
            index.stats().unwrap(),
            Stats {
                previous_files: 1,
                current_files: 0,
                hashes: 0
            }
        );
        assert!(index
            .find_by_path(Role::Previous, Path::new("a.txt"))
            .unwrap()
            .is_none());
        assert!(index
            .find_by_path(Role::Previous, Path::new("new.txt"))
            .unwrap()
            .is_some());
    }

    #[test]
    fn a_failed_run_does_not_advance_previous() {
        let f = fixture();
        let mut index = DiskIndex::open(&f.disk, "job").unwrap();
        index.ensure_previous(Some(&f.generation)).unwrap();
        let next = staged_generation(
            &f.disk.join("job"),
            "20260102",
            "token-2",
            &[("new.txt", "fresh")],
        );
        index.record_current(&next, "20260102", "token-2").unwrap();

        index.begin_current().unwrap(); // what a failed run's cleanup does

        assert_eq!(
            index
                .generation_row(Role::Previous)
                .unwrap()
                .unwrap()
                .generation,
            "20260101"
        );
        assert_eq!(index.stats().unwrap().previous_files, 4);
        assert_eq!(index.stats().unwrap().current_files, 0);
        assert!(index.commit_current().is_err());
    }

    #[test]
    fn previous_is_reused_only_when_it_still_describes_the_reference() {
        let f = fixture();
        let mut index = DiskIndex::open(&f.disk, "job").unwrap();
        assert_eq!(
            index.ensure_previous(Some(&f.generation)).unwrap(),
            PreviousState::Rebuilt(4)
        );
        assert_eq!(
            index.ensure_previous(Some(&f.generation)).unwrap(),
            PreviousState::Reused(4)
        );

        // A different completion token (the day was recreated): rebuild.
        write(&f.generation.join(COMPLETE_MARKER), "token-recreated");
        assert_eq!(
            index.ensure_previous(Some(&f.generation)).unwrap(),
            PreviousState::Rebuilt(4)
        );

        // A different generation (published, but the index was not advanced
        // before a crash): rebuild.
        let other = staged_generation(
            &f.disk.join("job"),
            "20260102",
            "token-2",
            &[("x", "1"), ("y", "2")],
        );
        assert_eq!(
            index.ensure_previous(Some(&other)).unwrap(),
            PreviousState::Rebuilt(2)
        );

        // Rows tampered with so the count no longer matches: rebuild.
        index
            .conn
            .execute(
                "DELETE FROM files WHERE job = 'job' AND role = 'previous' AND path = x'78'",
                [],
            )
            .unwrap();
        assert_eq!(
            index.ensure_previous(Some(&other)).unwrap(),
            PreviousState::Rebuilt(2)
        );

        // No previous generation: nothing indexed.
        assert_eq!(index.ensure_previous(None).unwrap(), PreviousState::None);
        assert_eq!(index.stats().unwrap().previous_files, 0);
    }

    #[test]
    fn a_missing_index_is_rebuilt_and_generations_stay_readable() {
        let f = fixture();
        {
            let mut index = DiskIndex::open(&f.disk, "job").unwrap();
            index.ensure_previous(Some(&f.generation)).unwrap();
        }
        fs::remove_dir_all(f.disk.join(INDEX_DIR)).unwrap();
        // Restoring needs nothing from the index.
        assert_eq!(
            fs::read_to_string(f.generation.join("dir/b.txt")).unwrap(),
            "bravo!"
        );

        let mut index = DiskIndex::open(&f.disk, "job").unwrap();
        assert_eq!(
            index.ensure_previous(Some(&f.generation)).unwrap(),
            PreviousState::Rebuilt(4)
        );
    }

    #[test]
    fn a_corrupt_index_is_set_aside_and_rebuilt() {
        let f = fixture();
        {
            let mut index = DiskIndex::open(&f.disk, "job").unwrap();
            index.ensure_previous(Some(&f.generation)).unwrap();
        }
        fs::write(
            index_path(&f.disk),
            b"this is not a sqlite database at all, just noise",
        )
        .unwrap();

        let mut index = DiskIndex::open(&f.disk, "job").unwrap();

        assert_eq!(
            index.ensure_previous(Some(&f.generation)).unwrap(),
            PreviousState::Rebuilt(4)
        );
        let aside: Vec<_> = fs::read_dir(f.disk.join(INDEX_DIR))
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(".corrupt-"))
            .collect();
        assert_eq!(aside.len(), 1);
        assert_eq!(
            fs::read_to_string(f.generation.join("a.txt")).unwrap(),
            "alpha"
        );
    }

    #[test]
    fn a_newer_schema_is_left_alone() {
        let f = fixture();
        {
            let _index = DiskIndex::open(&f.disk, "job").unwrap();
        }
        let conn = Connection::open(index_path(&f.disk)).unwrap();
        conn.execute(
            "UPDATE meta SET value = '99' WHERE key = 'schema_version'",
            [],
        )
        .unwrap();
        drop(conn);

        let err = DiskIndex::open(&f.disk, "job").err().expect("unsupported");

        assert!(matches!(err, IndexError::Unsupported(_)));
        assert!(index_path(&f.disk).is_file());
    }

    #[test]
    fn jobs_have_independent_rows_in_one_database() {
        let f = fixture();
        let other_gen =
            staged_generation(&f.disk.join("other"), "20260105", "t", &[("o.txt", "o")]);
        let mut etc = DiskIndex::open(&f.disk, "job").unwrap();
        let mut other = DiskIndex::open(&f.disk, "other").unwrap();
        etc.ensure_previous(Some(&f.generation)).unwrap();
        other.ensure_previous(Some(&other_gen)).unwrap();

        assert_eq!(etc.stats().unwrap().previous_files, 4);
        assert_eq!(other.stats().unwrap().previous_files, 1);
        assert!(etc
            .find_by_path(Role::Previous, Path::new("o.txt"))
            .unwrap()
            .is_none());
        assert_eq!(
            etc.generation_row(Role::Previous)
                .unwrap()
                .unwrap()
                .generation,
            "20260101"
        );
        assert_eq!(
            other
                .generation_row(Role::Previous)
                .unwrap()
                .unwrap()
                .generation,
            "20260105"
        );
    }

    #[test]
    fn parallel_jobs_can_use_one_disks_index_at_the_same_time() {
        let tmp = tempfile::tempdir().unwrap();
        let disk = tmp.path().join("disk");
        let workers: Vec<_> = (0..4)
            .map(|n| {
                let disk = disk.clone();
                std::thread::spawn(move || {
                    let job = format!("job{n}");
                    let files: Vec<(String, String)> = (0..40)
                        .map(|i| (format!("f{i}.txt"), format!("{n}-{i}")))
                        .collect();
                    let mut index = DiskIndex::open(&disk, &job).unwrap();
                    for round in 0..5 {
                        let day = format!("2026010{}", round + 1);
                        let refs: Vec<(&str, &str)> = files
                            .iter()
                            .map(|(a, b)| (a.as_str(), b.as_str()))
                            .collect();
                        let dir =
                            staged_generation(&disk.join(&job), &day, &format!("t{round}"), &refs);
                        index.ensure_previous(None).unwrap();
                        index.begin_current().unwrap();
                        index
                            .record_current(&dir, &day, &format!("t{round}"))
                            .unwrap();
                        index.commit_current().unwrap();
                    }
                    let stats = index.stats().unwrap();
                    (
                        job,
                        stats.previous_files,
                        index.generation_row(Role::Previous).unwrap().unwrap(),
                    )
                })
            })
            .collect();

        for worker in workers {
            let (job, files, row) = worker.join().unwrap();
            assert_eq!(files, 40, "{job}");
            assert_eq!(row.generation, "20260105", "{job}");
        }
    }

    /// Timing of the index on a large tree. Run with:
    /// `cargo test --release bench_index -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn bench_index_100k_files() {
        let tmp = tempfile::tempdir().unwrap();
        let disk = tmp.path().join("disk");
        let generation = disk.join("job/20260101");
        let files = 100_000;
        let created = Instant::now();
        for i in 0..files {
            let path = generation.join(format!("d{:03}/f{:06}.txt", i % 1000, i));
            if i < 1000 {
                fs::create_dir_all(path.parent().unwrap()).unwrap();
            }
            fs::write(&path, format!("content {i}")).unwrap();
        }
        write(&generation.join(COMPLETE_MARKER), "t");
        eprintln!("create {} files: {:?}", files, created.elapsed());

        let mut index = DiskIndex::open(&disk, "job").unwrap();
        let t = Instant::now();
        index.ensure_previous(Some(&generation)).unwrap();
        eprintln!("full build (scan + insert): {:?}", t.elapsed());

        let t = Instant::now();
        let state = index.ensure_previous(Some(&generation)).unwrap();
        eprintln!(
            "reuse check when unchanged: {:?} ({:?})",
            t.elapsed(),
            state
        );

        let t = Instant::now();
        index.begin_current().unwrap();
        index.record_current(&generation, "20260102", "t2").unwrap();
        index.commit_current().unwrap();
        eprintln!("record + commit a new generation: {:?}", t.elapsed());

        let t = Instant::now();
        let hit = index
            .find_by_name_size(Role::Previous, b"f050000.txt", 15)
            .unwrap();
        eprintln!("name+size lookup: {:?} ({} hit)", t.elapsed(), hit.len());

        let t = Instant::now();
        let mut walk = 0u64;
        scan_tree(&generation, |_| {
            walk += 1;
            Ok(())
        })
        .unwrap();
        eprintln!(
            "bare walk + stat of {} files (no database): {:?}",
            walk,
            t.elapsed()
        );
        eprintln!(
            "database size: {} bytes",
            fs::metadata(index_path(&disk)).unwrap().len()
        );
    }

    #[test]
    fn session_is_inert_when_disabled_or_dry_run() {
        let f = fixture();
        let dest = f.disk.join("job");
        let mode = RunMode {
            dry_run: false,
            safe_mode: false,
            verbose: false,
        };

        let off = Session::start(false, &dest, "job", mode);
        assert!(!off.is_active());
        let dry = Session::start(
            true,
            &dest,
            "job",
            RunMode {
                dry_run: true,
                ..mode
            },
        );
        assert!(!dry.is_active());
        assert!(!index_path(&f.disk).exists());
    }

    #[test]
    fn session_survives_an_unusable_index() {
        let f = fixture();
        {
            let _index = DiskIndex::open(&f.disk, "job").unwrap();
        }
        let conn = Connection::open(index_path(&f.disk)).unwrap();
        conn.execute(
            "UPDATE meta SET value = '99' WHERE key = 'schema_version'",
            [],
        )
        .unwrap();
        drop(conn);
        let mode = RunMode {
            dry_run: false,
            safe_mode: false,
            verbose: false,
        };

        let mut session = Session::start(true, &f.disk.join("job"), "job", mode);

        // Opening failed; every later step is a harmless no-op.
        assert!(!session.is_active());
        session.prepare(Some(&f.generation));
        session.record(&f.generation, "20260101", "t");
        session.commit();
        session.abandon();
    }
}
