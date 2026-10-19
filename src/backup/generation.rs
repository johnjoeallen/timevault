//! Snapshot generation lifecycle for a job directory (`<disk>/<job>/`).
//!
//! A generation is a plain directory named after the backup day. New
//! generations are built in a staging directory inside the job directory with
//! `rsync --link-dest` pointing at the last completed generation, then renamed
//! into place, so a completed generation is never modified and a failed run is
//! never visible as a generation.
//!
//! A generation published by this code carries a [`COMPLETE_MARKER`] file
//! written just before the publishing rename, so the marker is visible exactly
//! when the generation is. Replacing a same-day generation swaps the two
//! directories with `renameat2(RENAME_EXCHANGE)` where the filesystem supports
//! it, journalled by a small [`PUBLISH_INTENT`] file so an interruption can be
//! resolved deterministically on the next run.

use std::ffi::CString;
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::types::RunMode;

/// In-progress generation. Reserved name: never counted by retention.
pub const STAGING_NAME: &str = ".timevault-staging";
/// Prefix for a same-day generation set aside while it is being replaced.
pub const REPLACED_PREFIX: &str = ".timevault-replaced-";
/// Written into a generation just before it is published. Its content is a
/// token unique to the run that built the generation.
pub const COMPLETE_MARKER: &str = ".timevault-complete";
/// Records an in-flight same-day exchange: the day and the new generation's
/// marker token.
pub const PUBLISH_INTENT: &str = ".timevault-publishing";
/// Temporary symlink renamed over `current`, so `current` is replaced atomically.
pub const CURRENT_TMP: &str = ".timevault-current.tmp";
/// Scratch directory of hard links used by moved-file detection while a
/// generation is built. Holds only links, so removing it never touches data.
pub const RELOC_NAME: &str = ".timevault-reloc";

const RENAME_EXCHANGE: libc::c_uint = 2;

pub fn staging_path(dest: &Path) -> PathBuf {
    dest.join(STAGING_NAME)
}

fn replaced_path(dest: &Path, backup_day: &str) -> PathBuf {
    dest.join(format!("{}{}", REPLACED_PREFIX, backup_day))
}

/// True for entries Timevault manages itself, which retention must skip.
pub fn is_internal_name(name: &str) -> bool {
    name == STAGING_NAME
        || name == PUBLISH_INTENT
        || name == CURRENT_TMP
        || name.starts_with(RELOC_NAME)
        || name.starts_with(REPLACED_PREFIX)
}

/// Test hook for steps outside this module (retention).
pub fn failpoint_hit(name: &str) -> io::Result<()> {
    failpoint::hit(name)
}

fn new_token() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!(
        "{}-{}-{}",
        std::process::id(),
        nanos,
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// The token in a generation's completion marker, if it has one.
pub fn marker_token(dir: &Path) -> Option<String> {
    let text = fs::read_to_string(dir.join(COMPLETE_MARKER)).ok()?;
    let token = text.trim();
    (!token.is_empty()).then(|| token.to_string())
}

/// The last completed generation, as the absolute path rsync should use for
/// `--link-dest`. `current` is only repointed after a successful run, so it
/// ignores partial directories left by older versions. Returns `None` for the
/// initial backup or when `current` is not a symlink to a directory.
pub fn link_dest_base(dest: &Path) -> Option<PathBuf> {
    let current = dest.join("current");
    let meta = fs::symlink_metadata(&current).ok()?;
    if !meta.file_type().is_symlink() {
        return None;
    }
    let real = current.canonicalize().ok()?;
    real.is_dir().then_some(real)
}

/// Name of the generation `current` points at, when it is a plain relative
/// link to an existing generation in `dest`.
fn current_generation_name(dest: &Path) -> Option<String> {
    let target = fs::read_link(dest.join("current")).ok()?;
    let mut parts = target.components();
    let name = match (parts.next(), parts.next()) {
        (Some(std::path::Component::Normal(name)), None) => name.to_string_lossy().to_string(),
        _ => return None,
    };
    dest.join(&name).is_dir().then_some(name)
}

/// Whether `<dest>/<backup_day>` is a completed generation: a real directory
/// that either carries a completion marker or is what `current` points at
/// (`current` only ever moved after a successful run, so this recognises
/// generations from releases that predate the marker). Any other directory
/// under that name is a partial left by an old release, not a recovery point.
pub fn is_complete_target(dest: &Path, backup_day: &str) -> bool {
    let target = dest.join(backup_day);
    match fs::symlink_metadata(&target) {
        Ok(meta) if meta.is_dir() => {}
        _ => return false,
    }
    marker_token(&target).is_some() || current_generation_name(dest).as_deref() == Some(backup_day)
}

/// The newest generation directory of any kind, completed or not. Used to
/// refuse rebuilding a day that is not the latest.
pub fn newest_generation(dest: &Path) -> io::Result<Option<String>> {
    let mut newest: Option<String> = None;
    for entry in fs::read_dir(dest)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name == "current" || name == ".timevault" || is_internal_name(&name) {
            continue;
        }
        if entry.file_type()?.is_dir() && newest.as_deref().is_none_or(|n| name.as_str() > n) {
            newest = Some(name);
        }
    }
    Ok(newest)
}

/// The newest marked generation strictly older than `backup_day`.
fn previous_complete_before(dest: &Path, backup_day: &str) -> io::Result<Option<PathBuf>> {
    let mut best: Option<String> = None;
    for entry in fs::read_dir(dest)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name == "current" || name == ".timevault" || is_internal_name(&name) {
            continue;
        }
        if name.as_str() >= backup_day
            || !entry.file_type()?.is_dir()
            || marker_token(&entry.path()).is_none()
        {
            continue;
        }
        if best.as_deref().is_none_or(|b| name.as_str() > b) {
            best = Some(name);
        }
    }
    Ok(best.and_then(|name| dest.join(name).canonicalize().ok()))
}

/// The `--link-dest` references for building `backup_day`, best first.
///
/// A normal run links against the last completed generation (`current`). When
/// the day is being recreated and `current` is that very day, the previous
/// generation is the base and the existing day is added as a second, read-only
/// reference, so files unchanged since it are linked from it instead of stored
/// again. The existing day is retired only after the replacement is published.
pub fn link_dest_refs(dest: &Path, backup_day: &str, recreate: bool) -> io::Result<Vec<PathBuf>> {
    let base = link_dest_base(dest);
    if !recreate || !dest.is_dir() {
        return Ok(base.into_iter().collect());
    }
    let existing = is_complete_target(dest, backup_day)
        .then(|| dest.join(backup_day).canonicalize().ok())
        .flatten();
    let mut refs = Vec::new();
    match (&base, &existing) {
        (Some(base), Some(existing)) if base == existing => {
            refs.extend(previous_complete_before(dest, backup_day)?);
        }
        (Some(base), _) => refs.push(base.clone()),
        (None, _) => {}
    }
    if let Some(existing) = existing {
        refs.push(existing);
    }
    Ok(refs)
}

/// The newest generation that carries a completion marker. Generations from
/// releases that predate the marker are not considered.
pub fn newest_complete(dest: &Path) -> io::Result<Option<String>> {
    let mut newest: Option<String> = None;
    for entry in fs::read_dir(dest)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name == "current" || name == ".timevault" || is_internal_name(&name) {
            continue;
        }
        if !entry.file_type()?.is_dir() || marker_token(&entry.path()).is_none() {
            continue;
        }
        if newest.as_deref().is_none_or(|n| name.as_str() > n) {
            newest = Some(name);
        }
    }
    Ok(newest)
}

/// Point `current` at `backup_day`, replacing the link atomically (a temporary
/// symlink is renamed over it), so there is never a moment without a valid
/// `current`. A `current` that is a real directory is left alone.
pub fn update_current(dest: &Path, backup_day: &str, run_mode: RunMode) -> io::Result<()> {
    let current = dest.join("current");
    let existing = fs::symlink_metadata(&current).ok();
    if existing.as_ref().is_some_and(|meta| meta.is_dir()) {
        crate::pnote!(
            "skip updating current (directory exists): {}",
            current.display()
        );
        return Ok(());
    }
    if run_mode.dry_run {
        crate::pnote!("dry-run: ln -sfn {} {}", backup_day, current.display());
        return Ok(());
    }
    if run_mode.safe_mode && existing.is_some() {
        crate::pnote!("skip remove (safe-mode): {}", current.display());
        return Ok(());
    }
    let tmp = dest.join(CURRENT_TMP);
    match fs::remove_file(&tmp) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    symlink(backup_day, &tmp)?;
    failpoint::hit("before-current-rename")?;
    fs::rename(&tmp, &current)
}

/// Repair `current` after an interruption between publishing a generation and
/// moving the link: if `current` is missing, dangling, or behind the newest
/// marked generation, point it at that generation. Never touches a `current`
/// that is already at or ahead of every marked generation.
pub fn repair_current(dest: &Path, run_mode: RunMode) -> io::Result<()> {
    if run_mode.dry_run || !dest.is_dir() {
        return Ok(());
    }
    let Some(newest) = newest_complete(dest)? else {
        return Ok(());
    };
    let behind = match current_generation_name(dest) {
        Some(name) => name < newest,
        None => true,
    };
    if !behind {
        return Ok(());
    }
    crate::pnote!("recover: pointing current at {}", newest);
    update_current(dest, &newest, run_mode)
}

/// Finish or undo a same-day replacement interrupted by a crash.
pub fn recover_interrupted(dest: &Path, run_mode: RunMode) -> io::Result<()> {
    if run_mode.dry_run || !dest.is_dir() {
        return Ok(());
    }
    recover_exchange(dest)?;
    // A shadow directory left by an interrupted run holds only hard links to
    // files that stay referenced elsewhere, so it is always safe to remove.
    match fs::remove_dir_all(dest.join(RELOC_NAME)) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    match fs::remove_file(dest.join(format!("{}.list", RELOC_NAME))) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    for entry in fs::read_dir(dest)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(day) = name.strip_prefix(REPLACED_PREFIX) else {
            continue;
        };
        let replaced = entry.path();
        let final_dir = dest.join(day);
        if !final_dir.exists() {
            crate::pnote!("recover: restoring {}", final_dir.display());
            fs::rename(&replaced, &final_dir)?;
        } else if run_mode.safe_mode {
            crate::pnote!("skip delete (safe-mode): {}", replaced.display());
        } else {
            crate::pnote!("delete: {}", replaced.display());
            fs::remove_dir_all(&replaced)?;
        }
    }
    Ok(())
}

/// Resolve an interrupted `RENAME_EXCHANGE` publish using the intent file.
///
/// The new generation's marker token is recorded before the swap. Afterwards
/// the token is found in the final directory iff the swap happened:
/// - swapped: the staging path holds the displaced old generation; set it aside
///   as a replaced directory so the normal cleanup below disposes of it;
/// - not swapped: staging still holds the unpublished new generation, which the
///   next run discards.
fn recover_exchange(dest: &Path) -> io::Result<()> {
    let intent = dest.join(PUBLISH_INTENT);
    let text = match fs::read_to_string(&intent) {
        Ok(text) => text,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    let mut lines = text.lines();
    let day = lines.next().unwrap_or("").trim();
    let token = lines.next().unwrap_or("").trim();
    if crate::util::paths::is_safe_name(day) && !token.is_empty() {
        let final_dir = dest.join(day);
        let staging = staging_path(dest);
        let swapped = marker_token(&final_dir).as_deref() == Some(token);
        if swapped && staging.exists() {
            let replaced = replaced_path(dest, day);
            if replaced.exists() {
                fs::remove_dir_all(&replaced)?;
            }
            crate::pnote!(
                "recover: setting aside displaced generation {}",
                final_dir.display()
            );
            fs::rename(&staging, &replaced)?;
        }
    }
    fs::remove_file(&intent)
}

/// Get the staging directory ready for rsync.
///
/// With a link-dest base a leftover staging directory is discarded: files in
/// it may already be hard links to completed generations, and rsync updates
/// attribute-only changes on an existing destination file in place, which
/// would alter those generations. A staging directory that carries a
/// completion marker is a finished generation, never a resumable partial, so
/// it is discarded the same way. Without a base (initial backup) and without a
/// marker nothing in staging is shared, so it is kept and rsync resumes where
/// it stopped.
pub fn prepare_staging(dest: &Path, has_base: bool, run_mode: RunMode) -> io::Result<PathBuf> {
    let staging = staging_path(dest);
    if run_mode.dry_run {
        crate::pnote!("dry-run: mkdir -p {}", staging.display());
        return Ok(staging);
    }
    if staging.exists() && (has_base || marker_token(&staging).is_some()) {
        if run_mode.safe_mode {
            crate::pnote!(
                "skip delete (safe-mode): {} (resuming into it)",
                staging.display()
            );
        } else {
            crate::pnote!("delete incomplete staging: {}", staging.display());
            fs::remove_dir_all(&staging)?;
        }
    }
    fs::create_dir_all(&staging)?;
    Ok(staging)
}

/// Flush a finished staging tree to disk and stamp it complete.
///
/// The data is flushed before the marker exists, and the marker appears by
/// rename, so a power loss can never leave a marked generation whose data was
/// not yet on disk. The marker is replaced by rename, never written in place:
/// a cascaded source tree can carry its own marker, which rsync may have
/// hard-linked from an earlier generation. Returns the marker token.
pub fn finalize_staging(staging: &Path, run_mode: RunMode) -> io::Result<String> {
    if run_mode.dry_run {
        crate::pnote!("dry-run: mark {} complete", staging.display());
        return Ok(String::new());
    }
    if !fs::symlink_metadata(staging)?.is_dir() {
        return Err(io::Error::other(format!(
            "staging {} is not a directory",
            staging.display()
        )));
    }
    sync_filesystem(staging)?;
    let token = new_token();
    let tmp = staging.join(".timevault-complete.new");
    fs::write(&tmp, &token)?;
    fs::File::open(&tmp)?.sync_all()?;
    fs::rename(&tmp, staging.join(COMPLETE_MARKER))?;
    Ok(token)
}

fn sync_filesystem(path: &Path) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let dir = fs::File::open(path)?;
    // SAFETY: the fd is valid for the duration of the call.
    if unsafe { libc::syncfs(dir.as_raw_fd()) } != 0 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::ENOSYS) {
            return Err(err);
        }
    }
    Ok(())
}

enum Exchange {
    Done,
    Unsupported,
}

/// Atomically swap two directories with `renameat2(RENAME_EXCHANGE)`.
fn exchange_dirs(first: &Path, second: &Path) -> io::Result<Exchange> {
    if failpoint::exchange_disabled() {
        return Ok(Exchange::Unsupported);
    }
    let to_c = |path: &Path| {
        CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::other("path contains a NUL byte"))
    };
    let (first, second) = (to_c(first)?, to_c(second)?);
    // SAFETY: both pointers are valid NUL-terminated strings for the call.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            first.as_ptr(),
            libc::AT_FDCWD,
            second.as_ptr(),
            RENAME_EXCHANGE,
        )
    };
    if rc == 0 {
        return Ok(Exchange::Done);
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::ENOSYS) | Some(libc::EINVAL) | Some(libc::EOPNOTSUPP) => {
            Ok(Exchange::Unsupported)
        }
        _ => Err(err),
    }
}

/// Test helper: whether the filesystem under these two directories supports
/// `RENAME_EXCHANGE` (swaps them as a side effect when it does).
#[cfg(test)]
pub fn exchange_probe(first: &Path, second: &Path) -> io::Result<bool> {
    Ok(matches!(exchange_dirs(first, second)?, Exchange::Done))
}

/// Atomically publish the staged generation as `<dest>/<backup_day>`.
///
/// If that name does not exist the staging directory is simply renamed into
/// place. If it does (a same-day re-run, or a partial left by an older
/// version) the two directories are swapped in one step where the filesystem
/// allows, so the day's name never disappears; otherwise the old directory is
/// renamed aside first. The displaced directory is removed afterwards. Either
/// way a crash in between is repaired by [`recover_interrupted`].
pub fn publish(dest: &Path, staging: &Path, backup_day: &str, run_mode: RunMode) -> io::Result<()> {
    let final_dir = dest.join(backup_day);
    if run_mode.dry_run {
        crate::pnote!("dry-run: mv {} {}", staging.display(), final_dir.display());
        return Ok(());
    }
    let had_existing = fs::symlink_metadata(&final_dir).is_ok();
    if !had_existing {
        fs::rename(staging, &final_dir)?;
        return failpoint::hit("after-publish-rename");
    }
    let token = marker_token(staging).unwrap_or_default();
    if !token.is_empty() && publish_by_exchange(dest, staging, &final_dir, backup_day, &token)? {
        return dispose_displaced(dest, staging, backup_day, run_mode);
    }
    publish_by_aside(dest, staging, &final_dir, backup_day, run_mode)
}

/// Swap staging and the existing generation. Returns `false` when the
/// filesystem does not support the exchange (nothing was changed).
fn publish_by_exchange(
    dest: &Path,
    staging: &Path,
    final_dir: &Path,
    backup_day: &str,
    token: &str,
) -> io::Result<bool> {
    let intent = dest.join(PUBLISH_INTENT);
    fs::write(&intent, format!("{}\n{}\n", backup_day, token))?;
    failpoint::hit("before-exchange")?;
    match exchange_dirs(staging, final_dir) {
        Ok(Exchange::Done) => {
            failpoint::hit("after-exchange")?;
            Ok(true)
        }
        Ok(Exchange::Unsupported) => {
            let _ = fs::remove_file(&intent);
            Ok(false)
        }
        Err(err) => {
            let _ = fs::remove_file(&intent);
            Err(err)
        }
    }
}

/// After an exchange the staging path holds the displaced generation.
fn dispose_displaced(
    dest: &Path,
    staging: &Path,
    backup_day: &str,
    run_mode: RunMode,
) -> io::Result<()> {
    let replaced = replaced_path(dest, backup_day);
    if replaced.exists() {
        fs::remove_dir_all(&replaced)?;
    }
    fs::rename(staging, &replaced)?;
    failpoint::hit("after-set-aside")?;
    fs::remove_file(dest.join(PUBLISH_INTENT))?;
    remove_replaced(&replaced, run_mode);
    Ok(())
}

fn remove_replaced(replaced: &Path, run_mode: RunMode) {
    if run_mode.safe_mode {
        crate::pnote!("skip delete (safe-mode): {}", replaced.display());
    } else if let Err(err) = fs::remove_dir_all(replaced) {
        crate::pnote!("warning: could not remove {}: {}", replaced.display(), err);
    }
}

/// Fallback for filesystems without `RENAME_EXCHANGE`: set the old directory
/// aside, rename staging into place, then remove the old one.
fn publish_by_aside(
    dest: &Path,
    staging: &Path,
    final_dir: &Path,
    backup_day: &str,
    run_mode: RunMode,
) -> io::Result<()> {
    let replaced = replaced_path(dest, backup_day);
    fs::rename(final_dir, &replaced)?;
    failpoint::hit("after-aside")?;
    if let Err(err) = fs::rename(staging, final_dir) {
        let _ = fs::rename(&replaced, final_dir);
        return Err(err);
    }
    remove_replaced(&replaced, run_mode);
    Ok(())
}

/// Test-only fail points, used to simulate a crash at a named step by making
/// that step return an error with no cleanup. Compiled out of release builds.
#[cfg(test)]
pub mod failpoint {
    use std::cell::{Cell, RefCell};
    use std::io;

    thread_local! {
        static ACTIVE: RefCell<Option<String>> = const { RefCell::new(None) };
        static NO_EXCHANGE: Cell<bool> = const { Cell::new(false) };
    }

    pub struct Guard;

    impl Drop for Guard {
        fn drop(&mut self) {
            ACTIVE.with(|a| *a.borrow_mut() = None);
            NO_EXCHANGE.with(|n| n.set(false));
        }
    }

    /// Make the step called `name` fail on this thread until the guard drops.
    pub fn set(name: &str) -> Guard {
        ACTIVE.with(|a| *a.borrow_mut() = Some(name.to_string()));
        Guard
    }

    /// Pretend the filesystem does not support `RENAME_EXCHANGE`.
    pub fn disable_exchange() -> Guard {
        NO_EXCHANGE.with(|n| n.set(true));
        Guard
    }

    pub fn exchange_disabled() -> bool {
        NO_EXCHANGE.with(Cell::get)
    }

    pub fn hit(name: &str) -> io::Result<()> {
        ACTIVE.with(|a| match a.borrow().as_deref() {
            Some(active) if active == name => Err(io::Error::other(format!("failpoint {}", name))),
            _ => Ok(()),
        })
    }
}

#[cfg(not(test))]
mod failpoint {
    use std::io;

    pub fn exchange_disabled() -> bool {
        false
    }

    pub fn hit(_name: &str) -> io::Result<()> {
        Ok(())
    }
}
