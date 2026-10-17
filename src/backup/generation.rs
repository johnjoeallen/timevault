//! Snapshot generation lifecycle for a job directory (`<disk>/<job>/`).
//!
//! A generation is a plain directory named after the backup day. New
//! generations are built in a staging directory inside the job directory with
//! `rsync --link-dest` pointing at the last completed generation, then renamed
//! into place, so a completed generation is never modified and a failed run is
//! never visible as a generation.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::types::RunMode;

/// In-progress generation. Reserved name: never counted by retention.
pub const STAGING_NAME: &str = ".timevault-staging";
/// Prefix for a same-day generation set aside while it is being replaced.
pub const REPLACED_PREFIX: &str = ".timevault-replaced-";

pub fn staging_path(dest: &Path) -> PathBuf {
    dest.join(STAGING_NAME)
}

fn replaced_path(dest: &Path, backup_day: &str) -> PathBuf {
    dest.join(format!("{}{}", REPLACED_PREFIX, backup_day))
}

/// True for directories Timevault manages itself, which retention must skip.
pub fn is_internal_name(name: &str) -> bool {
    name == STAGING_NAME || name.starts_with(REPLACED_PREFIX)
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

/// Finish or undo a same-day replacement interrupted by a crash.
pub fn recover_interrupted(dest: &Path, run_mode: RunMode) -> io::Result<()> {
    if run_mode.dry_run || !dest.is_dir() {
        return Ok(());
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

/// Get the staging directory ready for rsync.
///
/// With a link-dest base a leftover staging directory is discarded: files in
/// it may already be hard links to completed generations, and rsync updates
/// attribute-only changes on an existing destination file in place, which
/// would alter those generations. Without a base (initial backup) nothing in
/// staging is shared, so it is kept and rsync resumes where it stopped.
pub fn prepare_staging(dest: &Path, has_base: bool, run_mode: RunMode) -> io::Result<PathBuf> {
    let staging = staging_path(dest);
    if run_mode.dry_run {
        crate::pnote!("dry-run: mkdir -p {}", staging.display());
        return Ok(staging);
    }
    if staging.exists() && has_base {
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

/// Atomically publish the staged generation as `<dest>/<backup_day>`.
///
/// If that name already exists (a same-day re-run, or a partial left by an
/// older version) it is renamed aside first and removed afterwards; a crash in
/// between is repaired by [`recover_interrupted`].
pub fn publish(
    dest: &Path,
    staging: &Path,
    backup_day: &str,
    run_mode: RunMode,
) -> io::Result<()> {
    let final_dir = dest.join(backup_day);
    if run_mode.dry_run {
        crate::pnote!(
            "dry-run: mv {} {}",
            staging.display(),
            final_dir.display()
        );
        return Ok(());
    }
    let replaced = replaced_path(dest, backup_day);
    let had_existing = fs::symlink_metadata(&final_dir).is_ok();
    if had_existing {
        fs::rename(&final_dir, &replaced)?;
    }
    if let Err(err) = fs::rename(staging, &final_dir) {
        if had_existing {
            let _ = fs::rename(&replaced, &final_dir);
        }
        return Err(err);
    }
    if had_existing {
        if run_mode.safe_mode {
            crate::pnote!("skip delete (safe-mode): {}", replaced.display());
        } else if let Err(err) = fs::remove_dir_all(&replaced) {
            crate::pnote!("warning: could not remove {}: {}", replaced.display(), err);
        }
    }
    Ok(())
}
