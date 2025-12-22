use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use chrono::{Duration, Local};
use walkdir::WalkDir;

use crate::backup::rsync::run_rsync;
use crate::config::model::Job;
use crate::error::{Result, TimevaultError};
use crate::types::RunMode;
use crate::util::paths::job_lock_path;

pub mod rsync;

const TIMEVAULT_MARKER: &str = ".timevault";

struct LockGuard {
    path: PathBuf,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = unlock_file(&self.path);
    }
}

pub fn print_job_details(job: &Job) {
    let excludes = if job.excludes.is_empty() {
        "<none>".to_string()
    } else {
        job.excludes.join(", ")
    };
    let disk_ids = match &job.disk_ids {
        Some(ids) if !ids.is_empty() => ids.join(", "),
        _ => "<any>".to_string(),
    };
    println!("job: {}", job.name);
    println!("  source: {}", job.source);
    println!("  backup dir: {}", job.name);
    println!("  copies: {}", job.copies);
    println!("  run: {}", job.run_policy.as_str());
    println!("  excludes: {}", excludes);
    println!("  disks: {}", disk_ids);
}

pub fn run_backup(
    jobs: Vec<Job>,
    rsync_extra: &[String],
    run_mode: RunMode,
    disk_mount: &Path,
) -> Result<()> {
    for job in jobs {
        let _lock = acquire_lock_for_job(&job.name, run_mode)?;
        let home = env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let tmp_dir = Path::new(&home).join("tmp");
        if !run_mode.dry_run {
            fs::create_dir_all(&tmp_dir)?;
        }
        let excludes_file = tmp_dir.join("timevault.excludes");
        if run_mode.dry_run {
            println!("dry-run: would write excludes file {}", excludes_file.display());
        } else {
            create_excludes_file(&job, &excludes_file)?;
        }

        let backup_day = (Local::now() - Duration::days(1)).format("%Y%m%d").to_string();
        if run_mode.verbose {
            println!("  backup day: {}", backup_day);
        }

        let dest = resolve_job_dest(&job, disk_mount)?;
        if run_mode.verbose {
            println!("job: {}", job.name);
            println!("  run: {}", job.run_policy.as_str());
            println!("  source: {}", job.source);
            println!("  backup dir: {}", dest.display());
            println!("  copies: {}", job.copies);
            println!("  excludes: {}", job.excludes.len());
        }

        if !dest.exists() {
            if run_mode.dry_run {
                println!("dry-run: mkdir -p {}", dest.display());
            } else {
                fs::create_dir_all(&dest)?;
            }
        }

        expire_old_backups(&job, &dest, run_mode)?;

        let current = dest.join("current");
        let backup_dir = dest.join(&backup_day);

        if current.exists() && !backup_dir.exists() {
            if run_mode.dry_run {
                println!("dry-run: mkdir -p {}", backup_dir.display());
            } else {
                fs::create_dir_all(&backup_dir)?;
            }
            copy_snapshot_without_symlinks(&current, &backup_dir, run_mode)?;
        }

        let mut rc = 1;
        for attempt in 1..=3 {
            rc = run_rsync(&job.source, &backup_dir, &excludes_file, rsync_extra, run_mode)?;
            if rc == 0 || rc == 24 {
                break;
            }
            if attempt < 3 {
                println!("rsync failed with exit code {}; retrying ({}/3)", rc, attempt + 1);
            }
        }
        let rsync_ok = rc == 0 || rc == 24;
        if !rsync_ok {
            println!("rsync failed with exit code {}; current not updated", rc);
        }

        if rsync_ok && backup_dir.exists() {
            let current_link = dest.join("current");
            if let Ok(meta) = fs::symlink_metadata(&current_link) {
                if meta.file_type().is_symlink() || meta.is_file() {
                    if run_mode.safe_mode || run_mode.dry_run {
                        if run_mode.dry_run {
                            println!("dry-run: rm -f {}", current_link.display());
                        } else {
                            println!("skip remove (safe-mode): {}", current_link.display());
                        }
                    } else {
                        let _ = fs::remove_file(&current_link);
                    }
                } else if meta.is_dir() {
                    println!("skip updating current (directory exists): {}", current_link.display());
                }
            }
            if !current_link.exists() {
                if run_mode.dry_run {
                    println!("dry-run: ln -s {} {}", backup_day, current_link.display());
                } else {
                    symlink(&backup_day, &current_link)?;
                }
            }
        }
    }
    Ok(())
}

fn create_excludes_file(job: &Job, filename: &Path) -> io::Result<()> {
    let mut f = File::create(filename)?;
    for exclude in &job.excludes {
        writeln!(f, "{}", exclude)?;
    }
    Ok(())
}

fn expire_old_backups(job: &Job, dest: &Path, run_mode: RunMode) -> io::Result<()> {
    if !dest.exists() {
        return Ok(());
    }
    let mut backups = Vec::new();
    for entry in fs::read_dir(dest)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name == "." || name == ".." || name == "current" || name == TIMEVAULT_MARKER {
            continue;
        }
        backups.push(name);
    }

    backups.sort();
    if backups.len() <= job.copies {
        return Ok(());
    }

    let to_delete = backups.len() - job.copies;
    for name in backups.iter().take(to_delete) {
        let target = dest.join(name);
        let meta = fs::symlink_metadata(&target)?;
        if meta.file_type().is_symlink() {
            println!("skip symlink delete: {}", target.display());
            continue;
        }
        if meta.is_dir() {
            if run_mode.safe_mode || run_mode.dry_run {
                if run_mode.dry_run {
                    println!("dry-run: rm -rf {}", target.display());
                } else {
                    println!("skip delete (safe-mode): {}", target.display());
                }
            } else {
                println!("delete: {}", target.display());
                fs::remove_dir_all(&target)?;
            }
        } else {
            println!("skip non-dir delete: {}", target.display());
        }
    }

    Ok(())
}

fn copy_snapshot_without_symlinks(source: &Path, dest: &Path, run_mode: RunMode) -> io::Result<()> {
    for entry in WalkDir::new(source).follow_links(false) {
        let entry = entry?;
        let src_path = entry.path();
        let rel = src_path.strip_prefix(source).unwrap_or(src_path);
        if rel.as_os_str().is_empty() {
            continue;
        }
        let target = dest.join(rel);
        let ft = entry.file_type();
        if ft.is_symlink() {
            if run_mode.dry_run {
                println!("dry-run: skip symlink {}", src_path.display());
            }
            continue;
        }
        if ft.is_dir() {
            if run_mode.dry_run {
                println!("dry-run: mkdir -p {}", target.display());
            } else {
                fs::create_dir_all(&target)?;
            }
            continue;
        }
        if ft.is_file() {
            if run_mode.dry_run {
                println!("dry-run: ln {} {}", src_path.display(), target.display());
            } else {
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::hard_link(src_path, &target)?;
            }
        }
    }
    Ok(())
}

fn resolve_job_dest(job: &Job, disk_mount: &Path) -> Result<PathBuf> {
    if !crate::util::paths::is_safe_name(&job.name) {
        return Err(TimevaultError::message(format!(
            "job {} name must use only letters, digits, '.', '-', '_'",
            job.name
        )));
    }
    Ok(disk_mount.join(&job.name))
}

fn lock_file(path: &Path) -> io::Result<bool> {
    for _ in 0..3 {
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut f) => {
                writeln!(f, "{}", std::process::id())?;
                return Ok(true);
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                let pid = match fs::read_to_string(path) {
                    Ok(text) => text.trim().parse::<u32>().ok(),
                    Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                    Err(err) => return Err(err),
                };
                if let Some(pid) = pid {
                    if Path::new("/proc").join(pid.to_string()).exists() {
                        return Ok(false);
                    }
                }
                match fs::remove_file(path) {
                    Ok(()) => continue,
                    Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                    Err(err) => return Err(err),
                }
            }
            Err(err) => return Err(err),
        }
    }
    Ok(false)
}

fn unlock_file(path: &Path) -> io::Result<()> {
    let pid = fs::read_to_string(path).ok();
    if let Some(pid) = pid {
        let pid = pid.trim();
        if !pid.is_empty()
            && pid == std::process::id().to_string()
            && Path::new("/proc").join(pid).exists()
        {
            let _ = fs::remove_file(path);
        }
    }
    Ok(())
}

fn acquire_lock_for_job(job_name: &str, run_mode: RunMode) -> Result<Option<LockGuard>> {
    if run_mode.dry_run {
        return Ok(None);
    }
    let path = job_lock_path(job_name)?;
    match lock_file(&path) {
        Ok(true) => Ok(Some(LockGuard { path })),
        Ok(false) => Err(TimevaultError::message(format!(
            "job {} is already running",
            job_name
        ))),
        Err(e) => Err(TimevaultError::message(format!(
            "failed to lock {}: {}",
            path.display(),
            e
        ))),
    }
}
