use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use chrono::{Duration, Local};
use walkdir::WalkDir;

use crate::backup::pristine::{build_pristine_excludes_for_source, PristineSource};
use crate::backup::rsync::run_rsync;
use crate::config::model::Job;
use crate::error::{Result, TimevaultError};
use crate::types::RunMode;
use crate::util::paths::job_lock_path;

pub mod pristine;
pub mod rsync;

const TIMEVAULT_MARKER: &str = ".timevault";

#[derive(Debug, Clone, Copy)]
pub struct BackupOptions {
    pub exclude_pristine: bool,
    pub exclude_pristine_only: bool,
}

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
    options: BackupOptions,
) -> Result<()> {
    let pristine_excludes = build_pristine_excludes_for_jobs(&jobs, options, run_mode.verbose)?;
    for job in jobs {
        let _lock = acquire_lock_for_job(&job.name, run_mode)?;
        let home = env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let tmp_dir = Path::new(&home).join("tmp");
        if !run_mode.dry_run {
            fs::create_dir_all(&tmp_dir)?;
        }
        let excludes_file = tmp_dir.join("timevault.excludes");
        let excludes = build_exclude_list(&job, &pristine_excludes)?;
        if run_mode.dry_run {
            println!(
                "dry-run: would write excludes file {}",
                excludes_file.display()
            );
        } else {
            create_excludes_file(&excludes, &excludes_file)?;
        }
        if options.exclude_pristine_only {
            if run_mode.verbose {
                println!(
                    "pristine: exclude-only mode enabled; skipping backup for job {}",
                    job.name
                );
            }
            continue;
        }

        let backup_day = (Local::now() - Duration::days(1))
            .format("%Y%m%d")
            .to_string();
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
            rc = run_rsync(
                &job.source,
                &backup_dir,
                &excludes_file,
                rsync_extra,
                run_mode,
            )?;
            if rc == 0 || rc == 24 {
                break;
            }
            if attempt < 3 {
                println!(
                    "rsync failed with exit code {}; retrying ({}/3)",
                    rc,
                    attempt + 1
                );
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
                    println!(
                        "skip updating current (directory exists): {}",
                        current_link.display()
                    );
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

pub fn run_pristine_only(jobs: Vec<Job>, run_mode: RunMode, options: BackupOptions) -> Result<()> {
    if run_mode.verbose {
        println!("pristine: exclude-only mode enabled; skipping backup");
    }
    let pristine_excludes = build_pristine_excludes_for_jobs(&jobs, options, run_mode.verbose)?;
    for job in jobs {
        let _lock = acquire_lock_for_job(&job.name, run_mode)?;
        let home = env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let tmp_dir = Path::new(&home).join("tmp");
        if !run_mode.dry_run {
            fs::create_dir_all(&tmp_dir)?;
        }
        let excludes_file = tmp_dir.join("timevault.excludes");
        let excludes = build_exclude_list(&job, &pristine_excludes)?;
        if run_mode.dry_run {
            println!(
                "dry-run: would write excludes file {}",
                excludes_file.display()
            );
        } else {
            create_excludes_file(&excludes, &excludes_file)?;
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
struct PristineExcludes {
    local: Option<Vec<String>>,
    remote: HashMap<String, Vec<String>>,
}

fn build_exclude_list(job: &Job, pristine_excludes: &PristineExcludes) -> Result<Vec<String>> {
    let mut excludes = job.excludes.clone();
    if let Some(pristine) = pristine_excludes_for_job(job, pristine_excludes) {
        excludes.extend(pristine.iter().cloned());
    }
    Ok(excludes)
}

fn build_pristine_excludes_for_jobs(
    jobs: &[Job],
    options: BackupOptions,
    verbose: bool,
) -> Result<PristineExcludes> {
    if !options.exclude_pristine {
        return Ok(PristineExcludes::default());
    }
    let mut excludes = PristineExcludes::default();
    if jobs
        .iter()
        .any(|job| pristine_source_for_job(job) == Some(PristineSource::Local))
    {
        excludes.local = Some(build_pristine_excludes_for_source(
            &PristineSource::Local,
            verbose,
        )?);
    }
    let mut remote_hosts = jobs
        .iter()
        .filter_map(|job| match pristine_source_for_job(job) {
            Some(PristineSource::RemoteSsh { host }) => Some(host),
            _ => None,
        })
        .collect::<Vec<_>>();
    remote_hosts.sort();
    remote_hosts.dedup();
    for host in remote_hosts {
        let source = PristineSource::RemoteSsh { host: host.clone() };
        let host_excludes = build_pristine_excludes_for_source(&source, verbose)?;
        excludes.remote.insert(host, host_excludes);
    }
    if verbose && excludes.local.is_none() && excludes.remote.is_empty() {
        println!(
            "pristine: skip package analysis; selected job sources are not supported for pristine analysis"
        );
    }
    Ok(excludes)
}

fn pristine_excludes_for_job<'a>(
    job: &Job,
    pristine_excludes: &'a PristineExcludes,
) -> Option<&'a [String]> {
    match pristine_source_for_job(job) {
        Some(PristineSource::Local) => pristine_excludes.local.as_deref(),
        Some(PristineSource::RemoteSsh { host }) => {
            pristine_excludes.remote.get(&host).map(Vec::as_slice)
        }
        None => None,
    }
}

fn pristine_source_for_job(job: &Job) -> Option<PristineSource> {
    if let Some(host) = remote_ssh_host_from_source(&job.source) {
        return Some(PristineSource::RemoteSsh { host });
    }
    if job.source.trim().starts_with("rsync://") {
        return None;
    }
    Some(PristineSource::Local)
}

fn remote_ssh_host_from_source(source: &str) -> Option<String> {
    let source = source.trim();
    if source.starts_with('/') || source.starts_with("rsync://") {
        return None;
    }
    let (host, path) = source.split_once(':')?;
    if host.is_empty() || !path.starts_with('/') {
        return None;
    }
    Some(host.to_string())
}

fn create_excludes_file(excludes: &[String], filename: &Path) -> io::Result<()> {
    let mut f = File::create(filename)?;
    for exclude in excludes {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RunPolicy;

    fn job(source: &str) -> Job {
        Job {
            name: "test".to_string(),
            source: source.to_string(),
            copies: 1,
            run_policy: RunPolicy::Auto,
            excludes: vec!["/tmp".to_string()],
            disk_ids: None,
        }
    }

    #[test]
    fn detects_remote_rsync_sources() {
        assert_eq!(
            remote_ssh_host_from_source("root@example.com:/").as_deref(),
            Some("root@example.com")
        );
        assert_eq!(
            remote_ssh_host_from_source("example.com:/var").as_deref(),
            Some("example.com")
        );
        assert_eq!(
            remote_ssh_host_from_source("rsync://example.com/module"),
            None
        );
        assert_eq!(remote_ssh_host_from_source("/"), None);
        assert_eq!(remote_ssh_host_from_source("/srv/data"), None);
    }

    #[test]
    fn remote_jobs_get_matching_remote_pristine_excludes() {
        let mut pristine = PristineExcludes::default();
        pristine.remote.insert(
            "root@example.com".to_string(),
            vec!["/usr/bin/bash".to_string()],
        );
        let excludes =
            build_exclude_list(&job("root@example.com:/"), &pristine).expect("exclude list");

        assert_eq!(
            excludes,
            vec!["/tmp".to_string(), "/usr/bin/bash".to_string()]
        );
    }

    #[test]
    fn remote_jobs_do_not_get_local_pristine_excludes() {
        let pristine = PristineExcludes {
            local: Some(vec!["/usr/bin/bash".to_string()]),
            remote: HashMap::new(),
        };
        let excludes =
            build_exclude_list(&job("root@example.com:/"), &pristine).expect("exclude list");

        assert_eq!(excludes, vec!["/tmp".to_string()]);
    }

    #[test]
    fn local_jobs_get_pristine_excludes() {
        let pristine = PristineExcludes {
            local: Some(vec!["/usr/bin/bash".to_string()]),
            remote: HashMap::new(),
        };
        let excludes = build_exclude_list(&job("/"), &pristine).expect("exclude list");

        assert_eq!(
            excludes,
            vec!["/tmp".to_string(), "/usr/bin/bash".to_string()]
        );
    }
}
