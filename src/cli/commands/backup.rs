use std::process::Command;

use chrono::Local;

use crate::backup::{print_job_details, run_backup, run_pristine_only, BackupOptions};
use crate::cli::commands::exit_for_disk_error;
use crate::config::load::load_config;
use crate::disk::{
    connected_disks_in_order, device_path_for_uuid, mount_disk_guarded, mount_options_for_backup,
    select_first_connected,
};
use crate::disk::fs_type::detect_fs_type;
use crate::disk::identity::{identity_path, read_identity, verify_identity};
use crate::error::{DiskError, Result, TimevaultError};
use crate::mount::guard::MountGuard;
use crate::types::RunMode;
use crate::util::command::run_command;
use std::collections::HashMap;

pub fn run_backup_command(
    config_path: &std::path::Path,
    selected_jobs: &[String],
    print_order: bool,
    disk_id: Option<&str>,
    cascade: bool,
    run_mode: RunMode,
    rsync_extra_cli: &[String],
    options: BackupOptions,
) -> Result<()> {
    println!("{}", Local::now().format("%d-%m-%Y %H:%M"));

    let cfg = load_config(config_path.to_string_lossy().as_ref())?;
    let jobs = cfg.jobs.clone();
    let backup_disks = cfg.backup_disks.clone();
    let mount_base = std::path::PathBuf::from(cfg.mount_base.clone());
    let mut rsync_extra = cfg.options.rsync.clone();
    rsync_extra.extend(rsync_extra_cli.iter().cloned());
    let run_mode = RunMode {
        verbose: run_mode.verbose || cfg.options.verbose.unwrap_or(false),
        safe_mode: run_mode.safe_mode || cfg.options.safe.unwrap_or(false),
        ..run_mode
    };
    let cascade = cascade || cfg.options.cascade.unwrap_or(false);
    let options = BackupOptions {
        exclude_pristine: options.exclude_pristine || cfg.options.exclude_pristine.unwrap_or(false),
        exclude_pristine_only: options.exclude_pristine_only,
    };

    if backup_disks.is_empty() && !options.exclude_pristine_only {
        return Err(TimevaultError::message(
            "no backup disks enrolled; run `timevault disk enroll ...`".to_string(),
        ));
    }

    let selected_set: std::collections::HashSet<String> = selected_jobs.iter().cloned().collect();
    let mut jobs_to_run = Vec::new();
    if selected_set.is_empty() {
        for job in &jobs {
            if job.run_policy == crate::types::RunPolicy::Auto {
                jobs_to_run.push(job.clone());
            }
        }
    } else {
        for job in &jobs {
            if selected_set.contains(&job.name) {
                if job.run_policy == crate::types::RunPolicy::Off {
                    println!("job disabled (off): {}", job.name);
                    println!("requested job(s) are disabled; aborting");
                    std::process::exit(2);
                }
                jobs_to_run.push(job.clone());
            }
        }
        if jobs_to_run.len() != selected_set.len() {
            for name in &selected_set {
                if !jobs.iter().any(|job| &job.name == name) {
                    println!("job not found: {}", name);
                }
            }
            println!("no such job(s) found; aborting");
            std::process::exit(2);
        }
    }

    if jobs_to_run.is_empty() {
        if selected_set.is_empty() {
            println!("no jobs matched (no auto jobs enabled); aborting");
        } else {
            println!("no jobs matched selection; aborting");
        }
        std::process::exit(2);
    }

    let disk_filter = disk_id.map(|id| id.to_string());
    if let Some(ref disk_filter) = disk_filter {
        jobs_to_run.retain(|job| job_requires_disk(job, disk_filter));
        if jobs_to_run.is_empty() {
            println!("no jobs matched disk selection; aborting");
            std::process::exit(2);
        }
    }

    if print_order {
        for job in &jobs_to_run {
            print_job_details(job);
        }
        std::process::exit(0);
    }

    if run_mode.verbose {
        println!(
            "loaded config {} with {} job(s)",
            config_path.display(),
            jobs_to_run.len()
        );
    }
    if options.exclude_pristine_only {
        run_pristine_only(jobs_to_run, run_mode, options)?;
        return Ok(());
    }

    let connected = connected_disks_in_order(&backup_disks);
    if connected.is_empty() {
        exit_for_disk_error(&DiskError::NoDiskConnected);
    }

    if let Some(disk_id) = disk_id {
        let primary_disk = match select_first_connected(&backup_disks, Some(disk_id)) {
            Ok(disk) => disk,
            Err(TimevaultError::Disk(err)) => exit_for_disk_error(&err),
            Err(err) => return Err(err),
        };
        jobs_to_run.retain(|job| job_requires_disk(job, disk_id));
        if jobs_to_run.is_empty() {
            println!("no jobs matched disk selection; aborting");
            std::process::exit(2);
        }
        if let Some((code, message)) = run_jobs_for_primary(
            &primary_disk,
            jobs_to_run,
            &connected,
            cascade,
            run_mode,
            &rsync_extra,
            &mount_base,
            options,
        )? {
            println!("{}", message);
            std::process::exit(code);
        }
    } else {
        let mut groups: HashMap<String, Vec<crate::config::model::Job>> = HashMap::new();
        for job in jobs_to_run {
            let allowed = allowed_disks_for_job(&job, &connected);
            if allowed.is_empty() {
                println!("job {} has no connected disks; aborting", job.name);
                std::process::exit(2);
            }
            let primary = allowed[0].clone();
            groups
                .entry(primary.disk_id.clone())
                .or_default()
                .push(job);
        }

        for disk in &connected {
            let Some(jobs) = groups.remove(&disk.disk_id) else {
                continue;
            };
            if let Some((code, message)) = run_jobs_for_primary(
                disk,
                jobs,
                &connected,
                cascade,
                run_mode,
                &rsync_extra,
                &mount_base,
                options,
            )? {
                println!("{}", message);
                std::process::exit(code);
            }
        }
    }

    if !run_mode.dry_run {
        let mut sync_cmd = Command::new("sync");
        let _ = run_command(&mut sync_cmd, run_mode);
    }
    println!("{}", Local::now().format("%d-%m-%Y %H:%M"));

    Ok(())
}

fn job_requires_disk(job: &crate::config::model::Job, disk_id: &str) -> bool {
    match &job.disk_ids {
        Some(ids) => ids.iter().any(|id| id == disk_id),
        None => false,
    }
}

fn allowed_disks_for_job(
    job: &crate::config::model::Job,
    connected: &[crate::config::model::BackupDiskConfig],
) -> Vec<crate::config::model::BackupDiskConfig> {
    match &job.disk_ids {
        Some(ids) => connected
            .iter()
            .filter(|disk| ids.iter().any(|id| id == &disk.disk_id))
            .cloned()
            .collect(),
        None => connected.to_vec(),
    }
}

fn run_jobs_for_primary(
    primary_disk: &crate::config::model::BackupDiskConfig,
    jobs: Vec<crate::config::model::Job>,
    connected: &[crate::config::model::BackupDiskConfig],
    cascade: bool,
    run_mode: RunMode,
    rsync_extra: &[String],
    mount_base: &std::path::Path,
    options: BackupOptions,
) -> Result<Option<(i32, String)>> {
    let (primary_guard, primary_mount) = mount_and_verify(primary_disk, mount_base, run_mode)?;
    let primary_current_base = primary_mount.clone();

    if let Some((code, message)) =
        run_backup_checked(jobs.clone(), rsync_extra, run_mode, &primary_mount, options)
    {
        drop(primary_guard);
        return Ok(Some((code, message)));
    }

    if cascade {
        let mut cascades: HashMap<String, Vec<crate::config::model::Job>> = HashMap::new();
        for job in &jobs {
            let allowed = allowed_disks_for_job(job, connected);
            for disk in allowed {
                if disk.fs_uuid == primary_disk.fs_uuid {
                    continue;
                }
                cascades
                    .entry(disk.disk_id.clone())
                    .or_default()
                    .push(job.clone());
            }
        }

        for disk in connected {
            let Some(job_list) = cascades.get(&disk.disk_id) else {
                continue;
            };
            let (guard, mountpoint) = mount_and_verify(disk, mount_base, run_mode)?;
            let mut cascaded_jobs = Vec::new();
            for job in job_list {
                let mut job_override = job.clone();
                let source = primary_current_base.join(&job.name).join("current");
                if !run_mode.dry_run && !source.exists() {
                    drop(guard);
                    drop(primary_guard);
                    return Ok(Some((
                        1,
                        format!(
                            "missing cascade source {}; primary disk did not produce current snapshot",
                            source.display()
                        ),
                    )));
                }
                if run_mode.dry_run && !source.exists() {
                    println!("dry-run: skip cascade (missing {})", source.display());
                    continue;
                }
                job_override.source = source.to_string_lossy().to_string();
                cascaded_jobs.push(job_override);
            }
            if cascaded_jobs.is_empty() {
                drop(guard);
                continue;
            }
            if let Some((code, message)) =
                run_backup_checked(cascaded_jobs, rsync_extra, run_mode, &mountpoint, options)
            {
                drop(guard);
                drop(primary_guard);
                return Ok(Some((code, message)));
            }
            drop(guard);
        }
    }

    drop(primary_guard);
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RunPolicy;

    #[test]
    fn job_requires_disk_needs_explicit_match() {
        let job = crate::config::model::Job {
            name: "job".to_string(),
            source: "/".to_string(),
            copies: 1,
            run_policy: RunPolicy::Auto,
            excludes: Vec::new(),
            disk_ids: None,
        };
        assert!(!job_requires_disk(&job, "disk-a"));
        let job = crate::config::model::Job {
            name: "job".to_string(),
            source: "/".to_string(),
            copies: 1,
            run_policy: RunPolicy::Auto,
            excludes: Vec::new(),
            disk_ids: Some(vec!["disk-a".to_string()]),
        };
        assert!(job_requires_disk(&job, "disk-a"));
        assert!(!job_requires_disk(&job, "disk-b"));
    }

    #[test]
    fn allowed_disks_for_job_filters_connected() {
        let connected = vec![
            crate::config::model::BackupDiskConfig {
                disk_id: "disk-a".to_string(),
                fs_uuid: "uuid-a".to_string(),
                label: None,
                mount_options: None,
            },
            crate::config::model::BackupDiskConfig {
                disk_id: "disk-b".to_string(),
                fs_uuid: "uuid-b".to_string(),
                label: None,
                mount_options: None,
            },
        ];
        let job = crate::config::model::Job {
            name: "job".to_string(),
            source: "/".to_string(),
            copies: 1,
            run_policy: RunPolicy::Auto,
            excludes: Vec::new(),
            disk_ids: Some(vec!["disk-b".to_string()]),
        };
        let allowed = allowed_disks_for_job(&job, &connected);
        assert_eq!(allowed.len(), 1);
        assert_eq!(allowed[0].disk_id, "disk-b");
    }

    #[test]
    fn allowed_disks_for_job_without_filter_returns_all() {
        let connected = vec![
            crate::config::model::BackupDiskConfig {
                disk_id: "disk-a".to_string(),
                fs_uuid: "uuid-a".to_string(),
                label: None,
                mount_options: None,
            },
            crate::config::model::BackupDiskConfig {
                disk_id: "disk-b".to_string(),
                fs_uuid: "uuid-b".to_string(),
                label: None,
                mount_options: None,
            },
        ];
        let job = crate::config::model::Job {
            name: "job".to_string(),
            source: "/".to_string(),
            copies: 1,
            run_policy: RunPolicy::Auto,
            excludes: Vec::new(),
            disk_ids: None,
        };
        let allowed = allowed_disks_for_job(&job, &connected);
        assert_eq!(allowed.len(), 2);
    }
}

fn mount_and_verify(
    disk: &crate::config::model::BackupDiskConfig,
    mount_base: &std::path::Path,
    run_mode: RunMode,
) -> Result<(Option<MountGuard>, std::path::PathBuf)> {
    let options = mount_options_for_backup(disk);
    let (disk_guard, mountpoint) = if run_mode.dry_run {
        (None, mount_base.join(&disk.fs_uuid))
    } else {
        match mount_disk_guarded(disk, mount_base, &options) {
            Ok(result) => (Some(result.0), result.1),
            Err(TimevaultError::Disk(err)) => exit_for_disk_error(&err),
            Err(err) => return Err(err),
        }
    };

    if !run_mode.dry_run {
        let identity_path = identity_path(&mountpoint);
        if !identity_path.exists() {
            drop(disk_guard);
            exit_for_disk_error(&DiskError::IdentityMismatch(format!(
                "file missing at {}; expected diskId {} fsUuid {} (run `timevault disk enroll ...`)",
                identity_path.display(),
                disk.disk_id,
                disk.fs_uuid
            )));
        }
        let identity = match read_identity(&identity_path) {
            Ok(identity) => identity,
            Err(err) => {
                drop(disk_guard);
                return Err(TimevaultError::message(format!("identity file invalid: {}", err)));
            }
        };
        if let Err(err) = verify_identity(&identity, &disk.disk_id, &disk.fs_uuid) {
            drop(disk_guard);
            if let TimevaultError::Disk(disk_err) = err {
                exit_for_disk_error(&disk_err);
            }
            return Err(err);
        }

        let device = device_path_for_uuid(&disk.fs_uuid);
        let fs_type = detect_fs_type(device.to_string_lossy().as_ref())?;
        if !fs_type.is_allowed() {
            drop(disk_guard);
            return Err(TimevaultError::Disk(DiskError::Other(format!(
                "unsupported filesystem type {}",
                fs_type
            ))));
        }
        if let Some(identity_type) = identity.fs_type {
            if identity_type != fs_type {
                drop(disk_guard);
                return Err(TimevaultError::Disk(DiskError::IdentityMismatch(format!(
                    "fsType mismatch: expected {}, got {}",
                    identity_type,
                    fs_type
                ))));
            }
        }
    }

    Ok((disk_guard, mountpoint))
}

fn run_backup_checked(
    jobs: Vec<crate::config::model::Job>,
    rsync_extra: &[String],
    run_mode: RunMode,
    mountpoint: &std::path::Path,
    options: BackupOptions,
) -> Option<(i32, String)> {
    let backup_result = run_backup(jobs, rsync_extra, run_mode, mountpoint, options);
    if let Err(err) = backup_result {
        let message = err.to_string();
        if message.starts_with("job ") && message.ends_with(" is already running") {
            return Some((3, message));
        }
        if message.starts_with("failed to lock ") {
            return Some((
                2,
                format!(
                    "{} (need write permission; try sudo or adjust permissions)",
                    message
                ),
            ));
        }
        return Some((1, format!("backup failed: {}", message)));
    }
    None
}
