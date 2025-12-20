use std::process::Command;

use chrono::Local;

use crate::backup::{print_job_details, run_backup};
use crate::cli::commands::exit_for_disk_error;
use crate::config::load::load_config;
use crate::disk::{device_path_for_uuid, mount_disk_guarded, mount_options_for_backup, select_disk};
use crate::disk::fs_type::detect_fs_type;
use crate::disk::identity::{identity_path, read_identity, verify_identity};
use crate::error::{DiskError, Result, TimevaultError};
use crate::types::RunMode;
use crate::util::command::run_command;

pub fn run_backup_command(
    config_path: &std::path::Path,
    selected_jobs: &[String],
    print_order: bool,
    disk_id: Option<&str>,
    run_mode: RunMode,
    rsync_extra: &[String],
) -> Result<()> {
    println!("{}", Local::now().format("%d-%m-%Y %H:%M"));

    let cfg = load_config(config_path.to_string_lossy().as_ref())?;
    let jobs = cfg.jobs.clone();
    let backup_disks = cfg.backup_disks.clone();
    let mount_base = std::path::PathBuf::from(cfg.mount_base.clone());

    if backup_disks.is_empty() {
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

    let selected_disk = match select_disk(&backup_disks, disk_id) {
        Ok(disk) => disk,
        Err(TimevaultError::Disk(err)) => exit_for_disk_error(&err),
        Err(err) => return Err(err),
    };

    let options = mount_options_for_backup(&selected_disk);
    let (disk_guard, mountpoint) = if run_mode.dry_run {
        (None, mount_base.join(&selected_disk.fs_uuid))
    } else {
        match mount_disk_guarded(&selected_disk, &mount_base, &options) {
            Ok(result) => (Some(result.0), result.1),
            Err(TimevaultError::Disk(err)) => exit_for_disk_error(&err),
            Err(err) => return Err(err),
        }
    };

    let identity_path = identity_path(&mountpoint);
    if !identity_path.exists() {
        drop(disk_guard);
        exit_for_disk_error(&DiskError::IdentityMismatch(format!(
            "file missing at {}; expected diskId {} fsUuid {} (run `timevault disk enroll ...`)",
            identity_path.display(),
            selected_disk.disk_id,
            selected_disk.fs_uuid
        )));
    }
    let identity = match read_identity(&identity_path) {
        Ok(identity) => identity,
        Err(err) => {
            drop(disk_guard);
            return Err(TimevaultError::message(format!("identity file invalid: {}", err)));
        }
    };
    if let Err(err) = verify_identity(&identity, &selected_disk.disk_id, &selected_disk.fs_uuid) {
        drop(disk_guard);
        if let TimevaultError::Disk(disk_err) = err {
            exit_for_disk_error(&disk_err);
        }
        return Err(err);
    }

    let device = device_path_for_uuid(&selected_disk.fs_uuid);
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

    let backup_result = run_backup(jobs_to_run, rsync_extra, run_mode, &mountpoint);
    drop(disk_guard);

    if let Err(err) = backup_result {
        let message = err.to_string();
        if message.starts_with("job ") && message.ends_with(" is already running") {
            println!("{}", message);
            std::process::exit(3);
        }
        if message.starts_with("failed to lock ") {
            println!(
                "{} (need write permission; try sudo or adjust permissions)",
                message
            );
            std::process::exit(2);
        }
        println!("backup failed: {}", message);
        std::process::exit(1);
    }

    if !run_mode.dry_run {
        let mut sync_cmd = Command::new("sync");
        let _ = run_command(&mut sync_cmd, run_mode);
    }
    println!("{}", Local::now().format("%d-%m-%Y %H:%M"));

    Ok(())
}
