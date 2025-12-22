use std::process::Command;

use chrono::Local;

use crate::backup::{print_job_details, run_backup};
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

pub fn run_backup_command(
    config_path: &std::path::Path,
    selected_jobs: &[String],
    print_order: bool,
    disk_id: Option<&str>,
    cascade: bool,
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

    let connected = connected_disks_in_order(&backup_disks);
    let primary_disk = match select_first_connected(&backup_disks, disk_id) {
        Ok(disk) => disk,
        Err(TimevaultError::Disk(err)) => exit_for_disk_error(&err),
        Err(err) => return Err(err),
    };

    let mut disks_to_run = Vec::new();
    if cascade {
        disks_to_run.push(primary_disk.clone());
        for disk in connected {
            if disk.fs_uuid != primary_disk.fs_uuid {
                disks_to_run.push(disk);
            }
        }
    } else {
        disks_to_run.push(primary_disk.clone());
    }

    let (primary_guard, primary_mount) =
        mount_and_verify(&primary_disk, &mount_base, run_mode)?;
    let primary_current_base = primary_mount.clone();

    if let Some((code, message)) =
        run_backup_checked(jobs_to_run.clone(), rsync_extra, run_mode, &primary_mount)
    {
        drop(primary_guard);
        println!("{}", message);
        std::process::exit(code);
    }

    for disk in disks_to_run.iter().skip(1) {
        let (guard, mountpoint) = mount_and_verify(disk, &mount_base, run_mode)?;
        let mut cascaded_jobs = Vec::new();
        for job in &jobs_to_run {
            let mut job_override = job.clone();
            let source = primary_current_base.join(&job.name).join("current");
            job_override.source = source.to_string_lossy().to_string();
            cascaded_jobs.push(job_override);
        }
        if let Some((code, message)) =
            run_backup_checked(cascaded_jobs, rsync_extra, run_mode, &mountpoint)
        {
            drop(guard);
            drop(primary_guard);
            println!("{}", message);
            std::process::exit(code);
        }
        drop(guard);
    }

    drop(primary_guard);

    if !run_mode.dry_run {
        let mut sync_cmd = Command::new("sync");
        let _ = run_command(&mut sync_cmd, run_mode);
    }
    println!("{}", Local::now().format("%d-%m-%Y %H:%M"));

    Ok(())
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
) -> Option<(i32, String)> {
    let backup_result = run_backup(jobs, rsync_extra, run_mode, mountpoint);
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
