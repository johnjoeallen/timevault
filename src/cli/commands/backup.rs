use std::collections::HashMap;
use std::io::IsTerminal;
use std::process::Command;

use chrono::Local;

use crate::progress;

use crate::backup::report::{email_html_report, render_html, BackupJobStatus, BackupRunReport};
use crate::backup::{print_job_details, run_backup, run_pristine_only, BackupOptions};
use crate::cli::commands::exit_for_disk_error;
use crate::config::load::load_config;
use crate::config::model::BackupDiskConfig;
use crate::disk::fs_type::detect_fs_type;
use crate::disk::identity::{identity_path, read_identity, verify_identity};
use crate::disk::{
    connected_disks_in_order, device_path_for_uuid, mount_disk_for_backup,
    mount_options_for_backup, select_first_connected, sweep_stale_run_mounts,
};
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
    rsync_extra_cli: &[String],
    options: BackupOptions,
    send_report: bool,
    progress: bool,
) -> Result<()> {
    let cfg = load_config(config_path.to_string_lossy().as_ref())?;
    let jobs = cfg.jobs.clone();
    let backup_disks = cfg.backup_disks.clone();
    for disk in backup_disks.iter().filter(|disk| disk.rotated_out) {
        crate::pnote!("{}", rotated_out_deprecation_warning(&disk.disk_id));
    }
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
        session_seconds_override: options.session_seconds_override,
    };
    let report_options = cfg.options.report.clone();

    let want_progress = progress || cfg.options.progress.unwrap_or(false);
    if want_progress && (run_mode.verbose || run_mode.dry_run) {
        eprintln!("--progress ignored with --verbose / --dry-run");
    }
    progress::init(
        want_progress && !run_mode.verbose && !run_mode.dry_run && std::io::stderr().is_terminal(),
    );

    progress::note(Local::now().format("%d-%m-%Y %H:%M"));

    if !run_mode.dry_run {
        sweep_stale_run_mounts(&mount_base);
    }

    if backup_disks.is_empty() && !options.exclude_pristine_only {
        return Err(TimevaultError::message(
            "no backup disks enrolled; run `timevault disk enroll ...`".to_string(),
        ));
    }

    let selected_set: std::collections::HashSet<String> = selected_jobs.iter().cloned().collect();
    let mut jobs_to_run = Vec::new();
    if selected_set.is_empty() || selected_set.contains("all") {
        if selected_set.len() > 1 {
            return Err(TimevaultError::message(
                "--job all cannot be combined with other job names".to_string(),
            ));
        }
        for job in &jobs {
            if job.run_policy == crate::types::RunPolicy::Auto {
                jobs_to_run.push(job.clone());
            }
        }
    } else {
        for job in &jobs {
            if selected_set.contains(&job.name) {
                if job.run_policy == crate::types::RunPolicy::Off {
                    crate::pnote!("job disabled (off): {}", job.name);
                    crate::pnote!("requested job(s) are disabled; aborting");
                    progress::shutdown();
                    std::process::exit(2);
                }
                jobs_to_run.push(job.clone());
            }
        }
        if jobs_to_run.len() != selected_set.len() {
            for name in &selected_set {
                if !jobs.iter().any(|job| &job.name == name) {
                    crate::pnote!("job not found: {}", name);
                }
            }
            crate::pnote!("no such job(s) found; aborting");
            progress::shutdown();
            std::process::exit(2);
        }
    }

    if jobs_to_run.is_empty() {
        crate::pnote!("no jobs matched selection; aborting");
        progress::shutdown();
        std::process::exit(2);
    }

    let disk_filter = disk_id.map(|id| id.to_string());
    if let Some(ref disk_filter) = disk_filter {
        jobs_to_run.retain(|job| job_requires_disk(job, disk_filter));
        if jobs_to_run.is_empty() {
            crate::pnote!("no jobs matched disk selection; aborting");
            progress::shutdown();
            std::process::exit(2);
        }
    }

    if print_order {
        for job in &jobs_to_run {
            print_job_details(job);
        }
        progress::shutdown();
        std::process::exit(0);
    }

    if run_mode.verbose {
        crate::pnote!(
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
        if primary_disk.disabled {
            crate::pnote!(
                "disk {} is disabled for backups; aborting",
                primary_disk.disk_id
            );
            progress::shutdown();
            std::process::exit(2);
        }
        jobs_to_run.retain(|job| job_requires_disk(job, disk_id));
        if jobs_to_run.is_empty() {
            crate::pnote!("no jobs matched disk selection; aborting");
            progress::shutdown();
            std::process::exit(2);
        }
        let mut reports = Vec::new();
        if let Some((code, message)) = run_jobs_for_primary(
            &primary_disk,
            jobs_to_run,
            &connected,
            cascade,
            run_mode,
            &rsync_extra,
            &mount_base,
            options,
            &mut reports,
        )? {
            send_reports(&report_options, &reports, run_mode, send_report)?;
            progress::note(&message);
            progress::shutdown();
            std::process::exit(code);
        }
        send_reports(&report_options, &reports, run_mode, send_report)?;
    } else {
        let eligible_connected: Vec<BackupDiskConfig> = connected
            .iter()
            .filter(|disk| disk_is_eligible_for_automatic_backup(disk))
            .cloned()
            .collect();
        if eligible_connected.is_empty() {
            crate::pnote!("no eligible backup disks connected; aborting");
            progress::shutdown();
            std::process::exit(2);
        }

        let mut groups: HashMap<String, Vec<crate::config::model::Job>> = HashMap::new();
        for job in jobs_to_run {
            let allowed = allowed_disks_for_job(&job, &eligible_connected);
            if allowed.is_empty() {
                crate::pnote!("job {} has no connected disks; aborting", job.name);
                progress::shutdown();
                std::process::exit(2);
            }
            let primary = allowed[0].clone();
            groups.entry(primary.disk_id.clone()).or_default().push(job);
        }

        let mut reports = Vec::new();
        for disk in &eligible_connected {
            let Some(jobs) = groups.remove(&disk.disk_id) else {
                continue;
            };
            if let Some((code, message)) = run_jobs_for_primary(
                disk,
                jobs,
                &eligible_connected,
                cascade,
                run_mode,
                &rsync_extra,
                &mount_base,
                options,
                &mut reports,
            )? {
                send_reports(&report_options, &reports, run_mode, send_report)?;
                progress::note(&message);
                progress::shutdown();
                std::process::exit(code);
            }
        }
        send_reports(&report_options, &reports, run_mode, send_report)?;
    }

    if !run_mode.dry_run {
        progress::status("sync");
        let mut sync_cmd = Command::new("sync");
        let _ = run_command(&mut sync_cmd, run_mode);
    }
    progress::note(Local::now().format("%d-%m-%Y %H:%M"));
    progress::shutdown();

    Ok(())
}

fn send_reports(
    options: &Option<crate::config::model::ReportOptions>,
    reports: &[BackupRunReport],
    run_mode: RunMode,
    force_send: bool,
) -> Result<()> {
    let Some(options) = options else {
        return Ok(());
    };
    if reports.is_empty() {
        return Ok(());
    }
    if run_mode.dry_run && !force_send {
        if run_mode.verbose {
            crate::pnote!("dry-run: skip backup report email");
        }
        return Ok(());
    }
    let html = render_html(reports);
    email_html_report(options, &html)
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

/// One downstream disk in a cascade run and the jobs it is allowed to receive.
#[derive(Debug, Clone)]
struct CascadeStage {
    disk: BackupDiskConfig,
    jobs: Vec<crate::config::model::Job>,
}

/// Downstream cascade stages for a primary disk, in connected-disk order.
/// Every stage copies from the primary (star topology), so a stage depends
/// only on the primary's result for each job, never on another stage. Pure:
/// whether a job actually cascades also depends on the primary's per-job
/// status, checked by [`cascade_job_eligible`].
fn plan_cascade(
    primary: &BackupDiskConfig,
    jobs: &[crate::config::model::Job],
    connected: &[BackupDiskConfig],
) -> Vec<CascadeStage> {
    let mut by_disk: HashMap<String, Vec<crate::config::model::Job>> = HashMap::new();
    for job in jobs {
        for disk in allowed_disks_for_job(job, connected) {
            // A disabled disk is never used for backups, so it never receives
            // a cascade either, even when the primary was chosen with --disk-id.
            if disk.fs_uuid == primary.fs_uuid || disk.disabled {
                continue;
            }
            by_disk
                .entry(disk.disk_id.clone())
                .or_default()
                .push(job.clone());
        }
    }
    connected
        .iter()
        .filter_map(|disk| {
            by_disk.remove(&disk.disk_id).map(|jobs| CascadeStage {
                disk: disk.clone(),
                jobs,
            })
        })
        .collect()
}

/// A job may cascade only if its primary backup produced a snapshot this run.
fn cascade_job_eligible(primary_status: Option<BackupJobStatus>) -> bool {
    primary_status.is_some_and(BackupJobStatus::produced_snapshot)
}

/// Warning for a disk still carrying the deprecated `rotatedOut` flag.
pub fn rotated_out_deprecation_warning(disk_id: &str) -> String {
    format!(
        "warning: backupDisks entry {} sets rotatedOut, which is deprecated and will be removed; \
         to stop backing up to a disk use `timevault disk disable {}`, or unplug it",
        disk_id, disk_id
    )
}

/// Warning for the deprecated `disk rotate-in` / `disk rotate-out` commands.
pub fn rotate_command_deprecation_warning(command: &str) -> String {
    format!(
        "warning: `timevault disk {}` is deprecated and will be removed; \
         to stop backing up to a disk use `timevault disk disable <disk>` (it can still be mounted \
         and inspected), or unplug it",
        command
    )
}

fn disk_is_eligible_for_automatic_backup(disk: &BackupDiskConfig) -> bool {
    !disk.disabled && !disk.rotated_out
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
    reports: &mut Vec<BackupRunReport>,
) -> Result<Option<(i32, String)>> {
    let (primary_guard, primary_mount) = mount_and_verify(primary_disk, mount_base, run_mode)?;
    let primary_current_base = primary_mount.clone();

    if let Some((code, message)) = run_backup_checked(
        primary_disk.disk_id.clone(),
        jobs.clone(),
        rsync_extra,
        run_mode,
        &primary_mount,
        options,
        reports,
    ) {
        drop(primary_guard);
        return Ok(Some((code, message)));
    }

    if cascade {
        // A job only cascades if its backup to the primary disk produced a
        // snapshot this run: no point (and it would be stale) to copy the
        // primary's `current` for a job the primary skipped or failed. Snapshot
        // the statuses now — `reports` is borrowed mutably again below.
        let primary_statuses: HashMap<String, BackupJobStatus> = reports
            .last()
            .map(|report| {
                report
                    .jobs
                    .iter()
                    .map(|job| (job.name.clone(), job.status))
                    .collect()
            })
            .unwrap_or_default();

        for stage in plan_cascade(primary_disk, &jobs, connected) {
            let disk = &stage.disk;
            let (guard, mountpoint) = mount_and_verify(disk, mount_base, run_mode)?;
            let mut cascaded_jobs = Vec::new();
            for job in &stage.jobs {
                let primary_status = primary_statuses.get(&job.name).copied();
                match primary_status {
                    status if cascade_job_eligible(status) => {}
                    other => {
                        crate::pnote!(
                            "cascade skipped for job {} on disk {}: primary backup {}",
                            job.name,
                            disk.disk_id,
                            other.map_or("did not run", BackupJobStatus::as_str)
                        );
                        continue;
                    }
                }
                let mut job_override = job.clone();
                let source = primary_current_base.join(&job.name).join("current");
                if !run_mode.dry_run && !source.exists() {
                    drop(guard);
                    drop(primary_guard);
                    return Ok(Some((
                        1,
                        format!(
                            "cascade source {} missing though the primary backup for job {} succeeded",
                            source.display(),
                            job.name
                        ),
                    )));
                }
                if run_mode.dry_run && !source.exists() {
                    crate::pnote!("dry-run: skip cascade (missing {})", source.display());
                    continue;
                }
                job_override.source = source.to_string_lossy().to_string();
                cascaded_jobs.push(job_override);
            }
            if cascaded_jobs.is_empty() {
                drop(guard);
                continue;
            }
            crate::pstatus!("cascading to disk {}", disk.disk_id);
            if let Some((code, message)) = run_backup_checked(
                disk.disk_id.clone(),
                cascaded_jobs,
                rsync_extra,
                run_mode,
                &mountpoint,
                options,
                reports,
            ) {
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
            description: None,
            source: "/".to_string(),
            copies: 1,
            run_policy: RunPolicy::Auto,
            excludes: Vec::new(),
            includes: Vec::new(),
            disk_ids: None,
            remote: None,
        };
        assert!(!job_requires_disk(&job, "disk-a"));
        let job = crate::config::model::Job {
            name: "job".to_string(),
            description: None,
            source: "/".to_string(),
            copies: 1,
            run_policy: RunPolicy::Auto,
            excludes: Vec::new(),
            includes: Vec::new(),
            disk_ids: Some(vec!["disk-a".to_string()]),
            remote: None,
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
                disabled: false,
                rotated_out: false,
            },
            crate::config::model::BackupDiskConfig {
                disk_id: "disk-b".to_string(),
                fs_uuid: "uuid-b".to_string(),
                label: None,
                mount_options: None,
                disabled: false,
                rotated_out: false,
            },
        ];
        let job = crate::config::model::Job {
            name: "job".to_string(),
            description: None,
            source: "/".to_string(),
            copies: 1,
            run_policy: RunPolicy::Auto,
            excludes: Vec::new(),
            includes: Vec::new(),
            disk_ids: Some(vec!["disk-b".to_string()]),
            remote: None,
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
                disabled: false,
                rotated_out: false,
            },
            crate::config::model::BackupDiskConfig {
                disk_id: "disk-b".to_string(),
                fs_uuid: "uuid-b".to_string(),
                label: None,
                mount_options: None,
                disabled: false,
                rotated_out: false,
            },
        ];
        let job = crate::config::model::Job {
            name: "job".to_string(),
            description: None,
            source: "/".to_string(),
            copies: 1,
            run_policy: RunPolicy::Auto,
            excludes: Vec::new(),
            includes: Vec::new(),
            disk_ids: None,
            remote: None,
        };
        let allowed = allowed_disks_for_job(&job, &connected);
        assert_eq!(allowed.len(), 2);
    }

    fn plan_disk(id: &str) -> BackupDiskConfig {
        BackupDiskConfig {
            disk_id: id.to_string(),
            fs_uuid: format!("uuid-{id}"),
            label: None,
            mount_options: None,
            disabled: false,
            rotated_out: false,
        }
    }

    fn plan_job(name: &str, disk_ids: Option<&[&str]>) -> crate::config::model::Job {
        crate::config::model::Job {
            name: name.to_string(),
            description: None,
            source: "/".to_string(),
            copies: 1,
            run_policy: RunPolicy::Auto,
            excludes: Vec::new(),
            includes: Vec::new(),
            disk_ids: disk_ids.map(|ids| ids.iter().map(|id| id.to_string()).collect()),
            remote: None,
        }
    }

    fn stage_summary(stages: &[CascadeStage]) -> Vec<(String, Vec<String>)> {
        stages
            .iter()
            .map(|stage| {
                (
                    stage.disk.disk_id.clone(),
                    stage.jobs.iter().map(|job| job.name.clone()).collect(),
                )
            })
            .collect()
    }

    #[test]
    fn cascade_plan_is_a_star_from_the_primary_in_connected_order() {
        let connected = vec![plan_disk("a"), plan_disk("b"), plan_disk("c")];
        let jobs = vec![plan_job("etc", None), plan_job("home", None)];

        let stages = plan_cascade(&connected[0], &jobs, &connected);

        assert_eq!(
            stage_summary(&stages),
            vec![
                ("b".to_string(), vec!["etc".to_string(), "home".to_string()]),
                ("c".to_string(), vec!["etc".to_string(), "home".to_string()]),
            ]
        );
    }

    #[test]
    fn cascade_plan_uses_whichever_disk_is_primary() {
        // Rotation: with b and c connected and b first, b is the primary.
        let connected = vec![plan_disk("b"), plan_disk("c")];
        let jobs = vec![plan_job("etc", None)];

        let stages = plan_cascade(&connected[0], &jobs, &connected);

        assert_eq!(
            stage_summary(&stages),
            vec![("c".to_string(), vec!["etc".to_string()])]
        );
    }

    #[test]
    fn cascade_plan_respects_job_disk_allowlists() {
        let connected = vec![plan_disk("a"), plan_disk("b"), plan_disk("c")];
        let jobs = vec![
            plan_job("etc", Some(&["a", "c"])),
            plan_job("home", Some(&["a", "b"])),
        ];

        let stages = plan_cascade(&connected[0], &jobs, &connected);

        assert_eq!(
            stage_summary(&stages),
            vec![
                ("b".to_string(), vec!["home".to_string()]),
                ("c".to_string(), vec!["etc".to_string()]),
            ]
        );
    }

    #[test]
    fn cascade_plan_never_includes_a_disabled_disk() {
        let mut disabled = plan_disk("b");
        disabled.disabled = true;
        let connected = vec![plan_disk("a"), disabled, plan_disk("c")];
        let jobs = vec![plan_job("etc", None)];

        let stages = plan_cascade(&connected[0], &jobs, &connected);

        assert_eq!(
            stage_summary(&stages),
            vec![("c".to_string(), vec!["etc".to_string()])]
        );
    }

    #[test]
    fn deprecation_warnings_point_to_disable() {
        let flag = rotated_out_deprecation_warning("usb-a");
        assert!(flag.contains("usb-a") && flag.contains("deprecated"));
        assert!(flag.contains("timevault disk disable usb-a"));
        let command = rotate_command_deprecation_warning("rotate-out");
        assert!(command.contains("disk rotate-out") && command.contains("deprecated"));
        assert!(command.contains("timevault disk disable <disk>"));
    }

    #[test]
    fn cascade_plan_does_not_filter_on_rotated_out() {
        // A disk that is not connected is rotated out by being absent;
        // the rotatedOut flag only affects automatic selection of the primary.
        let mut parked = plan_disk("b");
        parked.rotated_out = true;
        let connected = vec![plan_disk("a"), parked];
        let jobs = vec![plan_job("etc", None)];

        let stages = plan_cascade(&connected[0], &jobs, &connected);

        assert_eq!(
            stage_summary(&stages),
            vec![("b".to_string(), vec!["etc".to_string()])]
        );
    }

    #[test]
    fn cascade_plan_is_empty_with_a_single_connected_disk() {
        let connected = vec![plan_disk("a")];
        let jobs = vec![plan_job("etc", None)];

        assert!(plan_cascade(&connected[0], &jobs, &connected).is_empty());
    }

    #[test]
    fn only_jobs_whose_primary_backup_produced_a_snapshot_cascade() {
        assert!(cascade_job_eligible(Some(BackupJobStatus::Success)));
        assert!(cascade_job_eligible(Some(BackupJobStatus::Partial)));
        assert!(!cascade_job_eligible(Some(BackupJobStatus::Failed)));
        assert!(!cascade_job_eligible(Some(BackupJobStatus::Offline)));
        assert!(!cascade_job_eligible(Some(BackupJobStatus::Skipped)));
        assert!(!cascade_job_eligible(None));
    }

    #[test]
    fn automatic_backup_excludes_disabled_and_rotated_out_disks() {
        let disabled = BackupDiskConfig {
            disk_id: "disk-a".to_string(),
            fs_uuid: "uuid-a".to_string(),
            label: None,
            mount_options: None,
            disabled: true,
            rotated_out: false,
        };
        let rotated = BackupDiskConfig {
            disk_id: "disk-b".to_string(),
            fs_uuid: "uuid-b".to_string(),
            label: None,
            mount_options: None,
            disabled: false,
            rotated_out: true,
        };
        let active = BackupDiskConfig {
            disk_id: "disk-c".to_string(),
            fs_uuid: "uuid-c".to_string(),
            label: None,
            mount_options: None,
            disabled: false,
            rotated_out: false,
        };

        assert!(!disk_is_eligible_for_automatic_backup(&disabled));
        assert!(!disk_is_eligible_for_automatic_backup(&rotated));
        assert!(disk_is_eligible_for_automatic_backup(&active));
    }
}

fn mount_and_verify(
    disk: &crate::config::model::BackupDiskConfig,
    mount_base: &std::path::Path,
    run_mode: RunMode,
) -> Result<(Option<MountGuard>, std::path::PathBuf)> {
    let options = mount_options_for_backup(disk);
    crate::pstatus!("mounting disk {}", disk.disk_id);
    let (disk_guard, mountpoint) = if run_mode.dry_run {
        (None, mount_base.join(&disk.fs_uuid))
    } else {
        match mount_disk_for_backup(disk, mount_base, &options) {
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
                return Err(TimevaultError::message(format!(
                    "identity file invalid: {}",
                    err
                )));
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
                    identity_type, fs_type
                ))));
            }
        }
    }

    Ok((disk_guard, mountpoint))
}

fn run_backup_checked(
    disk_id: String,
    jobs: Vec<crate::config::model::Job>,
    rsync_extra: &[String],
    run_mode: RunMode,
    mountpoint: &std::path::Path,
    options: BackupOptions,
    reports: &mut Vec<BackupRunReport>,
) -> Option<(i32, String)> {
    let backup_result = run_backup(jobs, rsync_extra, run_mode, mountpoint, options);
    match backup_result {
        Ok(mut report) => {
            report.disk_id = disk_id;
            reports.push(report);
            None
        }
        Err(err) => {
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
            Some((1, format!("backup failed: {}", message)))
        }
    }
}
