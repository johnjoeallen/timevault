use std::collections::HashMap;
use std::io::IsTerminal;
use std::process::Command;

use chrono::Local;

use crate::progress;

use crate::backup::report::{email_html_report, render_html, BackupJobStatus, BackupRunReport};
use crate::backup::{
    backup_day_now, print_job_details, run_backup_for_day, run_pristine_only, BackupOptions,
};
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
        recreate: options.recreate,
        index: cfg
            .options
            .index
            .unwrap_or(crate::backup::index::INDEX_DEFAULT),
        relocation: crate::backup::relocation::RelocationConfig::from_option(
            cfg.options.relocation.as_ref(),
        )
        .map_err(TimevaultError::message)?,
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

    let host = RealDisks {
        mount_base: mount_base.clone(),
    };
    let connected = host.connected(&backup_disks);
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
            &host,
            &primary_disk,
            jobs_to_run,
            &connected,
            cascade,
            run_mode,
            &rsync_extra,
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
        let mut reports = Vec::new();
        match run_automatic_backups(
            &host,
            &connected,
            jobs_to_run,
            cascade,
            run_mode,
            &rsync_extra,
            options,
            &mut reports,
        )? {
            AutoResult::Done => send_reports(&report_options, &reports, run_mode, send_report)?,
            AutoResult::Stopped(code, message) => {
                send_reports(&report_options, &reports, run_mode, send_report)?;
                progress::note(&message);
                progress::shutdown();
                std::process::exit(code);
            }
            AutoResult::NoEligibleDisks => {
                crate::pnote!("no eligible backup disks connected; aborting");
                progress::shutdown();
                std::process::exit(2);
            }
            AutoResult::JobWithoutDisk(job) => {
                crate::pnote!("job {} has no connected disks; aborting", job);
                progress::shutdown();
                std::process::exit(2);
            }
        }
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

/// One line per job saying which disks completed it, so a partially
/// successful cascade is visible in the run output. `expected` lists the
/// primary first, then each downstream disk planned for this run; a disk with
/// no report for a job (skipped, or the run stopped before reaching it) shows
/// as `not run`.
fn summarize_cascade(expected: &[String], reports: &[BackupRunReport]) -> Vec<String> {
    let Some(primary) = expected
        .first()
        .and_then(|id| reports.iter().find(|report| &report.disk_id == id))
    else {
        return Vec::new();
    };
    primary
        .jobs
        .iter()
        .map(|job| {
            let mut complete = 0;
            let parts: Vec<String> = expected
                .iter()
                .map(|disk_id| {
                    let status = reports
                        .iter()
                        .find(|report| &report.disk_id == disk_id)
                        .and_then(|report| report.jobs.iter().find(|j| j.name == job.name))
                        .map(|j| j.status);
                    if status.is_some_and(BackupJobStatus::produced_snapshot) {
                        complete += 1;
                    }
                    format!(
                        "{}: {}",
                        disk_id,
                        status.map_or("not run", BackupJobStatus::as_str)
                    )
                })
                .collect();
            format!(
                "cascade summary for job {}: {} of {} disks complete ({})",
                job.name,
                complete,
                expected.len(),
                parts.join(", ")
            )
        })
        .collect()
}

fn log_cascade_summary(expected: &[String], reports: &[BackupRunReport]) {
    for line in summarize_cascade(expected, reports) {
        crate::pnote!("{}", line);
    }
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

/// Where disks and the clock come from. Production discovers real devices and
/// mounts them; tests supply directories and a chosen backup day, so whole
/// rotation scenarios can run through the real orchestration.
trait DiskHost {
    /// The enrolled disks that are currently connected, in config order.
    fn connected(&self, disks: &[BackupDiskConfig]) -> Vec<BackupDiskConfig>;
    /// Mount (and verify) a disk, returning its guard and mountpoint.
    fn mount(
        &self,
        disk: &BackupDiskConfig,
        run_mode: RunMode,
    ) -> Result<(Option<MountGuard>, std::path::PathBuf)>;
    /// The generation name this run writes.
    fn backup_day(&self) -> String;
}

struct RealDisks {
    mount_base: std::path::PathBuf,
}

impl DiskHost for RealDisks {
    fn connected(&self, disks: &[BackupDiskConfig]) -> Vec<BackupDiskConfig> {
        connected_disks_in_order(disks)
    }

    fn mount(
        &self,
        disk: &BackupDiskConfig,
        run_mode: RunMode,
    ) -> Result<(Option<MountGuard>, std::path::PathBuf)> {
        mount_and_verify(disk, &self.mount_base, run_mode)
    }

    fn backup_day(&self) -> String {
        backup_day_now()
    }
}

/// How an automatic (no `--disk-id`) run ended.
#[derive(Debug, PartialEq, Eq)]
enum AutoResult {
    Done,
    /// A mount, lock or similar error stopped the run: exit code and message.
    Stopped(i32, String),
    NoEligibleDisks,
    /// This job is allowed on none of the connected, eligible disks.
    JobWithoutDisk(String),
}

/// Group jobs by the eligible disk that is their primary: the first eligible
/// connected disk allowed for the job, in config order. Returns the job that
/// has no disk if there is one.
fn plan_automatic(
    jobs: Vec<crate::config::model::Job>,
    eligible: &[BackupDiskConfig],
) -> std::result::Result<Vec<(BackupDiskConfig, Vec<crate::config::model::Job>)>, String> {
    let mut groups: HashMap<String, Vec<crate::config::model::Job>> = HashMap::new();
    for job in jobs {
        let allowed = allowed_disks_for_job(&job, eligible);
        let Some(primary) = allowed.first() else {
            return Err(job.name);
        };
        groups.entry(primary.disk_id.clone()).or_default().push(job);
    }
    Ok(eligible
        .iter()
        .filter_map(|disk| {
            groups
                .remove(&disk.disk_id)
                .map(|jobs| (disk.clone(), jobs))
        })
        .collect())
}

/// The automatic run: back each job up to its primary disk and, with
/// `cascade`, copy the result to the other connected disks.
#[allow(clippy::too_many_arguments)]
fn run_automatic_backups(
    host: &dyn DiskHost,
    connected: &[BackupDiskConfig],
    jobs: Vec<crate::config::model::Job>,
    cascade: bool,
    run_mode: RunMode,
    rsync_extra: &[String],
    options: BackupOptions,
    reports: &mut Vec<BackupRunReport>,
) -> Result<AutoResult> {
    let eligible: Vec<BackupDiskConfig> = connected
        .iter()
        .filter(|disk| disk_is_eligible_for_automatic_backup(disk))
        .cloned()
        .collect();
    if eligible.is_empty() {
        return Ok(AutoResult::NoEligibleDisks);
    }
    let plan = match plan_automatic(jobs, &eligible) {
        Ok(plan) => plan,
        Err(job) => return Ok(AutoResult::JobWithoutDisk(job)),
    };
    for (disk, jobs) in plan {
        if let Some((code, message)) = run_jobs_for_primary(
            host,
            &disk,
            jobs,
            &eligible,
            cascade,
            run_mode,
            rsync_extra,
            options,
            reports,
        )? {
            return Ok(AutoResult::Stopped(code, message));
        }
    }
    Ok(AutoResult::Done)
}

fn disk_is_eligible_for_automatic_backup(disk: &BackupDiskConfig) -> bool {
    !disk.disabled && !disk.rotated_out
}

#[allow(clippy::too_many_arguments)]
fn run_jobs_for_primary(
    host: &dyn DiskHost,
    primary_disk: &crate::config::model::BackupDiskConfig,
    jobs: Vec<crate::config::model::Job>,
    connected: &[crate::config::model::BackupDiskConfig],
    cascade: bool,
    run_mode: RunMode,
    rsync_extra: &[String],
    options: BackupOptions,
    reports: &mut Vec<BackupRunReport>,
) -> Result<Option<(i32, String)>> {
    let (primary_guard, primary_mount) = host.mount(primary_disk, run_mode)?;
    let primary_current_base = primary_mount.clone();
    let first_report = reports.len();

    if let Some((code, message)) = run_backup_checked(
        host,
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

        let stages = plan_cascade(primary_disk, &jobs, connected);
        let mut expected = vec![primary_disk.disk_id.clone()];
        expected.extend(stages.iter().map(|stage| stage.disk.disk_id.clone()));
        for stage in stages {
            let disk = &stage.disk;
            let (guard, mountpoint) = host.mount(disk, run_mode)?;
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
                host,
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
                log_cascade_summary(&expected, &reports[first_report..]);
                return Ok(Some((code, message)));
            }
            drop(guard);
        }
        log_cascade_summary(&expected, &reports[first_report..]);
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
            relocation: None,
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
            relocation: None,
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
            relocation: None,
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
            relocation: None,
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
            relocation: None,
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

    fn run_report(disk_id: &str, jobs: &[(&str, BackupJobStatus)]) -> BackupRunReport {
        let now = Local::now();
        BackupRunReport {
            disk_id: disk_id.to_string(),
            mountpoint: format!("/mnt/{disk_id}"),
            started_at: now,
            finished_at: now,
            jobs: jobs
                .iter()
                .map(|(name, status)| crate::backup::report::BackupJobReport {
                    name: name.to_string(),
                    description: None,
                    source: "/".to_string(),
                    destination: String::new(),
                    backup_day: "20261019".to_string(),
                    status: *status,
                    attempts: 1,
                    rsync_code: Some(0),
                    failure_reason: None,
                    power: None,
                    power_diagnostics: None,
                })
                .collect(),
        }
    }

    fn ids(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|id| id.to_string()).collect()
    }

    #[test]
    fn cascade_summary_reports_full_success() {
        let reports = vec![
            run_report("a", &[("etc", BackupJobStatus::Success)]),
            run_report("b", &[("etc", BackupJobStatus::Success)]),
        ];
        assert_eq!(
            summarize_cascade(&ids(&["a", "b"]), &reports),
            vec!["cascade summary for job etc: 2 of 2 disks complete (a: success, b: success)"]
        );
    }

    #[test]
    fn cascade_summary_shows_a_failed_downstream_disk() {
        let reports = vec![
            run_report("a", &[("etc", BackupJobStatus::Success)]),
            run_report("b", &[("etc", BackupJobStatus::Failed)]),
            run_report("c", &[("etc", BackupJobStatus::Success)]),
        ];
        assert_eq!(
            summarize_cascade(&ids(&["a", "b", "c"]), &reports),
            vec![
                "cascade summary for job etc: 2 of 3 disks complete (a: success, b: failed, c: success)"
            ]
        );
    }

    #[test]
    fn cascade_summary_marks_disks_the_run_never_reached() {
        // The run stopped (for example a mount error) before disk c.
        let reports = vec![
            run_report(
                "a",
                &[
                    ("etc", BackupJobStatus::Success),
                    ("home", BackupJobStatus::Failed),
                ],
            ),
            run_report("b", &[("etc", BackupJobStatus::Success)]),
        ];
        assert_eq!(
            summarize_cascade(&ids(&["a", "b", "c"]), &reports),
            vec![
                "cascade summary for job etc: 2 of 3 disks complete (a: success, b: success, c: not run)",
                "cascade summary for job home: 0 of 3 disks complete (a: failed, b: not run, c: not run)",
            ]
        );
    }

    #[test]
    fn cascade_summary_is_empty_without_a_primary_report() {
        assert!(summarize_cascade(&ids(&["a", "b"]), &[]).is_empty());
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
        // The generation exists intact, and disks lacking it still need it.
        assert!(cascade_job_eligible(Some(BackupJobStatus::AlreadyComplete)));
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

    // ---- rotation and cascade scenarios through the real orchestration ----

    use std::cell::RefCell;
    use std::collections::{BTreeSet, HashSet};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::path::{Path, PathBuf};

    /// Disks are directories and the clock is a chosen backup day.
    struct TestHost {
        root: PathBuf,
        connected: RefCell<Vec<String>>,
        day: RefCell<String>,
        failing_mounts: RefCell<HashSet<String>>,
    }

    impl DiskHost for TestHost {
        fn connected(&self, disks: &[BackupDiskConfig]) -> Vec<BackupDiskConfig> {
            disks
                .iter()
                .filter(|d| self.connected.borrow().contains(&d.disk_id))
                .cloned()
                .collect()
        }

        fn mount(
            &self,
            disk: &BackupDiskConfig,
            _run_mode: RunMode,
        ) -> Result<(Option<MountGuard>, PathBuf)> {
            if self.failing_mounts.borrow().contains(&disk.disk_id) {
                return Err(TimevaultError::message(format!(
                    "cannot mount {}",
                    disk.disk_id
                )));
            }
            let path = self.root.join(&disk.disk_id);
            std::fs::create_dir_all(&path).unwrap();
            Ok((None, path))
        }

        fn backup_day(&self) -> String {
            self.day.borrow().clone()
        }
    }

    fn day_name(n: u32) -> String {
        (chrono::NaiveDate::from_ymd_opt(2026, 10, 1).unwrap()
            + chrono::Duration::days(n as i64 - 1))
        .format("%Y%m%d")
        .to_string()
    }

    fn lab_run_mode() -> RunMode {
        RunMode {
            dry_run: false,
            safe_mode: false,
            verbose: false,
        }
    }

    struct Lab {
        _tmp: tempfile::TempDir,
        _locks: crate::util::paths::test_lock_dir::Guard,
        source: PathBuf,
        host: TestHost,
        disks: Vec<BackupDiskConfig>,
        relocation: std::cell::Cell<crate::backup::relocation::RelocationConfig>,
    }

    fn lab(disk_ids: &[&str]) -> Lab {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("locks")).unwrap();
        let locks = crate::util::paths::test_lock_dir::set(&tmp.path().join("locks"));
        let source = tmp.path().join("source");
        std::fs::create_dir_all(&source).unwrap();
        Lab {
            host: TestHost {
                root: tmp.path().join("disks"),
                connected: RefCell::new(Vec::new()),
                day: RefCell::new(String::new()),
                failing_mounts: RefCell::new(HashSet::new()),
            },
            _tmp: tmp,
            _locks: locks,
            source,
            disks: disk_ids.iter().map(|id| plan_disk(id)).collect(),
            relocation: std::cell::Cell::new(crate::backup::relocation::RelocationConfig::default()),
        }
    }

    impl Lab {
        fn job(&self, name: &str, source: &Path, copies: usize) -> crate::config::model::Job {
            let mut job = plan_job(name, None);
            job.source = source.to_string_lossy().to_string();
            job.copies = copies;
            job.excludes = Vec::new();
            job
        }

        /// A source whose `changing.txt` differs every day and whose
        /// `stable.txt` never does.
        fn fill_source(&self, source: &Path, day: u32) {
            std::fs::create_dir_all(source).unwrap();
            let stable = source.join("stable.txt");
            if !stable.exists() {
                std::fs::write(&stable, "unchanging content").unwrap();
                touch(&stable, "2020-01-01 00:00:00");
            }
            std::fs::write(
                source.join("changing.txt"),
                format!("day {day} {}", "x".repeat(day as usize)),
            )
            .unwrap();
        }

        fn run(
            &self,
            day: u32,
            connected: &[&str],
            cascade: bool,
            jobs: Vec<crate::config::model::Job>,
        ) -> Result<(AutoResult, Vec<BackupRunReport>)> {
            *self.host.day.borrow_mut() = day_name(day);
            *self.host.connected.borrow_mut() = connected.iter().map(|s| s.to_string()).collect();
            let connected_disks = self.host.connected(&self.disks);
            let mut reports = Vec::new();
            let result = run_automatic_backups(
                &self.host,
                &connected_disks,
                jobs,
                cascade,
                lab_run_mode(),
                &[],
                BackupOptions {
                    exclude_pristine: false,
                    exclude_pristine_only: false,
                    session_seconds_override: None,
                    recreate: false,
                    index: false,
                    relocation: self.relocation.get(),
                },
                &mut reports,
            )?;
            Ok((result, reports))
        }

        /// One simple daily job on `connected` disks for the given day.
        fn daily(
            &self,
            day: u32,
            connected: &[&str],
            cascade: bool,
            copies: usize,
        ) -> Vec<BackupRunReport> {
            self.fill_source(&self.source, day);
            let job = self.job("data", &self.source, copies);
            let (result, reports) = self.run(day, connected, cascade, vec![job]).unwrap();
            assert_eq!(result, AutoResult::Done);
            reports
        }

        fn job_dir(&self, disk: &str, job: &str) -> PathBuf {
            self.host.root.join(disk).join(job)
        }

        /// Generation names on a disk, oldest first.
        fn gens(&self, disk: &str, job: &str) -> Vec<String> {
            let mut names: Vec<String> = std::fs::read_dir(self.job_dir(disk, job))
                .map(|rd| {
                    rd.flatten()
                        .map(|e| e.file_name().to_string_lossy().to_string())
                        .filter(|n| !n.starts_with('.') && n != "current")
                        .collect()
                })
                .unwrap_or_default();
            names.sort();
            names
        }

        fn current(&self, disk: &str, job: &str) -> Option<String> {
            std::fs::read_link(self.job_dir(disk, job).join("current"))
                .ok()
                .map(|p| p.to_string_lossy().to_string())
        }

        fn ino(&self, disk: &str, job: &str, day: u32, file: &str) -> u64 {
            std::fs::metadata(self.job_dir(disk, job).join(day_name(day)).join(file))
                .unwrap()
                .ino()
        }

        fn text(&self, disk: &str, job: &str, day: u32, file: &str) -> String {
            std::fs::read_to_string(self.job_dir(disk, job).join(day_name(day)).join(file)).unwrap()
        }
    }

    fn touch(path: &Path, stamp: &str) {
        let status = Command::new("touch")
            .args(["-d", stamp])
            .arg(path)
            .status()
            .unwrap();
        assert!(status.success());
    }

    fn days(range: impl IntoIterator<Item = u32>) -> Vec<String> {
        range.into_iter().map(day_name).collect()
    }

    fn statuses(reports: &[BackupRunReport], disk: &str) -> Vec<BackupJobStatus> {
        reports
            .iter()
            .filter(|r| r.disk_id == disk)
            .flat_map(|r| r.jobs.iter().map(|j| j.status))
            .collect()
    }

    // Strategy A: one permanently connected disk.
    #[test]
    fn strategy_a_a_single_disk_keeps_a_continuous_history_within_copies() {
        let lab = lab(&["a"]);
        for day in 1..=10 {
            lab.daily(day, &["a"], false, 5);
        }

        assert_eq!(lab.gens("a", "data"), days(6..=10));
        assert_eq!(lab.current("a", "data"), Some(day_name(10)));
        // Unchanged files are shared between consecutive generations.
        assert_eq!(
            lab.ino("a", "data", 9, "stable.txt"),
            lab.ino("a", "data", 10, "stable.txt")
        );
        assert_ne!(
            lab.ino("a", "data", 9, "changing.txt"),
            lab.ino("a", "data", 10, "changing.txt")
        );
    }

    // Strategy B: two disks, one connected at a time, swapped weekly.
    #[test]
    fn strategy_b_alternating_disks_keep_independent_histories_with_gaps() {
        let lab = lab(&["a", "b"]);
        for day in 1..=28 {
            let week = (day - 1) / 7;
            let disk = if week % 2 == 0 { "a" } else { "b" };
            lab.daily(day, &[disk], false, 20);
        }

        assert_eq!(lab.gens("a", "data"), days((1..=7).chain(15..=21)));
        assert_eq!(lab.gens("b", "data"), days((8..=14).chain(22..=28)));
        // Each returning disk linked against its own latest generation, a week
        // old, and not against the other disk's.
        assert_eq!(
            lab.ino("a", "data", 7, "stable.txt"),
            lab.ino("a", "data", 15, "stable.txt")
        );
        assert_eq!(
            lab.ino("b", "data", 14, "stable.txt"),
            lab.ino("b", "data", 22, "stable.txt")
        );
        assert_eq!(
            lab.text("a", "data", 15, "changing.txt"),
            format!("day 15 {}", "x".repeat(15))
        );
        // Nothing was copied between the disks to fill their gaps.
        assert!(!lab.job_dir("b", "data").join(day_name(7)).exists());
        assert!(!lab.job_dir("a", "data").join(day_name(8)).exists());
    }

    // Strategy C: two permanently connected disks with cascade.
    #[test]
    fn strategy_c_two_connected_disks_both_get_every_day_through_cascade() {
        let lab = lab(&["a", "b"]);
        let mut last = Vec::new();
        for day in 1..=5 {
            last = lab.daily(day, &["a", "b"], true, 20);
        }

        assert_eq!(lab.gens("a", "data"), days(1..=5));
        assert_eq!(lab.gens("b", "data"), days(1..=5));
        assert_eq!(statuses(&last, "a"), vec![BackupJobStatus::Success]);
        assert_eq!(statuses(&last, "b"), vec![BackupJobStatus::Success]);
        // The second disk copied the primary's generation but keeps its own
        // inodes, and links against its own previous generation.
        assert_ne!(
            lab.ino("a", "data", 5, "stable.txt"),
            lab.ino("b", "data", 5, "stable.txt")
        );
        assert_eq!(
            lab.ino("b", "data", 4, "stable.txt"),
            lab.ino("b", "data", 5, "stable.txt")
        );
        assert_eq!(
            lab.text("b", "data", 5, "changing.txt"),
            lab.text("a", "data", 5, "changing.txt")
        );
        assert_eq!(last[0].disk_id, "a");
    }

    // Strategy D: three disks, two connected, one offline, rotating weekly.
    #[test]
    fn strategy_d_a_rolling_three_disk_rotation_keeps_every_disk_independent() {
        let lab = lab(&["a", "b", "c"]);
        let weeks: [&[&str]; 4] = [&["a", "b"], &["b", "c"], &["c", "a"], &["a", "b"]];
        let mut primaries = Vec::new();
        for (w, connected) in weeks.iter().enumerate() {
            for d in 1..=7 {
                let reports = lab.daily(w as u32 * 7 + d, connected, true, 40);
                if d == 1 {
                    primaries.push(reports[0].disk_id.clone());
                }
            }
        }

        // Each disk holds exactly the days it was connected for.
        assert_eq!(lab.gens("a", "data"), days((1..=7).chain(15..=28)));
        assert_eq!(lab.gens("b", "data"), days((1..=14).chain(22..=28)));
        assert_eq!(lab.gens("c", "data"), days(8..=21));
        // The primary is simply the first connected disk in config order, so
        // it moves as disks are swapped.
        assert_eq!(primaries, vec!["a", "b", "a", "a"]);
        // The disk that stayed connected across a swap has an unbroken run.
        assert_eq!(
            lab.ino("b", "data", 7, "stable.txt"),
            lab.ino("b", "data", 8, "stable.txt")
        );
        // A returning disk resumes from its own old generation ...
        assert_eq!(
            lab.ino("a", "data", 7, "stable.txt"),
            lab.ino("a", "data", 15, "stable.txt")
        );
        assert_eq!(
            lab.ino("b", "data", 14, "stable.txt"),
            lab.ino("b", "data", 22, "stable.txt")
        );
        // ... and a disk new to the job starts fresh, then links as usual.
        assert_eq!(
            lab.ino("c", "data", 8, "stable.txt"),
            lab.ino("c", "data", 9, "stable.txt")
        );
        // No historical generation was copied to fill a gap.
        assert!(!lab.job_dir("c", "data").join(day_name(7)).exists());
        // Content is right on a disk that held the day, whichever that was.
        for (disk, day) in [("a", 3), ("b", 10), ("c", 17), ("a", 25), ("b", 25)] {
            assert_eq!(
                lab.text(disk, "data", day, "changing.txt"),
                format!("day {day} {}", "x".repeat(day as usize))
            );
        }
    }

    // Strategy E: three disks, one connected at a time, swapped weekly.
    #[test]
    fn strategy_e_one_connected_disk_of_three_leaves_wide_gaps() {
        let lab = lab(&["a", "b", "c"]);
        for day in 1..=28 {
            let disk = ["a", "b", "c"][((day - 1) / 7 % 3) as usize];
            lab.daily(day, &[disk], false, 40);
        }

        assert_eq!(lab.gens("a", "data"), days((1..=7).chain(22..=28)));
        assert_eq!(lab.gens("b", "data"), days(8..=14));
        assert_eq!(lab.gens("c", "data"), days(15..=21));
        // Two weeks of gap, and the returning disk still links to its own past.
        assert_eq!(
            lab.ino("a", "data", 7, "stable.txt"),
            lab.ino("a", "data", 22, "stable.txt")
        );
    }

    /// `n` disks with `k` connected at a time, advancing one disk per week.
    fn rolling(n: usize, k: usize, weeks: usize, week_days: u32) -> (Lab, Vec<String>) {
        let ids: Vec<String> = (0..n).map(|i| format!("d{i}")).collect();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let lab = lab(&id_refs);
        for w in 0..weeks {
            let connected: Vec<&str> = (0..n)
                .filter(|i| (0..k).any(|j| (w + j) % n == *i))
                .map(|i| id_refs[i])
                .collect();
            for d in 1..=week_days {
                lab.daily(w as u32 * week_days + d, &connected, true, 60);
            }
        }
        (lab, ids)
    }

    fn assert_rolling(n: usize, k: usize, weeks: usize, week_days: u32) {
        let (lab, ids) = rolling(n, k, weeks, week_days);
        for (i, id) in ids.iter().enumerate() {
            let expected: BTreeSet<String> = (0..weeks)
                .filter(|w| (0..k).any(|j| (w + j) % n == i))
                .flat_map(|w| (1..=week_days).map(move |d| day_name(w as u32 * week_days + d)))
                .collect();
            let found: BTreeSet<String> = lab.gens(id, "data").into_iter().collect();
            assert_eq!(found, expected, "disk {id} of {n} ({k} connected)");
        }
        // Every day exists on exactly k disks: the recovery points are spread,
        // never lost, and never duplicated beyond the connected set.
        for day in 1..=(weeks as u32 * week_days) {
            let holders = ids
                .iter()
                .filter(|id| lab.job_dir(id, "data").join(day_name(day)).exists())
                .count();
            assert_eq!(holders, k, "day {day}");
        }
    }

    // Strategy F: four or more disks.
    #[test]
    fn strategy_f_four_disks_two_connected() {
        assert_rolling(4, 2, 6, 3);
    }

    #[test]
    fn strategy_f_five_disks_three_connected() {
        assert_rolling(5, 3, 5, 3);
    }

    #[test]
    fn strategy_f_six_disks_two_connected() {
        assert_rolling(6, 2, 6, 2);
    }

    // Strategy G: a permanent primary with rotating secondaries.
    #[test]
    fn strategy_g_a_permanent_primary_with_rotating_secondaries() {
        let lab = lab(&["a", "b", "c"]);
        for day in 1..=28 {
            let week = (day - 1) / 7;
            let second = if week % 2 == 0 { "b" } else { "c" };
            let reports = lab.daily(day, &["a", second], true, 60);
            assert_eq!(reports[0].disk_id, "a");
        }

        assert_eq!(lab.gens("a", "data"), days(1..=28));
        assert_eq!(lab.gens("b", "data"), days((1..=7).chain(15..=21)));
        assert_eq!(lab.gens("c", "data"), days((8..=14).chain(22..=28)));
    }

    #[test]
    fn a_disk_returning_after_weeks_resumes_without_a_full_backup() {
        let lab = lab(&["a"]);
        for day in 1..=3 {
            lab.daily(day, &["a"], false, 20);
        }
        // Offline for 40 days.
        lab.daily(44, &["a"], false, 20);

        assert_eq!(lab.gens("a", "data"), days([1, 2, 3, 44]));
        assert_eq!(
            lab.ino("a", "data", 3, "stable.txt"),
            lab.ino("a", "data", 44, "stable.txt"),
            "the unchanged file is still shared with the 40-day-old generation"
        );
        assert_ne!(
            lab.ino("a", "data", 3, "changing.txt"),
            lab.ino("a", "data", 44, "changing.txt")
        );
        assert_eq!(lab.current("a", "data"), Some(day_name(44)));
    }

    #[test]
    fn jobs_on_one_disk_keep_their_own_previous_generation() {
        let lab = lab(&["a"]);
        let (s1, s2) = (lab.source.join("one"), lab.source.join("two"));
        let run = |day: u32, with_two: bool| {
            lab.fill_source(&s1, day);
            if with_two {
                lab.fill_source(&s2, day);
            }
            let jobs = vec![lab.job("one", &s1, 20), lab.job("two", &s2, 20)];
            lab.run(day, &["a"], false, jobs).unwrap()
        };
        run(1, true);
        run(2, true);
        std::fs::remove_dir_all(&s2).unwrap();
        for day in 3..=5 {
            let (_, reports) = run(day, false);
            let two = reports[0].jobs.iter().find(|j| j.name == "two").unwrap();
            assert_eq!(two.status, BackupJobStatus::Failed);
        }
        let (_, reports) = run(6, true);
        assert!(reports[0]
            .jobs
            .iter()
            .all(|j| j.status == BackupJobStatus::Success));

        // Job one is continuous; job two shows the outage as a gap and resumed
        // from its own day-2 generation.
        assert_eq!(lab.gens("a", "one"), days(1..=6));
        assert_eq!(lab.gens("a", "two"), days([1, 2, 6]));
        assert_eq!(
            lab.ino("a", "two", 2, "stable.txt"),
            lab.ino("a", "two", 6, "stable.txt")
        );
        assert_eq!(lab.current("a", "two"), Some(day_name(6)));
    }

    #[test]
    fn a_failed_primary_stage_is_never_copied_to_the_other_disks() {
        let lab = lab(&["a", "b"]);
        for day in 1..=2 {
            lab.daily(day, &["a", "b"], true, 20);
        }
        let before_b = lab.gens("b", "data");
        let b_current = lab.current("b", "data");
        let b_snapshot_ino = lab.ino("b", "data", 2, "changing.txt");

        // The source disappears: the primary stage fails.
        std::fs::remove_dir_all(&lab.source).unwrap();
        let job = lab.job("data", &lab.source, 20);
        let (result, reports) = lab.run(3, &["a", "b"], true, vec![job]).unwrap();

        assert_eq!(result, AutoResult::Done);
        assert_eq!(statuses(&reports, "a"), vec![BackupJobStatus::Failed]);
        // Disk b was not given a stale or partial copy and kept what it had.
        assert!(statuses(&reports, "b").is_empty());
        assert_eq!(lab.gens("b", "data"), before_b);
        assert_eq!(lab.current("b", "data"), b_current);
        assert_eq!(lab.ino("b", "data", 2, "changing.txt"), b_snapshot_ino);
        assert_eq!(lab.gens("a", "data"), days(1..=2));
    }

    #[test]
    fn a_failing_downstream_disk_does_not_stop_the_other_disks() {
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("running as root: read-only directories do not stop writes; skipping");
            return;
        }
        let lab = lab(&["a", "b", "c"]);
        lab.daily(1, &["a", "b", "c"], true, 20);
        // Disk b's job directory becomes unwritable.
        let b_dir = lab.job_dir("b", "data");
        std::fs::set_permissions(&b_dir, std::fs::Permissions::from_mode(0o555)).unwrap();

        lab.fill_source(&lab.source, 2);
        let job = lab.job("data", &lab.source, 20);
        let (result, reports) = lab.run(2, &["a", "b", "c"], true, vec![job]).unwrap();
        std::fs::set_permissions(&b_dir, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(result, AutoResult::Done);
        assert_eq!(statuses(&reports, "a"), vec![BackupJobStatus::Success]);
        assert_eq!(statuses(&reports, "b"), vec![BackupJobStatus::Failed]);
        assert_eq!(statuses(&reports, "c"), vec![BackupJobStatus::Success]);
        assert_eq!(lab.gens("b", "data"), days([1]));
        assert_eq!(lab.current("b", "data"), Some(day_name(1)));
        assert_eq!(lab.gens("c", "data"), days([1, 2]));
    }

    #[test]
    fn a_mount_error_stops_the_run_and_later_disks_are_not_attempted() {
        let lab = lab(&["a", "b", "c"]);
        lab.daily(1, &["a", "b", "c"], true, 20);
        lab.host.failing_mounts.borrow_mut().insert("b".to_string());

        lab.fill_source(&lab.source, 2);
        let job = lab.job("data", &lab.source, 20);
        let err = lab
            .run(2, &["a", "b", "c"], true, vec![job])
            .err()
            .expect("the mount error stops the run");

        assert!(err.to_string().contains("cannot mount b"));
        // The primary had finished; the disk after the failure was not reached.
        assert_eq!(lab.gens("a", "data"), days([1, 2]));
        assert_eq!(lab.gens("c", "data"), days([1]));
    }

    #[test]
    fn a_disk_missing_the_day_is_filled_from_an_already_complete_primary() {
        let lab = lab(&["a", "b"]);
        lab.daily(5, &["a"], false, 20);

        // Same day again, now with both disks and cascade: the primary is
        // already complete, and the second disk still receives the day.
        let reports = lab.daily(5, &["a", "b"], true, 20);

        assert_eq!(
            statuses(&reports, "a"),
            vec![BackupJobStatus::AlreadyComplete]
        );
        assert_eq!(statuses(&reports, "b"), vec![BackupJobStatus::Success]);
        assert_eq!(lab.gens("b", "data"), days([5]));
        assert_eq!(
            lab.text("b", "data", 5, "changing.txt"),
            lab.text("a", "data", 5, "changing.txt")
        );
    }

    #[test]
    fn a_job_with_no_connected_allowed_disk_is_reported_and_nothing_runs() {
        let lab = lab(&["a", "b"]);
        lab.fill_source(&lab.source, 1);
        let mut job = lab.job("data", &lab.source, 20);
        job.disk_ids = Some(vec!["b".to_string()]);

        let (result, reports) = lab.run(1, &["a"], true, vec![job]).unwrap();

        assert_eq!(result, AutoResult::JobWithoutDisk("data".to_string()));
        assert!(reports.is_empty());
        assert!(lab.gens("a", "data").is_empty());
    }

    #[test]
    fn disabled_disks_never_take_part_in_an_automatic_run() {
        let mut lab = lab(&["a", "b"]);
        lab.disks[1].disabled = true;
        let reports = lab.daily(1, &["a", "b"], true, 20);

        assert_eq!(statuses(&reports, "a"), vec![BackupJobStatus::Success]);
        assert!(statuses(&reports, "b").is_empty());
        assert!(lab.gens("b", "data").is_empty());
    }

    // ---- real SSH (opt in: TIMEVAULT_TEST_SSH=1 cargo test -- --test-threads=1) ----

    #[test]
    fn remote_sources_over_real_ssh_cascade_and_reuse_moved_files() {
        let Some(ssh) = crate::util::testsupport::SshLoopback::enabled() else {
            return;
        };
        let lab = lab(&["a", "b"]);
        lab.relocation
            .set(crate::backup::relocation::RelocationConfig {
                enabled: true,
                min_size: 1,
                ..Default::default()
            });
        // A folder of tracks and one lone file, all with the same mtime.
        for i in 0..5 {
            let path = lab.source.join(format!("album/track{i}.flac"));
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, format!("audio-{i}-{}", "z".repeat(i * 10))).unwrap();
            touch(&path, "2020-05-05 10:00:00");
        }
        let lone = lab.source.join("lone.bin");
        std::fs::write(&lone, "a lone file with its own content").unwrap();
        touch(&lone, "2020-05-05 10:00:00");
        let remote = ssh.source(&lab.source);
        let job = |lab: &Lab| lab.job("data", Path::new(&remote), 20);

        // Day one: the remote host goes to the primary over SSH, and the
        // primary's generation goes to the second disk locally.
        *lab.host.day.borrow_mut() = String::new();
        let (result, reports) = lab.run(1, &["a", "b"], true, vec![job(&lab)]).unwrap();
        assert_eq!(result, AutoResult::Done);
        assert_eq!(statuses(&reports, "a"), vec![BackupJobStatus::Success]);
        assert_eq!(statuses(&reports, "b"), vec![BackupJobStatus::Success]);

        // The remote host reorganises: the folder is renamed and the lone file moves.
        std::fs::rename(lab.source.join("album"), lab.source.join("Music")).unwrap();
        std::fs::rename(&lone, lab.source.join("lone-renamed.bin")).unwrap();
        let (result, reports) = lab.run(2, &["a", "b"], true, vec![job(&lab)]).unwrap();
        assert_eq!(result, AutoResult::Done);
        assert_eq!(statuses(&reports, "a"), vec![BackupJobStatus::Success]);
        assert_eq!(statuses(&reports, "b"), vec![BackupJobStatus::Success]);

        for disk in ["a", "b"] {
            // The folder move (found without reading anything on the primary,
            // by content on the second disk) and the lone file (confirmed by
            // rsync's checksum on the primary) all reused existing data.
            for i in 0..5 {
                let old = lab
                    .job_dir(disk, "data")
                    .join(day_name(1))
                    .join(format!("album/track{i}.flac"));
                let new = lab
                    .job_dir(disk, "data")
                    .join(day_name(2))
                    .join(format!("Music/track{i}.flac"));
                assert_eq!(
                    std::fs::metadata(&old).unwrap().ino(),
                    std::fs::metadata(&new).unwrap().ino(),
                    "{disk} track {i}"
                );
            }
            assert_eq!(
                lab.ino(disk, "data", 1, "lone.bin"),
                lab.ino(disk, "data", 2, "lone-renamed.bin"),
                "{disk} lone file"
            );
        }
        assert_eq!(
            lab.text("a", "data", 2, "lone-renamed.bin"),
            "a lone file with its own content"
        );
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
    host: &dyn DiskHost,
    disk_id: String,
    jobs: Vec<crate::config::model::Job>,
    rsync_extra: &[String],
    run_mode: RunMode,
    mountpoint: &std::path::Path,
    options: BackupOptions,
    reports: &mut Vec<BackupRunReport>,
) -> Option<(i32, String)> {
    let backup_result = run_backup_for_day(
        jobs,
        rsync_extra,
        run_mode,
        mountpoint,
        options,
        &host.backup_day(),
    );
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
