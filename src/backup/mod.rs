use std::collections::{HashMap, HashSet};
use std::env;
use std::ffi::CStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, ToSocketAddrs, UdpSocket};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration as StdDuration, Instant};

use chrono::{Duration, Local, Utc};
use walkdir::WalkDir;

use crate::backup::pristine::{build_pristine_excludes_for_source, PristineSource};
use crate::backup::report::{BackupJobReport, BackupJobStatus, BackupRunReport};
use crate::backup::rsync::run_rsync;
use crate::config::model::{Job, RemoteAfterBackup, RemoteJobOptions};
use crate::error::{Result, TimevaultError};
use crate::types::RunMode;
use crate::util::command::maybe_print_command;
use crate::util::paths::job_lock_path;

pub mod pristine;
pub mod report;
pub mod rsync;

const TIMEVAULT_MARKER: &str = ".timevault";
const SCRIPT_DIR: &str = "/etc/timevault/scripts";
const SUSPEND_TARGETS: [&str; 4] = [
    "sleep.target",
    "suspend.target",
    "hibernate.target",
    "hybrid-sleep.target",
];
const PING_ATTEMPT_TIMEOUT: StdDuration = StdDuration::from_secs(2);
const DEFAULT_REMOTE_PROBE_TIMEOUT_SECONDS: u64 = 180;
const DEFAULT_REMOTE_MINIMUM_UPTIME_SECONDS: u64 = 600;
const DEFAULT_REMOTE_MINIMUM_SESSION_SECONDS: u64 = 300;
const REMOTE_ACTIVITY_WINDOW_SECONDS: i64 = 24 * 60 * 60;
const REMOTE_ACTIVITY_BUFFER_SECONDS: i64 = 10 * 60;
/// How far apart an sshd `Accepted` line and a `systemd-logind` `New session`
/// line may be while still counting as the same login.
const SESSION_CORRELATION_SECONDS: i64 = 10;
/// `MESSAGE_ID` of systemd-logind's "session created" / "session removed" journal
/// entries. Matching these (rather than grepping the message text) also gives us
/// the structured `SESSION_ID`, `USER_ID` and `_BOOT_ID` fields.
const LOGIND_SESSION_NEW_MESSAGE_ID: &str = "8d45620c1a4348dbb17410da57c60c66";
const LOGIND_SESSION_REMOVED_MESSAGE_ID: &str = "3354939424b4456d9802ca8333ed424a";
/// Absolute remote-vs-local clock offset, in seconds, at or above which Timevault
/// warns about a drifting backup-source clock on every run.
const REMOTE_CLOCK_DRIFT_WARN_SECONDS: i64 = 5;
/// Session owners that never count as a person using the host during the
/// cold-boot check: display-manager greeter accounts sitting at the login
/// screen. Overridable per job with `remote.ignoredSessionUsers`.
const DEFAULT_IGNORED_SESSION_USERS: [&str; 9] = [
    "gdm",
    "gdm3",
    "Debian-gdm",
    "sddm",
    "lightdm",
    "lxdm",
    "xdm",
    "kdm",
    "slim",
];

#[derive(Debug, Clone, Copy)]
pub struct BackupOptions {
    pub exclude_pristine: bool,
    pub exclude_pristine_only: bool,
    /// CLI override for `remote.minimumSessionSeconds` on every job this run
    /// (`--min-session-seconds`); handy when testing the cold-boot gate.
    pub session_seconds_override: Option<u64>,
}

struct LockGuard {
    path: PathBuf,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = unlock_file(&self.path);
    }
}

struct SuspendGuard {
    masked_targets: Vec<&'static str>,
    remote_host: Option<String>,
}

impl Drop for SuspendGuard {
    fn drop(&mut self) {
        if !self.masked_targets.is_empty() {
            let Some(remote_host) = self.remote_host.as_deref() else {
                return;
            };
            let mut cmd = remote_systemctl_command(remote_host, "unmask", &self.masked_targets);
            match cmd.status() {
                Ok(status) if status.success() => {}
                Ok(status) => crate::pnote!(
                    "failed to re-enable suspend on backup source host {} after backup: ssh exited with code {}",
                    remote_host,
                    status.code().unwrap_or(1)
                ),
                Err(err) => crate::pnote!(
                    "failed to re-enable suspend on backup source host {} after backup: {}",
                    remote_host, err
                ),
            }
        }
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
    crate::pnote!("job: {}", job.name);
    if let Some(description) = &job.description {
        crate::pnote!("  description: {}", description);
    }
    crate::pnote!("  source: {}", job.source);
    crate::pnote!("  backup dir: {}", job.name);
    crate::pnote!("  copies: {}", job.copies);
    crate::pnote!("  run: {}", job.run_policy.as_str());
    crate::pnote!("  excludes: {}", excludes);
    crate::pnote!("  disks: {}", disk_ids);
}

pub fn run_backup(
    jobs: Vec<Job>,
    rsync_extra: &[String],
    run_mode: RunMode,
    disk_mount: &Path,
    options: BackupOptions,
) -> Result<BackupRunReport> {
    let started_at = Local::now();
    let mut report = BackupRunReport {
        disk_id: disk_mount
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown")
            .to_string(),
        mountpoint: disk_mount.display().to_string(),
        started_at,
        finished_at: started_at,
        jobs: Vec::new(),
    };
    let mut pristine_excludes = PristineExcludes::default();
    let total = jobs.len();
    for (index, job) in jobs.into_iter().enumerate() {
        crate::pstatus!("job {}/{}: {}", index + 1, total, job.name);
        let backup_day = (Local::now() - Duration::days(1))
            .format("%Y%m%d")
            .to_string();
        match run_backup_job(
            &job,
            &backup_day,
            rsync_extra,
            run_mode,
            disk_mount,
            options,
            &mut pristine_excludes,
        ) {
            Ok(job_report) => report.jobs.push(job_report),
            Err(err) => {
                crate::pnote!("job {} failed: {}", job.name, err);
                report.jobs.push(failed_job_report(
                    &job,
                    disk_mount,
                    &backup_day,
                    run_mode,
                    err.to_string(),
                ));
            }
        }
    }
    report.finished_at = Local::now();
    Ok(report)
}

fn run_backup_job(
    job: &Job,
    backup_day: &str,
    rsync_extra: &[String],
    run_mode: RunMode,
    disk_mount: &Path,
    options: BackupOptions,
    pristine_excludes: &mut PristineExcludes,
) -> Result<BackupJobReport> {
    let _lock = acquire_lock_for_job(&job.name, run_mode)?;
    let dest = resolve_job_dest(job, disk_mount)?;
    let backup_dir = dest.join(backup_day);

    if options.exclude_pristine_only {
        if run_mode.verbose {
            crate::pnote!(
                "pristine: exclude-only mode enabled; skipping backup for job {}",
                job.name
            );
        }
        return Ok(BackupJobReport {
            name: job.name.clone(),
            description: job.description.clone(),
            source: job.source.clone(),
            destination: disk_mount.display().to_string(),
            backup_day: "-".to_string(),
            status: BackupJobStatus::Skipped,
            attempts: 0,
            rsync_code: None,
            failure_reason: None,
        });
    }

    if run_mode.verbose {
        crate::pnote!("  backup day: {}", backup_day);
    }

    if run_mode.verbose {
        crate::pnote!("job: {}", job.name);
        crate::pnote!("  run: {}", job.run_policy.as_str());
        crate::pnote!("  source: {}", job.source);
        crate::pnote!("  backup dir: {}", dest.display());
        crate::pnote!("  copies: {}", job.copies);
        crate::pnote!("  excludes: {}", job.excludes.len());
    }

    let mut remote_power_guard = match start_remote_power_guard(job, run_mode, options) {
        Ok(RemotePowerStart::Ready(guard)) => guard,
        Ok(RemotePowerStart::InactiveColdBoot {
            uptime_seconds,
            minimum_session_seconds,
            _power_guard,
        }) => {
            let reason = format!(
                "host was started by Wake-on-LAN, has only {}s uptime, and had no interactive session lasting at least {}s in the prior daily window",
                uptime_seconds, minimum_session_seconds
            );
            crate::pnote!("job {} skipped: {}", job.name, reason);
            return Ok(skipped_job_report(job, disk_mount, backup_day, reason));
        }
        Err(err) if remote_readiness_failed(&err) || remote_offline_if_unreachable(job) => {
            crate::pnote!("job {} offline: {}", job.name, err);
            return Ok(offline_job_report(
                job,
                disk_mount,
                backup_day,
                err.to_string(),
            ));
        }
        Err(err) => return Err(err),
    };
    let _suspend_guard = start_suspend_guard(job, run_mode)?;

    if let Some(script) = job_script_path(&job.name, JobScriptPhase::Pre) {
        let script_result = run_job_script(
            job,
            &script,
            JobScriptPhase::Pre,
            &backup_dir,
            backup_day,
            None,
            run_mode,
        )?;
        if script_result.exit_code != 0 {
            crate::pnote!(
                "pre script failed for job {} with exit code {}; skipping backup",
                job.name,
                script_result.exit_code
            );
            return Ok(BackupJobReport {
                name: job.name.clone(),
                description: job.description.clone(),
                source: job.source.clone(),
                destination: backup_dir.display().to_string(),
                backup_day: backup_day.to_string(),
                status: BackupJobStatus::Failed,
                attempts: 0,
                rsync_code: None,
                failure_reason: Some(script_failure_reason(
                    "pre",
                    script_result.exit_code,
                    &script_result.stderr,
                )),
            });
        }
    }
    if let Some(script_result) = run_remote_job_script(
        job,
        JobScriptPhase::Pre,
        &backup_dir,
        backup_day,
        None,
        run_mode,
    )? {
        if script_result.exit_code != 0 {
            crate::pnote!(
                "remote pre script failed for job {} with exit code {}; skipping backup",
                job.name,
                script_result.exit_code
            );
            return Ok(BackupJobReport {
                name: job.name.clone(),
                description: job.description.clone(),
                source: job.source.clone(),
                destination: backup_dir.display().to_string(),
                backup_day: backup_day.to_string(),
                status: BackupJobStatus::Failed,
                attempts: 0,
                rsync_code: None,
                failure_reason: Some(script_failure_reason(
                    "remote pre",
                    script_result.exit_code,
                    &script_result.stderr,
                )),
            });
        }
    }

    ensure_pristine_excludes_for_job(
        job,
        pristine_excludes,
        options,
        run_mode.verbose,
        run_mode.dry_run,
    )?;

    let home = env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    let tmp_dir = Path::new(&home).join("tmp");
    if !run_mode.dry_run {
        fs::create_dir_all(&tmp_dir)?;
    }
    let excludes_file = job_excludes_path(&tmp_dir, &job.name);
    let excludes = build_exclude_list(job, pristine_excludes)?;
    if run_mode.dry_run {
        crate::pnote!(
            "dry-run: would write excludes file {}",
            excludes_file.display()
        );
    } else {
        create_excludes_file(&excludes, &excludes_file)?;
    }

    if !dest.exists() {
        if run_mode.dry_run {
            crate::pnote!("dry-run: mkdir -p {}", dest.display());
        } else {
            fs::create_dir_all(&dest)?;
        }
    }

    crate::pstatus!("{}: pruning old snapshots", job.name);
    expire_old_backups(job, &dest, run_mode)?;

    let current = dest.join("current");
    let backup_dir = dest.join(backup_day);

    if current.exists() && !backup_dir.exists() {
        if run_mode.dry_run {
            crate::pnote!("dry-run: mkdir -p {}", backup_dir.display());
        } else {
            fs::create_dir_all(&backup_dir)?;
        }
        copy_snapshot_without_symlinks(&current, &backup_dir, run_mode)?;
    }

    let mut rc = 1;
    let mut attempts = 0;
    let mut rsync_stderr = String::new();
    for attempt in 1..=3 {
        attempts = attempt;
        crate::progress::status(if attempt == 1 {
            format!("{}: syncing", job.name)
        } else {
            format!("{}: syncing (attempt {}/3)", job.name, attempt)
        });
        let rsync_result = run_rsync(
            &job.source,
            &backup_dir,
            &excludes_file,
            rsync_extra,
            run_mode,
            &job.name,
        )?;
        rc = rsync_result.exit_code;
        rsync_stderr = rsync_result.stderr;
        if rc == 0 || rc == 24 {
            break;
        }
        if attempt < 3 {
            crate::progress::note(format!(
                "rsync failed with exit code {}; retrying ({}/3)",
                rc,
                attempt + 1
            ));
        }
    }
    let rsync_ok = rc == 0 || rc == 24;
    let mut failure_reason = if rsync_ok {
        None
    } else {
        Some(rsync_failure_reason(rc, &rsync_stderr))
    };
    if !rsync_ok {
        crate::progress::note(format!(
            "rsync failed with exit code {}; current not updated",
            rc
        ));
    }

    if rsync_ok && backup_dir.exists() {
        let current_link = dest.join("current");
        if let Ok(meta) = fs::symlink_metadata(&current_link) {
            if meta.file_type().is_symlink() || meta.is_file() {
                if run_mode.safe_mode || run_mode.dry_run {
                    if run_mode.dry_run {
                        crate::pnote!("dry-run: rm -f {}", current_link.display());
                    } else {
                        crate::pnote!("skip remove (safe-mode): {}", current_link.display());
                    }
                } else {
                    fs::remove_file(&current_link).map_err(|err| {
                        TimevaultError::message(format!(
                            "remove current link {}: {}",
                            current_link.display(),
                            err
                        ))
                    })?;
                }
            } else if meta.is_dir() {
                crate::pnote!(
                    "skip updating current (directory exists): {}",
                    current_link.display()
                );
            }
        }
        if !current_link.exists() {
            if run_mode.dry_run {
                crate::pnote!("dry-run: ln -s {} {}", backup_day, current_link.display());
            } else {
                symlink(backup_day, &current_link).map_err(|err| {
                    TimevaultError::message(format!(
                        "create current link {}: {}",
                        current_link.display(),
                        err
                    ))
                })?;
            }
        }
    }
    let mut status = status_for_rsync_code(rc);
    if let Some(script_result) = run_remote_job_script(
        job,
        JobScriptPhase::Post,
        &backup_dir,
        backup_day,
        Some(rc),
        run_mode,
    )? {
        if script_result.exit_code != 0 {
            crate::pnote!(
                "remote post script failed for job {} with exit code {}",
                job.name,
                script_result.exit_code
            );
            status = BackupJobStatus::Failed;
            failure_reason = Some(script_failure_reason(
                "remote post",
                script_result.exit_code,
                &script_result.stderr,
            ));
        }
    }
    if let Some(script) = job_script_path(&job.name, JobScriptPhase::Post) {
        let script_result = run_job_script(
            job,
            &script,
            JobScriptPhase::Post,
            &backup_dir,
            backup_day,
            Some(rc),
            run_mode,
        )?;
        if script_result.exit_code != 0 {
            crate::pnote!(
                "post script failed for job {} with exit code {}",
                job.name,
                script_result.exit_code
            );
            status = BackupJobStatus::Failed;
            failure_reason = Some(script_failure_reason(
                "post",
                script_result.exit_code,
                &script_result.stderr,
            ));
        }
    }
    if status != BackupJobStatus::Failed {
        if let Some(guard) = remote_power_guard.as_mut() {
            guard.backup_completed = true;
        }
    }
    Ok(BackupJobReport {
        name: job.name.clone(),
        description: job.description.clone(),
        source: job.source.clone(),
        destination: backup_dir.display().to_string(),
        backup_day: backup_day.to_string(),
        status,
        attempts,
        rsync_code: Some(rc),
        failure_reason,
    })
}

fn failed_job_report(
    job: &Job,
    disk_mount: &Path,
    backup_day: &str,
    run_mode: RunMode,
    failure_reason: String,
) -> BackupJobReport {
    let destination = resolve_job_dest(job, disk_mount)
        .map(|dest| dest.join(backup_day).display().to_string())
        .unwrap_or_else(|_| disk_mount.display().to_string());
    BackupJobReport {
        name: job.name.clone(),
        description: job.description.clone(),
        source: job.source.clone(),
        destination,
        backup_day: if run_mode.dry_run {
            "-".to_string()
        } else {
            backup_day.to_string()
        },
        status: BackupJobStatus::Failed,
        attempts: 0,
        rsync_code: None,
        failure_reason: Some(failure_reason),
    }
}

fn offline_job_report(
    job: &Job,
    disk_mount: &Path,
    backup_day: &str,
    reason: String,
) -> BackupJobReport {
    BackupJobReport {
        name: job.name.clone(),
        description: job.description.clone(),
        source: job.source.clone(),
        destination: disk_mount.display().to_string(),
        backup_day: backup_day.to_string(),
        status: BackupJobStatus::Offline,
        attempts: 0,
        rsync_code: None,
        failure_reason: Some(reason),
    }
}

fn skipped_job_report(
    job: &Job,
    disk_mount: &Path,
    backup_day: &str,
    reason: String,
) -> BackupJobReport {
    BackupJobReport {
        name: job.name.clone(),
        description: job.description.clone(),
        source: job.source.clone(),
        destination: disk_mount.display().to_string(),
        backup_day: backup_day.to_string(),
        status: BackupJobStatus::Skipped,
        attempts: 0,
        rsync_code: None,
        failure_reason: Some(reason),
    }
}

fn status_for_rsync_code(rc: i32) -> BackupJobStatus {
    match rc {
        0 | 24 => BackupJobStatus::Success,
        _ => BackupJobStatus::Failed,
    }
}

fn rsync_failure_reason(exit_code: i32, stderr: &str) -> String {
    let stderr = stderr.split_whitespace().collect::<Vec<_>>().join(" ");
    if stderr.is_empty() {
        format!("rsync failed with exit code {}", exit_code)
    } else {
        format!("rsync failed with exit code {}: {}", exit_code, stderr)
    }
}

fn script_failure_reason(kind: &str, exit_code: i32, stderr: &str) -> String {
    let detail = stderr.split_whitespace().collect::<Vec<_>>().join(" ");
    let summary = format!("{} script exited with code {}", kind, exit_code);
    if detail.is_empty() {
        summary
    } else {
        format!("{}: {}", summary, detail)
    }
}

fn start_suspend_guard(job: &Job, run_mode: RunMode) -> Result<SuspendGuard> {
    if !has_remote_suspend_guard_config(job) {
        return Ok(SuspendGuard {
            masked_targets: Vec::new(),
            remote_host: None,
        });
    }
    let Some(remote) = remote_ssh_source(&job.source) else {
        return Ok(SuspendGuard {
            masked_targets: Vec::new(),
            remote_host: None,
        });
    };

    if run_mode.dry_run {
        crate::pnote!(
            "dry-run: would check suspend state on backup source host {}: systemctl is-enabled {}",
            remote.host,
            SUSPEND_TARGETS.join(" ")
        );
        return Ok(SuspendGuard {
            masked_targets: Vec::new(),
            remote_host: None,
        });
    }

    let targets_to_mask = suspend_targets_to_mask(&remote.host, run_mode)?;
    if !targets_to_mask.is_empty() {
        let mut cmd = remote_systemctl_command(&remote.host, "mask", &targets_to_mask);
        maybe_print_command(&cmd, run_mode);
        let status = cmd.status().map_err(|err| {
            TimevaultError::message(format!(
                "failed to disable suspend on backup source host {} before backup: {}",
                remote.host, err
            ))
        })?;
        if !status.success() {
            return Err(TimevaultError::message(format!(
                "failed to disable suspend on backup source host {} before backup: ssh exited with code {}",
                remote.host,
                status.code().unwrap_or(1)
            )));
        }
        Ok(SuspendGuard {
            masked_targets: targets_to_mask,
            remote_host: Some(remote.host),
        })
    } else {
        crate::pnote!(
            "suspend on backup source host {} was already disabled before backup; leaving it disabled",
            remote.host
        );
        Ok(SuspendGuard {
            masked_targets: Vec::new(),
            remote_host: None,
        })
    }
}

fn suspend_targets_to_mask(remote_host: &str, run_mode: RunMode) -> Result<Vec<&'static str>> {
    let mut cmd = remote_systemctl_command(remote_host, "is-enabled", &SUSPEND_TARGETS);
    maybe_print_command(&cmd, run_mode);
    let output = cmd.output().map_err(|err| {
        TimevaultError::message(format!(
            "failed to detect suspend state on backup source host {}: {}",
            remote_host, err
        ))
    })?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}\n{}", stdout, stderr);
    if output.status.success() || output.status.code() != Some(255) && !combined.trim().is_empty() {
        Ok(suspend_targets_to_mask_from_systemctl_output(&combined))
    } else {
        Err(TimevaultError::message(format!(
            "failed to detect suspend state on backup source host {}: ssh exited with code {}",
            remote_host,
            output.status.code().unwrap_or(1)
        )))
    }
}

fn suspend_targets_to_mask_from_systemctl_output(output: &str) -> Vec<&'static str> {
    SUSPEND_TARGETS
        .iter()
        .zip(output.lines())
        .filter_map(|(target, state)| (state.trim() != "masked").then_some(*target))
        .collect()
}

fn remote_systemctl_command(remote_host: &str, action: &str, targets: &[&str]) -> Command {
    let mut cmd = Command::new("ssh");
    cmd.arg(remote_host)
        .arg("systemctl")
        .arg(action)
        .args(targets);
    cmd
}

pub fn run_pristine_only(jobs: Vec<Job>, run_mode: RunMode, options: BackupOptions) -> Result<()> {
    if run_mode.verbose {
        crate::pnote!("pristine: exclude-only mode enabled; skipping backup");
    }
    let pristine_excludes =
        build_pristine_excludes_for_jobs(&jobs, options, run_mode.verbose, run_mode.dry_run)?;
    for job in jobs {
        let _lock = acquire_lock_for_job(&job.name, run_mode)?;
        let home = env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let tmp_dir = Path::new(&home).join("tmp");
        if !run_mode.dry_run {
            fs::create_dir_all(&tmp_dir)?;
        }
        let excludes_file = job_excludes_path(&tmp_dir, &job.name);
        let excludes = build_exclude_list(&job, &pristine_excludes)?;
        if run_mode.dry_run {
            crate::pnote!(
                "dry-run: would write excludes file {}",
                excludes_file.display()
            );
        } else {
            create_excludes_file(&excludes, &excludes_file)?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum JobScriptPhase {
    Pre,
    Post,
}

impl JobScriptPhase {
    fn as_str(self) -> &'static str {
        match self {
            JobScriptPhase::Pre => "pre",
            JobScriptPhase::Post => "post",
        }
    }
}

fn run_job_script(
    job: &Job,
    script: &Path,
    phase: JobScriptPhase,
    destination: &Path,
    backup_day: &str,
    rsync_code: Option<i32>,
    run_mode: RunMode,
) -> Result<ScriptResult> {
    let mut cmd = Command::new("/bin/sh");
    cmd.arg(script)
        .env("TIMEVAULT_JOB_NAME", &job.name)
        .env("TIMEVAULT_JOB_SOURCE", &job.source)
        .env("TIMEVAULT_JOB_DESTINATION", destination)
        .env("TIMEVAULT_BACKUP_DAY", backup_day)
        .env("TIMEVAULT_SCRIPT_PHASE", phase.as_str());
    if let Some(code) = rsync_code {
        cmd.env("TIMEVAULT_RSYNC_CODE", code.to_string());
    }
    if run_mode.dry_run {
        crate::pnote!(
            "dry-run: would run {} script for job {}: {}",
            phase.as_str(),
            job.name,
            script.display()
        );
        return Ok(ScriptResult {
            exit_code: 0,
            stderr: String::new(),
        });
    }
    maybe_print_command(&cmd, run_mode);
    let output = cmd.output().map_err(|e| {
        TimevaultError::message(format!(
            "{} script for job {} ({}): {}",
            phase.as_str(),
            job.name,
            script.display(),
            e
        ))
    })?;
    io::stdout().write_all(&output.stdout)?;
    io::stderr().write_all(&output.stderr)?;
    Ok(ScriptResult {
        exit_code: output.status.code().unwrap_or(1),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

struct ScriptResult {
    exit_code: i32,
    stderr: String,
}

fn run_remote_job_script(
    job: &Job,
    phase: JobScriptPhase,
    destination: &Path,
    backup_day: &str,
    rsync_code: Option<i32>,
    run_mode: RunMode,
) -> Result<Option<ScriptResult>> {
    let Some(remote) = remote_ssh_source(&job.source) else {
        return Ok(None);
    };
    let script = remote_job_script_path(&job.name, phase);
    if run_mode.dry_run {
        crate::pnote!(
            "dry-run: would run remote {} script for job {} if present: {}:{}",
            phase.as_str(),
            job.name,
            remote.host,
            script
        );
        return Ok(Some(ScriptResult {
            exit_code: 0,
            stderr: String::new(),
        }));
    }

    let command = remote_script_command(
        job,
        &remote.source_path,
        &script,
        phase,
        destination,
        backup_day,
        rsync_code,
    );
    let mut cmd = Command::new("ssh");
    cmd.arg(&remote.host).arg(command);
    maybe_print_command(&cmd, run_mode);
    let output = cmd.output().map_err(|e| {
        TimevaultError::message(format!(
            "remote {} script for job {} ({}:{}): {}",
            phase.as_str(),
            job.name,
            remote.host,
            script,
            e
        ))
    })?;
    io::stdout().write_all(&output.stdout)?;
    io::stderr().write_all(&output.stderr)?;
    Ok(Some(ScriptResult {
        exit_code: output.status.code().unwrap_or(1),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    }))
}

struct WakeContext {
    host: String,
    ssh_host: String,
    wol: bool,
    targets: Vec<SocketAddrV4>,
    mac: Option<String>,
    keepalive_seconds: Option<u64>,
    probe_timeout: StdDuration,
    minimum_uptime_seconds: u64,
    minimum_session_seconds: u64,
    ignored_session_users: Vec<String>,
    after_backup: Option<RemoteAfterBackup>,
}

struct WakeKeepalive {
    stop: Option<Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for WakeKeepalive {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// The power state a backup source host was in when Timevault reached it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FoundPowerState {
    /// Already up: answered readiness probes without needing Wake-on-LAN.
    Running,
    /// Resumed from suspend by Wake-on-LAN (woken, but with real uptime).
    Suspended,
    /// Cold-booted by Wake-on-LAN (woken, with near-zero uptime).
    PoweredOff,
}

/// The concrete action to take once the backup finishes, after resolving
/// `afterBackup: return` against the host's [`FoundPowerState`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PowerAction {
    Leave,
    Suspend,
    PowerOff,
}

fn resolve_after_backup(
    configured: Option<RemoteAfterBackup>,
    found: FoundPowerState,
) -> PowerAction {
    match configured {
        Some(RemoteAfterBackup::None) => PowerAction::Leave,
        Some(RemoteAfterBackup::Suspend) => PowerAction::Suspend,
        Some(RemoteAfterBackup::Shutdown) => PowerAction::PowerOff,
        None | Some(RemoteAfterBackup::Return) => match found {
            FoundPowerState::Running => PowerAction::Leave,
            FoundPowerState::Suspended => PowerAction::Suspend,
            FoundPowerState::PoweredOff => PowerAction::PowerOff,
        },
    }
}

struct RemotePowerGuard {
    keepalive: Option<WakeKeepalive>,
    action: PowerAction,
    backup_completed: bool,
    remote_host: String,
    job_name: String,
}

enum RemotePowerStart {
    Ready(Option<RemotePowerGuard>),
    InactiveColdBoot {
        uptime_seconds: u64,
        minimum_session_seconds: u64,
        _power_guard: Option<RemotePowerGuard>,
    },
}

impl Drop for RemotePowerGuard {
    fn drop(&mut self) {
        drop(self.keepalive.take());
        if !self.backup_completed || self.action == PowerAction::Leave {
            return;
        }
        let (remote_command, verb) = match self.action {
            PowerAction::PowerOff => ("systemctl poweroff", "power off"),
            PowerAction::Suspend => ("systemctl suspend", "suspend"),
            PowerAction::Leave => return,
        };
        let mut cmd = Command::new("ssh");
        cmd.arg("-o")
            .arg("BatchMode=yes")
            .arg("-o")
            .arg("ConnectTimeout=5")
            .arg(&self.remote_host)
            .arg(remote_command)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        match cmd.status() {
            Ok(status) if status.success() => {}
            Ok(status) => crate::pnote!(
                "failed to {} remote host {} after job {}: ssh exited with code {}",
                verb,
                self.remote_host,
                self.job_name,
                status.code().unwrap_or(1)
            ),
            Err(err) => crate::pnote!(
                "failed to {} remote host {} after job {}: {}",
                verb,
                self.remote_host,
                self.job_name,
                err
            ),
        }
    }
}

pub fn wake_remote_job(job: &Job, run_mode: RunMode) -> Result<()> {
    if run_mode.dry_run {
        let Some((remote, _, host)) = remote_config(job) else {
            return Err(TimevaultError::message(format!(
                "job {} has no remote WOL configuration",
                job.name
            )));
        };
        if remote.wol != Some(true) {
            return Err(TimevaultError::message(format!(
                "job {} has remote.wol disabled",
                job.name
            )));
        }
        crate::pnote!(
            "dry-run: would send WOL for job {} to {} for {}",
            job.name,
            wake_target_description(remote),
            host
        );
        crate::pnote!(
            "dry-run: would send WOL, then wait up to {} seconds for ping and SSH readiness",
            remote
                .probe_timeout_seconds
                .unwrap_or(DEFAULT_REMOTE_PROBE_TIMEOUT_SECONDS)
        );
        return Ok(());
    }

    let Some(context) = remote_context(job)? else {
        return Err(TimevaultError::message(format!(
            "job {} has no remote WOL configuration",
            job.name
        )));
    };
    if !context.wol {
        return Err(TimevaultError::message(format!(
            "job {} has remote.wol disabled",
            job.name
        )));
    }
    let deadline = Instant::now() + context.probe_timeout;
    if ping_once(&context.host, run_mode)? {
        if run_mode.verbose {
            crate::pnote!(
                "wake host {} already responds to ping; skipping WOL",
                context.host
            );
        }
    } else {
        if run_mode.verbose {
            crate::pnote!(
                "wake host {} did not respond; sending WOL to {} target(s)",
                context.host,
                context.targets.len()
            );
        }
        send_wake_packets(
            context.mac.as_deref().expect("validated WOL MAC"),
            &context.targets,
        )?;
        wait_for_ping_after_wake(
            &context.host,
            deadline,
            context.mac.as_deref().expect("validated WOL MAC"),
            &context.targets,
            run_mode,
        )?;
    }
    wait_for_ssh_until(&context.ssh_host, deadline, run_mode)
}

fn start_remote_power_guard(
    job: &Job,
    run_mode: RunMode,
    options: BackupOptions,
) -> Result<RemotePowerStart> {
    if run_mode.dry_run {
        let Some((remote, _, host)) = remote_config(job) else {
            return Ok(RemotePowerStart::Ready(None));
        };
        crate::pnote!(
            "dry-run: would probe {} by ping and SSH for up to {} seconds",
            host,
            remote
                .probe_timeout_seconds
                .unwrap_or(DEFAULT_REMOTE_PROBE_TIMEOUT_SECONDS)
        );
        if remote.wol == Some(true) {
            crate::pnote!(
                "dry-run: would send WOL for job {} to {} if ping fails",
                job.name,
                wake_target_description(remote)
            );
            let effective_min_session = options.session_seconds_override.unwrap_or_else(|| {
                remote
                    .minimum_session_seconds
                    .unwrap_or(DEFAULT_REMOTE_MINIMUM_SESSION_SECONDS)
            });
            crate::pnote!(
                "dry-run: would check uptime after WOL; below {} seconds, it would inspect the remote persistent journal for a session tied to a non-Timevault SSH login and lasting at least {} seconds{} during the prior daily window (greeter, manager, cron and Timevault's own sessions do not count; sessions are keyed by boot id)",
                remote
                    .minimum_uptime_seconds
                    .unwrap_or(DEFAULT_REMOTE_MINIMUM_UPTIME_SECONDS),
                effective_min_session,
                if options.session_seconds_override.is_some() {
                    " (--min-session-seconds override)"
                } else {
                    ""
                }
            );
        }
        crate::pnote!(
            "dry-run: would skip the backup if {} is not ready before the timeout",
            host
        );
        if remote.wol == Some(true) && remote.keepalive_seconds.is_some() {
            let seconds = remote.keepalive_seconds.expect("checked above");
            crate::pnote!(
                "dry-run: would repeat WOL for job {} every {} seconds while backup runs if wake was needed",
                job.name, seconds
            );
        }
        match remote.after_backup.unwrap_or(RemoteAfterBackup::Return) {
            RemoteAfterBackup::None => {}
            RemoteAfterBackup::Suspend => crate::pnote!(
                "dry-run: would suspend remote host after a successful job {}",
                job.name
            ),
            RemoteAfterBackup::Shutdown => crate::pnote!(
                "dry-run: would power off remote host after a successful job {}",
                job.name
            ),
            RemoteAfterBackup::Return => crate::pnote!(
                "dry-run: would return remote host to its pre-backup power state (leave running, suspend, or power off) after a successful job {}",
                job.name
            ),
        }
        return Ok(RemotePowerStart::Ready(None));
    }

    let Some(mut context) = remote_context(job)? else {
        return Ok(RemotePowerStart::Ready(None));
    };
    if let Some(seconds) = options.session_seconds_override {
        if seconds != context.minimum_session_seconds {
            crate::pnote!(
                "  --min-session-seconds: cold-boot minimum session length {}s -> {}s for job {}",
                context.minimum_session_seconds,
                seconds,
                job.name
            );
        }
        context.minimum_session_seconds = seconds;
    }
    let deadline = Instant::now() + context.probe_timeout;
    crate::pstatus!("{}: reaching {}", job.name, context.host);
    let was_woken = if ping_once(&context.host, run_mode)? {
        if run_mode.verbose {
            crate::pnote!(
                "wake host {} already responds to ping; skipping WOL",
                context.host
            );
        }
        false
    } else if context.wol {
        if run_mode.verbose {
            crate::pnote!(
                "wake host {} did not respond; sending WOL to {} target(s)",
                context.host,
                context.targets.len()
            );
        }
        crate::pstatus!("{}: sending Wake-on-LAN to {}", job.name, context.host);
        send_wake_packets(
            context.mac.as_deref().expect("validated WOL MAC"),
            &context.targets,
        )?;
        wait_for_ping_after_wake(
            &context.host,
            deadline,
            context.mac.as_deref().expect("validated WOL MAC"),
            &context.targets,
            run_mode,
        )?;
        true
    } else {
        wait_for_ping_until(&context.host, deadline, run_mode)?;
        false
    };
    wait_for_ssh_until(&context.ssh_host, deadline, run_mode)?;

    let local_now = Utc::now().timestamp();
    let (remote_now, uptime_seconds) = remote_now_and_uptime(&context.ssh_host, run_mode)?;
    let clock_offset = remote_now - local_now;
    report_clock_drift(&context.ssh_host, clock_offset, run_mode);

    let found_state = if !was_woken {
        FoundPowerState::Running
    } else {
        if run_mode.verbose {
            crate::pnote!(
                "  remote {} uptime {}s (cold-boot threshold {}s)",
                context.ssh_host,
                uptime_seconds,
                context.minimum_uptime_seconds
            );
        }
        if uptime_seconds < context.minimum_uptime_seconds {
            // Everything from here is in the remote's clock frame: `remote_now`
            // and the journal timestamps share it, so a skewed remote clock no
            // longer slides the window or backdates boot-time sessions into it.
            let boot_start = remote_now - uptime_seconds as i64;
            let (window_start, window_end) = remote_activity_window(boot_start);
            crate::pstatus!("{}: cold-boot check", job.name);
            if run_mode.verbose {
                crate::pnote!(
                    "  {} looks cold-booted; inspecting the prior daily journal window for real use",
                    context.ssh_host
                );
            }
            if !remote_journal_has_qualifying_session(
                &context.ssh_host,
                window_start,
                window_end,
                boot_start,
                context.minimum_session_seconds,
                &context.ignored_session_users,
                run_mode,
            )? {
                return Ok(RemotePowerStart::InactiveColdBoot {
                    uptime_seconds,
                    minimum_session_seconds: context.minimum_session_seconds,
                    _power_guard: Some(RemotePowerGuard {
                        keepalive: None,
                        action: PowerAction::PowerOff,
                        backup_completed: true,
                        remote_host: context.ssh_host,
                        job_name: job.name.clone(),
                    }),
                });
            }
            FoundPowerState::PoweredOff
        } else {
            if run_mode.verbose {
                crate::pnote!(
                    "  remote {} uptime is above the cold-boot threshold; treating it as resumed from suspend",
                    context.ssh_host
                );
            }
            FoundPowerState::Suspended
        }
    };

    let keepalive = if was_woken {
        context.keepalive_seconds.map(|seconds| {
            let (stop, receiver) = mpsc::channel();
            let mac = context.mac.expect("WOL keepalive requires MAC");
            let targets = context.targets;
            let handle = thread::spawn(move || loop {
                match receiver.recv_timeout(StdDuration::from_secs(seconds)) {
                    Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                    Err(RecvTimeoutError::Timeout) => {
                        let _ = send_wake_packets(&mac, &targets);
                    }
                }
            });
            WakeKeepalive {
                stop: Some(stop),
                handle: Some(handle),
            }
        })
    } else {
        None
    };

    Ok(RemotePowerStart::Ready(Some(RemotePowerGuard {
        keepalive,
        action: resolve_after_backup(context.after_backup, found_state),
        backup_completed: false,
        remote_host: context.ssh_host,
        job_name: job.name.clone(),
    })))
}

fn remote_context(job: &Job) -> Result<Option<WakeContext>> {
    let Some((options, remote, host)) = remote_config(job) else {
        return Ok(None);
    };
    let wol = options.wol.unwrap_or(false);
    let targets = if wol {
        wake_targets(options, &host)?
    } else {
        Vec::new()
    };
    Ok(Some(WakeContext {
        host: host.to_string(),
        ssh_host: remote.host,
        targets,
        wol,
        mac: options.mac.clone(),
        keepalive_seconds: options.keepalive_seconds,
        probe_timeout: StdDuration::from_secs(
            options
                .probe_timeout_seconds
                .unwrap_or(DEFAULT_REMOTE_PROBE_TIMEOUT_SECONDS),
        ),
        minimum_uptime_seconds: options
            .minimum_uptime_seconds
            .unwrap_or(DEFAULT_REMOTE_MINIMUM_UPTIME_SECONDS),
        minimum_session_seconds: options
            .minimum_session_seconds
            .unwrap_or(DEFAULT_REMOTE_MINIMUM_SESSION_SECONDS),
        ignored_session_users: options.ignored_session_users.clone().unwrap_or_else(|| {
            DEFAULT_IGNORED_SESSION_USERS
                .iter()
                .map(|user| user.to_string())
                .collect()
        }),
        after_backup: options.after_backup,
    }))
}

fn remote_offline_if_unreachable(job: &Job) -> bool {
    remote_config(job)
        .and_then(|(remote, _, _)| remote.offline_if_unreachable)
        .unwrap_or(false)
}

fn remote_readiness_failed(error: &TimevaultError) -> bool {
    let message = error.to_string();
    message.contains("did not respond to ping") || message.contains("SSH readiness probe")
}

fn remote_config<'a>(job: &'a Job) -> Option<(&'a RemoteJobOptions, RemoteSshSource, String)> {
    let Some(remote_options) = &job.remote else {
        return None;
    };
    let Some(remote) = remote_ssh_source(&job.source) else {
        return None;
    };
    let host = remote_host(remote_options, &remote).to_string();
    Some((remote_options, remote, host))
}

fn has_active_remote_config(job: &Job) -> bool {
    remote_config(job).is_some()
}

fn has_remote_suspend_guard_config(job: &Job) -> bool {
    has_active_remote_config(job)
}

fn wake_target_description(remote: &RemoteJobOptions) -> String {
    match &remote.broadcast {
        Some(broadcast) => format!("{}:{}", broadcast, remote.port.unwrap_or(9)),
        None => format!(
            "DNS-inferred /24 broadcast or active interface broadcasts:{}",
            remote.port.unwrap_or(9)
        ),
    }
}

fn remote_host<'a>(remote_options: &'a RemoteJobOptions, remote: &'a RemoteSshSource) -> &'a str {
    remote_options
        .host
        .as_deref()
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .unwrap_or_else(|| remote_dns_host(&remote.host))
}

fn wake_targets(remote: &RemoteJobOptions, dns_host: &str) -> Result<Vec<SocketAddrV4>> {
    let port = remote.port.unwrap_or(9);
    if let Some(value) = &remote.broadcast {
        let broadcast = value.parse::<Ipv4Addr>().map_err(|err| {
            TimevaultError::message(format!("remote.broadcast {}: {}", value, err))
        })?;
        return Ok(vec![SocketAddrV4::new(broadcast, port)]);
    }

    match inferred_broadcast_for_host(dns_host) {
        Ok(broadcast) => Ok(vec![SocketAddrV4::new(broadcast, port)]),
        Err(dns_err) => {
            let targets = active_interface_broadcast_targets(remote.interface.as_deref(), port)?;
            if targets.is_empty() {
                return Err(TimevaultError::message(format!(
                    "{}; no active IPv4 broadcast interfaces found",
                    dns_err
                )));
            }
            Ok(targets)
        }
    }
}

fn inferred_broadcast_for_host(host: &str) -> Result<Ipv4Addr> {
    let mut resolved = (host, 0)
        .to_socket_addrs()
        .map_err(|err| TimevaultError::message(format!("resolve wake host {}: {}", host, err)))?;
    let Some(ip) = resolved.find_map(|addr| match addr {
        std::net::SocketAddr::V4(addr) => Some(*addr.ip()),
        std::net::SocketAddr::V6(_) => None,
    }) else {
        return Err(TimevaultError::message(format!(
            "resolve wake host {}: no IPv4 address",
            host
        )));
    };
    let mut octets = ip.octets();
    octets[3] = 255;
    Ok(Ipv4Addr::from(octets))
}

fn active_interface_broadcast_targets(
    interface: Option<&str>,
    port: u16,
) -> Result<Vec<SocketAddrV4>> {
    let interface = interface.map(str::trim).filter(|value| !value.is_empty());
    let broadcasts = active_interface_broadcasts(interface)?;
    Ok(broadcasts
        .into_iter()
        .map(|broadcast| SocketAddrV4::new(broadcast, port))
        .collect())
}

#[cfg(unix)]
fn active_interface_broadcasts(interface: Option<&str>) -> Result<Vec<Ipv4Addr>> {
    let mut addrs: *mut libc::ifaddrs = std::ptr::null_mut();
    if unsafe { libc::getifaddrs(&mut addrs) } != 0 {
        return Err(TimevaultError::message(format!(
            "enumerate network interfaces: {}",
            io::Error::last_os_error()
        )));
    }

    let mut broadcasts = Vec::new();
    let mut seen = HashSet::new();
    let mut cursor = addrs;
    while !cursor.is_null() {
        let ifaddr = unsafe { &*cursor };
        if !ifaddr.ifa_addr.is_null() && !ifaddr.ifa_ifu.is_null() {
            let name = unsafe { CStr::from_ptr(ifaddr.ifa_name) }.to_string_lossy();
            let flags = ifaddr.ifa_flags as libc::c_uint;
            let is_active = flags & (libc::IFF_UP as libc::c_uint) != 0
                && flags & (libc::IFF_RUNNING as libc::c_uint) != 0
                && flags & (libc::IFF_LOOPBACK as libc::c_uint) == 0
                && flags & (libc::IFF_BROADCAST as libc::c_uint) != 0;
            let matches_interface = interface.map_or(true, |expected| expected == name);
            if is_active && matches_interface {
                let family = unsafe { (*ifaddr.ifa_addr).sa_family as libc::c_int };
                if family == libc::AF_INET {
                    let broadcast = unsafe {
                        let sockaddr = &*(ifaddr.ifa_ifu as *const libc::sockaddr_in);
                        Ipv4Addr::from(u32::from_be(sockaddr.sin_addr.s_addr))
                    };
                    if seen.insert(broadcast) {
                        broadcasts.push(broadcast);
                    }
                }
            }
        }
        cursor = ifaddr.ifa_next;
    }

    unsafe { libc::freeifaddrs(addrs) };
    Ok(broadcasts)
}

#[cfg(not(unix))]
fn active_interface_broadcasts(_interface: Option<&str>) -> Result<Vec<Ipv4Addr>> {
    Err(TimevaultError::message(
        "enumerate network interfaces: unsupported platform",
    ))
}

fn send_wake_packets(mac: &str, targets: &[SocketAddrV4]) -> Result<()> {
    let mut failures = Vec::new();
    for target in targets {
        if let Err(err) = send_wake_packet(mac, *target) {
            failures.push(format!("{}: {}", target, err));
        }
    }
    if failures.len() == targets.len() {
        return Err(TimevaultError::message(format!(
            "send wake packet failed for all targets: {}",
            failures.join("; ")
        )));
    }
    Ok(())
}

fn send_wake_packet(mac: &str, target: SocketAddrV4) -> Result<()> {
    let packet = wake_packet(mac)?;
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
    socket.set_broadcast(true)?;
    socket.send_to(&packet, target)?;
    Ok(())
}

fn wake_packet(mac: &str) -> Result<[u8; 102]> {
    let mac = parse_mac_address(mac)
        .ok_or_else(|| TimevaultError::message(format!("invalid wake MAC address {}", mac)))?;
    let mut packet = [0xff_u8; 102];
    for index in 0..16 {
        let start = 6 + index * 6;
        packet[start..start + 6].copy_from_slice(&mac);
    }
    Ok(packet)
}

fn parse_mac_address(value: &str) -> Option<[u8; 6]> {
    let mut mac = [0_u8; 6];
    let mut count = 0;
    for (index, part) in value.split(':').enumerate() {
        if index >= mac.len() || part.len() != 2 {
            return None;
        }
        mac[index] = u8::from_str_radix(part, 16).ok()?;
        count += 1;
    }
    if count == mac.len() {
        Some(mac)
    } else {
        None
    }
}

fn wait_for_ping_until(host: &str, deadline: Instant, run_mode: RunMode) -> Result<()> {
    retry_remote_readiness_until(host, "respond to ping", deadline, || {
        if ping_once(host, run_mode)? {
            Ok(())
        } else {
            Err(TimevaultError::message(format!(
                "remote host {} did not respond to ping",
                host
            )))
        }
    })
}

fn wait_for_ping_after_wake(
    host: &str,
    deadline: Instant,
    mac: &str,
    targets: &[SocketAddrV4],
    run_mode: RunMode,
) -> Result<()> {
    loop {
        if ping_once(host, run_mode)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(TimevaultError::message(format!(
                "remote host {} did not respond to ping",
                host
            )));
        }
        if run_mode.verbose {
            crate::pnote!(
                "wake host {} is not reachable yet; sending another WOL packet to {} target(s)",
                host,
                targets.len()
            );
        }
        send_wake_packets(mac, targets)?;
        let delay = deadline
            .saturating_duration_since(Instant::now())
            .min(StdDuration::from_secs(2));
        thread::sleep(delay);
    }
}

fn wait_for_ssh_until(ssh_host: &str, deadline: Instant, run_mode: RunMode) -> Result<()> {
    retry_remote_readiness_until(ssh_host, "accept SSH connections", deadline, || {
        let mut cmd = Command::new("ssh");
        cmd.arg("-o")
            .arg("BatchMode=yes")
            .arg("-o")
            .arg("ConnectTimeout=5")
            .arg(ssh_host)
            .arg("echo")
            .stdin(Stdio::null())
            .stdout(Stdio::null());
        maybe_print_command(&cmd, run_mode);
        let status = cmd.status().map_err(|err| {
            TimevaultError::message(format!("SSH readiness probe for {}: {}", ssh_host, err))
        })?;
        if status.success() {
            Ok(())
        } else {
            Err(TimevaultError::message(format!(
                "SSH readiness probe for {} exited with code {}",
                ssh_host,
                status.code().unwrap_or(1)
            )))
        }
    })
}

/// Read the remote's wall-clock time and its uptime in one round trip. Both come
/// from the same clock, so `now - uptime` is the boot instant *in the remote's
/// own time frame* — the frame its journal timestamps use — even when the remote
/// clock disagrees with ours (a dead RTC, or NTP not yet resynced after a WOL
/// cold boot). Deriving `boot_start` from our clock instead would slide the whole
/// activity window by the offset.
fn remote_now_and_uptime(ssh_host: &str, run_mode: RunMode) -> Result<(i64, u64)> {
    let mut cmd = Command::new("ssh");
    cmd.arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("ConnectTimeout=5")
        .arg(ssh_host)
        .arg("date +%s; cut -d. -f1 /proc/uptime")
        .stdin(Stdio::null());
    maybe_print_command(&cmd, run_mode);
    let output = cmd.output().map_err(|err| {
        TimevaultError::message(format!("read remote clock from {}: {}", ssh_host, err))
    })?;
    if !output.status.success() {
        return Err(TimevaultError::message(format!(
            "read remote clock from {}: ssh exited with code {}",
            ssh_host,
            output.status.code().unwrap_or(1)
        )));
    }
    parse_now_and_uptime(&String::from_utf8_lossy(&output.stdout)).ok_or_else(|| {
        TimevaultError::message(format!(
            "read remote clock from {}: unexpected `date`/`/proc/uptime` output",
            ssh_host
        ))
    })
}

fn parse_now_and_uptime(output: &str) -> Option<(i64, u64)> {
    let mut fields = output.split_whitespace();
    let now = fields.next()?.parse::<i64>().ok()?;
    let uptime = fields.next()?.split('.').next()?.parse::<u64>().ok()?;
    Some((now, uptime))
}

/// Surface a drifting backup-source clock. Always prints when the offset is past
/// the warning threshold (it distorts rsync mtimes and the cold-boot window);
/// otherwise only under `--verbose`.
fn report_clock_drift(ssh_host: &str, offset_seconds: i64, run_mode: RunMode) {
    if offset_seconds.abs() >= REMOTE_CLOCK_DRIFT_WARN_SECONDS {
        crate::pnote!(
            "warning: backup source {} clock is {} — check NTP on that host",
            ssh_host,
            describe_clock_offset(offset_seconds)
        );
    } else if run_mode.verbose {
        crate::pnote!(
            "  backup source {} clock is {}",
            ssh_host,
            describe_clock_offset(offset_seconds)
        );
    }
}

/// Human-readable summary of how far the remote clock sits from ours.
fn describe_clock_offset(offset_seconds: i64) -> String {
    if offset_seconds.abs() <= 2 {
        return "in sync with this host".to_string();
    }
    let magnitude = offset_seconds.unsigned_abs();
    let (minutes, seconds) = (magnitude / 60, magnitude % 60);
    let amount = if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}s", seconds)
    };
    let direction = if offset_seconds > 0 {
        "ahead of"
    } else {
        "behind"
    };
    format!("{} {} this host", amount, direction)
}

fn remote_activity_window(boot_start: i64) -> (i64, i64) {
    (
        boot_start - REMOTE_ACTIVITY_WINDOW_SECONDS + REMOTE_ACTIVITY_BUFFER_SECONDS,
        boot_start - REMOTE_ACTIVITY_BUFFER_SECONDS,
    )
}

fn remote_journal_has_qualifying_session(
    ssh_host: &str,
    window_start: i64,
    window_end: i64,
    boot_start: i64,
    minimum_session_seconds: u64,
    ignored_users: &[String],
    run_mode: RunMode,
) -> Result<bool> {
    let logind_journal = remote_logind_journal(ssh_host, window_start, boot_start, run_mode)?;
    let sessions = parse_logind_sessions(&logind_journal);

    // An empty result is ambiguous: nobody used the host, or the journal simply
    // has no data for that window (not persisted across reboots, freshly rotated,
    // …). Only the first should power the host off, so when it looks empty, check
    // whether the journal holds *any* record in the window before deciding.
    if sessions.is_empty()
        && !remote_journal_covers_window(ssh_host, window_start, boot_start, run_mode)?
    {
        crate::pnote!(
            "warning: the journal on {} has no records for the prior-day window (not persisted across reboots?). Backing up without the cold-boot usage check.",
            ssh_host
        );
        return Ok(true);
    }

    // The only way a person uses a headless backup source is over SSH, so a
    // session counts only if it lines up with an `Accepted` SSH login from an
    // address that is *not* Timevault's own. That also drops greeter, manager,
    // cron/`@reboot` and Timevault's own rsync sessions in one move. We need the
    // address the remote sees us on ($SSH_CONNECTION) to tell the two apart; if
    // we cannot get it, fall back to session length alone.
    let origin_client = remote_ssh_client(ssh_host, run_mode)?;
    let ssh_journal = if origin_client.is_some() {
        remote_sshd_journal(ssh_host, window_start, boot_start, run_mode)?
    } else {
        String::new()
    };

    let logins = parse_ssh_logins(&ssh_journal);
    // A login is Timevault's own when it comes from Timevault's address *as the
    // user Timevault connects as* (root). A different user from that same host is
    // a person working via the backup box, and still counts.
    let timevault_user = ssh_user_of(ssh_host);
    let (timevault_keys, interactive_keys) = match origin_client.as_deref() {
        Some(origin) => {
            let is_timevault = |login: &SshLogin| {
                login.client == origin && timevault_user.is_none_or(|user| login.user == user)
            };
            (
                correlated_session_keys(&sessions, &logins, is_timevault),
                correlated_session_keys(&sessions, &logins, |login| !is_timevault(login)),
            )
        }
        None => (HashSet::new(), HashSet::new()),
    };
    let filter = SessionFilter {
        window_start,
        window_end,
        boot_start,
        minimum_session_seconds: minimum_session_seconds as i64,
        timevault_keys: &timevault_keys,
        interactive_keys: &interactive_keys,
        ignored_users,
        require_interactive: origin_client.is_some(),
    };

    let qualifies = any_session_counts(&sessions, &filter);

    // The skip case is explained at the call site; the per-session breakdown is
    // only for `--verbose`.
    if run_mode.verbose {
        crate::pnote!(
            "  cold-boot session check: prior-window @{}..@{} (boot @{}), minimum {}s",
            window_start,
            window_end,
            boot_start,
            minimum_session_seconds
        );
        match origin_client.as_deref() {
            Some(client) => crate::pnote!(
                "    Timevault reaches {} from {}; {} SSH login(s) in window ({} by a person, {} by Timevault)",
                ssh_host,
                client,
                logins.len(),
                interactive_keys.len(),
                timevault_keys.len(),
            ),
            None => crate::pnote!(
                "    could not read $SSH_CONNECTION from {}; falling back to session length alone",
                ssh_host
            ),
        }
        if !ignored_users.is_empty() {
            crate::pnote!(
                "    ignoring sessions owned by: {}",
                ignored_users.join(", ")
            );
        }
        if sessions.is_empty() {
            crate::pnote!("    no logind sessions recorded in the window");
        }
        for session in &sessions {
            let (verdict, duration) = classify_session(session, &filter);
            crate::pnote!(
                "    session {} (boot {}) user {} for {} -> {}",
                session.id,
                short_boot_id(&session.boot_id),
                if session.user.is_empty() {
                    "?"
                } else {
                    session.user.as_str()
                },
                format_session_duration(duration),
                verdict.as_str(),
            );
        }
        crate::pnote!(
            "    -> {}",
            if qualifies {
                "a person used the host in the prior day; backup proceeds"
            } else {
                "no qualifying session; skipping backup and powering the host off"
            }
        );
    }

    Ok(qualifies)
}

fn format_session_duration(seconds: i64) -> String {
    let s = seconds.max(0);
    if s >= 3600 {
        format!("{}h {:02}m {:02}s", s / 3600, (s % 3600) / 60, s % 60)
    } else if s >= 60 {
        format!("{}m {:02}s", s / 60, s % 60)
    } else {
        format!("{}s", s)
    }
}

/// Does the remote journal hold any record inside `[window_start, boot_start]`?
/// If not, the cold-boot check has nothing to reason about (the journal is not
/// kept across reboots, or was rotated) and must not conclude the host was idle.
fn remote_journal_covers_window(
    ssh_host: &str,
    window_start: i64,
    boot_start: i64,
    run_mode: RunMode,
) -> Result<bool> {
    let output = remote_journalctl(
        ssh_host,
        &format!(
            "journalctl --quiet --no-pager -n 1 --output=cat --since @{} --until @{}",
            window_start, boot_start
        ),
        "probe remote journal coverage",
        run_mode,
    )?;
    Ok(!output.trim().is_empty())
}

/// Fetch systemd-logind's session create/remove events for the activity window
/// as JSON (one object per line). Read from the start of the window through to
/// the boot we just woke: a session that starts inside the window can only be
/// logged out (or cut short by the reboot) after `window_end`, so the removal
/// lands in the [window_end, boot_start] tail. JSON carries `_BOOT_ID`, so a
/// session id reused across reboots is not mistaken for one long session.
fn remote_logind_journal(
    ssh_host: &str,
    window_start: i64,
    boot_start: i64,
    run_mode: RunMode,
) -> Result<String> {
    remote_journalctl(
        ssh_host,
        &format!(
            "journalctl --quiet --no-pager --output=json --since @{} --until @{} MESSAGE_ID={} + MESSAGE_ID={}",
            window_start, boot_start, LOGIND_SESSION_NEW_MESSAGE_ID, LOGIND_SESSION_REMOVED_MESSAGE_ID
        ),
        "read remote system journal",
        run_mode,
    )
}

/// Fetch sshd `Accepted` login lines for the activity window as JSON, so a
/// logind session can be tied to the address (and boot) it was opened from.
fn remote_sshd_journal(
    ssh_host: &str,
    window_start: i64,
    boot_start: i64,
    run_mode: RunMode,
) -> Result<String> {
    remote_journalctl(
        ssh_host,
        &format!(
            "journalctl --quiet --no-pager --output=json --since @{} --until @{} SYSLOG_IDENTIFIER=sshd + SYSLOG_IDENTIFIER=sshd-session --grep='Accepted '",
            window_start, boot_start
        ),
        "read remote SSH journal",
        run_mode,
    )
}

fn remote_journalctl(
    ssh_host: &str,
    remote_command: &str,
    context: &str,
    run_mode: RunMode,
) -> Result<String> {
    let mut cmd = Command::new("ssh");
    cmd.arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("ConnectTimeout=5")
        .arg(ssh_host)
        .arg(remote_command)
        .stdin(Stdio::null());
    maybe_print_command(&cmd, run_mode);
    let output = cmd.output().map_err(|err| {
        TimevaultError::message(format!("{} from {}: {}", context, ssh_host, err))
    })?;
    if !output.status.success() {
        return Err(TimevaultError::message(format!(
            "{} from {}: ssh exited with code {}",
            context,
            ssh_host,
            output.status.code().unwrap_or(1)
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Ask the remote which client address it sees this SSH connection coming from
/// (the first field of `$SSH_CONNECTION`). rsync, hooks and probes all reach the
/// host the same way, so this is the address Timevault's own sessions carry.
fn remote_ssh_client(ssh_host: &str, run_mode: RunMode) -> Result<Option<String>> {
    let mut cmd = Command::new("ssh");
    cmd.arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("ConnectTimeout=5")
        .arg(ssh_host)
        .arg("printf '%s' \"${SSH_CONNECTION:-}\"")
        .stdin(Stdio::null());
    maybe_print_command(&cmd, run_mode);
    let output = cmd.output().map_err(|err| {
        TimevaultError::message(format!("read $SSH_CONNECTION from {}: {}", ssh_host, err))
    })?;
    if !output.status.success() {
        return Err(TimevaultError::message(format!(
            "read $SSH_CONNECTION from {}: ssh exited with code {}",
            ssh_host,
            output.status.code().unwrap_or(1)
        )));
    }
    Ok(parse_ssh_connection_client(&String::from_utf8_lossy(
        &output.stdout,
    )))
}

fn parse_ssh_connection_client(value: &str) -> Option<String> {
    value
        .split_whitespace()
        .next()
        .map(|client| client.to_string())
}

/// The user part of an ssh target (`root@host` -> `root`); `None` for a bare host
/// or an ssh-config alias.
fn ssh_user_of(ssh_host: &str) -> Option<&str> {
    ssh_host
        .rsplit_once('@')
        .map(|(user, _)| user)
        .filter(|user| !user.is_empty())
}

/// A `systemd-logind` session reconstructed from its create/remove journal
/// entries. `boot_id` keeps sessions from different boots apart even though
/// logind restarts session numbers on every boot.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LogindSession {
    boot_id: String,
    id: String,
    user: String,
    start: i64,
    /// `None` when no removal entry appeared in range.
    end: Option<i64>,
}

/// An `Accepted` SSH login, kept only for matching against logind sessions.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SshLogin {
    boot_id: String,
    timestamp: i64,
    user: String,
    client: String,
}

/// Stable key for a session across the two journal streams: same boot, same id.
fn session_key(boot_id: &str, id: &str) -> String {
    format!("{}#{}", boot_id, id)
}

fn short_boot_id(boot_id: &str) -> &str {
    boot_id.get(..8).unwrap_or(boot_id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionVerdict {
    /// Counts as a person using the host.
    Counted,
    /// Started in the window but did not stay open long enough.
    TooShort,
    /// Started before the 24h activity window.
    BeforeWindow,
    /// Started after `window_end`, i.e. created around this boot rather than
    /// during the prior day (linger/manager sessions, `@reboot` jobs, …).
    NearBoot,
    /// Not tied to an SSH login by a person (greeter, manager, cron, console).
    NotInteractive,
    /// Opened by Timevault's own SSH access.
    TimevaultBackup,
    /// Owned by an ignored account, e.g. a display-manager greeter.
    IgnoredUser,
}

impl SessionVerdict {
    fn as_str(self) -> &'static str {
        match self {
            SessionVerdict::Counted => "counted",
            SessionVerdict::TooShort => "too short",
            SessionVerdict::BeforeWindow => "before window",
            SessionVerdict::NearBoot => "started at/after boot",
            SessionVerdict::NotInteractive => "no SSH login by a person",
            SessionVerdict::TimevaultBackup => "ignored (Timevault backup)",
            SessionVerdict::IgnoredUser => "ignored (display-manager / listed account)",
        }
    }
}

/// The inputs the cold-boot check weighs each `systemd-logind` session against.
struct SessionFilter<'a> {
    window_start: i64,
    window_end: i64,
    boot_start: i64,
    minimum_session_seconds: i64,
    /// Session keys opened by Timevault's own SSH activity.
    timevault_keys: &'a HashSet<String>,
    /// Session keys tied to an `Accepted` SSH login from a non-Timevault address.
    interactive_keys: &'a HashSet<String>,
    /// Session owners that never count as use (display-manager greeters, …).
    ignored_users: &'a [String],
    /// Require a session to be in `interactive_keys` to count. False only when
    /// Timevault could not learn its own client address and falls back to
    /// session length alone.
    require_interactive: bool,
}

impl SessionFilter<'_> {
    fn user_is_ignored(&self, user: &str) -> bool {
        self.ignored_users.iter().any(|ignored| ignored == user)
    }
}

fn token_after<'a>(tokens: &[&'a str], key: &str) -> Option<&'a str> {
    tokens
        .iter()
        .position(|token| *token == key)
        .and_then(|idx| tokens.get(idx + 1))
        .map(|token| token.trim_end_matches('.'))
}

fn json_str<'a>(entry: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    entry.get(key).and_then(serde_json::Value::as_str)
}

/// `__REALTIME_TIMESTAMP` (microseconds since the epoch, as a string) in whole
/// seconds. This is the remote's own clock, matching `boot_start`.
fn journal_realtime_seconds(entry: &serde_json::Value) -> Option<i64> {
    json_str(entry, "__REALTIME_TIMESTAMP")?
        .parse::<i64>()
        .ok()
        .map(|micros| micros / 1_000_000)
}

/// Parse `journalctl -o json` lines for logind's session create/remove events.
fn parse_logind_sessions(journal: &str) -> Vec<LogindSession> {
    // key -> (start, user, boot_id, id)
    let mut starts: HashMap<String, (i64, String, String, String)> = HashMap::new();
    let mut ends: HashMap<String, i64> = HashMap::new();

    for line in journal.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let (Some(message_id), Some(ts), Some(id)) = (
            json_str(&entry, "MESSAGE_ID"),
            journal_realtime_seconds(&entry),
            json_str(&entry, "SESSION_ID"),
        ) else {
            continue;
        };
        let boot_id = json_str(&entry, "_BOOT_ID").unwrap_or("");
        let key = session_key(boot_id, id);
        if message_id == LOGIND_SESSION_NEW_MESSAGE_ID {
            let user = json_str(&entry, "USER_ID").unwrap_or("").to_string();
            starts
                .entry(key)
                .and_modify(|(first, ..)| *first = (*first).min(ts))
                .or_insert((ts, user, boot_id.to_string(), id.to_string()));
        } else if message_id == LOGIND_SESSION_REMOVED_MESSAGE_ID {
            ends.entry(key)
                .and_modify(|last| *last = (*last).max(ts))
                .or_insert(ts);
        }
    }

    let mut sessions: Vec<LogindSession> = starts
        .into_iter()
        .map(|(key, (start, user, boot_id, id))| LogindSession {
            boot_id,
            id,
            user,
            start,
            end: ends.get(&key).copied(),
        })
        .collect();
    sessions.sort_by(|a, b| {
        a.start
            .cmp(&b.start)
            .then_with(|| a.boot_id.cmp(&b.boot_id))
            .then_with(|| a.id.cmp(&b.id))
    });
    sessions
}

/// Parse `journalctl -o json` lines for sshd `Accepted` logins.
fn parse_ssh_logins(journal: &str) -> Vec<SshLogin> {
    let mut logins = Vec::new();
    for line in journal.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let (Some(timestamp), Some(message)) = (
            journal_realtime_seconds(&entry),
            json_str(&entry, "MESSAGE"),
        ) else {
            continue;
        };
        let tokens: Vec<&str> = message.split_whitespace().collect();
        let Some(accepted) = tokens.iter().position(|token| *token == "Accepted") else {
            continue;
        };
        // "Accepted publickey for <user> from <client> port <n> ssh2: ..."
        let tail = &tokens[accepted..];
        let (Some(user), Some(client)) = (token_after(tail, "for"), token_after(tail, "from"))
        else {
            continue;
        };
        logins.push(SshLogin {
            boot_id: json_str(&entry, "_BOOT_ID").unwrap_or("").to_string(),
            timestamp,
            user: user.to_string(),
            client: client.to_string(),
        });
    }
    logins
}

fn login_matches_session(login: &SshLogin, session: &LogindSession) -> bool {
    login.boot_id == session.boot_id
        && login.user == session.user
        && (login.timestamp - session.start).abs() <= SESSION_CORRELATION_SECONDS
}

/// Keys of the sessions that line up with an `Accepted` SSH login `want` selects
/// (by client address), in the same boot and for the same user.
fn correlated_session_keys(
    sessions: &[LogindSession],
    logins: &[SshLogin],
    want: impl Fn(&SshLogin) -> bool,
) -> HashSet<String> {
    sessions
        .iter()
        .filter(|session| {
            logins
                .iter()
                .any(|login| want(login) && login_matches_session(login, session))
        })
        .map(|session| session_key(&session.boot_id, &session.id))
        .collect()
}

/// Classify a single session and report the length credited to it. An unclosed
/// session is measured up to `boot_start` (nothing outlives the reboot that woke
/// us).
fn classify_session(session: &LogindSession, filter: &SessionFilter) -> (SessionVerdict, i64) {
    let key = session_key(&session.boot_id, &session.id);
    let end = session
        .end
        .unwrap_or(filter.boot_start)
        .min(filter.boot_start);
    let duration = end - session.start;
    let verdict = if filter.timevault_keys.contains(&key) {
        SessionVerdict::TimevaultBackup
    } else if filter.user_is_ignored(&session.user) {
        SessionVerdict::IgnoredUser
    } else if session.start < filter.window_start {
        SessionVerdict::BeforeWindow
    } else if session.start > filter.window_end {
        SessionVerdict::NearBoot
    } else if filter.require_interactive && !filter.interactive_keys.contains(&key) {
        SessionVerdict::NotInteractive
    } else if duration < filter.minimum_session_seconds {
        SessionVerdict::TooShort
    } else {
        SessionVerdict::Counted
    };
    (verdict, duration)
}

fn any_session_counts(sessions: &[LogindSession], filter: &SessionFilter) -> bool {
    sessions
        .iter()
        .any(|session| classify_session(session, filter).0 == SessionVerdict::Counted)
}

fn retry_remote_readiness_until<T, F>(
    host: &str,
    probe: &str,
    deadline: Instant,
    mut action: F,
) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    loop {
        match action() {
            Ok(value) => return Ok(value),
            Err(err) if Instant::now() >= deadline => return Err(err),
            Err(_) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let delay = remaining.min(StdDuration::from_secs(2));
                crate::pnote!(
                    "host {} did not {} yet; retrying in {}s",
                    host,
                    probe,
                    delay.as_secs()
                );
                thread::sleep(delay);
            }
        }
    }
}

fn ping_once(host: &str, run_mode: RunMode) -> Result<bool> {
    ping_once_with_timeout(host, PING_ATTEMPT_TIMEOUT, run_mode)
}

fn ping_once_with_timeout(host: &str, timeout: StdDuration, run_mode: RunMode) -> Result<bool> {
    let deadline = Instant::now() + timeout;
    let addresses = resolve_host_ipv4_with_timeout(host, timeout, run_mode)?;
    if addresses.is_empty() {
        if run_mode.verbose {
            crate::pnote!("wake host {} did not resolve to an IPv4 address", host);
        }
        return Ok(false);
    }
    for address in addresses {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        if run_mode.verbose {
            crate::pnote!("checking wake host {} at {}", host, address);
        }
        if ping_ipv4_once_with_timeout(address, remaining)? {
            return Ok(true);
        }
        if run_mode.verbose {
            crate::pnote!(
                "wake host {} resolved to {}, but ping failed",
                host,
                address
            );
        }
    }
    Ok(false)
}

fn resolve_host_ipv4_with_timeout(
    host: &str,
    timeout: StdDuration,
    run_mode: RunMode,
) -> Result<Vec<Ipv4Addr>> {
    if let Ok(address) = host.parse::<Ipv4Addr>() {
        if run_mode.verbose {
            crate::pnote!("wake host {} is already an IPv4 address", host);
        }
        return Ok(vec![address]);
    }

    if run_mode.verbose {
        crate::pnote!(
            "resolving wake host {} with a {} second timeout",
            host,
            timeout.as_secs()
        );
    }
    let mut child = Command::new("getent")
        .arg("ahostsv4")
        .arg(host)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| TimevaultError::message(format!("resolve {}: {}", host, err)))?;

    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            if !status.success() {
                return Err(TimevaultError::message(format!(
                    "wake host {} DNS lookup failed with exit code {}",
                    host,
                    status.code().unwrap_or(1)
                )));
            }
            let mut output = String::new();
            if let Some(mut stdout) = child.stdout.take() {
                stdout.read_to_string(&mut output)?;
            }
            let addresses = parse_getent_ahostsv4(&output);
            if addresses.is_empty() {
                return Err(TimevaultError::message(format!(
                    "wake host {} DNS lookup returned no IPv4 addresses",
                    host
                )));
            }
            if run_mode.verbose {
                crate::pnote!(
                    "wake host {} resolved to {}",
                    host,
                    addresses
                        .iter()
                        .map(Ipv4Addr::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            return Ok(addresses);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(TimevaultError::message(format!(
                "wake host {} DNS lookup timed out after {} seconds",
                host,
                timeout.as_secs()
            )));
        }
        thread::sleep(StdDuration::from_millis(50));
    }
}

fn parse_getent_ahostsv4(output: &str) -> Vec<Ipv4Addr> {
    let mut addresses = Vec::new();
    let mut seen = HashSet::new();
    for line in output.lines() {
        let Some(first) = line.split_whitespace().next() else {
            continue;
        };
        let Ok(address) = first.parse::<Ipv4Addr>() else {
            continue;
        };
        if seen.insert(address) {
            addresses.push(address);
        }
    }
    addresses
}

fn ping_ipv4_once_with_timeout(address: Ipv4Addr, timeout: StdDuration) -> Result<bool> {
    let mut child = Command::new("ping")
        .arg("-c")
        .arg("1")
        .arg("-W")
        .arg("1")
        .arg(address.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| TimevaultError::message(format!("ping {}: {}", address, err)))?;

    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status.success());
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(false);
        }
        thread::sleep(StdDuration::from_millis(50));
    }
}

fn remote_dns_host(ssh_host: &str) -> &str {
    if let Some((_, host)) = ssh_host.rsplit_once('@') {
        host
    } else {
        ssh_host
    }
}

fn job_script_path(job_name: &str, phase: JobScriptPhase) -> Option<PathBuf> {
    let path = Path::new(SCRIPT_DIR).join(format!("{}.{}", job_name, phase.as_str()));
    match fs::metadata(&path) {
        Ok(meta) if meta.is_file() => Some(path),
        Ok(_) => None,
        Err(err) if err.kind() == io::ErrorKind::NotFound => None,
        Err(_) => None,
    }
}

fn remote_job_script_path(job_name: &str, phase: JobScriptPhase) -> String {
    format!("{}/{}.{}", SCRIPT_DIR, job_name, phase.as_str())
}

fn remote_script_command(
    job: &Job,
    remote_source_path: &str,
    script: &str,
    phase: JobScriptPhase,
    destination: &Path,
    backup_day: &str,
    rsync_code: Option<i32>,
) -> String {
    let mut assignments = vec![
        env_assignment("TIMEVAULT_JOB_NAME", &job.name),
        env_assignment("TIMEVAULT_JOB_SOURCE", &job.source),
        env_assignment("TIMEVAULT_JOB_REMOTE_SOURCE", remote_source_path),
        env_assignment(
            "TIMEVAULT_JOB_DESTINATION",
            &destination.display().to_string(),
        ),
        env_assignment("TIMEVAULT_BACKUP_DAY", backup_day),
        env_assignment("TIMEVAULT_SCRIPT_PHASE", phase.as_str()),
    ];
    if let Some(code) = rsync_code {
        assignments.push(env_assignment("TIMEVAULT_RSYNC_CODE", &code.to_string()));
    }
    format!(
        "if [ -f {script} ]; then {env} /bin/sh {script}; fi",
        script = shell_quote(script),
        env = assignments.join(" ")
    )
}

fn env_assignment(name: &str, value: &str) -> String {
    format!("{}={}", name, shell_quote(value))
}

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[derive(Debug, Default)]
struct PristineExcludes {
    local: Option<Vec<String>>,
    remote: HashMap<String, Vec<String>>,
}

fn ensure_pristine_excludes_for_job(
    job: &Job,
    excludes: &mut PristineExcludes,
    options: BackupOptions,
    verbose: bool,
    dry_run: bool,
) -> Result<()> {
    if !options.exclude_pristine {
        return Ok(());
    }
    if dry_run {
        if verbose {
            crate::pnote!("pristine: dry-run; skip package analysis");
        }
        return Ok(());
    }
    match pristine_source_for_job(job) {
        Some(PristineSource::Local) if excludes.local.is_none() => {
            excludes.local = Some(build_pristine_excludes_for_source(
                &PristineSource::Local,
                verbose,
            )?);
        }
        Some(PristineSource::RemoteSsh { host }) if !excludes.remote.contains_key(&host) => {
            let source = PristineSource::RemoteSsh { host: host.clone() };
            let host_excludes = build_pristine_excludes_for_source(&source, verbose)?;
            excludes.remote.insert(host, host_excludes);
        }
        None if verbose => {
            crate::pnote!(
                "pristine: skip package analysis; job {} source is not supported for pristine analysis",
                job.name
            );
        }
        _ => {}
    }
    Ok(())
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
    dry_run: bool,
) -> Result<PristineExcludes> {
    if !options.exclude_pristine {
        return Ok(PristineExcludes::default());
    }
    if dry_run {
        if verbose {
            crate::pnote!("pristine: dry-run; skip package analysis");
        }
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
        crate::pnote!(
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteSshSource {
    host: String,
    source_path: String,
}

fn remote_ssh_host_from_source(source: &str) -> Option<String> {
    remote_ssh_source(source).map(|remote| remote.host)
}

fn remote_ssh_source(source: &str) -> Option<RemoteSshSource> {
    let source = source.trim();
    if source.starts_with('/') || source.starts_with("rsync://") {
        return None;
    }
    let (host, path) = source.split_once(':')?;
    if host.is_empty() || !path.starts_with('/') {
        return None;
    }
    Some(RemoteSshSource {
        host: host.to_string(),
        source_path: path.to_string(),
    })
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
            crate::pnote!("skip symlink delete: {}", target.display());
            continue;
        }
        if meta.is_dir() {
            if run_mode.safe_mode || run_mode.dry_run {
                if run_mode.dry_run {
                    crate::pnote!("dry-run: rm -rf {}", target.display());
                } else {
                    crate::pnote!("skip delete (safe-mode): {}", target.display());
                }
            } else {
                crate::pnote!("delete: {}", target.display());
                fs::remove_dir_all(&target)?;
            }
        } else {
            crate::pnote!("skip non-dir delete: {}", target.display());
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
                crate::pnote!("dry-run: skip symlink {}", src_path.display());
            }
            continue;
        }
        if ft.is_dir() {
            if run_mode.dry_run {
                crate::pnote!("dry-run: mkdir -p {}", target.display());
            } else {
                fs::create_dir_all(&target)?;
            }
            continue;
        }
        if ft.is_file() {
            if run_mode.dry_run {
                crate::pnote!("dry-run: ln {} {}", src_path.display(), target.display());
            } else {
                hard_link_if_missing(src_path, &target)?;
            }
        }
    }
    Ok(())
}

fn hard_link_if_missing(source: &Path, target: &Path) -> io::Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }

    match fs::symlink_metadata(target) {
        Ok(_) => return Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }

    match fs::hard_link(source, target) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(err) => Err(err),
    }
}

/// Per-job rsync excludes file. Distinct per job so parallel `timevault` runs for
/// different jobs never clobber each other's list (the per-job lock keeps a
/// single writer per job). Job names are `is_safe_name`-restricted, so safe here.
fn job_excludes_path(tmp_dir: &Path, job_name: &str) -> PathBuf {
    tmp_dir.join(format!("timevault.{}.excludes", job_name))
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
    use std::os::unix::fs::MetadataExt;

    fn job(source: &str) -> Job {
        Job {
            name: "test".to_string(),
            description: None,
            source: source.to_string(),
            copies: 1,
            run_policy: RunPolicy::Auto,
            excludes: vec!["/tmp".to_string()],
            disk_ids: None,
            remote: None,
        }
    }

    fn run_mode() -> RunMode {
        RunMode {
            dry_run: false,
            safe_mode: false,
            verbose: false,
        }
    }

    fn same_inode(left: &Path, right: &Path) -> bool {
        let left = fs::metadata(left).expect("left metadata");
        let right = fs::metadata(right).expect("right metadata");
        left.dev() == right.dev() && left.ino() == right.ino()
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
    fn parses_remote_rsync_source_path() {
        assert_eq!(
            remote_ssh_source("root@example.com:/srv/data"),
            Some(RemoteSshSource {
                host: "root@example.com".to_string(),
                source_path: "/srv/data".to_string(),
            })
        );
    }

    #[test]
    fn remote_script_command_exports_environment() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let command = remote_script_command(
            &job("root@example.com:/srv/data"),
            "/srv/data",
            "/etc/timevault/scripts/test.post",
            JobScriptPhase::Post,
            tmp.path(),
            "20260101",
            Some(24),
        );

        assert!(command.contains("if [ -f '/etc/timevault/scripts/test.post' ]; then"));
        assert!(command.contains("TIMEVAULT_JOB_NAME='test'"));
        assert!(command.contains("TIMEVAULT_JOB_SOURCE='root@example.com:/srv/data'"));
        assert!(command.contains("TIMEVAULT_JOB_REMOTE_SOURCE='/srv/data'"));
        assert!(command.contains("TIMEVAULT_SCRIPT_PHASE='post'"));
        assert!(command.contains("TIMEVAULT_RSYNC_CODE='24'"));
        assert!(command.contains("/bin/sh '/etc/timevault/scripts/test.post'"));
    }

    #[test]
    fn shell_quote_handles_single_quotes() {
        assert_eq!(shell_quote("can't"), "'can'\"'\"'t'");
    }

    #[test]
    fn wake_packet_contains_magic_header_and_repeated_mac() {
        let packet = wake_packet("aa:bb:cc:dd:ee:ff").expect("packet");

        assert_eq!(&packet[0..6], &[0xff; 6]);
        assert_eq!(&packet[6..12], &[0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]);
        assert_eq!(&packet[96..102], &[0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]);
    }

    #[test]
    fn inferred_broadcast_uses_resolved_ipv4_subnet() {
        assert_eq!(
            inferred_broadcast_for_host("127.0.0.1").expect("broadcast"),
            Ipv4Addr::new(127, 0, 0, 255)
        );
    }

    #[test]
    fn wake_targets_uses_explicit_configured_broadcast() {
        let remote = RemoteJobOptions {
            wol: Some(true),
            mac: Some("aa:bb:cc:dd:ee:ff".to_string()),
            broadcast: Some("192.0.2.255".to_string()),
            port: Some(7),
            ..RemoteJobOptions::default()
        };

        assert_eq!(
            wake_targets(&remote, "does-not-need-dns").expect("targets"),
            vec![SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 255), 7)]
        );
    }

    #[test]
    fn parse_getent_ahostsv4_deduplicates_addresses() {
        let output = "\
192.0.2.10 STREAM example.com
192.0.2.10 DGRAM
192.0.2.11 STREAM example.com
";

        assert_eq!(
            parse_getent_ahostsv4(output),
            vec![Ipv4Addr::new(192, 0, 2, 10), Ipv4Addr::new(192, 0, 2, 11)]
        );
    }

    #[test]
    fn remote_dns_host_strips_ssh_user() {
        assert_eq!(remote_dns_host("root@spitfire"), "spitfire");
        assert_eq!(remote_dns_host("spitfire"), "spitfire");
    }

    #[test]
    fn wake_host_prefers_explicit_config() {
        let remote = RemoteSshSource {
            host: "root@ssh-alias".to_string(),
            source_path: "/".to_string(),
        };
        let options = RemoteJobOptions {
            mac: Some("aa:bb:cc:dd:ee:ff".to_string()),
            host: Some("actual-host".to_string()),
            ..RemoteJobOptions::default()
        };

        assert_eq!(remote_host(&options, &remote), "actual-host");
    }

    #[test]
    fn probe_timeout_defaults_and_can_be_overridden() {
        let mut remote_job = job("root@example.com:/srv/data");
        remote_job.remote = Some(crate::config::model::RemoteJobOptions {
            wol: Some(false),
            ..RemoteJobOptions::default()
        });
        assert_eq!(
            remote_context(&remote_job)
                .expect("context")
                .expect("remote")
                .probe_timeout,
            StdDuration::from_secs(DEFAULT_REMOTE_PROBE_TIMEOUT_SECONDS)
        );
        remote_job.remote.as_mut().unwrap().probe_timeout_seconds = Some(30);
        assert_eq!(
            remote_context(&remote_job)
                .expect("context")
                .expect("remote")
                .probe_timeout,
            StdDuration::from_secs(30)
        );
    }

    #[test]
    fn minimum_uptime_defaults_and_can_be_overridden() {
        let mut remote_job = job("root@example.com:/srv/data");
        remote_job.remote = Some(RemoteJobOptions {
            wol: Some(true),
            mac: Some("aa:bb:cc:dd:ee:ff".to_string()),
            broadcast: Some("192.0.2.255".to_string()),
            ..RemoteJobOptions::default()
        });
        assert_eq!(DEFAULT_REMOTE_MINIMUM_UPTIME_SECONDS, 600);
        assert_eq!(
            remote_context(&remote_job)
                .expect("context")
                .expect("remote")
                .minimum_uptime_seconds,
            DEFAULT_REMOTE_MINIMUM_UPTIME_SECONDS
        );
        remote_job.remote.as_mut().unwrap().minimum_uptime_seconds = Some(600);
        assert_eq!(
            remote_context(&remote_job)
                .expect("context")
                .expect("remote")
                .minimum_uptime_seconds,
            600
        );
    }

    #[test]
    fn minimum_session_defaults_and_can_be_overridden() {
        let mut remote_job = job("root@example.com:/srv/data");
        remote_job.remote = Some(RemoteJobOptions {
            wol: Some(true),
            mac: Some("aa:bb:cc:dd:ee:ff".to_string()),
            broadcast: Some("192.0.2.255".to_string()),
            ..RemoteJobOptions::default()
        });
        assert_eq!(DEFAULT_REMOTE_MINIMUM_SESSION_SECONDS, 300);
        assert_eq!(
            remote_context(&remote_job)
                .expect("context")
                .expect("remote")
                .minimum_session_seconds,
            DEFAULT_REMOTE_MINIMUM_SESSION_SECONDS
        );
        remote_job.remote.as_mut().unwrap().minimum_session_seconds = Some(120);
        assert_eq!(
            remote_context(&remote_job)
                .expect("context")
                .expect("remote")
                .minimum_session_seconds,
            120
        );
    }

    #[test]
    fn parses_remote_now_and_uptime() {
        assert_eq!(
            parse_now_and_uptime("1788781182\n5704.19\n"),
            Some((1788781182, 5704))
        );
        assert_eq!(
            parse_now_and_uptime("1788781182 5704.19"),
            Some((1788781182, 5704))
        );
        assert_eq!(parse_now_and_uptime("1788781182"), None);
        assert_eq!(parse_now_and_uptime("not a number"), None);
    }

    #[test]
    fn describes_clock_offset() {
        assert_eq!(describe_clock_offset(0), "in sync with this host");
        assert_eq!(describe_clock_offset(-2), "in sync with this host");
        assert_eq!(describe_clock_offset(37), "37s ahead of this host");
        assert_eq!(describe_clock_offset(-5704), "95m 4s behind this host");
    }

    // --- session-check helpers -------------------------------------------------

    /// One `journalctl -o json` line for a logind session create/remove event.
    fn logind_line(new: bool, boot: &str, id: &str, user: &str, secs: i64) -> String {
        let message_id = if new {
            LOGIND_SESSION_NEW_MESSAGE_ID
        } else {
            LOGIND_SESSION_REMOVED_MESSAGE_ID
        };
        format!(
            r#"{{"MESSAGE_ID":"{message_id}","_BOOT_ID":"{boot}","SESSION_ID":"{id}","USER_ID":"{user}","__REALTIME_TIMESTAMP":"{}"}}"#,
            secs * 1_000_000
        )
    }

    /// One `journalctl -o json` line for an sshd `Accepted` login.
    fn sshd_line(boot: &str, user: &str, client: &str, secs: i64) -> String {
        format!(
            r#"{{"_BOOT_ID":"{boot}","__REALTIME_TIMESTAMP":"{}","MESSAGE":"Accepted publickey for {user} from {client} port 46614 ssh2: ED25519 SHA256:abc"}}"#,
            secs * 1_000_000
        )
    }

    /// Prior window [1000, 2000], boot at 5000, threshold 300s.
    fn test_filter<'a>(
        timevault_keys: &'a HashSet<String>,
        interactive_keys: &'a HashSet<String>,
        ignored_users: &'a [String],
        require_interactive: bool,
    ) -> SessionFilter<'a> {
        SessionFilter {
            window_start: 1000,
            window_end: 2000,
            boot_start: 5000,
            minimum_session_seconds: 300,
            timevault_keys,
            interactive_keys,
            ignored_users,
            require_interactive,
        }
    }

    #[test]
    fn boot_time_session_from_a_skewed_clock_does_not_count() {
        // spitfire booted at (remote-clock) @1788775478 and its clock is slow, so
        // a session logind opens at boot lands "before" the boot in the journal.
        // With the window derived from the remote's own clock, boot_start is
        // @1788775478 and window_end is 600s earlier, so the session is NearBoot.
        let boot_start = 1788775478;
        let (window_start, window_end) = remote_activity_window(boot_start);
        let session = LogindSession {
            boot_id: "b0".to_string(),
            id: "18".to_string(),
            user: "jallen".to_string(),
            start: boot_start + 1,
            end: None,
        };
        let empty = HashSet::new();
        let filter = SessionFilter {
            window_start,
            window_end,
            boot_start,
            minimum_session_seconds: 300,
            timevault_keys: &empty,
            interactive_keys: &empty,
            ignored_users: &[],
            require_interactive: true,
        };
        assert_eq!(
            classify_session(&session, &filter).0,
            SessionVerdict::NearBoot
        );
        assert!(!any_session_counts(std::slice::from_ref(&session), &filter));
    }

    #[test]
    fn parses_ssh_connection_client_takes_first_field() {
        assert_eq!(
            parse_ssh_connection_client("10.0.0.5 51234 10.0.0.9 22\n").as_deref(),
            Some("10.0.0.5")
        );
        assert_eq!(
            parse_ssh_connection_client("2001:db8::5 40100 2001:db8::9 22").as_deref(),
            Some("2001:db8::5")
        );
        assert_eq!(parse_ssh_connection_client("   "), None);
    }

    #[test]
    fn ssh_user_of_extracts_login_name() {
        assert_eq!(ssh_user_of("root@spitfire"), Some("root"));
        assert_eq!(ssh_user_of("backup@host.example.com"), Some("backup"));
        assert_eq!(ssh_user_of("spitfire"), None);
        assert_eq!(ssh_user_of("ssh-alias"), None);
        assert_eq!(ssh_user_of("@host"), None);
    }

    #[test]
    fn a_person_from_the_timevault_host_still_counts() {
        // Timevault connects as root@10.0.0.1; an admin on that same box SSHes to
        // the source as themselves and works for an hour -> that is real use.
        let journal = [
            logind_line(true, "b", "2", "root", 1100), // Timevault's rsync
            logind_line(false, "b", "2", "root", 4800),
            logind_line(true, "b", "5", "jallen", 1500), // admin, from 10.0.0.1
            logind_line(false, "b", "5", "jallen", 4800),
        ]
        .join("\n");
        let sshd = [
            sshd_line("b", "root", "10.0.0.1", 1099),
            sshd_line("b", "jallen", "10.0.0.1", 1501),
        ]
        .join("\n");
        let sessions = parse_logind_sessions(&journal);
        let logins = parse_ssh_logins(&sshd);
        let origin = "10.0.0.1";
        let tv_user = Some("root");
        let is_tv = |l: &SshLogin| l.client == origin && tv_user.is_none_or(|u| l.user == u);
        let tv = correlated_session_keys(&sessions, &logins, is_tv);
        let interactive = correlated_session_keys(&sessions, &logins, |l| !is_tv(l));
        let filter = test_filter(&tv, &interactive, &[], true);
        assert_eq!(
            classify_session(sessions.iter().find(|s| s.id == "2").unwrap(), &filter).0,
            SessionVerdict::TimevaultBackup
        );
        assert_eq!(
            classify_session(sessions.iter().find(|s| s.id == "5").unwrap(), &filter).0,
            SessionVerdict::Counted
        );
    }

    #[test]
    fn parses_logind_sessions_from_json_with_boot_user_and_open_end() {
        let journal = [
            logind_line(true, "bootA", "5", "alex", 1100),
            logind_line(false, "bootA", "5", "alex", 1180),
            logind_line(true, "bootA", "6", "root", 1300),
        ]
        .join("\n");
        assert_eq!(
            parse_logind_sessions(&journal),
            vec![
                LogindSession {
                    boot_id: "bootA".to_string(),
                    id: "5".to_string(),
                    user: "alex".to_string(),
                    start: 1100,
                    end: Some(1180),
                },
                LogindSession {
                    boot_id: "bootA".to_string(),
                    id: "6".to_string(),
                    user: "root".to_string(),
                    start: 1300,
                    end: None,
                },
            ]
        );
    }

    #[test]
    fn reused_session_ids_across_boots_are_not_merged() {
        // The spitfire bug: `New session 18` on one boot, `Removed session 18` on
        // the next. Keyed by (boot, id) they stay two ~1s sessions, not one 5704s.
        let journal = [
            logind_line(true, "bootA", "18", "jallen", 1788775478),
            logind_line(false, "bootA", "18", "jallen", 1788775479),
            logind_line(true, "bootB", "18", "jallen", 1788781182),
            logind_line(false, "bootB", "18", "jallen", 1788781182),
        ]
        .join("\n");
        let sessions = parse_logind_sessions(&journal);
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].end, Some(1788775479));
        assert_eq!(sessions[0].start, 1788775478);
        assert_eq!(sessions[1].boot_id, "bootB");
        assert!(sessions.iter().all(|s| s.end.unwrap() - s.start <= 1));
    }

    #[test]
    fn parses_ssh_logins_from_json() {
        let journal = [
            sshd_line("bootA", "root", "10.0.0.5", 1200),
            r#"{"_BOOT_ID":"bootA","__REALTIME_TIMESTAMP":"1500000000","MESSAGE":"Connection from 10.0.0.5 port 46620"}"#.to_string(),
        ]
        .join("\n");
        assert_eq!(
            parse_ssh_logins(&journal),
            vec![SshLogin {
                boot_id: "bootA".to_string(),
                timestamp: 1200,
                user: "root".to_string(),
                client: "10.0.0.5".to_string(),
            }]
        );
    }

    #[test]
    fn correlation_needs_same_boot_user_and_time() {
        let sessions = vec![
            LogindSession {
                boot_id: "bootA".to_string(),
                id: "2".to_string(),
                user: "root".to_string(),
                start: 1201,
                end: Some(4000),
            },
            // same id/user/time but a different boot -> must not correlate.
            LogindSession {
                boot_id: "bootB".to_string(),
                id: "2".to_string(),
                user: "root".to_string(),
                start: 1201,
                end: Some(4000),
            },
        ];
        let logins = vec![sshd_line_login("bootA", "root", "10.0.0.5", 1200)];
        let matched = correlated_session_keys(&sessions, &logins, |l| l.client == "10.0.0.5");
        assert!(matched.contains(&session_key("bootA", "2")));
        assert!(!matched.contains(&session_key("bootB", "2")));
    }

    fn sshd_line_login(boot: &str, user: &str, client: &str, secs: i64) -> SshLogin {
        parse_ssh_logins(&sshd_line(boot, user, client, secs))
            .pop()
            .expect("one login")
    }

    #[test]
    fn only_a_non_timevault_ssh_login_makes_a_session_count() {
        // Three ~1h sessions in the window, all long enough on duration alone.
        let journal = [
            logind_line(true, "b", "2", "jallen", 1100), // Timevault's rsync
            logind_line(false, "b", "2", "jallen", 4800),
            logind_line(true, "b", "3", "jallen", 1500), // a person over SSH
            logind_line(false, "b", "3", "jallen", 4800),
            logind_line(true, "b", "4", "root", 1700), // a cron @reboot job
            logind_line(false, "b", "4", "root", 4800),
        ]
        .join("\n");
        let sshd = [
            sshd_line("b", "jallen", "10.0.0.9", 1099), // Timevault's address
            sshd_line("b", "jallen", "192.0.2.50", 1501), // a person's laptop
        ]
        .join("\n");
        let sessions = parse_logind_sessions(&journal);
        let logins = parse_ssh_logins(&sshd);
        let origin = "10.0.0.9";
        let tv = correlated_session_keys(&sessions, &logins, |l| l.client == origin);
        let interactive = correlated_session_keys(&sessions, &logins, |l| l.client != origin);
        let filter = test_filter(&tv, &interactive, &[], true);

        let verdict =
            |id: &str| classify_session(sessions.iter().find(|s| s.id == id).unwrap(), &filter).0;
        assert_eq!(verdict("2"), SessionVerdict::TimevaultBackup);
        assert_eq!(verdict("3"), SessionVerdict::Counted);
        assert_eq!(verdict("4"), SessionVerdict::NotInteractive);
        assert!(any_session_counts(&sessions, &filter));
    }

    #[test]
    fn timevault_backup_session_does_not_keep_a_cold_host_in_service() {
        // Only activity was Timevault's own hour-long rsync as jallen.
        let journal = [
            logind_line(true, "b", "2", "jallen", 1200),
            logind_line(false, "b", "2", "jallen", 4800),
        ]
        .join("\n");
        let sshd = sshd_line("b", "jallen", "10.0.0.9", 1199);
        let sessions = parse_logind_sessions(&journal);
        let logins = parse_ssh_logins(&sshd);
        let tv = correlated_session_keys(&sessions, &logins, |l| l.client == "10.0.0.9");
        let interactive = correlated_session_keys(&sessions, &logins, |l| l.client != "10.0.0.9");
        assert!(tv.contains(&session_key("b", "2")));
        let filter = test_filter(&tv, &interactive, &[], true);
        assert_eq!(
            classify_session(&sessions[0], &filter).0,
            SessionVerdict::TimevaultBackup
        );
        assert!(!any_session_counts(&sessions, &filter));
    }

    #[test]
    fn display_manager_greeter_does_not_count_as_use() {
        let journal = logind_line(true, "b", "c1", "Debian-gdm", 1100);
        let sessions = parse_logind_sessions(&journal);
        let empty = HashSet::new();
        let defaults: Vec<String> = DEFAULT_IGNORED_SESSION_USERS
            .iter()
            .map(|user| user.to_string())
            .collect();
        // Ignored by name...
        assert_eq!(
            classify_session(&sessions[0], &test_filter(&empty, &empty, &defaults, true)).0,
            SessionVerdict::IgnoredUser
        );
        // ...and even off the list it has no SSH login, so it still cannot count.
        assert_eq!(
            classify_session(&sessions[0], &test_filter(&empty, &empty, &[], true)).0,
            SessionVerdict::NotInteractive
        );
    }

    #[test]
    fn falls_back_to_session_length_when_origin_unknown() {
        // $SSH_CONNECTION unreadable -> require_interactive = false, so a long
        // in-window session counts on duration alone (old behaviour).
        let journal = [
            logind_line(true, "b", "7", "alex", 1200),
            logind_line(false, "b", "7", "alex", 1900),
        ]
        .join("\n");
        let sessions = parse_logind_sessions(&journal);
        let empty = HashSet::new();
        assert!(any_session_counts(
            &sessions,
            &test_filter(&empty, &empty, &[], false)
        ));
        // still respects the minimum
        let short = [
            logind_line(true, "b", "8", "alex", 1200),
            logind_line(false, "b", "8", "alex", 1300),
        ]
        .join("\n");
        assert!(!any_session_counts(
            &parse_logind_sessions(&short),
            &test_filter(&empty, &empty, &[], false)
        ));
    }

    #[test]
    fn unset_after_backup_returns_host_to_found_state() {
        assert_eq!(
            resolve_after_backup(None, FoundPowerState::Running),
            PowerAction::Leave
        );
        assert_eq!(
            resolve_after_backup(None, FoundPowerState::Suspended),
            PowerAction::Suspend
        );
        assert_eq!(
            resolve_after_backup(None, FoundPowerState::PoweredOff),
            PowerAction::PowerOff
        );
    }

    #[test]
    fn return_after_backup_matches_found_state() {
        assert_eq!(
            resolve_after_backup(Some(RemoteAfterBackup::Return), FoundPowerState::Suspended),
            PowerAction::Suspend
        );
        assert_eq!(
            resolve_after_backup(Some(RemoteAfterBackup::Return), FoundPowerState::PoweredOff),
            PowerAction::PowerOff
        );
    }

    #[test]
    fn explicit_after_backup_ignores_found_state() {
        for found in [
            FoundPowerState::Running,
            FoundPowerState::Suspended,
            FoundPowerState::PoweredOff,
        ] {
            assert_eq!(
                resolve_after_backup(Some(RemoteAfterBackup::None), found),
                PowerAction::Leave
            );
            assert_eq!(
                resolve_after_backup(Some(RemoteAfterBackup::Suspend), found),
                PowerAction::Suspend
            );
            assert_eq!(
                resolve_after_backup(Some(RemoteAfterBackup::Shutdown), found),
                PowerAction::PowerOff
            );
        }
    }

    #[test]
    fn remote_activity_window_excludes_ten_minutes_at_each_boundary() {
        let boot_start = 1_000_000;
        assert_eq!(
            remote_activity_window(boot_start),
            (boot_start - 24 * 60 * 60 + 10 * 60, boot_start - 10 * 60)
        );
    }

    #[test]
    fn readiness_failures_are_reported_as_offline() {
        assert!(remote_readiness_failed(&TimevaultError::message(
            "remote host example did not respond to ping"
        )));
        assert!(remote_readiness_failed(&TimevaultError::message(
            "SSH readiness probe for example exited with code 255"
        )));
        assert!(!remote_readiness_failed(&TimevaultError::message(
            "read remote system journal from example: ssh exited with code 1"
        )));
    }

    #[test]
    fn suspend_guard_scope_requires_active_wake_source() {
        let mut remote_job = job("root@example.com:/srv/data");
        remote_job.remote = Some(crate::config::model::RemoteJobOptions {
            inhibit_suspend: Some(true),
            wol: Some(true),
            mac: Some("aa:bb:cc:dd:ee:ff".to_string()),
            host: Some("example.com".to_string()),
            broadcast: Some("192.0.2.255".to_string()),
            ..RemoteJobOptions::default()
        });

        assert!(has_active_remote_config(&remote_job));
        assert!(has_remote_suspend_guard_config(&remote_job));

        let mut cascade_job = remote_job.clone();
        cascade_job.source = "/mnt/primary/test/current".to_string();
        assert!(!has_active_remote_config(&cascade_job));
        assert!(!has_remote_suspend_guard_config(&cascade_job));

        let no_wake_job = job("root@example.com:/srv/data");
        assert!(!has_active_remote_config(&no_wake_job));
        assert!(!has_remote_suspend_guard_config(&no_wake_job));

        let mut wake_without_inhibit_job = remote_job.clone();
        wake_without_inhibit_job
            .remote
            .as_mut()
            .unwrap()
            .inhibit_suspend = None;
        assert!(has_active_remote_config(&wake_without_inhibit_job));
        assert!(has_remote_suspend_guard_config(&wake_without_inhibit_job));
    }

    #[test]
    fn failed_job_does_not_stop_later_jobs() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let source = tmp.path().join("source");
        fs::create_dir_all(&source).expect("source");
        let mut failed = job(source.to_string_lossy().as_ref());
        failed.name = "../bad".to_string();
        let mut next = job(source.to_string_lossy().as_ref());
        next.name = "next".to_string();
        let mut mode = run_mode();
        mode.dry_run = true;

        let report = run_backup(
            vec![failed, next],
            &[],
            mode,
            tmp.path(),
            BackupOptions {
                exclude_pristine: false,
                exclude_pristine_only: false,
                session_seconds_override: None,
            },
        )
        .expect("backup report");

        assert_eq!(report.jobs.len(), 2);
        assert_eq!(report.jobs[0].name, "../bad");
        assert_eq!(report.jobs[0].status, BackupJobStatus::Failed);
        assert_eq!(report.jobs[1].name, "next");
        assert_eq!(report.jobs[1].status, BackupJobStatus::Success);
    }

    #[test]
    fn suspend_static_targets_are_masked_for_backup() {
        let output = "static\nstatic\nstatic\nstatic\n";

        assert_eq!(
            suspend_targets_to_mask_from_systemctl_output(output),
            SUSPEND_TARGETS
        );
    }

    #[test]
    fn suspend_masked_targets_are_preserved() {
        let output = "static\nmasked\nstatic\nstatic\n";

        assert_eq!(
            suspend_targets_to_mask_from_systemctl_output(output),
            vec!["sleep.target", "hibernate.target", "hybrid-sleep.target"]
        );
    }

    #[test]
    fn unmasked_states_are_masked_for_backup() {
        let output = "disabled\nstatic\nenabled\nindirect\n";

        assert_eq!(
            suspend_targets_to_mask_from_systemctl_output(output),
            SUSPEND_TARGETS
        );
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

    #[test]
    fn dry_run_skips_pristine_analysis() {
        let excludes = build_pristine_excludes_for_jobs(
            &[job("root@example.com:/")],
            BackupOptions {
                exclude_pristine: true,
                exclude_pristine_only: false,
                session_seconds_override: None,
            },
            false,
            true,
        )
        .expect("pristine excludes");

        assert!(excludes.local.is_none());
        assert!(excludes.remote.is_empty());
    }

    #[test]
    fn rsync_vanished_files_are_reported_as_success() {
        assert_eq!(status_for_rsync_code(24), BackupJobStatus::Success);
    }

    #[test]
    fn rsync_failure_reason_includes_stderr() {
        assert_eq!(
            rsync_failure_reason(11, "rsync: write failed: No space left on device\n"),
            "rsync failed with exit code 11: rsync: write failed: No space left on device"
        );
    }

    #[test]
    fn script_failure_reason_includes_stderr() {
        assert_eq!(
            script_failure_reason(
                "remote pre",
                255,
                "ssh: connect to host mail.moyville.net port 22: Connection timed out\n",
            ),
            "remote pre script exited with code 255: ssh: connect to host mail.moyville.net port 22: Connection timed out"
        );
    }

    #[test]
    fn dry_run_job_script_does_not_execute() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let script = tmp.path().join("script.sh");
        let marker = tmp.path().join("marker");
        fs::write(&script, format!("#!/bin/sh\ntouch {}\n", marker.display()))
            .expect("write script");

        let rc = run_job_script(
            &job("/"),
            &script,
            JobScriptPhase::Pre,
            tmp.path(),
            "20260101",
            None,
            RunMode {
                dry_run: true,
                safe_mode: false,
                verbose: false,
            },
        )
        .expect("script");

        assert_eq!(rc.exit_code, 0);
        assert!(!marker.exists());
    }

    #[test]
    fn job_script_receives_environment() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().expect("tempdir");
        let script = tmp.path().join("script.sh");
        let output = tmp.path().join("env.txt");
        fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf '%s|%s|%s|%s|%s|%s' \"$TIMEVAULT_JOB_NAME\" \"$TIMEVAULT_JOB_SOURCE\" \"$TIMEVAULT_JOB_DESTINATION\" \"$TIMEVAULT_BACKUP_DAY\" \"$TIMEVAULT_SCRIPT_PHASE\" \"$TIMEVAULT_RSYNC_CODE\" > {}\n",
                output.display()
            ),
        )
        .expect("write script");
        let mut perms = fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).expect("chmod");

        let rc = run_job_script(
            &job("/source"),
            &script,
            JobScriptPhase::Post,
            tmp.path(),
            "20260101",
            Some(24),
            run_mode(),
        )
        .expect("script");

        assert_eq!(rc.exit_code, 0);
        assert_eq!(
            fs::read_to_string(output).expect("read output"),
            format!("test|/source|{}|20260101|post|24", tmp.path().display())
        );
    }

    #[test]
    fn missing_current_file_is_hard_linked_from_previous() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let previous = tmp.path().join("previous");
        let current = tmp.path().join("current");
        let previous_file = previous.join("nested/file.txt");
        let current_file = current.join("nested/file.txt");
        fs::create_dir_all(previous_file.parent().expect("parent")).expect("mkdir previous");
        fs::write(&previous_file, "previous").expect("write previous");

        copy_snapshot_without_symlinks(&previous, &current, run_mode()).expect("seed");

        assert_eq!(
            fs::read_to_string(&current_file).expect("read current"),
            "previous"
        );
        assert!(same_inode(&previous_file, &current_file));
    }

    #[test]
    fn existing_current_file_is_not_replaced() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let previous = tmp.path().join("previous");
        let current = tmp.path().join("current");
        let previous_file = previous.join("file.txt");
        let current_file = current.join("file.txt");
        fs::create_dir_all(&previous).expect("mkdir previous");
        fs::create_dir_all(&current).expect("mkdir current");
        fs::write(&previous_file, "same").expect("write previous");
        fs::write(&current_file, "same").expect("write current");
        let current_before = fs::metadata(&current_file).expect("metadata before");

        copy_snapshot_without_symlinks(&previous, &current, run_mode()).expect("seed");

        let current_after = fs::metadata(&current_file).expect("metadata after");
        assert_eq!(
            fs::read_to_string(&current_file).expect("read current"),
            "same"
        );
        assert_eq!(current_before.dev(), current_after.dev());
        assert_eq!(current_before.ino(), current_after.ino());
        assert!(!same_inode(&previous_file, &current_file));
    }

    #[test]
    fn job_excludes_path_is_per_job() {
        let tmp = Path::new("/root/tmp");
        assert_eq!(
            job_excludes_path(tmp, "spitfire-primary"),
            tmp.join("timevault.spitfire-primary.excludes")
        );
        assert_ne!(job_excludes_path(tmp, "a"), job_excludes_path(tmp, "b"));
    }

    #[test]
    fn rerunning_seed_operation_is_idempotent() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let previous = tmp.path().join("previous");
        let current = tmp.path().join("current");
        let previous_file = previous.join("file.txt");
        let current_file = current.join("file.txt");
        fs::create_dir_all(&previous).expect("mkdir previous");
        fs::write(&previous_file, "previous").expect("write previous");

        copy_snapshot_without_symlinks(&previous, &current, run_mode()).expect("first seed");
        let current_before = fs::metadata(&current_file).expect("metadata before");
        copy_snapshot_without_symlinks(&previous, &current, run_mode()).expect("second seed");
        let current_after = fs::metadata(&current_file).expect("metadata after");

        assert_eq!(
            fs::read_to_string(&current_file).expect("read current"),
            "previous"
        );
        assert!(same_inode(&previous_file, &current_file));
        assert_eq!(current_before.dev(), current_after.dev());
        assert_eq!(current_before.ino(), current_after.ino());
    }

    #[test]
    fn existing_current_file_with_different_contents_remains_unchanged() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let previous = tmp.path().join("previous");
        let current = tmp.path().join("current");
        let previous_file = previous.join("file.txt");
        let current_file = current.join("file.txt");
        fs::create_dir_all(&previous).expect("mkdir previous");
        fs::create_dir_all(&current).expect("mkdir current");
        fs::write(&previous_file, "previous").expect("write previous");
        fs::write(&current_file, "current").expect("write current");
        let current_before = fs::metadata(&current_file).expect("metadata before");

        copy_snapshot_without_symlinks(&previous, &current, run_mode()).expect("seed");

        let current_after = fs::metadata(&current_file).expect("metadata after");
        assert_eq!(
            fs::read_to_string(&current_file).expect("read current"),
            "current"
        );
        assert_eq!(current_before.dev(), current_after.dev());
        assert_eq!(current_before.ino(), current_after.ino());
        assert!(!same_inode(&previous_file, &current_file));
    }
}
