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

use chrono::{Duration, Local};
use walkdir::WalkDir;

use crate::backup::pristine::{build_pristine_excludes_for_source, PristineSource};
use crate::backup::report::{BackupJobReport, BackupJobStatus, BackupRunReport};
use crate::backup::rsync::run_rsync;
use crate::config::model::Job;
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

struct SuspendGuard {
    changed_suspend_state: bool,
    remote_host: Option<String>,
}

impl Drop for SuspendGuard {
    fn drop(&mut self) {
        if self.changed_suspend_state {
            let Some(remote_host) = self.remote_host.as_deref() else {
                return;
            };
            let mut cmd = remote_systemctl_command(remote_host, "unmask");
            match cmd.status() {
                Ok(status) if status.success() => {}
                Ok(status) => eprintln!(
                    "failed to re-enable suspend on backup source host {} after backup: ssh exited with code {}",
                    remote_host,
                    status.code().unwrap_or(1)
                ),
                Err(err) => eprintln!(
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
    println!("job: {}", job.name);
    if let Some(description) = &job.description {
        println!("  description: {}", description);
    }
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
    for job in jobs {
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
                println!("job {} failed: {}", job.name, err);
                report
                    .jobs
                    .push(failed_job_report(&job, disk_mount, &backup_day, run_mode));
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
            println!(
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
        });
    }

    if run_mode.verbose {
        println!("  backup day: {}", backup_day);
    }

    if run_mode.verbose {
        println!("job: {}", job.name);
        println!("  run: {}", job.run_policy.as_str());
        println!("  source: {}", job.source);
        println!("  backup dir: {}", dest.display());
        println!("  copies: {}", job.copies);
        println!("  excludes: {}", job.excludes.len());
    }

    let _remote_power_guard = start_remote_power_guard(job, run_mode)?;
    let _suspend_guard = start_suspend_guard(job, run_mode)?;

    if let Some(script) = job_script_path(&job.name, JobScriptPhase::Pre) {
        let script_rc = run_job_script(
            job,
            &script,
            JobScriptPhase::Pre,
            &backup_dir,
            backup_day,
            None,
            run_mode,
        )?;
        if script_rc != 0 {
            println!(
                "pre script failed for job {} with exit code {}; skipping backup",
                job.name, script_rc
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
            });
        }
    }
    if let Some(script_rc) = run_remote_job_script(
        job,
        JobScriptPhase::Pre,
        &backup_dir,
        backup_day,
        None,
        run_mode,
    )? {
        if script_rc != 0 {
            println!(
                "remote pre script failed for job {} with exit code {}; skipping backup",
                job.name, script_rc
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
    let excludes_file = tmp_dir.join("timevault.excludes");
    let excludes = build_exclude_list(job, pristine_excludes)?;
    if run_mode.dry_run {
        println!(
            "dry-run: would write excludes file {}",
            excludes_file.display()
        );
    } else {
        create_excludes_file(&excludes, &excludes_file)?;
    }

    if !dest.exists() {
        if run_mode.dry_run {
            println!("dry-run: mkdir -p {}", dest.display());
        } else {
            fs::create_dir_all(&dest)?;
        }
    }

    expire_old_backups(job, &dest, run_mode)?;

    let current = dest.join("current");
    let backup_dir = dest.join(backup_day);

    if current.exists() && !backup_dir.exists() {
        if run_mode.dry_run {
            println!("dry-run: mkdir -p {}", backup_dir.display());
        } else {
            fs::create_dir_all(&backup_dir)?;
        }
        copy_snapshot_without_symlinks(&current, &backup_dir, run_mode)?;
    }

    let mut rc = 1;
    let mut attempts = 0;
    for attempt in 1..=3 {
        attempts = attempt;
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
                symlink(backup_day, &current_link)?;
            }
        }
    }
    let mut status = status_for_rsync_code(rc);
    if let Some(script_rc) = run_remote_job_script(
        job,
        JobScriptPhase::Post,
        &backup_dir,
        backup_day,
        Some(rc),
        run_mode,
    )? {
        if script_rc != 0 {
            println!(
                "remote post script failed for job {} with exit code {}",
                job.name, script_rc
            );
            status = BackupJobStatus::Failed;
        }
    }
    if let Some(script) = job_script_path(&job.name, JobScriptPhase::Post) {
        let script_rc = run_job_script(
            job,
            &script,
            JobScriptPhase::Post,
            &backup_dir,
            backup_day,
            Some(rc),
            run_mode,
        )?;
        if script_rc != 0 {
            println!(
                "post script failed for job {} with exit code {}",
                job.name, script_rc
            );
            status = BackupJobStatus::Failed;
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
    })
}

fn failed_job_report(
    job: &Job,
    disk_mount: &Path,
    backup_day: &str,
    run_mode: RunMode,
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
    }
}

fn status_for_rsync_code(rc: i32) -> BackupJobStatus {
    match rc {
        0 | 24 => BackupJobStatus::Success,
        _ => BackupJobStatus::Failed,
    }
}

fn start_suspend_guard(job: &Job, run_mode: RunMode) -> Result<SuspendGuard> {
    if !has_remote_suspend_guard_config(job) {
        return Ok(SuspendGuard {
            changed_suspend_state: false,
            remote_host: None,
        });
    }
    let Some(remote) = remote_ssh_source(&job.source) else {
        return Ok(SuspendGuard {
            changed_suspend_state: false,
            remote_host: None,
        });
    };

    if run_mode.dry_run {
        println!(
            "dry-run: would check suspend state on backup source host {}: systemctl is-enabled {}",
            remote.host,
            SUSPEND_TARGETS.join(" ")
        );
        return Ok(SuspendGuard {
            changed_suspend_state: false,
            remote_host: None,
        });
    }

    if suspend_is_allowed(&remote.host, run_mode)? {
        let mut cmd = remote_systemctl_command(&remote.host, "mask");
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
            changed_suspend_state: true,
            remote_host: Some(remote.host),
        })
    } else {
        println!(
            "suspend on backup source host {} was already disabled before backup; leaving it disabled",
            remote.host
        );
        Ok(SuspendGuard {
            changed_suspend_state: false,
            remote_host: None,
        })
    }
}

fn suspend_is_allowed(remote_host: &str, run_mode: RunMode) -> Result<bool> {
    let mut cmd = remote_systemctl_command(remote_host, "is-enabled");
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
    if output.status.success() || !combined.trim().is_empty() {
        Ok(suspend_is_allowed_from_systemctl_output(&combined))
    } else {
        Err(TimevaultError::message(format!(
            "failed to detect suspend state on backup source host {}: ssh exited with code {}",
            remote_host,
            output.status.code().unwrap_or(1)
        )))
    }
}

fn suspend_is_allowed_from_systemctl_output(output: &str) -> bool {
    !output.lines().any(|line| line.trim() == "masked")
}

fn remote_systemctl_command(remote_host: &str, action: &str) -> Command {
    let mut cmd = Command::new("ssh");
    cmd.arg(remote_host)
        .arg("systemctl")
        .arg(action)
        .args(SUSPEND_TARGETS);
    cmd
}

pub fn run_pristine_only(jobs: Vec<Job>, run_mode: RunMode, options: BackupOptions) -> Result<()> {
    if run_mode.verbose {
        println!("pristine: exclude-only mode enabled; skipping backup");
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
) -> Result<i32> {
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
        println!(
            "dry-run: would run {} script for job {}: {}",
            phase.as_str(),
            job.name,
            script.display()
        );
        return Ok(0);
    }
    maybe_print_command(&cmd, run_mode);
    let status = cmd.status().map_err(|e| {
        TimevaultError::message(format!(
            "{} script for job {} ({}): {}",
            phase.as_str(),
            job.name,
            script.display(),
            e
        ))
    })?;
    Ok(status.code().unwrap_or(1))
}

fn run_remote_job_script(
    job: &Job,
    phase: JobScriptPhase,
    destination: &Path,
    backup_day: &str,
    rsync_code: Option<i32>,
    run_mode: RunMode,
) -> Result<Option<i32>> {
    let Some(remote) = remote_ssh_source(&job.source) else {
        return Ok(None);
    };
    let script = remote_job_script_path(&job.name, phase);
    if run_mode.dry_run {
        println!(
            "dry-run: would run remote {} script for job {} if present: {}:{}",
            phase.as_str(),
            job.name,
            remote.host,
            script
        );
        return Ok(Some(0));
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
    let status = cmd.status().map_err(|e| {
        TimevaultError::message(format!(
            "remote {} script for job {} ({}:{}): {}",
            phase.as_str(),
            job.name,
            remote.host,
            script,
            e
        ))
    })?;
    Ok(Some(status.code().unwrap_or(1)))
}

struct WakeContext {
    host: String,
    ssh_host: String,
    targets: Vec<SocketAddrV4>,
    mac: String,
    keepalive_seconds: Option<u64>,
    wait_seconds: u64,
    suspend_after_backup: bool,
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

struct RemotePowerGuard {
    keepalive: Option<WakeKeepalive>,
    suspend_after_backup: bool,
    remote_host: String,
    job_name: String,
}

impl Drop for RemotePowerGuard {
    fn drop(&mut self) {
        drop(self.keepalive.take());
        if !self.suspend_after_backup {
            return;
        }
        let mut cmd = Command::new("ssh");
        cmd.arg("-o")
            .arg("BatchMode=yes")
            .arg("-o")
            .arg("ConnectTimeout=5")
            .arg(&self.remote_host)
            .arg("systemctl suspend")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        match cmd.status() {
            Ok(status) if status.success() => {}
            Ok(status) => eprintln!(
                "failed to suspend remote host {} after job {}: ssh exited with code {}",
                self.remote_host,
                self.job_name,
                status.code().unwrap_or(1)
            ),
            Err(err) => eprintln!(
                "failed to suspend remote host {} after job {}: {}",
                self.remote_host, self.job_name, err
            ),
        }
    }
}

pub fn wake_remote_job(job: &Job, run_mode: RunMode) -> Result<()> {
    if run_mode.dry_run {
        let Some((wake, _, host)) = wake_config(job) else {
            return Err(TimevaultError::message(format!(
                "job {} has no remote.wake configuration",
                job.name
            )));
        };
        println!(
            "dry-run: would send WOL for job {} to {} for {}",
            job.name,
            wake_target_description(wake),
            host
        );
        println!(
            "dry-run: would wait up to {} seconds for {} to respond to ping, repeating WOL between checks",
            wake_wait_seconds(wake),
            host
        );
        return Ok(());
    }

    let Some(context) = wake_context(job)? else {
        return Err(TimevaultError::message(format!(
            "job {} has no remote.wake configuration",
            job.name
        )));
    };
    if ping_once(&context.host, run_mode)? {
        if run_mode.verbose {
            println!(
                "wake host {} already responds to ping; skipping WOL",
                context.host
            );
        }
        return Ok(());
    }
    if run_mode.verbose {
        println!(
            "wake host {} did not respond; sending WOL to {} target(s)",
            context.host,
            context.targets.len()
        );
    }
    send_wake_packets(&context.mac, &context.targets)?;
    wait_for_ping_with_wake(
        &context.host,
        context.wait_seconds,
        &context.mac,
        &context.targets,
        run_mode,
    )
}

fn start_remote_power_guard(job: &Job, run_mode: RunMode) -> Result<Option<RemotePowerGuard>> {
    if run_mode.dry_run {
        let Some((wake, _, host)) = wake_config(job) else {
            return Ok(None);
        };
        println!(
            "dry-run: would check whether {} responds to ping before wake",
            host
        );
        println!(
            "dry-run: would send WOL for job {} to {} for {} if ping fails",
            job.name,
            wake_target_description(wake),
            host
        );
        println!(
            "dry-run: would wait up to {} seconds for {} to respond to ping, repeating WOL between checks",
            wake_wait_seconds(wake),
            host
        );
        if let Some(seconds) = wake.keepalive_seconds {
            println!(
                "dry-run: would repeat WOL for job {} every {} seconds while backup runs if wake was needed",
                job.name, seconds
            );
        }
        if wake.suspend_after_backup == Some(true) {
            println!(
                "dry-run: would suspend remote host after job {} if it was woken for the backup",
                job.name
            );
        }
        return Ok(None);
    }

    let Some(context) = wake_context(job)? else {
        return Ok(None);
    };
    if ping_once(&context.host, run_mode)? {
        if run_mode.verbose {
            println!(
                "wake host {} already responds to ping; skipping WOL",
                context.host
            );
        }
        return Ok(None);
    }

    if run_mode.verbose {
        println!(
            "wake host {} did not respond; sending WOL to {} target(s)",
            context.host,
            context.targets.len()
        );
    }
    send_wake_packets(&context.mac, &context.targets)?;
    wait_for_ping_with_wake(
        &context.host,
        context.wait_seconds,
        &context.mac,
        &context.targets,
        run_mode,
    )?;

    let keepalive = if let Some(seconds) = context.keepalive_seconds {
        let (stop, receiver) = mpsc::channel();
        let mac = context.mac;
        let targets = context.targets;
        let handle = thread::spawn(move || loop {
            match receiver.recv_timeout(StdDuration::from_secs(seconds)) {
                Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                Err(RecvTimeoutError::Timeout) => {
                    let _ = send_wake_packets(&mac, &targets);
                }
            }
        });
        Some(WakeKeepalive {
            stop: Some(stop),
            handle: Some(handle),
        })
    } else {
        None
    };

    Ok(Some(RemotePowerGuard {
        keepalive,
        suspend_after_backup: context.suspend_after_backup,
        remote_host: context.ssh_host,
        job_name: job.name.clone(),
    }))
}

fn wake_context(job: &Job) -> Result<Option<WakeContext>> {
    let Some((wake, remote, host)) = wake_config(job) else {
        return Ok(None);
    };
    let targets = wake_targets(wake, &host)?;
    Ok(Some(WakeContext {
        host: host.to_string(),
        ssh_host: remote.host,
        targets,
        mac: wake.mac.clone(),
        keepalive_seconds: wake.keepalive_seconds,
        wait_seconds: wake_wait_seconds(wake),
        suspend_after_backup: wake.suspend_after_backup.unwrap_or(false),
    }))
}

fn wake_config<'a>(
    job: &'a Job,
) -> Option<(
    &'a crate::config::model::RemoteWakeOptions,
    RemoteSshSource,
    String,
)> {
    let Some(remote_options) = &job.remote else {
        return None;
    };
    let Some(wake) = &remote_options.wake else {
        return None;
    };
    let Some(remote) = remote_ssh_source(&job.source) else {
        return None;
    };
    let host = wake_host(wake, &remote).to_string();
    Some((wake, remote, host))
}

fn has_active_wake_config(job: &Job) -> bool {
    wake_config(job).is_some()
}

fn has_remote_suspend_guard_config(job: &Job) -> bool {
    job.remote
        .as_ref()
        .is_some_and(|remote| remote.inhibit_suspend == Some(true))
        && has_active_wake_config(job)
}

fn wake_target_description(wake: &crate::config::model::RemoteWakeOptions) -> String {
    match &wake.broadcast {
        Some(broadcast) => format!("{}:{}", broadcast, wake.port.unwrap_or(9)),
        None => format!(
            "DNS-inferred /24 broadcast or active interface broadcasts:{}",
            wake.port.unwrap_or(9)
        ),
    }
}

fn wake_wait_seconds(wake: &crate::config::model::RemoteWakeOptions) -> u64 {
    wake.wait_seconds.unwrap_or(15)
}

fn wake_host<'a>(
    wake: &'a crate::config::model::RemoteWakeOptions,
    remote: &'a RemoteSshSource,
) -> &'a str {
    wake.host
        .as_deref()
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .unwrap_or_else(|| remote_dns_host(&remote.host))
}

fn wake_targets(
    wake: &crate::config::model::RemoteWakeOptions,
    dns_host: &str,
) -> Result<Vec<SocketAddrV4>> {
    let port = wake.port.unwrap_or(9);
    if let Some(value) = &wake.broadcast {
        let broadcast = value.parse::<Ipv4Addr>().map_err(|err| {
            TimevaultError::message(format!("remote.wake.broadcast {}: {}", value, err))
        })?;
        return Ok(vec![SocketAddrV4::new(broadcast, port)]);
    }

    match inferred_broadcast_for_host(dns_host) {
        Ok(broadcast) => Ok(vec![SocketAddrV4::new(broadcast, port)]),
        Err(dns_err) => {
            let targets = active_interface_broadcast_targets(wake.interface.as_deref(), port)?;
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

fn wait_for_ping_with_wake(
    host: &str,
    timeout_seconds: u64,
    mac: &str,
    targets: &[SocketAddrV4],
    run_mode: RunMode,
) -> Result<()> {
    let deadline = Instant::now() + StdDuration::from_secs(timeout_seconds);
    if run_mode.verbose {
        println!(
            "waiting up to {} seconds for wake host {}",
            timeout_seconds, host
        );
    }
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(TimevaultError::message(format!(
                "wake host {} did not respond to ping within {} seconds",
                host, timeout_seconds
            )));
        }

        if ping_once_with_timeout(host, remaining.min(PING_ATTEMPT_TIMEOUT), run_mode)? {
            if run_mode.verbose {
                println!("wake host {} is reachable", host);
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(TimevaultError::message(format!(
                "wake host {} did not respond to ping within {} seconds",
                host, timeout_seconds
            )));
        }

        if run_mode.verbose {
            println!(
                "wake host {} is not reachable yet; sending another WOL packet to {} target(s)",
                host,
                targets.len()
            );
        }
        send_wake_packets(mac, targets)?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if !remaining.is_zero() {
            thread::sleep(remaining.min(StdDuration::from_secs(1)));
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
            println!("wake host {} did not resolve to an IPv4 address", host);
        }
        return Ok(false);
    }
    for address in addresses {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        if run_mode.verbose {
            println!("checking wake host {} at {}", host, address);
        }
        if ping_ipv4_once_with_timeout(address, remaining)? {
            return Ok(true);
        }
        if run_mode.verbose {
            println!(
                "wake host {} resolved to {}, but ping failed",
                host, address
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
            println!("wake host {} is already an IPv4 address", host);
        }
        return Ok(vec![address]);
    }

    if run_mode.verbose {
        println!(
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
                if run_mode.verbose {
                    println!(
                        "wake host {} DNS lookup failed with exit code {}",
                        host,
                        status.code().unwrap_or(1)
                    );
                }
                return Ok(Vec::new());
            }
            let mut output = String::new();
            if let Some(mut stdout) = child.stdout.take() {
                stdout.read_to_string(&mut output)?;
            }
            let addresses = parse_getent_ahostsv4(&output);
            if run_mode.verbose {
                if addresses.is_empty() {
                    println!("wake host {} DNS lookup returned no IPv4 addresses", host);
                } else {
                    println!(
                        "wake host {} resolved to {}",
                        host,
                        addresses
                            .iter()
                            .map(Ipv4Addr::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
            }
            return Ok(addresses);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            if run_mode.verbose {
                println!("wake host {} DNS lookup timed out", host);
            }
            return Ok(Vec::new());
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
            println!("pristine: dry-run; skip package analysis");
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
            println!(
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
            println!("pristine: dry-run; skip package analysis");
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
        let wake = crate::config::model::RemoteWakeOptions {
            mac: "aa:bb:cc:dd:ee:ff".to_string(),
            host: None,
            broadcast: Some("192.0.2.255".to_string()),
            port: Some(7),
            interface: None,
            keepalive_seconds: None,
            wait_seconds: None,
            suspend_after_backup: None,
        };

        assert_eq!(
            wake_targets(&wake, "does-not-need-dns").expect("targets"),
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
        let wake = crate::config::model::RemoteWakeOptions {
            mac: "aa:bb:cc:dd:ee:ff".to_string(),
            host: Some("actual-host".to_string()),
            broadcast: None,
            port: None,
            interface: None,
            keepalive_seconds: None,
            wait_seconds: None,
            suspend_after_backup: None,
        };

        assert_eq!(wake_host(&wake, &remote), "actual-host");
    }

    #[test]
    fn suspend_guard_scope_requires_active_wake_source() {
        let mut remote_job = job("root@example.com:/srv/data");
        remote_job.remote = Some(crate::config::model::RemoteJobOptions {
            inhibit_suspend: Some(true),
            wake: Some(crate::config::model::RemoteWakeOptions {
                mac: "aa:bb:cc:dd:ee:ff".to_string(),
                host: Some("example.com".to_string()),
                broadcast: Some("192.0.2.255".to_string()),
                port: None,
                interface: None,
                keepalive_seconds: None,
                wait_seconds: None,
                suspend_after_backup: None,
            }),
        });

        assert!(has_active_wake_config(&remote_job));
        assert!(has_remote_suspend_guard_config(&remote_job));

        let mut cascade_job = remote_job.clone();
        cascade_job.source = "/mnt/primary/test/current".to_string();
        assert!(!has_active_wake_config(&cascade_job));
        assert!(!has_remote_suspend_guard_config(&cascade_job));

        let mut no_wake_job = job("root@example.com:/srv/data");
        no_wake_job.remote = Some(crate::config::model::RemoteJobOptions {
            inhibit_suspend: Some(true),
            wake: None,
        });
        assert!(!has_active_wake_config(&no_wake_job));
        assert!(!has_remote_suspend_guard_config(&no_wake_job));

        let mut wake_without_inhibit_job = remote_job.clone();
        wake_without_inhibit_job
            .remote
            .as_mut()
            .unwrap()
            .inhibit_suspend = None;
        assert!(has_active_wake_config(&wake_without_inhibit_job));
        assert!(!has_remote_suspend_guard_config(&wake_without_inhibit_job));
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
    fn suspend_static_targets_are_allowed() {
        let output = "static\nstatic\nstatic\nstatic\n";

        assert!(suspend_is_allowed_from_systemctl_output(output));
    }

    #[test]
    fn suspend_masked_target_means_already_disabled() {
        let output = "static\nmasked\nstatic\nstatic\n";

        assert!(!suspend_is_allowed_from_systemctl_output(output));
    }

    #[test]
    fn suspend_disabled_but_unmasked_targets_are_allowed() {
        let output = "disabled\nstatic\nenabled\nindirect\n";

        assert!(suspend_is_allowed_from_systemctl_output(output));
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

        assert_eq!(rc, 0);
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

        assert_eq!(rc, 0);
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
