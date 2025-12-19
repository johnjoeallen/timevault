use chrono::{Duration, Local};
use std::env;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::Command;
use walkdir::WalkDir;
use serde::Deserialize;

const LOCK_FILE: &str = "/var/run/gbackup.pid";
const CONFIG_FILE: &str = "/etc/timevault.yaml";
const TIMEVAULT_MARKER: &str = ".timevault";

#[derive(Debug, Clone, Copy)]
struct RunMode {
    dry_run: bool,
    safe_mode: bool,
    verbose: bool,
}

#[derive(Debug, Clone)]
struct Job {
    name: String,
    source: String,
    dest: String,
    copies: usize,
    mount: Option<String>,
    run_policy: RunPolicy,
    excludes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunPolicy {
    Auto,
    Demand,
    Off,
}

#[derive(Debug, Deserialize)]
struct Config {
    jobs: Vec<JobConfig>,
    #[serde(default)]
    excludes: Vec<String>,
    #[serde(default)]
    mount_prefix: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JobConfig {
    name: String,
    source: String,
    dest: String,
    copies: usize,
    #[serde(default)]
    mount: Option<String>,
    #[serde(default = "default_run_policy")]
    run: String,
    #[serde(default)]
    excludes: Vec<String>,
}

fn default_run_policy() -> String {
    "auto".to_string()
}

fn lock() -> io::Result<bool> {
    let pid = fs::read_to_string(LOCK_FILE).ok();
    if let Some(pid) = pid {
        let pid = pid.trim();
        if !pid.is_empty() && Path::new("/proc").join(pid).exists() {
            return Ok(false);
        }
    }

    let mut f = File::create(LOCK_FILE)?;
    writeln!(f, "{}", std::process::id())?;
    Ok(true)
}

fn unlock() -> io::Result<()> {
    let pid = fs::read_to_string(LOCK_FILE).ok();
    if let Some(pid) = pid {
        let pid = pid.trim();
        if !pid.is_empty() && Path::new("/proc").join(pid).exists() {
            let _ = fs::remove_file(LOCK_FILE);
        }
    }
    Ok(())
}

fn get_config(path: &str) -> io::Result<(Vec<Job>, Option<String>)> {
    parse_config_yaml(path)
}

fn parse_config_yaml(path: &str) -> io::Result<(Vec<Job>, Option<String>)> {
    let mut contents = String::new();
    File::open(path)?.read_to_string(&mut contents)?;
    let cfg: Config =
        serde_yaml::from_str(&contents).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let global_excludes = cfg.excludes;
    let mount_prefix = cfg.mount_prefix;
    let mut jobs = Vec::new();
    for job in cfg.jobs {
        let run_policy = parse_run_policy(&job.run).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("job {}: {}", job.name, e),
            )
        })?;
        let mut excludes = global_excludes.clone();
        excludes.extend(job.excludes);
        jobs.push(Job {
            name: job.name,
            source: job.source,
            dest: job.dest,
            copies: job.copies,
            mount: job.mount,
            run_policy,
            excludes,
        });
    }

    Ok((jobs, mount_prefix))
}

fn parse_run_policy(value: &str) -> Result<RunPolicy, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "auto" => Ok(RunPolicy::Auto),
        "demand" => Ok(RunPolicy::Demand),
        "off" => Ok(RunPolicy::Off),
        _ => Err(format!(
            "invalid run policy {}; expected auto, demand, or off",
            value
        )),
    }
}

fn get_mount_prefix(path: &str) -> io::Result<Option<String>> {
    if !Path::new(path).exists() {
        return Ok(None);
    }
    let (_, mount_prefix) = parse_config_yaml(path)?;
    Ok(mount_prefix)
}

fn create_excludes_file(job: &Job, filename: &Path) -> io::Result<()> {
    let mut f = File::create(filename)?;
    for exclude in &job.excludes {
        writeln!(f, "{}", exclude)?;
    }
    Ok(())
}

fn verify_destination(job: &Job, mount_prefix: Option<&str>) -> Result<PathBuf, String> {
    if job.dest.trim().is_empty() {
        return Err("destination path is empty".to_string());
    }
    let mount = job
        .mount
        .as_ref()
        .filter(|m| !m.trim().is_empty())
        .ok_or_else(|| "mount is required for all jobs".to_string())?;
    if let Some(prefix) = mount_prefix {
        if !mount.starts_with(prefix) {
            return Err(format!(
                "mount {} does not start with required prefix {}",
                mount, prefix
            ));
        }
    }
    let dest_path = Path::new(&job.dest);
    let dest_canonical = dest_path
        .canonicalize()
        .map_err(|e| format!("cannot access destination {}: {}", job.dest, e))?;

    if dest_canonical == Path::new("/") {
        return Err("destination resolves to /".to_string());
    }

    let mount_path = Path::new(mount);
    let mount_canonical = mount_path
        .canonicalize()
        .map_err(|e| format!("cannot access mount {}: {}", mount, e))?;
    if mount_canonical == Path::new("/") {
        return Err("mount resolves to /".to_string());
    }
    if !mount_is_mounted(&mount_canonical)? {
        return Err(format!(
            "mount {} is not mounted",
            mount_canonical.display()
        ));
    }
    if !mount_in_fstab(&mount_canonical)? {
        return Err(format!(
            "mount {} not found in /etc/fstab",
            mount_canonical.display()
        ));
    }

    let marker = mount_canonical.join(TIMEVAULT_MARKER);
    if !marker.is_file() {
        return Err(format!(
            "target device is not a timevault device (missing {} at {})",
            TIMEVAULT_MARKER,
            marker.display()
        ));
    }

    Ok(dest_canonical)
}

fn init_timevault(
    mount: &str,
    mount_prefix: Option<&str>,
    run_mode: RunMode,
    force_init: bool,
) -> Result<(), String> {
    if mount.trim().is_empty() {
        return Err("mount path is empty".to_string());
    }
    if let Some(prefix) = mount_prefix {
        if !mount.starts_with(prefix) {
            return Err(format!(
                "mount {} does not start with required prefix {}",
                mount, prefix
            ));
        }
        if run_mode.verbose {
            println!("mount prefix verified: {}", prefix);
        }
    }
    let mount_path = Path::new(mount);
    let mount_canonical = mount_path
        .canonicalize()
        .map_err(|e| format!("cannot access mount {}: {}", mount, e))?;
    if mount_canonical == Path::new("/") {
        return Err("mount resolves to /".to_string());
    }
    if !mount_in_fstab(&mount_canonical)? {
        return Err(format!(
            "mount {} not found in /etc/fstab",
            mount_canonical.display()
        ));
    }
    if run_mode.verbose {
        println!("mount is present in /etc/fstab");
    }

    let mut cmd = Command::new("mount");
    cmd.arg(mount);
    let _ =
        run_command(&mut cmd, run_mode).map_err(|e| format!("mount {}: {}", mount, e))?;

    if !mount_is_mounted(&mount_canonical)? {
        return Err(format!(
            "mount {} is not mounted",
            mount_canonical.display()
        ));
    }
    if run_mode.verbose {
        println!("mount is active");
    }

    let mut cmd = Command::new("mount");
    cmd.arg("-oremount,rw").arg(mount);
    let _ = run_command(&mut cmd, run_mode)
        .map_err(|e| format!("remount rw {}: {}", mount, e))?;
    if run_mode.verbose {
        println!("remounted read/write");
    }

    let result = (|| {
        let mut is_empty = true;
        for entry in fs::read_dir(&mount_canonical).map_err(|e| {
            format!(
                "cannot read mount {}: {}",
                mount_canonical.display(),
                e
            )
        })? {
            let entry = entry.map_err(|e| e.to_string())?;
            let name = entry.file_name();
            if !name.to_string_lossy().is_empty() {
                is_empty = false;
                break;
            }
        }
        if !is_empty && !force_init {
            return Err(format!(
                "mount {} is not empty; aborting init (use --force-init to override)",
                mount_canonical.display()
            ));
        }
        if run_mode.verbose {
            println!("mount empty check: {}", if is_empty { "empty" } else { "not empty" });
        }

        let marker = mount_canonical.join(TIMEVAULT_MARKER);
        if marker.exists() {
            println!("timevault marker already exists: {}", marker.display());
            return Ok(());
        }
        if run_mode.dry_run {
            println!("dry-run: touch {}", marker.display());
        } else {
            File::create(&marker)
                .map_err(|e| format!("create {}: {}", marker.display(), e))?;
        }
        Ok(())
    })();

    let mut cmd = Command::new("mount");
    cmd.arg("-oremount,ro").arg(mount);
    let _ = run_command(&mut cmd, run_mode)
        .map_err(|e| format!("remount ro {}: {}", mount, e))?;

    let mut cmd = Command::new("umount");
    cmd.arg(mount);
    let _ = run_command(&mut cmd, run_mode).map_err(|e| format!("umount {}: {}", mount, e))?;
    result?;

    Ok(())
}

fn mount_is_mounted(mount: &Path) -> Result<bool, String> {
    let contents =
        fs::read_to_string("/proc/mounts").map_err(|e| format!("read /proc/mounts: {}", e))?;
    for line in contents.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            continue;
        }
        if Path::new(fields[1]) == mount {
            return Ok(true);
        }
    }
    Ok(false)
}

fn mount_in_fstab(mount: &Path) -> Result<bool, String> {
    let contents =
        fs::read_to_string("/etc/fstab").map_err(|e| format!("read /etc/fstab: {}", e))?;
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            continue;
        }
        if Path::new(fields[1]) == mount {
            return Ok(true);
        }
    }
    Ok(false)
}

fn expire_old_backups(job: &Job, dest: &Path, run_mode: RunMode) -> io::Result<()> {
    let mut backups: Vec<String> = Vec::new();
    for entry in fs::read_dir(dest)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == "." || name == ".." || name == "current" || name == TIMEVAULT_MARKER {
            continue;
        }
        backups.push(name.to_string());
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

fn delete_symlinks(root: &Path, run_mode: RunMode) -> io::Result<()> {
    if run_mode.safe_mode || run_mode.dry_run {
        if run_mode.dry_run {
            println!("dry-run: find {} -type l -delete", root.display());
        } else {
            println!("skip symlink cleanup (safe-mode): {}", root.display());
        }
        return Ok(());
    }
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry?;
        let ft = entry.file_type();
        if ft.is_symlink() {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn maybe_print_command(cmd: &Command, run_mode: RunMode) {
    if !run_mode.dry_run && !run_mode.verbose {
        return;
    }
    let program = cmd.get_program().to_string_lossy();
    let args: Vec<String> = cmd
        .get_args()
        .map(|a| a.to_string_lossy().to_string())
        .collect();
    println!("{} {}", program, args.join(" "));
}

fn run_command(cmd: &mut Command, run_mode: RunMode) -> io::Result<i32> {
    maybe_print_command(cmd, run_mode);
    let status = cmd.status()?;
    Ok(status.code().unwrap_or(1))
}

fn run_nice_ionice(args: &[String], run_mode: RunMode) -> io::Result<i32> {
    let mut cmd = Command::new("nice");
    cmd.arg("-n")
        .arg("19")
        .arg("ionice")
        .arg("-c")
        .arg("3")
        .arg("-n7");
    for arg in args {
        cmd.arg(arg);
    }
    if run_mode.dry_run {
        maybe_print_command(&cmd, run_mode);
        Ok(0)
    } else {
        run_command(&mut cmd, run_mode)
    }
}

fn backup(
    jobs: Vec<Job>,
    rsync_extra: &[String],
    run_mode: RunMode,
    mount_prefix: Option<&str>,
) -> io::Result<()> {
    for job in jobs {
        if run_mode.verbose {
            let policy = match job.run_policy {
                RunPolicy::Auto => "auto",
                RunPolicy::Demand => "demand",
                RunPolicy::Off => "off",
            };
            println!("job: {}", job.name);
            println!("  run: {}", policy);
            println!("  source: {}", job.source);
            println!("  dest: {}", job.dest);
            println!(
                "  mount: {}",
                job.mount.as_deref().unwrap_or("<unset>")
            );
            println!("  copies: {}", job.copies);
            println!("  excludes: {}", job.excludes.len());
        }
        let home = env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let tmp_dir = Path::new(&home).join("tmp");
        if !run_mode.dry_run {
            fs::create_dir_all(&tmp_dir)?;
        }
        let excludes_file = tmp_dir.join("gbackup.excludes");
        if run_mode.dry_run {
            println!("dry-run: would write excludes file {}", excludes_file.display());
        } else {
            create_excludes_file(&job, &excludes_file)?;
        }

        let backup_day = (Local::now() - Duration::days(1)).format("%Y%m%d").to_string();
        if run_mode.verbose {
            println!("  backup day: {}", backup_day);
        }

        let mount = match job.mount.as_ref().filter(|m| !m.trim().is_empty()) {
            Some(mount) => mount,
            None => {
                println!("skip job {}: mount is required for all jobs", job.name);
                continue;
            }
        };
        let mut cmd = Command::new("mount");
        cmd.arg(mount);
        let _ = run_command(&mut cmd, run_mode);

        let mut cmd = Command::new("mount");
        cmd.arg("-oremount,rw").arg(mount);
        let _ = run_command(&mut cmd, run_mode);

        let dest = match verify_destination(&job, mount_prefix) {
            Ok(dest) => dest,
            Err(err) => {
                println!("skip job {}: {}", job.name, err);
                let mut cmd = Command::new("mount");
                cmd.arg("-oremount,ro").arg(mount);
                let _ = run_command(&mut cmd, run_mode);
                continue;
            }
        };

        expire_old_backups(&job, &dest, run_mode)?;

        let current = dest.join("current");
        let backup_dir = dest.join(&backup_day);

        if current.exists() && !backup_dir.exists() {
            if run_mode.dry_run {
                println!("dry-run: mkdir -p {}", backup_dir.display());
            } else {
                fs::create_dir_all(&backup_dir)?;
            }
            let args = vec![
                "cp".to_string(),
                "-ralf".to_string(),
                format!("{}/.", current.display()),
                backup_dir.to_string_lossy().to_string(),
            ];
            let _ = run_nice_ionice(&args, run_mode);
            delete_symlinks(&backup_dir, run_mode)?;
        }

        let mut rsync_args = vec![
            "rsync".to_string(),
            "-ar".to_string(),
            "--stats".to_string(),
            format!("--exclude-from={}", excludes_file.display()),
        ];
        if !run_mode.safe_mode {
            rsync_args.push("--delete-after".to_string());
            rsync_args.push("--delete-excluded".to_string());
        }
        rsync_args.extend(rsync_extra.iter().cloned());
        rsync_args.push(job.source.clone());
        rsync_args.push(backup_dir.to_string_lossy().to_string());

        let mut rc = 1;
        for _ in 0..3 {
            rc = run_nice_ionice(&rsync_args, run_mode)?;
        }

        if rc == 0 && backup_dir.exists() {
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
                    println!(
                        "dry-run: ln -s {} {}",
                        backup_day,
                        current_link.display()
                    );
                } else {
                    symlink(&backup_day, &current_link)?;
                }
            }
        }

        let mut cmd = Command::new("mount");
        cmd.arg("-oremount,ro").arg(mount);
        let _ = run_command(&mut cmd, run_mode);

        let mut cmd = Command::new("umount");
        cmd.arg(mount);
        let _ = run_command(&mut cmd, run_mode);
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let mut rsync_extra = Vec::new();
    let mut run_mode = RunMode {
        dry_run: false,
        safe_mode: false,
        verbose: false,
    };
    let mut config_path = CONFIG_FILE.to_string();
    let mut init_mount: Option<String> = None;
    let mut force_init = false;
    let mut selected_jobs: Vec<String> = Vec::new();
    let mut args = env::args().skip(1).peekable();
    while let Some(arg) = args.next() {
        if arg == "--backup" {
            continue;
        } else if arg == "--dry-run" {
            run_mode.dry_run = true;
            continue;
        } else if arg == "--safe" {
            run_mode.safe_mode = true;
            continue;
        } else if arg == "--verbose" || arg == "-v" {
            run_mode.verbose = true;
            continue;
        } else if arg == "--init" {
            match args.next() {
                Some(path) => init_mount = Some(path),
                None => {
                    println!("--init requires a mount path");
                    std::process::exit(2);
                }
            }
            continue;
        } else if arg == "--force-init" {
            match args.next() {
                Some(path) => {
                    if init_mount.is_some() {
                        println!("use only one of --init or --force-init");
                        std::process::exit(2);
                    }
                    init_mount = Some(path);
                    force_init = true;
                }
                None => {
                    println!("--force-init requires a mount path");
                    std::process::exit(2);
                }
            }
            continue;
        } else if arg == "--config" {
            match args.next() {
                Some(path) => config_path = path,
                None => {
                    println!("--config requires a path");
                    std::process::exit(2);
                }
            }
            continue;
        } else if arg == "--job" {
            match args.next() {
                Some(name) => selected_jobs.push(name),
                None => {
                    println!("--job requires a name");
                    std::process::exit(2);
                }
            }
            continue;
        }
        rsync_extra.push(arg);
    }

    match lock() {
        Ok(true) => {}
        Ok(false) => {
            println!("gbackup is already running");
            std::process::exit(3);
        }
        Err(e) => {
            println!(
                "failed to lock {}: {} (need write permission; try sudo or adjust permissions)",
                LOCK_FILE, e
            );
            std::process::exit(2);
        }
    }

    println!("{}", Local::now().format("%d-%m-%Y %H:%M"));
    if let Some(mount) = init_mount {
        if run_mode.verbose {
            println!("init requested for mount {}", mount);
        }
        let mount_prefix = match get_mount_prefix(&config_path) {
            Ok(prefix) => prefix,
            Err(e) => {
                println!("failed to load config {}: {}", config_path, e);
                let _ = unlock();
                std::process::exit(2);
            }
        };
        match init_timevault(&mount, mount_prefix.as_deref(), run_mode, force_init) {
            Ok(()) => {
                println!("initialized timevault at {}", mount);
            }
            Err(e) => {
                println!("init failed: {}", e);
                let _ = unlock();
                std::process::exit(2);
            }
        }
        let _ = unlock();
        println!("{}", Local::now().format("%d-%m-%Y %H:%M"));
        return Ok(());
    }

    let config = match get_config(&config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            println!("failed to load config {}: {}", config_path, e);
            let _ = unlock();
            std::process::exit(2);
        }
    };

    let (jobs, mount_prefix) = config;
    let selected_set: HashSet<String> = selected_jobs.iter().cloned().collect();
    let (jobs_to_run, missing_jobs, off_jobs) = if selected_set.is_empty() {
        let jobs_to_run: Vec<Job> = jobs
            .into_iter()
            .filter(|j| j.run_policy == RunPolicy::Auto)
            .collect();
        (jobs_to_run, Vec::new(), Vec::new())
    } else {
        let mut by_name = std::collections::HashMap::new();
        for job in jobs {
            by_name.insert(job.name.clone(), job);
        }
        let mut out = Vec::new();
        let mut missing = Vec::new();
        let mut off = Vec::new();
        let mut seen = HashSet::new();
        for name in &selected_jobs {
            if !seen.insert(name.clone()) {
                continue;
            }
            match by_name.remove(name) {
                Some(job) => {
                    if job.run_policy == RunPolicy::Off {
                        off.push(name.clone());
                    } else {
                        out.push(job);
                    }
                }
                None => missing.push(name.clone()),
            }
        }
        (out, missing, off)
    };
    if !missing_jobs.is_empty() {
        for name in &missing_jobs {
            println!("job not found: {}", name);
        }
        println!("no such job(s) found; aborting");
        let _ = unlock();
        std::process::exit(2);
    }
    if !off_jobs.is_empty() {
        for name in &off_jobs {
            println!("job disabled (off): {}", name);
        }
        println!("requested job(s) are disabled; aborting");
        let _ = unlock();
        std::process::exit(2);
    }
    if jobs_to_run.is_empty() {
        if selected_set.is_empty() {
            println!("no jobs matched (no auto jobs enabled); aborting");
        } else {
            println!("no jobs matched selection; aborting");
        }
        let _ = unlock();
        std::process::exit(2);
    }
    if run_mode.verbose {
        println!(
            "loaded config {} with {} job(s)",
            config_path,
            jobs_to_run.len()
        );
        if let Some(prefix) = mount_prefix.as_deref() {
            println!("mount prefix: {}", prefix);
        }
    }
    if let Err(e) = backup(jobs_to_run, &rsync_extra, run_mode, mount_prefix.as_deref()) {
        println!("backup failed: {}", e);
    }

    let _ = unlock();
    if !run_mode.dry_run {
        let mut sync_cmd = Command::new("sync");
        let _ = run_command(&mut sync_cmd, run_mode);
    }
    println!("{}", Local::now().format("%d-%m-%Y %H:%M"));

    Ok(())
}
