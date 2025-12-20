use chrono::{Duration, Local};
use serde::Deserialize;
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use std::collections::HashSet;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::symlink;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use timevault::{RunMode, get_configuration, print_banner, print_copyright};
use walkdir::WalkDir;

const LOCK_FILE: &str = "/var/run/timevault.pid";
const CONFIG_FILE: &str = "/etc/timevault.yaml";
const TIMEVAULT_MARKER: &str = ".timevault";

type MountTracker = Arc<Mutex<HashSet<PathBuf>>>;

struct LockGuard;

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = unlock();
    }
}

#[derive(Debug, Clone)]
struct Job {
    name: String,
    source: String,
    dest: String,
    copies: usize,
    mount: Option<PathBuf>,
    run_policy: RunPolicy,
    excludes: Vec<String>,
    depends_on: Vec<String>,
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
    mount: Option<PathBuf>,
    #[serde(default = "default_run_policy")]
    run: String,
    #[serde(default)]
    excludes: Vec<String>,
    #[serde(default)]
    depends_on: Vec<String>,
}

fn default_run_policy() -> String {
    "auto".to_string()
}

fn lock() -> io::Result<bool> {
    for _ in 0..3 {
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(LOCK_FILE)
        {
            Ok(mut f) => {
                writeln!(f, "{}", std::process::id())?;
                return Ok(true);
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                let pid = match fs::read_to_string(LOCK_FILE) {
                    Ok(text) => text.trim().parse::<u32>().ok(),
                    Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                    Err(err) => return Err(err),
                };
                if let Some(pid) = pid
                    && Path::new("/proc").join(pid.to_string()).exists()
                {
                    return Ok(false);
                }
                match fs::remove_file(LOCK_FILE) {
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

fn unlock() -> io::Result<()> {
    let pid = fs::read_to_string(LOCK_FILE).ok();
    if let Some(pid) = pid {
        let pid = pid.trim();
        if !pid.is_empty()
            && pid == std::process::id().to_string()
            && Path::new("/proc").join(pid).exists()
        {
            let _ = fs::remove_file(LOCK_FILE);
        }
    }
    Ok(())
}

fn get_config(path: &PathBuf) -> io::Result<(Vec<Job>, Option<String>)> {
    parse_config_yaml(path)
}

fn path_has_parent_dir(path: &Path) -> bool {
    path.components().any(|c| matches!(c, Component::ParentDir))
}

fn validate_job_paths(job: &JobConfig, mount_prefix: Option<&str>) -> Result<(), String> {
    let dest = job.dest.trim();
    if dest.is_empty() {
        return Err("destination path is empty".to_string());
    }
    let mount = job
        .mount
        .clone()
        .ok_or_else(|| "mount is required for all jobs".to_string())?;
    let dest_path = Path::new(dest);
    let mount_path = Path::new(&mount);
    if !dest_path.is_absolute() {
        return Err("destination path must be absolute".to_string());
    }
    if !mount_path.is_absolute() {
        return Err("mount path must be absolute".to_string());
    }
    if path_has_parent_dir(dest_path) {
        return Err("destination path must not contain ..".to_string());
    }
    if path_has_parent_dir(mount_path) {
        return Err("mount path must not contain ..".to_string());
    }
    if let Some(prefix) = mount_prefix
        && !mount_path.starts_with(Path::new(prefix))
    {
        return Err(format!(
            "mount {mount:?} does not start with required prefix {prefix}",
        ));
    }
    if !dest_path.starts_with(mount_path) {
        return Err(format!("destination {dest} is not under mount {mount:?}"));
    }
    if dest_path == mount_path {
        return Err("destination must be a subdirectory of mount".to_string());
    }
    Ok(())
}

fn parse_config_yaml(path: &PathBuf) -> io::Result<(Vec<Job>, Option<String>)> {
    let mut contents = String::new();
    File::open(path)?.read_to_string(&mut contents)?;
    let cfg: Config = serde_yaml::from_str(&contents).map_err(io::Error::other)?;

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
        if let Err(e) = validate_job_paths(&job, mount_prefix.as_deref()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("job {}: {}", job.name, e),
            ));
        }
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
            depends_on: job.depends_on,
        });
    }

    if let Err(err) = validate_dependencies(&jobs) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, err));
    }
    Ok((jobs, mount_prefix))
}

fn validate_dependencies(jobs: &[Job]) -> Result<(), String> {
    let mut names = HashSet::new();
    for job in jobs {
        if !names.insert(job.name.clone()) {
            return Err(format!("duplicate job name {}", job.name));
        }
    }
    for job in jobs {
        for dep in &job.depends_on {
            if dep == &job.name {
                return Err(format!("job {} depends on itself", job.name));
            }
            if !names.contains(dep) {
                return Err(format!("job {} depends on missing job {}", job.name, dep));
            }
        }
    }
    let _ = topo_sort_jobs(jobs.to_vec())?;
    Ok(())
}

fn topo_sort_jobs(jobs: Vec<Job>) -> Result<Vec<Job>, String> {
    let mut by_name = std::collections::HashMap::new();
    let mut order = Vec::new();

    for job in jobs {
        if by_name.contains_key(&job.name) {
            return Err(format!("duplicate job name {}", job.name));
        }
        order.push(job.name.clone());
        by_name.insert(job.name.clone(), job);
    }

    let mut dependents: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    let mut indegree: std::collections::HashMap<String, usize> = order
        .iter()
        .map(|name| (name.clone(), 0))
        .collect::<std::collections::HashMap<_, _>>();

    for name in &order {
        let job = by_name
            .get(name)
            .ok_or_else(|| format!("missing job {}", name))?;
        for dep in &job.depends_on {
            if !by_name.contains_key(dep) {
                return Err(format!("job {} depends on missing job {}", name, dep));
            }
            *indegree.entry(name.clone()).or_insert(0) += 1;
            dependents
                .entry(dep.clone())
                .or_default()
                .push(name.clone());
        }
    }
    let mut queue = std::collections::VecDeque::new();
    for name in &order {
        if indegree.get(name).copied().unwrap_or(0) == 0 {
            queue.push_back(name.clone());
        }
    }
    let mut out = Vec::new();
    while let Some(name) = queue.pop_front() {
        let job = by_name
            .remove(&name)
            .ok_or_else(|| format!("missing job {}", name))?;
        out.push(job);
        if let Some(children) = dependents.get(&name) {
            for child in children {
                if let Some(count) = indegree.get_mut(child) {
                    *count -= 1;
                    if *count == 0 {
                        queue.push_back(child.clone());
                    }
                }
            }
        }
    }
    if out.len() != order.len() {
        return Err("job dependencies contain a cycle".to_string());
    }
    Ok(out)
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

fn get_mount_prefix(path: &PathBuf) -> io::Result<Option<String>> {
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
        .clone()
        .ok_or_else(|| "mount is required for all jobs".to_string())?;
    if let Some(prefix) = mount_prefix
        && !mount.starts_with(prefix)
    {
        return Err(format!(
            "mount {mount:?} does not start with required prefix {prefix}",
        ));
    }
    let dest_path = Path::new(&job.dest);
    let dest_canonical = dest_path
        .canonicalize()
        .map_err(|e| format!("cannot access destination {}: {}", job.dest, e))?;

    if dest_canonical == Path::new("/") {
        return Err("destination resolves to /".to_string());
    }

    let mount_path = Path::new(&mount);
    let mount_canonical = mount_path
        .canonicalize()
        .map_err(|e| format!("cannot access mount {mount:?}: {e}"))?;
    if mount_canonical == Path::new("/") {
        return Err("mount resolves to /".to_string());
    }
    if !dest_canonical.starts_with(&mount_canonical) {
        return Err(format!(
            "destination {} is not under mount {}",
            dest_canonical.display(),
            mount_canonical.display()
        ));
    }
    if dest_canonical == mount_canonical {
        return Err("destination must be a subdirectory of mount".to_string());
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
    mount: &PathBuf,
    mount_prefix: Option<&str>,
    run_mode: RunMode,
    force_init: bool,
    mounts: &MountTracker,
) -> Result<(), String> {
    if let Some(prefix) = mount_prefix {
        if !mount.starts_with(prefix) {
            return Err(format!(
                "mount {:?} does not start with required prefix {}",
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
        .map_err(|e| format!("cannot access mount {:?}: {}", mount, e))?;
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

    ensure_unmounted(mount, &mount_canonical, &run_mode, mounts)?;
    let mut cmd = Command::new("mount");
    cmd.arg(mount);
    let _ = run_command(&mut cmd, &run_mode).map_err(|e| format!("mount {mount:?}: {e}"))?;

    if !mount_is_mounted(&mount_canonical)? {
        return Err(format!(
            "mount {} is not mounted",
            mount_canonical.display()
        ));
    }
    track_mount(mount, mounts);
    if run_mode.verbose {
        println!("mount is active");
    }

    let mut cmd = Command::new("mount");
    cmd.arg("-oremount,rw").arg(mount);
    let _ = run_command(&mut cmd, &run_mode).map_err(|e| format!("remount rw {mount:?}: {e}"))?;
    if run_mode.verbose {
        println!("remounted read/write");
    }

    let result = (|| {
        if mount_is_readonly(&mount_canonical)? {
            return Err(format!("mount {} is read-only", mount_canonical.display()));
        }
        let mut is_empty = true;
        for entry in fs::read_dir(&mount_canonical)
            .map_err(|e| format!("cannot read mount {}: {}", mount_canonical.display(), e))?
        {
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
            println!(
                "mount empty check: {}",
                if is_empty { "empty" } else { "not empty" }
            );
        }

        let marker = mount_canonical.join(TIMEVAULT_MARKER);
        if marker.exists() {
            println!("timevault marker already exists: {}", marker.display());
            return Ok(());
        }
        if run_mode.dry_run {
            println!("dry-run: touch {}", marker.display());
        } else {
            File::create(&marker).map_err(|e| format!("create {}: {}", marker.display(), e))?;
        }
        Ok(())
    })();

    let mut cmd = Command::new("mount");
    cmd.arg("-oremount,ro").arg(mount);
    let _ = run_command(&mut cmd, &run_mode).map_err(|e| format!("remount ro {mount:?}: {e}"))?;

    let mut cmd = Command::new("umount");
    cmd.arg(mount);
    let _ = run_command(&mut cmd, &run_mode).map_err(|e| format!("umount {mount:?}: {e}"))?;
    untrack_mount(mount, mounts);
    result?;

    Ok(())
}

fn mount_is_mounted(mount: &Path) -> Result<bool, String> {
    let contents =
        fs::read_to_string("/proc/mounts").map_err(|e| format!("read /proc/mounts: {e}"))?;
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

fn mount_is_readonly(mount: &Path) -> Result<bool, String> {
    let contents =
        fs::read_to_string("/proc/mounts").map_err(|e| format!("read /proc/mounts: {e}"))?;
    for line in contents.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 4 {
            continue;
        }
        if Path::new(fields[1]) == mount {
            let mut opts = fields[3].split(',');
            return Ok(opts.any(|opt| opt == "ro"));
        }
    }
    Err(format!("mount {} is not mounted", mount.display()))
}

fn ensure_unmounted(
    mount: &PathBuf,
    mount_path: &Path,
    run_mode: &RunMode,
    mounts: &MountTracker,
) -> Result<(), String> {
    let is_mounted = mount_is_mounted(mount_path)?;
    if !is_mounted {
        if run_mode.verbose {
            println!("mount not active, skip umount: {:?}", mount);
        }
        return Ok(());
    }
    if run_mode.verbose {
        println!("unmounting {:?}", mount);
    }
    let mut cmd = Command::new("umount");
    cmd.arg(mount);
    let rc = run_command(&mut cmd, run_mode).map_err(|e| format!("umount {mount:?}: {e}"))?;
    if rc != 0 {
        return Err(format!("umount {mount:?} failed with exit code {rc}"));
    }
    if mount_is_mounted(mount_path)? {
        return Err(format!("umount {mount:?} did not detach"));
    }
    untrack_mount(mount, mounts);
    Ok(())
}

fn track_mount(mount: &Path, mounts: &MountTracker) {
    if let Ok(mut set) = mounts.lock() {
        set.insert(mount.to_path_buf());
    }
}

fn untrack_mount(mount: &PathBuf, mounts: &MountTracker) {
    if let Ok(mut set) = mounts.lock() {
        set.remove(mount);
    }
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

fn expire_old_backups(job: &Job, dest: &Path, run_mode: &RunMode) -> io::Result<()> {
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

fn delete_symlinks(root: &Path, run_mode: &RunMode) -> io::Result<()> {
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

fn maybe_print_command(cmd: &Command, run_mode: &RunMode) {
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

fn run_command(cmd: &mut Command, run_mode: &RunMode) -> io::Result<i32> {
    maybe_print_command(cmd, run_mode);
    let status = cmd.status()?;
    Ok(status.code().unwrap_or(1))
}

fn run_nice_ionice(args: &[String], run_mode: &RunMode) -> io::Result<i32> {
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

fn acquire_lock_for_job(run_mode: &RunMode) -> io::Result<Option<LockGuard>> {
    if run_mode.dry_run {
        return Ok(None);
    }
    match lock() {
        Ok(true) => Ok(Some(LockGuard)),
        Ok(false) => Err(io::Error::other("timevault is already running")),
        Err(e) => Err(io::Error::other(format!(
            "failed to lock {}: {}",
            LOCK_FILE, e
        ))),
    }
}

fn run_policy_label(policy: RunPolicy) -> &'static str {
    match policy {
        RunPolicy::Auto => "auto",
        RunPolicy::Demand => "demand",
        RunPolicy::Off => "off",
    }
}

fn print_job_details(job: &Job) {
    let depends = if job.depends_on.is_empty() {
        "<none>".to_string()
    } else {
        job.depends_on.join(", ")
    };
    let excludes = if job.excludes.is_empty() {
        "<none>".to_string()
    } else {
        job.excludes.join(", ")
    };
    println!("job: {}", job.name);
    println!("  source: {}", job.source);
    println!("  dest: {}", job.dest);
    println!("  copies: {}", job.copies);
    println!("  mount: {:?}", job.mount);
    println!("  run: {}", run_policy_label(job.run_policy));
    println!("  depends_on: {}", depends);
    println!("  excludes: {}", excludes);
}

fn backup(
    jobs: Vec<Job>,
    rsync_extra: &[String],
    run_mode: &RunMode,
    mount_prefix: Option<&str>,
    mounts: &MountTracker,
) -> io::Result<()> {
    for job in jobs {
        let _lock = acquire_lock_for_job(run_mode)?;
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
            println!("  mount: {:?}", job.mount);
            println!("  copies: {}", job.copies);
            println!("  excludes: {}", job.excludes.len());
        }
        let home = env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let tmp_dir = Path::new(&home).join("tmp");
        if !run_mode.dry_run {
            fs::create_dir_all(&tmp_dir)?;
        }
        let excludes_file = tmp_dir.join("timevault.excludes");
        if run_mode.dry_run {
            println!(
                "dry-run: would write excludes file {}",
                excludes_file.display()
            );
        } else {
            create_excludes_file(&job, &excludes_file)?;
        }

        let backup_day = (Local::now() - Duration::days(1))
            .format("%Y%m%d")
            .to_string();
        if run_mode.verbose {
            println!("  backup day: {}", backup_day);
        }

        let Some(mount) = job.mount.clone() else {
            println!("skip job {}: mount is required for all jobs", job.name);
            continue;
        };
        let mount_path = Path::new(&mount);
        if let Err(err) = ensure_unmounted(&mount, mount_path, run_mode, mounts) {
            println!("skip job {}: {err}", job.name);
            continue;
        }
        let mut cmd = Command::new("mount");
        cmd.arg(mount.clone());
        let _ = run_command(&mut cmd, run_mode);
        if let Ok(true) = mount_is_mounted(mount_path) {
            track_mount(&mount, mounts);
        }

        let mut cmd = Command::new("mount");
        cmd.arg("-oremount,rw").arg(mount.clone());
        let _ = run_command(&mut cmd, run_mode);
        if let Ok(true) = mount_is_readonly(mount_path) {
            println!("skip job {}: mount {mount:?} is read-only", job.name);
            let mut cmd = Command::new("mount");
            cmd.arg("-oremount,ro").arg(mount.clone());
            let _ = run_command(&mut cmd, run_mode);
            let mut cmd = Command::new("umount");
            cmd.arg(mount.clone());
            let _ = run_command(&mut cmd, run_mode);
            untrack_mount(&mount, mounts);
            continue;
        }

        let dest = match verify_destination(&job, mount_prefix) {
            Ok(dest) => dest,
            Err(err) => {
                println!("skip job {}: {}", job.name, err);
                let mut cmd = Command::new("mount");
                cmd.arg("-oremount,ro").arg(mount.clone());
                let _ = run_command(&mut cmd, run_mode);
                let mut cmd = Command::new("umount");
                cmd.arg(mount.clone());
                let _ = run_command(&mut cmd, run_mode);
                untrack_mount(&mount, mounts);
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

        let mut cmd = Command::new("mount");
        cmd.arg("-oremount,ro").arg(mount.clone());
        let _ = run_command(&mut cmd, run_mode);

        let mut cmd = Command::new("umount");
        cmd.arg(mount.clone());
        let _ = run_command(&mut cmd, run_mode);
        untrack_mount(&mount, mounts);
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let mounts: MountTracker = Arc::new(Mutex::new(HashSet::new()));
    let mounts_for_signals = Arc::clone(&mounts);
    thread::spawn(move || {
        let mut signals = match Signals::new([SIGINT, SIGTERM]) {
            Ok(signals) => signals,
            Err(err) => {
                eprintln!("signal handler setup failed: {}", err);
                return;
            }
        };
        if signals.forever().next().is_some() {
            let mut list: Vec<_> = match mounts_for_signals.lock() {
                Ok(set) => set.iter().cloned().collect(),
                Err(_) => Vec::new(),
            };
            list.sort_by_key(|m| std::cmp::Reverse(m.as_os_str().len()));
            for mount in list {
                let mut cmd = Command::new("umount");
                cmd.arg(&mount);
                let _ = cmd.status();
            }
            std::process::exit(1);
        }
    });

    let rsync_extra = Vec::new();

    let configuration = get_configuration();
    let selected_jobs: Vec<String> = Vec::new();
    let force_init = configuration.force_init.is_some();

    if configuration.version {
        print_copyright();
        return Ok(());
    }

    let mut have_lock = false;
    println!("{}", print_banner());
    println!("{}", Local::now().format("%d-%m-%Y %H:%M"));

    let run_mode = configuration.run_mode.unwrap_or_default();
    let config_path = configuration.config.unwrap_or(PathBuf::from(CONFIG_FILE));

    if let Some(mount) = configuration.init {
        if !run_mode.dry_run && !configuration.print_order {
            match lock() {
                Ok(true) => {
                    have_lock = true;
                }
                Ok(false) => {
                    println!("timevault is already running");
                    std::process::exit(3);
                }
                Err(e) => {
                    println!(
                        "failed to lock {LOCK_FILE}: {e} (need write permission; try sudo or adjust permissions)",
                    );
                    std::process::exit(2);
                }
            }
        }
        if run_mode.verbose {
            println!("init requested for mount {mount:?}");
        }
        let mount_prefix = match get_mount_prefix(&config_path) {
            Ok(prefix) => prefix,
            Err(e) => {
                println!("failed to load config {config_path:?}: {e}");
                if have_lock {
                    let _ = unlock();
                }
                std::process::exit(2);
            }
        };
        match init_timevault(
            &mount,
            mount_prefix.as_deref(),
            run_mode,
            force_init,
            &mounts,
        ) {
            Ok(()) => {
                println!("initialized timevault at {mount:?}");
            }
            Err(e) => {
                println!("init failed: {e}");
                if have_lock {
                    let _ = unlock();
                }
                std::process::exit(2);
            }
        }
        if have_lock {
            let _ = unlock();
        }
        println!("{}", Local::now().format("%d-%m-%Y %H:%M"));
        return Ok(());
    }

    let maybe_config = get_config(&config_path);

    let Ok(config) = maybe_config else {
        println!("failed to load config {config_path:?}: {maybe_config:?}");
        if have_lock {
            let _ = unlock();
        }
        std::process::exit(2);
    };

    let (jobs, mount_prefix) = config;
    let selected_set: HashSet<String> = selected_jobs.iter().cloned().collect();
    let jobs_by_name: std::collections::HashMap<String, Job> =
        jobs.iter().map(|j| (j.name.clone(), j.clone())).collect();

    if !selected_set.is_empty() {
        let mut missing = Vec::new();
        let mut seen = HashSet::new();
        for name in &selected_jobs {
            if !seen.insert(name.clone()) {
                continue;
            }
            if !jobs_by_name.contains_key(name) {
                missing.push(name.clone());
            }
        }
        if !missing.is_empty() {
            for name in &missing {
                println!("job not found: {name}");
            }
            println!("no such job(s) found; aborting");
            if have_lock {
                let _ = unlock();
            }
            std::process::exit(2);
        }
    }
    let mut roots = Vec::new();
    if selected_set.is_empty() {
        for job in &jobs {
            if job.run_policy == RunPolicy::Auto {
                roots.push(job.name.clone());
            }
        }
    } else {
        let mut seen = HashSet::new();
        for name in &selected_jobs {
            if seen.insert(name.clone()) {
                roots.push(name.clone());
            }
        }
    }
    let mut included = HashSet::new();
    let mut stack: Vec<(String, Option<String>)> = roots.into_iter().map(|n| (n, None)).collect();
    while let Some((name, parent)) = stack.pop() {
        if included.contains(&name) {
            continue;
        }
        let job = match jobs_by_name.get(&name) {
            Some(job) => job,
            None => {
                println!("job not found: {}", name);
                if have_lock {
                    let _ = unlock();
                }
                std::process::exit(2);
            }
        };
        if job.run_policy == RunPolicy::Off {
            if let Some(parent) = parent {
                println!("job disabled (off): {} (required by {})", name, parent);
            } else {
                println!("job disabled (off): {}", name);
            }
            println!("requested job(s) are disabled; aborting");
            if have_lock {
                let _ = unlock();
            }
            std::process::exit(2);
        }
        included.insert(name.clone());
        for dep in &job.depends_on {
            if !jobs_by_name.contains_key(dep) {
                println!("dependency {} not found for job {}", dep, job.name);
                if have_lock {
                    let _ = unlock();
                }
                std::process::exit(2);
            }
            stack.push((dep.clone(), Some(job.name.clone())));
        }
    }
    let selected_jobs_vec: Vec<Job> = jobs
        .into_iter()
        .filter(|job| included.contains(&job.name))
        .collect();
    let jobs_to_run = match topo_sort_jobs(selected_jobs_vec) {
        Ok(jobs) => jobs,
        Err(err) => {
            println!("dependency order failed: {}", err);
            if have_lock {
                let _ = unlock();
            }
            std::process::exit(2);
        }
    };
    if jobs_to_run.is_empty() {
        if selected_set.is_empty() {
            println!("no jobs matched (no auto jobs enabled); aborting");
        } else {
            println!("no jobs matched selection; aborting");
        }
        if have_lock {
            let _ = unlock();
        }
        std::process::exit(2);
    }
    if configuration.print_order {
        for job in &jobs_to_run {
            print_job_details(job);
        }
        if have_lock {
            let _ = unlock();
        }
        std::process::exit(0);
    }
    if run_mode.verbose {
        println!(
            "loaded config {config_path:?} with {} job(s)",
            jobs_to_run.len()
        );
        if let Some(prefix) = mount_prefix.as_deref() {
            println!("mount prefix: {}", prefix);
        }
    }
    if let Err(e) = backup(
        jobs_to_run,
        &rsync_extra,
        &run_mode,
        mount_prefix.as_deref(),
        &mounts,
    ) {
        let message = e.to_string();
        if message == "timevault is already running" {
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
        let _ = run_command(&mut sync_cmd, &run_mode);
    }
    println!("{}", Local::now().format("%d-%m-%Y %H:%M"));

    Ok(())
}
