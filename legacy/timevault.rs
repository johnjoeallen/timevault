use std::collections::HashSet;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use walkdir::WalkDir;
use serde::{Deserialize, Serialize};
use chrono::{Duration, Local, Utc};

const CONFIG_FILE: &str = "/etc/timevault.yaml";
const TIMEVAULT_MARKER: &str = ".timevault";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const LICENSE_NAME: &str = "GNU GPL v3 or later";
const COPYRIGHT: &str = "Copyright (C) 2025 John Allen (john.joe.allen@gmail.com)";
const PROJECT_URL: &str = "https://github.com/johnjoeallen/timevault";
const DEFAULT_MOUNT_BASE: &str = "/run/timevault/mounts";
const DEFAULT_USER_MOUNT_BASE: &str = "/run/timevault/user-mounts";
const DEFAULT_BACKUP_MOUNT_OPTS: &str = "rw,nodev,nosuid,noexec";
const DEFAULT_RESTORE_MOUNT_OPTS: &str = "ro,nodev,nosuid,noexec";
const IDENTITY_FILE: &str = ".timevault";
const IDENTITY_VERSION: u32 = 1;
const DISK_ADD_ALLOWED_ENTRIES: [&str; 1] = ["lost+found"];
const EXIT_NO_DISK: i32 = 10;
const EXIT_MULTI_DISK: i32 = 11;
const EXIT_IDENTITY_MISMATCH: i32 = 12;
const EXIT_DISK_NOT_EMPTY: i32 = 13;
const EXIT_MOUNT_FAILURE: i32 = 14;


#[derive(Debug, Clone, Copy)]
struct RunMode {
    dry_run: bool,
    safe_mode: bool,
    verbose: bool,
}

struct LockGuard {
    path: String,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = unlock_file(&self.path);
    }
}

struct MountGuard {
    mountpoint: PathBuf,
    remove_dir: bool,
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        let _ = unmount_path(&self.mountpoint);
        if self.remove_dir {
            let _ = fs::remove_dir(&self.mountpoint);
        }
    }
}

#[derive(Debug, Clone)]
struct Job {
    name: String,
    source: String,
    copies: usize,
    run_policy: RunPolicy,
    excludes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunPolicy {
    Auto,
    Demand,
    Off,
}

struct RuntimeConfig {
    jobs: Vec<Job>,
    backup_disks: Vec<BackupDiskConfig>,
    mount_base: PathBuf,
    user_mount_base: PathBuf,
}

struct DiskInitArgs {
    disk_id: String,
    fs_uuid: Option<String>,
    device: Option<String>,
    label: Option<String>,
    mount_options: Option<String>,
    force: bool,
}

struct MountArgs {
    disk_id: Option<String>,
}

struct UmountArgs {}

enum CommandKind {
    Backup,
    DiskEnroll,
    Mount,
    Umount,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Config {
    jobs: Vec<JobConfig>,
    #[serde(default)]
    excludes: Vec<String>,
    #[serde(default, rename = "backupDisks")]
    backup_disks: Vec<BackupDiskConfig>,
    #[serde(default, rename = "mountBase", skip_serializing_if = "Option::is_none")]
    mount_base: Option<String>,
    #[serde(default, rename = "userMountBase", skip_serializing_if = "Option::is_none")]
    user_mount_base: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct JobConfig {
    name: String,
    source: String,
    copies: usize,
    #[serde(default = "default_run_policy")]
    run: String,
    #[serde(default)]
    excludes: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct BackupDiskConfig {
    #[serde(rename = "diskId")]
    disk_id: String,
    #[serde(rename = "fsUuid")]
    fs_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    #[serde(default, rename = "mountOptions", skip_serializing_if = "Option::is_none")]
    mount_options: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct DiskIdentity {
    version: u32,
    #[serde(rename = "diskId")]
    disk_id: String,
    #[serde(rename = "fsUuid")]
    fs_uuid: String,
    created: String,
}

fn default_run_policy() -> String {
    "auto".to_string()
}

fn lock_file(path: &str) -> io::Result<bool> {
    for _ in 0..3 {
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
        {
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

fn unlock_file(path: &str) -> io::Result<()> {
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

fn get_config(path: &str) -> io::Result<RuntimeConfig> {
    parse_config_yaml(path)
}

fn validate_job_paths(job: &JobConfig) -> Result<(), String> {
    if job.source.trim().is_empty() {
        return Err("source path is empty".to_string());
    }
    if job.name.trim().is_empty() {
        return Err("job name is required".to_string());
    }
    Ok(())
}

fn parse_config_yaml(path: &str) -> io::Result<RuntimeConfig> {
    let mut contents = String::new();
    File::open(path)?.read_to_string(&mut contents)?;
    let cfg: Config =
        serde_yaml::from_str(&contents).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    parse_config_struct(cfg)
}

fn parse_config_struct(cfg: Config) -> io::Result<RuntimeConfig> {
    let global_excludes = cfg.excludes;
    let mut jobs = Vec::new();
    for job in cfg.jobs {
        let run_policy = parse_run_policy(&job.run).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("job {}: {}", job.name, e),
            )
        })?;
        if let Err(e) = validate_job_paths(&job) {
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
            copies: job.copies,
            run_policy,
            excludes,
        });
    }

    if let Err(err) = validate_dependencies(&jobs) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, err));
    }
    let mount_base = cfg
        .mount_base
        .unwrap_or_else(|| DEFAULT_MOUNT_BASE.to_string());
    let user_mount_base = cfg
        .user_mount_base
        .unwrap_or_else(|| DEFAULT_USER_MOUNT_BASE.to_string());
    Ok(RuntimeConfig {
        jobs,
        backup_disks: cfg.backup_disks,
        mount_base: PathBuf::from(mount_base),
        user_mount_base: PathBuf::from(user_mount_base),
    })
}

fn validate_dependencies(jobs: &[Job]) -> Result<(), String> {
    let mut names = HashSet::new();
    for job in jobs {
        if !is_safe_job_name(&job.name) {
            return Err(format!(
                "job {} name must use only letters, digits, '.', '-', '_'",
                job.name
            ));
        }
        if !names.insert(job.name.clone()) {
            return Err(format!("duplicate job name {}", job.name));
        }
    }
    Ok(())
}

fn is_safe_job_name(name: &str) -> bool {
    if name.is_empty() || name == "." || name == ".." {
        return false;
    }
    name.chars().all(|c| {
        c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.'
    })
}

fn lock_path_for_job(name: &str) -> io::Result<String> {
    if !is_safe_job_name(name) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "job {} name must use only letters, digits, '.', '-', '_'",
                name
            ),
        ));
    }
    Ok(format!("/var/run/timevault.{}.pid", name))
}

fn ensure_base_dir(path: &Path) -> Result<(), String> {
    if path.exists() {
        let meta = fs::metadata(path).map_err(|e| format!("stat {}: {}", path.display(), e))?;
        if !meta.is_dir() {
            return Err(format!("{} is not a directory", path.display()));
        }
        if meta.uid() != 0 {
            return Err(format!(
                "{} must be owned by root",
                path.display()
            ));
        }
        let mut perms = meta.permissions();
        perms.set_mode(0o700);
        fs::set_permissions(path, perms)
            .map_err(|e| format!("chmod {}: {}", path.display(), e))?;
        return Ok(());
    }
    fs::create_dir_all(path)
        .map_err(|e| format!("create {}: {}", path.display(), e))?;
    let meta = fs::metadata(path)
        .map_err(|e| format!("stat {}: {}", path.display(), e))?;
    if meta.uid() != 0 {
        return Err(format!(
            "{} must be owned by root",
            path.display()
        ));
    }
    let mut perms = meta.permissions();
    perms.set_mode(0o700);
    fs::set_permissions(path, perms)
        .map_err(|e| format!("chmod {}: {}", path.display(), e))?;
    Ok(())
}

fn create_temp_dir(base: &Path, prefix: &str) -> Result<PathBuf, String> {
    ensure_base_dir(base)?;
    let ts = Utc::now().format("%Y%m%d%H%M%S%3f");
    let candidate = base.join(format!("{}-{}-{}", prefix, std::process::id(), ts));
    fs::create_dir_all(&candidate)
        .map_err(|e| format!("create {}: {}", candidate.display(), e))?;
    let mut perms = fs::metadata(&candidate)
        .map_err(|e| format!("stat {}: {}", candidate.display(), e))?
        .permissions();
    perms.set_mode(0o700);
    fs::set_permissions(&candidate, perms)
        .map_err(|e| format!("chmod {}: {}", candidate.display(), e))?;
    Ok(candidate)
}

fn list_entries(path: &Path) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for entry in fs::read_dir(path).map_err(|e| format!("read {}: {}", path.display(), e))? {
        let entry = entry.map_err(|e| format!("read {}: {}", path.display(), e))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name == "." || name == ".." {
            continue;
        }
        out.push(name);
    }
    Ok(out)
}

fn is_disk_empty(root: &Path) -> Result<bool, String> {
    let entries = list_entries(root)?;
    for entry in entries {
        if DISK_ADD_ALLOWED_ENTRIES.contains(&entry.as_str()) {
            continue;
        }
        return Ok(false);
    }
    Ok(true)
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

fn create_excludes_file(job: &Job, filename: &Path) -> io::Result<()> {
    let mut f = File::create(filename)?;
    for exclude in &job.excludes {
        writeln!(f, "{}", exclude)?;
    }
    Ok(())
}


fn device_path_for_uuid(uuid: &str) -> PathBuf {
    Path::new("/dev/disk/by-uuid").join(uuid)
}

fn device_is_mounted(device: &Path) -> Result<bool, String> {
    let contents =
        fs::read_to_string("/proc/self/mounts").map_err(|e| format!("read /proc/self/mounts: {}", e))?;
    let device_real = device
        .canonicalize()
        .map_err(|e| format!("resolve {}: {}", device.display(), e))?;
    for line in contents.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            continue;
        }
        let mounted_dev = Path::new(fields[0]);
        let mounted_real = mounted_dev.canonicalize().unwrap_or_else(|_| mounted_dev.to_path_buf());
        if mounted_real == device_real {
            return Ok(true);
        }
    }
    Ok(false)
}

fn find_device_mountpoint(device: &Path) -> Result<Option<PathBuf>, String> {
    let contents =
        fs::read_to_string("/proc/self/mounts").map_err(|e| format!("read /proc/self/mounts: {}", e))?;
    let device_real = device
        .canonicalize()
        .map_err(|e| format!("resolve {}: {}", device.display(), e))?;
    for line in contents.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            continue;
        }
        let mounted_dev = Path::new(fields[0]);
        let mounted_real = mounted_dev.canonicalize().unwrap_or_else(|_| mounted_dev.to_path_buf());
        if mounted_real == device_real {
            return Ok(Some(PathBuf::from(fields[1])));
        }
    }
    Ok(None)
}

fn is_fat_filesystem(device: &Path) -> bool {
    let device_real = match device.canonicalize() {
        Ok(path) => path,
        Err(_) => return false,
    };
    let contents = match fs::read_to_string("/proc/self/mounts") {
        Ok(contents) => contents,
        Err(_) => return false,
    };
    for line in contents.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 3 {
            continue;
        }
        let mounted_dev = Path::new(fields[0]);
        let mounted_real = mounted_dev.canonicalize().unwrap_or_else(|_| mounted_dev.to_path_buf());
        if mounted_real == device_real {
            let fstype = fields[2];
            if fstype.eq_ignore_ascii_case("vfat")
                || fstype.eq_ignore_ascii_case("fat")
                || fstype.eq_ignore_ascii_case("msdos")
            {
                return true;
            }
        }
    }
    let output = Command::new("blkid")
        .arg("-o")
        .arg("value")
        .arg("-s")
        .arg("TYPE")
        .arg(&device_real)
        .output();
    if let Ok(output) = output {
        if output.status.success() {
            let fstype = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if fstype.eq_ignore_ascii_case("vfat")
                || fstype.eq_ignore_ascii_case("fat")
                || fstype.eq_ignore_ascii_case("msdos")
            {
                return true;
            }
        }
    }
    false
}

fn mountpoint_is_mounted(mountpoint: &Path) -> Result<bool, String> {
    let contents =
        fs::read_to_string("/proc/self/mounts").map_err(|e| format!("read /proc/self/mounts: {}", e))?;
    for line in contents.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            continue;
        }
        if Path::new(fields[1]) == mountpoint {
            return Ok(true);
        }
    }
    Ok(false)
}

fn mount_device(device: &Path, mountpoint: &Path, options: &str) -> Result<(), String> {
    let mut cmd = Command::new("mount");
    cmd.arg("-o").arg(options).arg(device).arg(mountpoint);
    let rc = run_command(&mut cmd, RunMode { dry_run: false, safe_mode: false, verbose: false })
        .map_err(|e| format!("mount {}: {}", device.display(), e))?;
    if rc != 0 {
        return Err(format!("mount {} failed with exit code {}", device.display(), rc));
    }
    Ok(())
}

fn mount_device_silent(device: &Path, mountpoint: &Path, options: &str) -> Result<(), String> {
    let status = Command::new("mount")
        .arg("-o")
        .arg(options)
        .arg(device)
        .arg(mountpoint)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| format!("mount {}: {}", device.display(), e))?;
    if !status.success() {
        return Err(format!(
            "mount {} failed with exit code {}",
            device.display(),
            status.code().unwrap_or(1)
        ));
    }
    Ok(())
}

fn unmount_path(mountpoint: &Path) -> Result<(), String> {
    let mut cmd = Command::new("umount");
    cmd.arg(mountpoint);
    let rc = run_command(&mut cmd, RunMode { dry_run: false, safe_mode: false, verbose: false })
        .map_err(|e| format!("umount {}: {}", mountpoint.display(), e))?;
    if rc != 0 {
        return Err(format!(
            "umount {} failed with exit code {}",
            mountpoint.display(),
            rc
        ));
    }
    Ok(())
}

fn read_identity(path: &Path) -> Result<DiskIdentity, String> {
    let mut contents = String::new();
    File::open(path)
        .map_err(|e| format!("open {}: {}", path.display(), e))?
        .read_to_string(&mut contents)
        .map_err(|e| format!("read {}: {}", path.display(), e))?;
    serde_yaml::from_str(&contents)
        .map_err(|e| format!("parse {}: {}", path.display(), e))
}

fn write_identity(path: &Path, identity: &DiskIdentity) -> Result<(), String> {
    let data = serde_yaml::to_string(identity).map_err(|e| format!("encode identity: {}", e))?;
    let mut file =
        File::create(path).map_err(|e| format!("create {}: {}", path.display(), e))?;
    file.write_all(data.as_bytes())
        .map_err(|e| format!("write {}: {}", path.display(), e))?;
    Ok(())
}

fn verify_identity(identity: &DiskIdentity, disk: &BackupDiskConfig) -> Result<(), String> {
    if identity.version != IDENTITY_VERSION {
        return Err(format!(
            "identity version mismatch: expected {}, got {}",
            IDENTITY_VERSION, identity.version
        ));
    }
    if identity.disk_id != disk.disk_id {
        return Err(format!(
            "identity diskId mismatch: expected {}, got {}",
            disk.disk_id, identity.disk_id
        ));
    }
    if identity.fs_uuid != disk.fs_uuid {
        return Err(format!(
            "identity fsUuid mismatch: expected {}, got {}",
            disk.fs_uuid, identity.fs_uuid
        ));
    }
    Ok(())
}

fn mount_options_for_backup(disk: &BackupDiskConfig) -> String {
    disk.mount_options
        .clone()
        .unwrap_or_else(|| DEFAULT_BACKUP_MOUNT_OPTS.to_string())
}

fn mount_options_for_restore(_disk: &BackupDiskConfig) -> String {
    DEFAULT_RESTORE_MOUNT_OPTS.to_string()
}

fn is_uuid_connected(uuid: &str) -> bool {
    device_path_for_uuid(uuid).exists()
}

fn select_disk(
    disks: &[BackupDiskConfig],
    disk_id: Option<&str>,
) -> Result<BackupDiskConfig, String> {
    let connected = disks
        .iter()
        .filter(|disk| is_uuid_connected(&disk.fs_uuid))
        .map(|disk| disk.fs_uuid.clone())
        .collect::<HashSet<_>>();
    select_disk_from_connected(disks, disk_id, &connected)
}

fn select_disk_from_connected(
    disks: &[BackupDiskConfig],
    disk_id: Option<&str>,
    connected_uuids: &HashSet<String>,
) -> Result<BackupDiskConfig, String> {
    if disks.is_empty() {
        return Err("no backup disks enrolled; run `timevault disk enroll ...`".to_string());
    }
    if let Some(disk_id) = disk_id {
        let disk = disks
            .iter()
            .find(|disk| disk.disk_id == disk_id)
            .ok_or_else(|| format!("disk-id {} not found in config", disk_id))?;
        if !connected_uuids.contains(&disk.fs_uuid) {
            return Err(format!("disk-id {} not connected", disk.disk_id));
        }
        return Ok(disk.clone());
    }
    let connected: Vec<BackupDiskConfig> = disks
        .iter()
        .filter(|disk| connected_uuids.contains(&disk.fs_uuid))
        .cloned()
        .collect();
    if connected.is_empty() {
        return Err("no enrolled backup disk connected".to_string());
    }
    if connected.len() > 1 {
        return Err(
            "multiple enrolled backup disks connected; specify --disk-id".to_string(),
        );
    }
    Ok(connected[0].clone())
}

fn resolve_fs_uuid(
    fs_uuid: Option<&str>,
    device: Option<&str>,
) -> Result<String, String> {
    if let Some(uuid) = fs_uuid {
        return Ok(uuid.to_string());
    }
    if let Some(device) = device {
        let device_path = Path::new(device);
        let device_real = device_path
            .canonicalize()
            .map_err(|e| format!("resolve {}: {}", device, e))?;
        let entries = fs::read_dir("/dev/disk/by-uuid")
            .map_err(|e| format!("read /dev/disk/by-uuid: {}", e))?;
        for entry in entries {
            let entry = entry.map_err(|e| format!("read /dev/disk/by-uuid: {}", e))?;
            let link_path = entry.path();
            let target = link_path
                .canonicalize()
                .map_err(|e| format!("resolve {}: {}", link_path.display(), e))?;
            if target == device_real {
                let name = entry.file_name().to_string_lossy().to_string();
                return Ok(name);
            }
        }
        return Err(format!(
            "no filesystem UUID found for device {}",
            device
        ));
    }
    let entries = fs::read_dir("/dev/disk/by-uuid")
        .map_err(|e| format!("read /dev/disk/by-uuid: {}", e))?;
    let mut uuids = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| format!("read /dev/disk/by-uuid: {}", e))?;
        let name = entry.file_name().to_string_lossy().to_string();
        uuids.push(name);
    }
    if uuids.len() == 1 {
        return Ok(uuids[0].clone());
    }
    if uuids.is_empty() {
        return Err("no filesystem UUIDs found; specify --fs-uuid or --device".to_string());
    }
    Err("multiple filesystem UUIDs found; specify --fs-uuid or --device".to_string())
}

fn ensure_disk_not_mounted(device: &Path) -> Result<(), String> {
    if device_is_mounted(device)? {
        return Err(format!(
            "device {} is already mounted",
            device.display()
        ));
    }
    Ok(())
}

fn mount_disk_guarded(
    disk: &BackupDiskConfig,
    mount_base: &Path,
    options: &str,
) -> Result<(MountGuard, PathBuf), String> {
    let device = device_path_for_uuid(&disk.fs_uuid);
    if !device.exists() {
        return Err(format!("device {} not found", device.display()));
    }
    ensure_disk_not_mounted(&device)?;
    ensure_base_dir(mount_base)?;
    let mountpoint = mount_base.join(&disk.fs_uuid);
    if mountpoint.exists() && !mountpoint.is_dir() {
        return Err(format!(
            "mountpoint {} exists and is not a directory",
            mountpoint.display()
        ));
    }
    if !mountpoint.exists() {
        fs::create_dir_all(&mountpoint)
            .map_err(|e| format!("create {}: {}", mountpoint.display(), e))?;
        let mut perms = fs::metadata(&mountpoint)
            .map_err(|e| format!("stat {}: {}", mountpoint.display(), e))?
            .permissions();
        perms.set_mode(0o700);
        fs::set_permissions(&mountpoint, perms)
            .map_err(|e| format!("chmod {}: {}", mountpoint.display(), e))?;
    }
    if mountpoint_is_mounted(&mountpoint)? {
        return Err(format!(
            "mountpoint {} is already in use",
            mountpoint.display()
        ));
    }
    if let Err(err) = mount_device(&device, &mountpoint, options) {
        return Err(err);
    }
    Ok((
        MountGuard {
            mountpoint: mountpoint.clone(),
            remove_dir: false,
        },
        mountpoint,
    ))
}

fn identity_path(root: &Path) -> PathBuf {
    root.join(IDENTITY_FILE)
}

fn verify_identity_on_disk(root: &Path, disk: &BackupDiskConfig) -> Result<(), String> {
    let path = identity_path(root);
    if !path.exists() {
        return Err(format!(
            "identity file missing at {}; expected diskId {} fsUuid {} (run `timevault disk enroll ...`)",
            path.display(),
            disk.disk_id,
            disk.fs_uuid
        ));
    }
    let identity = read_identity(&path)
        .map_err(|e| format!("identity file invalid: {}", e))?;
    verify_identity(&identity, disk)
}

fn check_disk_empty(root: &Path, force: bool) -> Result<(), String> {
    let empty = is_disk_empty(root)?;
    if !empty && !force {
        let entries = list_entries(root)?;
        let unexpected: Vec<String> = entries
            .into_iter()
            .filter(|entry| !DISK_ADD_ALLOWED_ENTRIES.contains(&entry.as_str()))
            .collect();
        return Err(format!(
            "disk not empty; unexpected entries: {} (use --force to override)",
            unexpected.join(", ")
        ));
    }
    Ok(())
}

fn write_config(path: &str, cfg: &Config) -> Result<(), String> {
    let data = serde_yaml::to_string(cfg).map_err(|e| format!("encode config: {}", e))?;
    let mut file =
        File::create(path).map_err(|e| format!("write config {}: {}", path, e))?;
    file.write_all(data.as_bytes())
        .map_err(|e| format!("write config {}: {}", path, e))?;
    Ok(())
}

fn enroll_backup_disk(config_path: &str, args: DiskInitArgs) -> Result<(), String> {
    let mut contents = String::new();
    File::open(config_path)
        .map_err(|e| format!("open config {}: {}", config_path, e))?
        .read_to_string(&mut contents)
        .map_err(|e| format!("read config {}: {}", config_path, e))?;
    let mut cfg: Config =
        serde_yaml::from_str(&contents).map_err(|e| format!("parse config: {}", e))?;

    if !is_safe_job_name(&args.disk_id) {
        return Err(format!(
            "disk-id {} must use only letters, digits, '.', '-', '_'",
            args.disk_id
        ));
    }
    for disk in &cfg.backup_disks {
        if disk.disk_id == args.disk_id {
            return Err(format!("disk-id {} already enrolled", args.disk_id));
        }
        if let Some(fs_uuid) = args.fs_uuid.as_deref() {
            if disk.fs_uuid == fs_uuid {
                return Err(format!("fs-uuid {} already enrolled", fs_uuid));
            }
        }
    }

    let fs_uuid = resolve_fs_uuid(args.fs_uuid.as_deref(), args.device.as_deref())?;
    if cfg
        .backup_disks
        .iter()
        .any(|disk| disk.fs_uuid == fs_uuid)
    {
        return Err(format!("fs-uuid {} already enrolled", fs_uuid));
    }
    let device = device_path_for_uuid(&fs_uuid);
    if !device.exists() {
        return Err(format!("device {} not found", device.display()));
    }
    ensure_disk_not_mounted(&device)?;

    let mount_base = cfg
        .mount_base
        .clone()
        .unwrap_or_else(|| DEFAULT_MOUNT_BASE.to_string());
    let mount_base = PathBuf::from(mount_base);
    let options = DEFAULT_BACKUP_MOUNT_OPTS;
    let (guard, mountpoint) = mount_disk_guarded(
        &BackupDiskConfig {
            disk_id: args.disk_id.clone(),
            fs_uuid: fs_uuid.clone(),
            label: args.label.clone(),
            mount_options: args.mount_options.clone(),
        },
        &mount_base,
        options,
    )?;

    let result: Result<(), String> = (|| {
        let identity_file = identity_path(&mountpoint);
        if identity_file.exists() && !args.force {
            return Err("identity file already exists; use --force to reinitialize".to_string());
        }
        check_disk_empty(&mountpoint, args.force)?;
        let identity = DiskIdentity {
            version: IDENTITY_VERSION,
            disk_id: args.disk_id.clone(),
            fs_uuid: fs_uuid.clone(),
            created: Utc::now().to_rfc3339(),
        };
        write_identity(&identity_file, &identity)?;
        Ok(())
    })();

    drop(guard);
    result?;

    cfg.backup_disks.push(BackupDiskConfig {
        disk_id: args.disk_id,
        fs_uuid,
        label: args.label,
        mount_options: args.mount_options,
    });
    write_config(config_path, &cfg)?;
    Ok(())
}

fn mount_for_restore(cfg: &RuntimeConfig, args: MountArgs) -> Result<PathBuf, String> {
    let disk = select_disk(&cfg.backup_disks, args.disk_id.as_deref())?;
    let device = device_path_for_uuid(&disk.fs_uuid);
    if !device.exists() {
        return Err(format!("device {} not found", device.display()));
    }
    ensure_disk_not_mounted(&device)?;

    ensure_base_dir(&cfg.user_mount_base)?;
    let mountpoint = create_temp_dir(&cfg.user_mount_base, "tv")?;

    if mountpoint_is_mounted(&mountpoint)? {
        return Err(format!(
            "mountpoint {} is already in use",
            mountpoint.display()
        ));
    }

    let options = mount_options_for_restore(&disk);
    mount_device(&device, &mountpoint, &options)?;
    if let Err(err) = verify_identity_on_disk(&mountpoint, &disk) {
        let _ = unmount_path(&mountpoint);
        return Err(err);
    }
    Ok(mountpoint)
}

fn base_block_device_name(dev: &Path) -> Option<String> {
    let name = dev.file_name()?.to_string_lossy();
    let s = name.as_ref();
    if (s.starts_with("nvme") || s.starts_with("mmcblk")) && s.contains('p') {
        if let Some(pos) = s.rfind('p') {
            if pos + 1 < s.len() && s[pos + 1..].chars().all(|c| c.is_ascii_digit()) {
                return Some(s[..pos].to_string());
            }
        }
    }
    let trimmed = s.trim_end_matches(|c: char| c.is_ascii_digit());
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn is_removable_device(device: &Path) -> Option<bool> {
    let base = base_block_device_name(device)?;
    let path = Path::new("/sys/block").join(base).join("removable");
    let value = fs::read_to_string(path).ok()?;
    match value.trim() {
        "1" => Some(true),
        "0" => Some(false),
        _ => None,
    }
}

fn is_raid_member(device: &Path) -> bool {
    let name = match device.file_name() {
        Some(name) => name.to_string_lossy().to_string(),
        None => return false,
    };
    let base = match base_block_device_name(device) {
        Some(base) => base,
        None => return false,
    };
    let entries = match fs::read_dir("/sys/block") {
        Ok(entries) => entries,
        Err(_) => return false,
    };
    for entry in entries.flatten() {
        let md_name = entry.file_name().to_string_lossy().to_string();
        if !md_name.starts_with("md") {
            continue;
        }
        let slaves = entry.path().join("slaves");
        let slaves_entries = match fs::read_dir(slaves) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for slave in slaves_entries.flatten() {
            let slave_name = slave.file_name().to_string_lossy().to_string();
            if slave_name == name || slave_name == base {
                return true;
            }
        }
    }
    false
}

fn list_backup_candidates(cfg: &RuntimeConfig) -> Result<(), String> {
    let enrolled: HashSet<String> = cfg
        .backup_disks
        .iter()
        .map(|disk| disk.fs_uuid.clone())
        .collect();
    let mut candidates: Vec<(
        String,
        PathBuf,
        Option<PathBuf>,
        Option<bool>,
        Option<bool>,
        Vec<&'static str>,
        Option<DiskIdentity>,
        bool,
    )> = Vec::new();
    let mut swap_devices = HashSet::new();
    if let Ok(contents) = fs::read_to_string("/proc/swaps") {
        for line in contents.lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.is_empty() {
                continue;
            }
            let path = Path::new(fields[0]);
            if let Ok(real) = path.canonicalize() {
                swap_devices.insert(real);
            }
        }
    }
    let path = Path::new("/dev/disk/by-uuid");
    let entries =
        fs::read_dir(path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("read {}: {}", path.display(), e))?;
        let uuid = entry.file_name().to_string_lossy().to_string();
        let device = entry
            .path()
            .canonicalize()
            .map_err(|e| format!("resolve {}: {}", entry.path().display(), e))?;
        if is_raid_member(&device) {
            continue;
        }
        if swap_devices.contains(&device) {
            continue;
        }
        if is_fat_filesystem(&device) {
            continue;
        }
        let is_enrolled = enrolled.contains(&uuid);
        let mut temp_mount: Option<PathBuf> = None;
        let mut mounted_path: Option<PathBuf> = None;
        let mountpoint = match find_device_mountpoint(&device)? {
            Some(path) => {
                mounted_path = Some(path.clone());
                path
            }
            None => {
                ensure_base_dir(&cfg.user_mount_base)?;
                let probe = create_temp_dir(&cfg.user_mount_base, "discover")?;
                if let Err(_err) = mount_device_silent(&device, &probe, DEFAULT_RESTORE_MOUNT_OPTS) {
                    let removable = is_removable_device(&device);
                    if removable == Some(true) {
                        candidates.push((
                            uuid,
                            device,
                            None,
                            None,
                            removable,
                            vec!["removable", "probe-failed"],
                            None,
                            is_enrolled,
                        ));
                    }
                    continue;
                }
                temp_mount = Some(probe.clone());
                probe
            }
        };
        let empty = is_disk_empty(&mountpoint).ok();
        let identity_file = identity_path(&mountpoint);
        let identity = if identity_file.exists() {
            read_identity(&identity_file).ok()
        } else {
            None
        };
        let removable = is_removable_device(&device);
        let mut reasons = Vec::new();
        if removable == Some(true) {
            reasons.push("removable");
        }
        if empty == Some(true) {
            reasons.push("mounted-empty");
        }
        if identity.is_some() {
            reasons.push("timevault-identity");
        }
        if is_enrolled {
            reasons.push("enrolled");
        }
        if let Some(temp) = temp_mount {
            let _ = unmount_path(&temp);
            let _ = fs::remove_dir(&temp);
        }
        if reasons.is_empty() {
            continue;
        }
        candidates.push((
            uuid,
            device,
            mounted_path,
            empty,
            removable,
            reasons,
            identity,
            is_enrolled,
        ));
    }
    if candidates.is_empty() {
        println!("no candidate backup devices found");
        return Ok(());
    }
    for (uuid, device, mountpoint, empty, removable, reasons, identity, enrolled) in candidates {
        println!("uuid: {}", uuid);
        println!("  device: {}", device.display());
        if let Some(mp) = mountpoint {
            println!("  mounted: {}", mp.display());
        } else {
            println!("  mounted: no");
        }
        println!("  enrolled: {}", if enrolled { "yes" } else { "no" });
        if let Some(identity) = identity {
            println!("  identity.diskId: {}", identity.disk_id);
            println!("  identity.fsUuid: {}", identity.fs_uuid);
            println!("  identity.created: {}", identity.created);
        }
        match empty {
            Some(value) => println!("  empty: {}", if value { "yes" } else { "no" }),
            None => println!("  empty: unknown"),
        }
        match removable {
            Some(value) => println!("  removable: {}", if value { "yes" } else { "no" }),
            None => println!("  removable: unknown"),
        }
        println!("  reason: {}", reasons.join(", "));
        println!();
    }
    Ok(())
}

fn find_timevault_mounts(base: &Path) -> Result<Vec<PathBuf>, String> {
    let contents =
        fs::read_to_string("/proc/self/mounts").map_err(|e| format!("read /proc/self/mounts: {}", e))?;
    let mut mounts = Vec::new();
    for line in contents.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            continue;
        }
        let mountpoint = Path::new(fields[1]);
        if mountpoint.starts_with(base) {
            mounts.push(mountpoint.to_path_buf());
        }
    }
    Ok(mounts)
}

fn umount_restore(cfg: &RuntimeConfig, _args: UmountArgs) -> Result<(), String> {
    let mounts = find_timevault_mounts(&cfg.user_mount_base)?;
    if mounts.is_empty() {
        return Err("no timevault mounts found".to_string());
    }
    if mounts.len() > 1 {
        return Err("multiple timevault mounts found; unmount manually".to_string());
    }
    let mountpoint = mounts[0].clone();
    unmount_path(&mountpoint)?;
    if mountpoint.starts_with(&cfg.user_mount_base) {
        let _ = fs::remove_dir(&mountpoint);
    }
    Ok(())
}

fn expire_old_backups(job: &Job, dest: &Path, run_mode: RunMode) -> io::Result<()> {
    if !dest.exists() {
        return Ok(());
    }
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

fn copy_snapshot_without_symlinks(
    source: &Path,
    dest: &Path,
    run_mode: RunMode,
) -> io::Result<()> {
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
                println!(
                    "dry-run: ln {} {}",
                    src_path.display(),
                    target.display()
                );
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

fn resolve_job_dest(job: &Job, disk_mount: &Path) -> Result<PathBuf, String> {
    if !is_safe_job_name(&job.name) {
        return Err(format!(
            "job {} name must use only letters, digits, '.', '-', '_'",
            job.name
        ));
    }
    Ok(disk_mount.join(&job.name))
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

fn print_banner() {
    println!("Timevault {}", VERSION);
}

fn print_copyright() {
    println!("{}", COPYRIGHT);
}

fn print_help() {
    println!("Usage:");
    println!("  timevault [backup] [options]");
    println!("  timevault --disk-discover");
    println!("  timevault --disk-enroll --disk-id <id> [--fs-uuid <uuid> | --device <path>] [--label <label>] [--mount-options <opts>] [--force]");
    println!("  timevault disk enroll --disk-id <id> [--fs-uuid <uuid> | --device <path>] [--label <label>] [--mount-options <opts>] [--force]");
    println!("  timevault disk discover");
    println!("  timevault mount [--disk-id <id>]");
    println!("  timevault umount");
    println!("  timevault --version");
    println!();
    println!("Options:");
    println!("  --config <path>        Config file path");
    println!("  --job <name>           Run only selected job(s)");
    println!("  --dry-run              Do not write data");
    println!("  --safe                 Do not delete files");
    println!("  --verbose              Verbose logging");
    println!("  --print-order          Print resolved job order and exit");
    println!("  --rsync <args...>      Pass remaining args to rsync");
    println!("  --disk-id <id>         Select enrolled backup disk");
    println!("  --fs-uuid <uuid>       Filesystem UUID (disk enroll)");
    println!("  --device <path>        Block device path (disk enroll)");
    println!("  --label <label>        Optional disk label (disk enroll)");
    println!("  --mount-options <opt>  Mount options (disk enroll)");
    println!("  --force                Force disk enroll on non-empty root or existing identity");
}

fn exit_for_disk_error(message: &str) -> ! {
    let code = if message == "no enrolled backup disk connected" {
        EXIT_NO_DISK
    } else if message.starts_with("multiple enrolled backup disks connected") {
        EXIT_MULTI_DISK
    } else if message.starts_with("identity ") {
        EXIT_IDENTITY_MISMATCH
    } else if message.starts_with("disk not empty") {
        EXIT_DISK_NOT_EMPTY
    } else if message.starts_with("mount ")
        || message.starts_with("umount ")
        || message.contains("mount failed")
    {
        EXIT_MOUNT_FAILURE
    } else {
        2
    };
    println!("{}", message);
    std::process::exit(code);
}

fn acquire_lock_for_job(job_name: &str, run_mode: RunMode) -> io::Result<Option<LockGuard>> {
    if run_mode.dry_run {
        return Ok(None);
    }
    let path = lock_path_for_job(job_name)?;
    match lock_file(&path) {
        Ok(true) => Ok(Some(LockGuard { path })),
        Ok(false) => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("job {} is already running", job_name),
        )),
        Err(e) => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("failed to lock {}: {}", path, e),
        )),
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
    let excludes = if job.excludes.is_empty() {
        "<none>".to_string()
    } else {
        job.excludes.join(", ")
    };
    println!("job: {}", job.name);
    println!("  source: {}", job.source);
    println!("  backup dir: {}", job.name);
    println!("  copies: {}", job.copies);
    println!("  run: {}", run_policy_label(job.run_policy));
    println!("  excludes: {}", excludes);
}

fn backup(
    jobs: Vec<Job>,
    rsync_extra: &[String],
    run_mode: RunMode,
    disk_mount: &Path,
) -> io::Result<()> {
    for job in jobs {
        let _lock = acquire_lock_for_job(&job.name, run_mode)?;
        let home = env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let tmp_dir = Path::new(&home).join("tmp");
        if !run_mode.dry_run {
            fs::create_dir_all(&tmp_dir)?;
        }
        let excludes_file = tmp_dir.join("timevault.excludes");
        if run_mode.dry_run {
            println!("dry-run: would write excludes file {}", excludes_file.display());
        } else {
            create_excludes_file(&job, &excludes_file)?;
        }

        let backup_day = (Local::now() - Duration::days(1)).format("%Y%m%d").to_string();
        if run_mode.verbose {
            println!("  backup day: {}", backup_day);
        }

        let dest = match resolve_job_dest(&job, disk_mount) {
            Ok(dest) => dest,
            Err(err) => {
                println!("skip job {}: {}", job.name, err);
                continue;
            }
        };
        if run_mode.verbose {
            let policy = match job.run_policy {
                RunPolicy::Auto => "auto",
                RunPolicy::Demand => "demand",
                RunPolicy::Off => "off",
            };
            println!("job: {}", job.name);
            println!("  run: {}", policy);
            println!("  source: {}", job.source);
            println!("  backup dir: {}", dest.display());
            println!("  copies: {}", job.copies);
            println!("  excludes: {}", job.excludes.len());
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
        for attempt in 1..=3 {
            rc = run_nice_ionice(&rsync_args, run_mode)?;
            if rc == 0 || rc == 24 {
                break;
            }
            if attempt < 3 {
                println!("rsync failed with exit code {}; retrying ({}/3)", rc, attempt + 1);
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
    let mut selected_jobs: Vec<String> = Vec::new();
    let mut print_order = false;
    let mut show_version = false;
    let mut show_help = false;
    let mut rsync_passthrough = false;
    let mut command = CommandKind::Backup;
    let mut disk_subcommand: Option<String> = None;
    let mut selected_disk_id: Option<String> = None;
    let mut disk_init_args = DiskInitArgs {
        disk_id: String::new(),
        fs_uuid: None,
        device: None,
        label: None,
        mount_options: None,
        force: false,
    };
    let mut mount_args = MountArgs { disk_id: None };
    let umount_args = UmountArgs {};
    let mut args = env::args().skip(1).peekable();
    while let Some(arg) = args.next() {
        if rsync_passthrough {
            rsync_extra.push(arg);
            continue;
        }
        if arg == "disk" {
            command = CommandKind::DiskEnroll;
            continue;
        } else if arg == "--disk-enroll" {
            command = CommandKind::DiskEnroll;
            disk_subcommand = Some("enroll".to_string());
            continue;
        } else if arg == "--disk-discover" {
            command = CommandKind::DiskEnroll;
            disk_subcommand = Some("discover".to_string());
            continue;
        } else if arg == "mount" {
            command = CommandKind::Mount;
            continue;
        } else if arg == "umount" {
            command = CommandKind::Umount;
            continue;
        } else if arg == "backup" {
            command = CommandKind::Backup;
            continue;
        }
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
        } else if arg == "--print-order" {
            print_order = true;
            continue;
        } else if arg == "--version" {
            show_version = true;
            continue;
        } else if arg == "--help" || arg == "-h" {
            show_help = true;
            continue;
        } else if arg == "--rsync" {
            rsync_passthrough = true;
            continue;
        } else if arg == "--disk-id" {
            match args.next() {
                Some(value) => {
                    if matches!(command, CommandKind::DiskEnroll) {
                        disk_init_args.disk_id = value;
                    } else {
                        selected_disk_id = Some(value.clone());
                        mount_args.disk_id = Some(value);
                    }
                }
                None => {
                    println!("--disk-id requires a value");
                    std::process::exit(2);
                }
            }
            continue;
        } else if matches!(command, CommandKind::DiskEnroll) && arg == "--fs-uuid" {
            match args.next() {
                Some(value) => disk_init_args.fs_uuid = Some(value),
                None => {
                    println!("--fs-uuid requires a value");
                    std::process::exit(2);
                }
            }
            continue;
        } else if matches!(command, CommandKind::DiskEnroll)
            && (arg == "--device" || arg == "--block-id")
        {
            match args.next() {
                Some(value) => disk_init_args.device = Some(value),
                None => {
                    println!("--device/--block-id requires a value");
                    std::process::exit(2);
                }
            }
            continue;
        } else if matches!(command, CommandKind::DiskEnroll) && arg == "--label" {
            match args.next() {
                Some(value) => disk_init_args.label = Some(value),
                None => {
                    println!("--label requires a value");
                    std::process::exit(2);
                }
            }
            continue;
        } else if matches!(command, CommandKind::DiskEnroll) && arg == "--mount-options" {
            match args.next() {
                Some(value) => disk_init_args.mount_options = Some(value),
                None => {
                    println!("--mount-options requires a value");
                    std::process::exit(2);
                }
            }
            continue;
        } else if matches!(command, CommandKind::DiskEnroll) && arg == "--force" {
            disk_init_args.force = true;
            continue;
        }
        if arg.starts_with('-') {
            println!("unknown option {}", arg);
            std::process::exit(2);
        }
        if matches!(command, CommandKind::DiskEnroll) && disk_subcommand.is_none() {
            disk_subcommand = Some(arg);
            continue;
        }
        if matches!(command, CommandKind::Backup) {
            rsync_extra.push(arg);
        } else {
            println!("unexpected argument {}", arg);
            std::process::exit(2);
        }
    }

    print_banner();
    if show_help {
        print_help();
        return Ok(());
    }
    if show_version {
        print_copyright();
        println!("Project: {}", PROJECT_URL);
        println!("License: {}", LICENSE_NAME);
        return Ok(());
    }

    match command {
        CommandKind::DiskEnroll => match disk_subcommand.as_deref() {
            Some("enroll") | None => {
                if disk_init_args.disk_id.trim().is_empty() {
                    println!("disk enroll requires --disk-id");
                    std::process::exit(2);
                }
                if let Err(err) = enroll_backup_disk(&config_path, disk_init_args) {
                    exit_for_disk_error(&err);
                }
                return Ok(());
            }
            Some("discover") => {
                let cfg = match get_config(&config_path) {
                    Ok(cfg) => cfg,
                    Err(e) => {
                        println!("failed to load config {}: {}", config_path, e);
                        std::process::exit(2);
                    }
                };
                if let Err(err) = list_backup_candidates(&cfg) {
                    println!("{}", err);
                    std::process::exit(2);
                }
                return Ok(());
            }
            Some(_) => {
                println!("missing or invalid disk subcommand (expected: enroll or discover)");
                std::process::exit(2);
            }
        },
        CommandKind::Mount => {
            let cfg = match get_config(&config_path) {
                Ok(cfg) => cfg,
                Err(e) => {
                    println!("failed to load config {}: {}", config_path, e);
                    std::process::exit(2);
                }
            };
            if let Some(id) = selected_disk_id {
                mount_args.disk_id = Some(id);
            }
            match mount_for_restore(&cfg, mount_args) {
                Ok(path) => {
                    println!("{}", path.display());
                    return Ok(());
                }
                Err(err) => {
                    exit_for_disk_error(&err);
                }
            }
        }
        CommandKind::Umount => {
            let cfg = match get_config(&config_path) {
                Ok(cfg) => cfg,
                Err(e) => {
                    println!("failed to load config {}: {}", config_path, e);
                    std::process::exit(2);
                }
            };
            if let Err(err) = umount_restore(&cfg, umount_args) {
                exit_for_disk_error(&err);
            }
            return Ok(());
        }
        CommandKind::Backup => {}
    }

    println!("{}", Local::now().format("%d-%m-%Y %H:%M"));

    let cfg = match get_config(&config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            println!("failed to load config {}: {}", config_path, e);
            std::process::exit(2);
        }
    };

    let jobs = cfg.jobs.clone();
    let backup_disks = cfg.backup_disks.clone();
    let mount_base = cfg.mount_base.clone();
    let selected_set: HashSet<String> = selected_jobs.iter().cloned().collect();
    let mut jobs_by_name = std::collections::HashMap::new();
    for job in &jobs {
        jobs_by_name.insert(job.name.clone(), job.clone());
    }
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
                println!("job not found: {}", name);
            }
            println!("no such job(s) found; aborting");
            std::process::exit(2);
        }
    }
    let mut jobs_to_run = Vec::new();
    if selected_set.is_empty() {
        for job in &jobs {
            if job.run_policy == RunPolicy::Auto {
                jobs_to_run.push(job.clone());
            }
        }
    } else {
        for job in &jobs {
            if selected_set.contains(&job.name) {
                if job.run_policy == RunPolicy::Off {
                    println!("job disabled (off): {}", job.name);
                    println!("requested job(s) are disabled; aborting");
                    std::process::exit(2);
                }
                jobs_to_run.push(job.clone());
            }
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
            config_path,
            jobs_to_run.len()
        );
    }
    if backup_disks.is_empty() {
        println!("no backup disks enrolled; run `timevault disk enroll ...`");
        std::process::exit(2);
    }
    let selected_disk = match select_disk(&backup_disks, selected_disk_id.as_deref()) {
        Ok(disk) => disk,
        Err(err) => {
            exit_for_disk_error(&err);
        }
    };
    let options = mount_options_for_backup(&selected_disk);
    let (disk_guard, mountpoint) = match mount_disk_guarded(
        &selected_disk,
        &mount_base,
        &options,
    ) {
        Ok(result) => result,
        Err(err) => {
            exit_for_disk_error(&err);
        }
    };
    if let Err(err) = verify_identity_on_disk(&mountpoint, &selected_disk) {
        drop(disk_guard);
        exit_for_disk_error(&err);
    }
    let backup_result = backup(
        jobs_to_run,
        &rsync_extra,
        run_mode,
        &mountpoint,
    );
    drop(disk_guard);
    if let Err(e) = backup_result {
        let message = e.to_string();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_config_with_backup_disks() {
        let cfg = Config {
            jobs: vec![JobConfig {
                name: "primary".to_string(),
                source: "/".to_string(),
                copies: 2,
                run: "auto".to_string(),
                excludes: vec![],
            }],
            excludes: vec![],
            backup_disks: vec![BackupDiskConfig {
                disk_id: "disk1".to_string(),
                fs_uuid: "uuid-1".to_string(),
                label: None,
                mount_options: None,
            }],
            mount_base: None,
            user_mount_base: None,
        };
        let runtime = parse_config_struct(cfg).unwrap();
        assert_eq!(runtime.backup_disks.len(), 1);
        assert_eq!(runtime.backup_disks[0].disk_id, "disk1");
    }

    #[test]
    fn verify_identity_matches() {
        let disk = BackupDiskConfig {
            disk_id: "disk1".to_string(),
            fs_uuid: "uuid-1".to_string(),
            label: None,
            mount_options: None,
        };
        let identity = DiskIdentity {
            version: IDENTITY_VERSION,
            disk_id: "disk1".to_string(),
            fs_uuid: "uuid-1".to_string(),
            created: "2025-01-01T00:00:00Z".to_string(),
        };
        assert!(verify_identity(&identity, &disk).is_ok());
    }

    #[test]
    fn select_disk_with_connected_uuids() {
        let disks = vec![
            BackupDiskConfig {
                disk_id: "a".to_string(),
                fs_uuid: "uuid-a".to_string(),
                label: None,
                mount_options: None,
            },
            BackupDiskConfig {
                disk_id: "b".to_string(),
                fs_uuid: "uuid-b".to_string(),
                label: None,
                mount_options: None,
            },
        ];
        let connected = ["uuid-b".to_string()].into_iter().collect();
        let selected = select_disk_from_connected(&disks, None, &connected).unwrap();
        assert_eq!(selected.disk_id, "b");
    }
}
