use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::model::BackupDiskConfig;
use crate::disk::fs_type::{detect_fs_type, FsType};
use crate::disk::identity::{identity_path, read_identity, DiskIdentity};
use crate::disk::{DEFAULT_RESTORE_MOUNT_OPTS};
use crate::error::{Result, TimevaultError};
use crate::mount::inspect::find_device_mountpoint;
use crate::mount::ops::mount_device_silent;
use crate::util::paths::create_temp_dir;

#[derive(Debug, Clone)]
pub struct DiskCandidate {
    pub uuid: String,
    pub device: PathBuf,
    pub mounted_at: Option<PathBuf>,
    pub empty: Option<bool>,
    pub removable: Option<bool>,
    pub reasons: Vec<String>,
    pub identity: Option<DiskIdentity>,
    pub enrolled: bool,
    pub fs_type: Option<FsType>,
}

pub fn list_candidates(
    enrolled_disks: &[BackupDiskConfig],
    user_mount_base: &Path,
) -> Result<Vec<DiskCandidate>> {
    let enrolled: HashSet<String> = enrolled_disks
        .iter()
        .map(|disk| disk.fs_uuid.clone())
        .collect();
    let mut candidates = Vec::new();
    let swap_devices = load_swap_devices();

    let entries = fs::read_dir("/dev/disk/by-uuid")
        .map_err(|e| TimevaultError::message(format!("read /dev/disk/by-uuid: {}", e)))?;
    for entry in entries {
        let entry = entry
            .map_err(|e| TimevaultError::message(format!("read /dev/disk/by-uuid: {}", e)))?;
        let uuid = entry.file_name().to_string_lossy().to_string();
        let device = entry
            .path()
            .canonicalize()
            .map_err(|e| TimevaultError::message(format!("resolve {}: {}", entry.path().display(), e)))?;

        if swap_devices.contains(&device) {
            continue;
        }
        if is_raid_member(&device) {
            continue;
        }
        let fs_type = detect_fs_type(device.to_string_lossy().as_ref()).ok();
        if let Some(fs_type) = &fs_type {
            if fs_type.is_rejected() || !fs_type.is_allowed() {
                continue;
            }
        }

        let enrolled_flag = enrolled.contains(&uuid);
        let mut temp_mount: Option<PathBuf> = None;
        let mut mounted_path: Option<PathBuf> = None;
        let mountpoint = match find_device_mountpoint(&device)? {
            Some(path) => {
                mounted_path = Some(path.clone());
                path
            }
            None => {
                let probe = create_temp_dir(user_mount_base, "discover")?;
                if mount_device_silent(&device, &probe, DEFAULT_RESTORE_MOUNT_OPTS).is_err() {
                    let removable = is_removable_device(&device);
                    let mut reasons = Vec::new();
                    if removable == Some(true) {
                        reasons.push("removable".to_string());
                        reasons.push("probe-failed".to_string());
                    }
                    if !reasons.is_empty() {
                        candidates.push(DiskCandidate {
                            uuid,
                            device,
                            mounted_at: None,
                            empty: None,
                            removable,
                            reasons,
                            identity: None,
                            enrolled: enrolled_flag,
                            fs_type,
                        });
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
            reasons.push("removable".to_string());
        }
        if empty == Some(true) {
            reasons.push("mounted-empty".to_string());
        }
        if identity.is_some() {
            reasons.push("timevault-identity".to_string());
        }
        if enrolled_flag {
            reasons.push("enrolled".to_string());
        }
        if let Some(temp) = temp_mount {
            let _ = crate::mount::ops::unmount_path(&temp);
            let _ = fs::remove_dir(&temp);
        }
        if reasons.is_empty() {
            continue;
        }
        candidates.push(DiskCandidate {
            uuid,
            device,
            mounted_at: mounted_path,
            empty,
            removable,
            reasons,
            identity,
            enrolled: enrolled_flag,
            fs_type,
        });
    }

    Ok(candidates)
}

fn load_swap_devices() -> HashSet<PathBuf> {
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
    swap_devices
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

fn is_disk_empty(root: &Path) -> Result<bool> {
    let entries = crate::util::paths::list_entries(root)?;
    for entry in entries {
        if crate::disk::DISK_ADD_ALLOWED_ENTRIES.contains(&entry.as_str()) {
            continue;
        }
        return Ok(false);
    }
    Ok(true)
}
