use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use chrono::Utc;
use tempfile::Builder;

use crate::cli::args::DiskAddArgs;
use crate::config::model::{BackupDiskConfig, Config};
use crate::config::save::save_config;
use crate::disk::discovery::list_candidates;
use crate::disk::fs_type::detect_fs_type;
use crate::disk::identity::{identity_path, write_identity, DiskIdentity, IDENTITY_VERSION};
use crate::disk::{device_path_for_uuid, ensure_disk_not_mounted, resolve_fs_uuid, DISK_ADD_ALLOWED_ENTRIES};
use crate::error::{DiskError, Result, TimevaultError};
use crate::mount::guard::MountGuard;
use crate::mount::ops::mount_device;
use crate::types::DiskId;
use crate::util::paths::ensure_base_dir;

pub fn run_enroll(config_path: &Path, disk_id: Option<&str>, args: DiskAddArgs) -> Result<()> {
    let disk_id_raw = disk_id.unwrap_or_default().to_string();
    if disk_id_raw.trim().is_empty() {
        println!("disk enroll requires --disk-id");
        std::process::exit(2);
    }
    let disk_id = match disk_id_raw.parse::<DiskId>() {
        Ok(disk_id) => disk_id,
        Err(_) => {
            println!(
                "disk-id {} must use only letters, digits, '.', '-', '_'",
                disk_id_raw
            );
            std::process::exit(2);
        }
    };

    let mut contents = String::new();
    File::open(config_path)
        .map_err(|e| TimevaultError::message(format!("open config {}: {}", config_path.display(), e)))?
        .read_to_string(&mut contents)
        .map_err(|e| TimevaultError::message(format!("read config {}: {}", config_path.display(), e)))?;
    let mut cfg: Config = serde_yaml::from_str(&contents)
        .map_err(|e| TimevaultError::message(format!("parse config: {}", e)))?;

    for disk in &cfg.backup_disks {
        if disk.disk_id == disk_id.as_str() {
            return Err(DiskError::Other(format!("disk-id {} already enrolled", disk_id.as_str())).into());
        }
        if let Some(fs_uuid) = args.fs_uuid.as_deref() {
            if disk.fs_uuid == fs_uuid {
                return Err(DiskError::Other(format!("fs-uuid {} already enrolled", fs_uuid)).into());
            }
        }
    }

    let fs_uuid = resolve_fs_uuid(args.fs_uuid.as_deref(), args.device.as_deref())?;
    if cfg
        .backup_disks
        .iter()
        .any(|disk| disk.fs_uuid == fs_uuid.as_str())
    {
        return Err(DiskError::Other(format!("fs-uuid {} already enrolled", fs_uuid)).into());
    }

    let device = device_path_for_uuid(fs_uuid.as_str());
    if !device.exists() {
        return Err(DiskError::Other(format!("device {} not found", device.display())).into());
    }
    ensure_disk_not_mounted(&device)?;

    let fs_type = detect_fs_type(device.to_string_lossy().as_ref())?;
    if fs_type.is_rejected() || !fs_type.is_allowed() {
        return Err(DiskError::Other(format!(
            "unsupported filesystem type {}",
            fs_type
        ))
        .into());
    }

    let mount_base = cfg
        .mount_base
        .clone()
        .unwrap_or_else(|| "/run/timevault/mounts".to_string());
    let mount_base = PathBuf::from(mount_base);
    ensure_base_dir(&mount_base)?;
    let temp_dir = Builder::new()
        .prefix("add-")
        .tempdir_in(&mount_base)
        .map_err(|e| TimevaultError::message(format!("create {}: {}", mount_base.display(), e)))?;
    let mountpoint = temp_dir.path().to_path_buf();

    mount_device(&device, &mountpoint, "rw,nodev,nosuid,noexec")?;
    let guard = MountGuard::new(mountpoint.clone(), false);

    let identity_file = identity_path(&mountpoint);
    if identity_file.exists() && !args.force {
        return Err(DiskError::Other(
            "identity file already exists; use --force to reinitialize".to_string(),
        )
        .into());
    }
    let empty = is_disk_empty(&mountpoint)?;
    if !empty && !args.force {
        let entries = list_unexpected_entries(&mountpoint)?;
        return Err(DiskError::DiskNotEmpty(entries.join(", ")).into());
    }

    let identity = DiskIdentity {
        version: IDENTITY_VERSION,
        disk_id: disk_id.as_str().to_string(),
        fs_uuid: fs_uuid.as_str().to_string(),
        fs_type: Some(fs_type),
        created: Utc::now().to_rfc3339(),
    };
    write_identity(&identity_file, &identity)?;
    drop(guard);

    cfg.backup_disks.push(BackupDiskConfig {
        disk_id: disk_id.as_str().to_string(),
        fs_uuid: fs_uuid.as_str().to_string(),
        label: args.label,
        mount_options: args.mount_options,
    });
    save_config(config_path.to_string_lossy().as_ref(), &cfg)?;
    Ok(())
}

pub fn run_discover(config_path: &Path) -> Result<()> {
    let cfg = crate::config::load::load_config(config_path.to_string_lossy().as_ref())?;
    let candidates = list_candidates(&cfg.backup_disks, Path::new(&cfg.user_mount_base))?;
    if candidates.is_empty() {
        println!("no candidate backup devices found");
        return Ok(());
    }
    for candidate in candidates {
        println!("uuid: {}", candidate.uuid);
        println!("  device: {}", candidate.device.display());
        if let Some(mp) = candidate.mounted_at {
            println!("  mounted: {}", mp.display());
        } else {
            println!("  mounted: no");
        }
        println!("  enrolled: {}", if candidate.enrolled { "yes" } else { "no" });
        if let Some(identity) = candidate.identity {
            println!("  identity.diskId: {}", identity.disk_id);
            println!("  identity.fsUuid: {}", identity.fs_uuid);
            if let Some(fs_type) = identity.fs_type {
                println!("  identity.fsType: {}", fs_type);
            }
            println!("  identity.created: {}", identity.created);
        }
        if let Some(fs_type) = candidate.fs_type {
            println!("  fsType: {}", fs_type);
        }
        match candidate.empty {
            Some(value) => println!("  empty: {}", if value { "yes" } else { "no" }),
            None => println!("  empty: unknown"),
        }
        match candidate.removable {
            Some(value) => println!("  removable: {}", if value { "yes" } else { "no" }),
            None => println!("  removable: unknown"),
        }
        println!("  reason: {}", candidate.reasons.join(", "));
        println!();
    }
    Ok(())
}

fn is_disk_empty(root: &Path) -> Result<bool> {
    let entries = crate::util::paths::list_entries(root)?;
    for entry in entries {
        if DISK_ADD_ALLOWED_ENTRIES.contains(&entry.as_str()) {
            continue;
        }
        return Ok(false);
    }
    Ok(true)
}

fn list_unexpected_entries(root: &Path) -> Result<Vec<String>> {
    let entries = crate::util::paths::list_entries(root)?;
    Ok(entries
        .into_iter()
        .filter(|entry| !DISK_ADD_ALLOWED_ENTRIES.contains(&entry.as_str()))
        .collect())
}
