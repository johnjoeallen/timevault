use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use chrono::Utc;
use tempfile::Builder;

use crate::cli::args::{DiskAddArgs, DiskUnenrollArgs};
use crate::config::model::{BackupDiskConfig, Config};
use crate::config::save::save_config;
use crate::disk::discovery::list_candidates;
use crate::disk::fs_type::detect_fs_type;
use crate::disk::identity::{identity_path, read_identity, write_identity, DiskIdentity, IDENTITY_VERSION};
use crate::disk::{device_path_for_uuid, ensure_disk_not_mounted, mount_disk_guarded, mount_options_for_backup, resolve_fs_uuid, DEFAULT_BACKUP_MOUNT_OPTS, DISK_ADD_ALLOWED_ENTRIES};
use crate::error::{DiskError, Result, TimevaultError};
use crate::mount::guard::MountGuard;
use crate::mount::ops::mount_device;
use crate::types::DiskId;
use crate::util::paths::{create_temp_dir, ensure_base_dir};

pub fn run_enroll(config_path: &Path, disk_id: Option<&str>, args: DiskAddArgs) -> Result<()> {
    let disk_id_arg = disk_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let mut contents = String::new();
    File::open(config_path)
        .map_err(|e| TimevaultError::message(format!("open config {}: {}", config_path.display(), e)))?
        .read_to_string(&mut contents)
        .map_err(|e| TimevaultError::message(format!("read config {}: {}", config_path.display(), e)))?;
    let mut cfg: Config = serde_yaml::from_str(&contents)
        .map_err(|e| TimevaultError::message(format!("parse config: {}", e)))?;

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
    let (disk_id_raw, existing_identity) = if identity_file.exists() {
        let identity = read_identity(&identity_file)?;
        if identity.fs_uuid != fs_uuid.as_str() {
            return Err(DiskError::Other(format!(
                "fsUuid mismatch: expected {}, got {}",
                fs_uuid.as_str(),
                identity.fs_uuid
            ))
            .into());
        }
        let disk_id_raw = match &disk_id_arg {
            Some(disk_id_arg) if !args.force => {
                if disk_id_arg != &identity.disk_id {
                    return Err(DiskError::Other(format!(
                        "disk-id {} does not match identity disk-id {} (use --force to reinitialize)",
                        disk_id_arg, identity.disk_id
                    ))
                    .into());
                }
                identity.disk_id
            }
            Some(disk_id_arg) => disk_id_arg.clone(),
            None => identity.disk_id,
        };
        (disk_id_raw, !args.force)
    } else {
        let disk_id_raw = match disk_id_arg {
            Some(disk_id_raw) => disk_id_raw,
            None => {
                println!("disk enroll requires --disk-id");
                std::process::exit(2);
            }
        };
        (disk_id_raw, false)
    };

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

    for disk in &cfg.backup_disks {
        if disk.disk_id == disk_id.as_str() {
            return Err(DiskError::Other(format!("disk-id {} already enrolled", disk_id.as_str())).into());
        }
    }

    if !existing_identity {
        let empty = is_disk_empty(&mountpoint)?;
        if !empty && !args.force {
            let entries = list_unexpected_entries(&mountpoint)?;
            return Err(DiskError::DiskNotEmpty(entries.join(", ")).into());
        }
    }

    let identity = DiskIdentity {
        version: IDENTITY_VERSION,
        disk_id: disk_id.as_str().to_string(),
        fs_uuid: fs_uuid.as_str().to_string(),
        fs_type: Some(fs_type),
        created: Utc::now().to_rfc3339(),
    };
    if args.force || !existing_identity {
        write_identity(&identity_file, &identity)?;
    }
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
    warn_duplicate_disk_ids(&cfg);
    let candidates = list_candidates(&cfg.backup_disks, Path::new(&cfg.user_mount_base))?;
    warn_duplicate_discovered_ids(&cfg, &candidates);
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
        if let Some(capacity_bytes) = candidate.capacity_bytes {
            println!("  capacity: {}", human_size(capacity_bytes));
        } else {
            println!("  capacity: unknown");
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

fn warn_duplicate_disk_ids(cfg: &crate::config::model::RuntimeConfig) {
    let mut seen = std::collections::HashSet::new();
    let mut dupes = std::collections::HashSet::new();
    for disk in &cfg.backup_disks {
        if !seen.insert(&disk.disk_id) {
            dupes.insert(disk.disk_id.clone());
        }
    }
    if !dupes.is_empty() {
        let mut list: Vec<String> = dupes.into_iter().collect();
        list.sort();
        println!();
        println!(
            "WARNING: duplicate disk-id(s) found: {} (rename with `timevault disk rename --fs-uuid <uuid> --new-id <id>`)",
            list.join(", ")
        );
        println!();
    }
}

fn warn_duplicate_discovered_ids(
    cfg: &crate::config::model::RuntimeConfig,
    candidates: &[crate::disk::discovery::DiskCandidate],
) {
    let mut dupes = find_duplicate_disk_ids(cfg, candidates);
    if dupes.is_empty() {
        return;
    }
    dupes.sort();
    println!();
    println!(
        "WARNING: duplicate disk-id(s) detected across config and identities: {} (rename with `timevault disk rename --fs-uuid <uuid> --new-id <id>`)",
        dupes.join(", ")
    );
    println!();
}

fn find_duplicate_disk_ids(
    cfg: &crate::config::model::RuntimeConfig,
    candidates: &[crate::disk::discovery::DiskCandidate],
) -> Vec<String> {
    let mut ids: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::new();
    for disk in &cfg.backup_disks {
        ids.entry(disk.disk_id.clone())
            .or_default()
            .insert(disk.fs_uuid.clone());
    }
    for candidate in candidates {
        if let Some(identity) = &candidate.identity {
            ids.entry(identity.disk_id.clone())
                .or_default()
                .insert(identity.fs_uuid.clone());
        }
    }
    let mut dupes: Vec<String> = ids
        .into_iter()
        .filter_map(|(id, uuids)| if uuids.len() > 1 { Some(id) } else { None })
        .collect();
    dupes.sort();
    dupes
}

fn human_size(bytes: u64) -> String {
    let units = ["MB", "GB", "TB", "PB"];
    let mut value = bytes as f64 / 1_000_000f64;
    let mut idx = 0usize;
    while value >= 1000.0 && idx + 1 < units.len() {
        value /= 1000.0;
        idx += 1;
    }
    if value < 0.1 {
        format!("0.1 {}", units[idx])
    } else if value < 10.0 {
        format!("{:.2} {}", value, units[idx])
    } else if value < 100.0 {
        format!("{:.1} {}", value, units[idx])
    } else {
        format!("{:.0} {}", value, units[idx])
    }
}

pub fn run_rename(config_path: &Path, args: crate::cli::args::DiskRenameArgs) -> Result<()> {
    let new_id = match args.new_id.parse::<DiskId>() {
        Ok(id) => id,
        Err(_) => {
            return Err(TimevaultError::message(format!(
                "disk-id {} must use only letters, digits, '.', '-', '_'",
                args.new_id
            )));
        }
    };

    if args.disk_id.is_none() && args.fs_uuid.is_none() {
        return Err(TimevaultError::message(
            "disk rename requires --disk-id or --fs-uuid".to_string(),
        ));
    }

    let mut contents = String::new();
    File::open(config_path)
        .map_err(|e| TimevaultError::message(format!("open config {}: {}", config_path.display(), e)))?
        .read_to_string(&mut contents)
        .map_err(|e| TimevaultError::message(format!("read config {}: {}", config_path.display(), e)))?;
    let mut cfg: Config = serde_yaml::from_str(&contents)
        .map_err(|e| TimevaultError::message(format!("parse config: {}", e)))?;

    if cfg
        .backup_disks
        .iter()
        .any(|disk| disk.disk_id == new_id.as_str())
    {
        return Err(TimevaultError::message(format!(
            "disk-id {} already exists",
            new_id.as_str()
        )));
    }

    let idx: Option<usize> = if let Some(fs_uuid) = args.fs_uuid.as_deref() {
        cfg.backup_disks.iter().position(|disk| {
            disk.fs_uuid == fs_uuid
                && args
                    .disk_id
                    .as_deref()
                    .map(|id| disk.disk_id == id)
                    .unwrap_or(true)
        })
    } else {
        let matches: Vec<usize> = cfg
            .backup_disks
            .iter()
            .enumerate()
            .filter_map(|(idx, disk)| {
                if args.disk_id.as_deref().map(|id| disk.disk_id == id).unwrap_or(false) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();
        if matches.is_empty() {
            return Err(TimevaultError::message(
                "disk-id not found in config; use --fs-uuid".to_string(),
            ));
        }
        if matches.len() > 1 {
            return Err(TimevaultError::message(
                "multiple disks with disk-id; use --fs-uuid to disambiguate".to_string(),
            ));
        }
        Some(matches[0])
    };

    if let Some(idx) = idx {
        let fs_uuid = cfg.backup_disks[idx].fs_uuid.clone();
        let old_id = cfg.backup_disks[idx].disk_id.clone();
        cfg.backup_disks[idx].disk_id = new_id.as_str().to_string();
        save_config(config_path.to_string_lossy().as_ref(), &cfg)?;

        let device = device_path_for_uuid(&fs_uuid);
        if device.exists() {
            let disk = cfg.backup_disks[idx].clone();
            let options = mount_options_for_backup(&disk);
            let mount_base = PathBuf::from(
                cfg.mount_base
                    .clone()
                    .unwrap_or_else(|| "/run/timevault/mounts".to_string()),
            );
            let (guard, mountpoint) = mount_disk_guarded(&disk, &mount_base, &options)?;
            let identity_file = identity_path(&mountpoint);
            let identity = if identity_file.exists() {
                read_identity(&identity_file)?
            } else {
                return Err(DiskError::IdentityMismatch(format!(
                    "file missing at {}; expected diskId {} fsUuid {} (run `timevault disk enroll ...`)",
                    identity_file.display(),
                    old_id,
                    fs_uuid
                ))
                .into());
            };
            if identity.fs_uuid != fs_uuid {
                return Err(DiskError::IdentityMismatch(format!(
                    "fsUuid mismatch: expected {}, got {}",
                    fs_uuid, identity.fs_uuid
                ))
                .into());
            }
            let updated = DiskIdentity {
                version: identity.version,
                disk_id: new_id.as_str().to_string(),
                fs_uuid: identity.fs_uuid,
                fs_type: identity.fs_type,
                created: identity.created,
            };
            write_identity(&identity_file, &updated)?;
            drop(guard);
        }
        return Ok(());
    }

    let fs_uuid = match args.fs_uuid.as_deref() {
        Some(fs_uuid) => fs_uuid,
        None => {
            return Err(TimevaultError::message(
                "disk rename requires --fs-uuid when disk-id is not in config".to_string(),
            ));
        }
    };
    let device = device_path_for_uuid(fs_uuid);
    if !device.exists() {
        return Err(DiskError::Other(format!("device {} not found", device.display())).into());
    }
    ensure_disk_not_mounted(&device)?;
    let mount_base = PathBuf::from(
        cfg.mount_base
            .clone()
            .unwrap_or_else(|| "/run/timevault/mounts".to_string()),
    );
    ensure_base_dir(&mount_base)?;
    let mountpoint = create_temp_dir(&mount_base, "rename")?;
    mount_device(&device, &mountpoint, DEFAULT_BACKUP_MOUNT_OPTS)?;
    let guard = MountGuard::new(mountpoint.clone(), true);
    let identity_file = identity_path(&mountpoint);
    let identity = if identity_file.exists() {
        read_identity(&identity_file)?
    } else {
        return Err(DiskError::IdentityMismatch(format!(
            "file missing at {}; expected fsUuid {} (run `timevault disk enroll ...`)",
            identity_file.display(),
            fs_uuid
        ))
        .into());
    };
    if identity.fs_uuid != fs_uuid {
        return Err(DiskError::IdentityMismatch(format!(
            "fsUuid mismatch: expected {}, got {}",
            fs_uuid, identity.fs_uuid
        ))
        .into());
    }
    let updated = DiskIdentity {
        version: identity.version,
        disk_id: new_id.as_str().to_string(),
        fs_uuid: identity.fs_uuid,
        fs_type: identity.fs_type,
        created: identity.created,
    };
    write_identity(&identity_file, &updated)?;
    drop(guard);

    Ok(())
}

pub fn run_unenroll(config_path: &Path, args: DiskUnenrollArgs) -> Result<()> {
    let disk_id = match args.disk_id.as_deref() {
        Some(disk_id) => {
            let parsed = disk_id.parse::<DiskId>().map_err(|_| {
                TimevaultError::message(format!(
                    "disk-id {} must use only letters, digits, '.', '-', '_'",
                    disk_id
                ))
            })?;
            Some(parsed.as_str().to_string())
        }
        None => None,
    };

    if disk_id.is_none() && args.fs_uuid.is_none() {
        return Err(TimevaultError::message(
            "disk unenroll requires --disk-id or --fs-uuid".to_string(),
        ));
    }

    let mut contents = String::new();
    File::open(config_path)
        .map_err(|e| TimevaultError::message(format!("open config {}: {}", config_path.display(), e)))?
        .read_to_string(&mut contents)
        .map_err(|e| TimevaultError::message(format!("read config {}: {}", config_path.display(), e)))?;
    let mut cfg: Config = serde_yaml::from_str(&contents)
        .map_err(|e| TimevaultError::message(format!("parse config: {}", e)))?;

    let idx: Option<usize> = if let Some(fs_uuid) = args.fs_uuid.as_deref() {
        cfg.backup_disks.iter().position(|disk| {
            disk.fs_uuid == fs_uuid
                && disk_id
                    .as_deref()
                    .map(|id| disk.disk_id == id)
                    .unwrap_or(true)
        })
    } else {
        let matches: Vec<usize> = cfg
            .backup_disks
            .iter()
            .enumerate()
            .filter_map(|(idx, disk)| {
                if disk_id.as_deref().map(|id| disk.disk_id == id).unwrap_or(false) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();
        if matches.is_empty() {
            return Err(TimevaultError::message(
                "disk-id not found in config; use --fs-uuid".to_string(),
            ));
        }
        if matches.len() > 1 {
            return Err(TimevaultError::message(
                "multiple disks with disk-id; use --fs-uuid to disambiguate".to_string(),
            ));
        }
        Some(matches[0])
    };

    if let Some(idx) = idx {
        cfg.backup_disks.remove(idx);
        save_config(config_path.to_string_lossy().as_ref(), &cfg)?;
        return Ok(());
    }

    Err(TimevaultError::message(
        "disk not found in config".to_string(),
    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::{BackupDiskConfig, RuntimeConfig};
    use crate::disk::discovery::DiskCandidate;
    use crate::disk::identity::DiskIdentity;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn candidate_with_identity(disk_id: &str, fs_uuid: &str) -> DiskCandidate {
        DiskCandidate {
            uuid: fs_uuid.to_string(),
            device: PathBuf::from("/dev/sdz1"),
            mounted_at: None,
            capacity_bytes: None,
            empty: None,
            removable: None,
            reasons: Vec::new(),
            identity: Some(DiskIdentity {
                version: IDENTITY_VERSION,
                disk_id: disk_id.to_string(),
                fs_uuid: fs_uuid.to_string(),
                fs_type: None,
                created: "2025-01-01T00:00:00Z".to_string(),
            }),
            enrolled: false,
            fs_type: None,
        }
    }

    #[test]
    fn duplicate_disk_ids_ignores_matching_config_and_identity() {
        let cfg = RuntimeConfig {
            jobs: Vec::new(),
            backup_disks: vec![BackupDiskConfig {
                disk_id: "disk-a".to_string(),
                fs_uuid: "uuid-a".to_string(),
                label: None,
                mount_options: None,
            }],
            mount_base: "/run/timevault/mounts".to_string(),
            user_mount_base: "/run/timevault/user-mounts".to_string(),
        };
        let candidates = vec![candidate_with_identity("disk-a", "uuid-a")];
        let dupes = find_duplicate_disk_ids(&cfg, &candidates);
        assert!(dupes.is_empty());
    }

    #[test]
    fn duplicate_disk_ids_reports_mismatched_fs_uuid() {
        let cfg = RuntimeConfig {
            jobs: Vec::new(),
            backup_disks: vec![BackupDiskConfig {
                disk_id: "disk-a".to_string(),
                fs_uuid: "uuid-a".to_string(),
                label: None,
                mount_options: None,
            }],
            mount_base: "/run/timevault/mounts".to_string(),
            user_mount_base: "/run/timevault/user-mounts".to_string(),
        };
        let candidates = vec![candidate_with_identity("disk-a", "uuid-b")];
        let dupes = find_duplicate_disk_ids(&cfg, &candidates);
        assert_eq!(dupes, vec!["disk-a".to_string()]);
    }

    #[test]
    fn duplicate_disk_ids_reports_duplicate_config_entries() {
        let cfg = RuntimeConfig {
            jobs: Vec::new(),
            backup_disks: vec![
                BackupDiskConfig {
                    disk_id: "disk-a".to_string(),
                    fs_uuid: "uuid-a".to_string(),
                    label: None,
                    mount_options: None,
                },
                BackupDiskConfig {
                    disk_id: "disk-a".to_string(),
                    fs_uuid: "uuid-b".to_string(),
                    label: None,
                    mount_options: None,
                },
            ],
            mount_base: "/run/timevault/mounts".to_string(),
            user_mount_base: "/run/timevault/user-mounts".to_string(),
        };
        let dupes = find_duplicate_disk_ids(&cfg, &[]);
        assert_eq!(dupes, vec!["disk-a".to_string()]);
    }

    #[test]
    fn disk_unenroll_by_fs_uuid_removes_entry() {
        let dir = TempDir::new().expect("tempdir");
        let config_path = dir.path().join("timevault.yaml");
        let cfg = Config {
            jobs: Vec::new(),
            excludes: Vec::new(),
            backup_disks: vec![
                BackupDiskConfig {
                    disk_id: "disk-a".to_string(),
                    fs_uuid: "uuid-a".to_string(),
                    label: None,
                    mount_options: None,
                },
                BackupDiskConfig {
                    disk_id: "disk-b".to_string(),
                    fs_uuid: "uuid-b".to_string(),
                    label: None,
                    mount_options: None,
                },
            ],
            mount_base: None,
            user_mount_base: None,
        };
        save_config(config_path.to_string_lossy().as_ref(), &cfg).expect("save");

        let args = DiskUnenrollArgs {
            disk_id: None,
            fs_uuid: Some("uuid-a".to_string()),
        };
        run_unenroll(&config_path, args).expect("unenroll");

        let contents = std::fs::read_to_string(&config_path).expect("read");
        let updated: Config = serde_yaml::from_str(&contents).expect("parse");
        assert_eq!(updated.backup_disks.len(), 1);
        assert_eq!(updated.backup_disks[0].disk_id, "disk-b");
    }

    #[test]
    fn disk_unenroll_by_disk_id_removes_entry() {
        let dir = TempDir::new().expect("tempdir");
        let config_path = dir.path().join("timevault.yaml");
        let cfg = Config {
            jobs: Vec::new(),
            excludes: Vec::new(),
            backup_disks: vec![
                BackupDiskConfig {
                    disk_id: "disk-a".to_string(),
                    fs_uuid: "uuid-a".to_string(),
                    label: None,
                    mount_options: None,
                },
                BackupDiskConfig {
                    disk_id: "disk-b".to_string(),
                    fs_uuid: "uuid-b".to_string(),
                    label: None,
                    mount_options: None,
                },
            ],
            mount_base: None,
            user_mount_base: None,
        };
        save_config(config_path.to_string_lossy().as_ref(), &cfg).expect("save");

        let args = DiskUnenrollArgs {
            disk_id: Some("disk-b".to_string()),
            fs_uuid: None,
        };
        run_unenroll(&config_path, args).expect("unenroll");

        let contents = std::fs::read_to_string(&config_path).expect("read");
        let updated: Config = serde_yaml::from_str(&contents).expect("parse");
        assert_eq!(updated.backup_disks.len(), 1);
        assert_eq!(updated.backup_disks[0].disk_id, "disk-a");
    }

    #[test]
    fn disk_unenroll_requires_selector() {
        let dir = TempDir::new().expect("tempdir");
        let config_path = dir.path().join("timevault.yaml");
        let cfg = Config {
            jobs: Vec::new(),
            excludes: Vec::new(),
            backup_disks: Vec::new(),
            mount_base: None,
            user_mount_base: None,
        };
        save_config(config_path.to_string_lossy().as_ref(), &cfg).expect("save");

        let args = DiskUnenrollArgs {
            disk_id: None,
            fs_uuid: None,
        };
        assert!(run_unenroll(&config_path, args).is_err());
    }

    #[test]
    fn disk_unenroll_disk_id_requires_fs_uuid_when_duplicate() {
        let dir = TempDir::new().expect("tempdir");
        let config_path = dir.path().join("timevault.yaml");
        let cfg = Config {
            jobs: Vec::new(),
            excludes: Vec::new(),
            backup_disks: vec![
                BackupDiskConfig {
                    disk_id: "disk-a".to_string(),
                    fs_uuid: "uuid-a".to_string(),
                    label: None,
                    mount_options: None,
                },
                BackupDiskConfig {
                    disk_id: "disk-a".to_string(),
                    fs_uuid: "uuid-b".to_string(),
                    label: None,
                    mount_options: None,
                },
            ],
            mount_base: None,
            user_mount_base: None,
        };
        save_config(config_path.to_string_lossy().as_ref(), &cfg).expect("save");

        let args = DiskUnenrollArgs {
            disk_id: Some("disk-a".to_string()),
            fs_uuid: None,
        };
        assert!(run_unenroll(&config_path, args).is_err());
    }

    #[test]
    fn disk_unenroll_disk_id_not_found() {
        let dir = TempDir::new().expect("tempdir");
        let config_path = dir.path().join("timevault.yaml");
        let cfg = Config {
            jobs: Vec::new(),
            excludes: Vec::new(),
            backup_disks: vec![BackupDiskConfig {
                disk_id: "disk-a".to_string(),
                fs_uuid: "uuid-a".to_string(),
                label: None,
                mount_options: None,
            }],
            mount_base: None,
            user_mount_base: None,
        };
        save_config(config_path.to_string_lossy().as_ref(), &cfg).expect("save");

        let args = DiskUnenrollArgs {
            disk_id: Some("disk-b".to_string()),
            fs_uuid: None,
        };
        assert!(run_unenroll(&config_path, args).is_err());
    }
}
