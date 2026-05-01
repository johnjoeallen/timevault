use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use chrono::Utc;
use tempfile::Builder;

use crate::cli::args::{DiskAddArgs, DiskStateArgs, DiskUnenrollArgs};
use crate::config::model::{BackupDiskConfig, Config};
use crate::config::save::save_config;
use crate::disk::discovery::list_candidates;
use crate::disk::fs_type::detect_fs_type;
use crate::disk::identity::{
    identity_path, read_identity, write_identity, DiskIdentity, IDENTITY_VERSION,
};
use crate::disk::{
    device_path_for_uuid, disk_matches_selector, ensure_disk_not_mounted, mount_disk_guarded,
    mount_options_for_backup, resolve_fs_uuid, DEFAULT_BACKUP_MOUNT_OPTS, DISK_ADD_ALLOWED_ENTRIES,
};
use crate::error::{DiskError, Result, TimevaultError};
use crate::mount::guard::MountGuard;
use crate::mount::ops::mount_device;
use crate::types::DiskId;
use crate::util::paths::{create_temp_dir, ensure_base_dir};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskListOutput {
    Verbose,
    Short,
    Columns,
}

pub fn run_enroll(config_path: &Path, disk_id: Option<&str>, args: DiskAddArgs) -> Result<()> {
    let disk_id_arg = args
        .disk_id
        .as_deref()
        .or(disk_id)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let mut contents = String::new();
    File::open(config_path)
        .map_err(|e| {
            TimevaultError::message(format!("open config {}: {}", config_path.display(), e))
        })?
        .read_to_string(&mut contents)
        .map_err(|e| {
            TimevaultError::message(format!("read config {}: {}", config_path.display(), e))
        })?;
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
        return Err(DiskError::Other(format!("unsupported filesystem type {}", fs_type)).into());
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
                println!("disk enroll requires <disk-id> or --disk-id");
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
            return Err(
                DiskError::Other(format!("disk-id {} already enrolled", disk_id.as_str())).into(),
            );
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
        disabled: false,
        rotated_out: false,
    });
    save_config(config_path.to_string_lossy().as_ref(), &cfg)?;
    Ok(())
}

pub fn run_discover(config_path: &Path) -> Result<()> {
    run_discover_with_output(config_path, DiskListOutput::Verbose)
}

pub fn run_discover_with_output(config_path: &Path, output: DiskListOutput) -> Result<()> {
    let cfg = crate::config::load::load_config(config_path.to_string_lossy().as_ref())?;
    warn_duplicate_disk_ids(&cfg);
    let candidates = list_candidates(&cfg.backup_disks, Path::new(&cfg.user_mount_base))?;
    warn_registered_identity_mismatches(&cfg, &candidates);
    warn_duplicate_discovered_ids(&cfg, &candidates);
    if candidates.is_empty() && cfg.backup_disks.is_empty() {
        println!("no candidate backup devices found");
        return Ok(());
    }
    if output != DiskListOutput::Verbose {
        return print_discover_table(&cfg, &candidates, output);
    }
    let mut seen_enrolled_uuids = std::collections::HashSet::new();
    for candidate in &candidates {
        if candidate.enrolled {
            seen_enrolled_uuids.insert(candidate.uuid.clone());
        }
        let enrolled_disk = cfg
            .backup_disks
            .iter()
            .find(|disk| disk.fs_uuid == candidate.uuid);
        println!("uuid: {}", candidate.uuid);
        println!("  device: {}", candidate.device.display());
        if let Some(mp) = &candidate.mounted_at {
            println!("  mounted: {}", mp.display());
        } else {
            println!("  mounted: no");
        }
        if let Some(capacity_bytes) = candidate.capacity_bytes {
            println!("  capacity: {}", human_size(capacity_bytes));
        } else {
            println!("  capacity: unknown");
        }
        println!(
            "  serial: {}",
            candidate.serial.as_deref().unwrap_or("unknown")
        );
        println!(
            "  registered: {}",
            if candidate.enrolled { "yes" } else { "no" }
        );
        match enrolled_disk {
            Some(disk) => {
                println!("  enabled: {}", if disk.disabled { "no" } else { "yes" });
                println!(
                    "  rotation: {}",
                    if disk.rotated_out { "out" } else { "in" }
                );
            }
            None => {
                println!("  enabled: n/a");
                println!("  rotation: n/a");
            }
        }
        if let Some(identity) = &candidate.identity {
            println!("  identity.diskId: {}", identity.disk_id);
            println!("  identity.fsUuid: {}", identity.fs_uuid);
            if let Some(fs_type) = &identity.fs_type {
                println!("  identity.fsType: {}", fs_type);
            }
            println!("  identity.created: {}", identity.created);
        }
        if let Some(fs_type) = &candidate.fs_type {
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
    for disk in &cfg.backup_disks {
        if seen_enrolled_uuids.contains(&disk.fs_uuid) {
            continue;
        }
        println!("uuid: {}", disk.fs_uuid);
        println!("  diskId: {}", disk.disk_id);
        println!("  device: offline");
        println!("  mounted: no");
        println!("  capacity: unknown");
        println!("  serial: unknown");
        println!("  registered: yes");
        println!("  enabled: {}", if disk.disabled { "no" } else { "yes" });
        println!(
            "  rotation: {}",
            if disk.rotated_out { "out" } else { "in" }
        );
        if let Some(label) = &disk.label {
            println!("  label: {}", label);
        }
        println!("  reason: registered, offline");
        println!();
    }
    Ok(())
}

fn print_discover_table(
    cfg: &crate::config::model::RuntimeConfig,
    candidates: &[crate::disk::discovery::DiskCandidate],
    output: DiskListOutput,
) -> Result<()> {
    let mut seen_enrolled_uuids = std::collections::HashSet::new();
    if output == DiskListOutput::Columns {
        println!(
            "{:<20} {:<36} {:<8} {:<10} {:<7} {:<18} {:<12} DEVICE",
            "DISK ID", "FS UUID", "STATUS", "REGISTERED", "ENABLED", "SERIAL", "CAPACITY"
        );
    }
    for candidate in candidates {
        if candidate.enrolled {
            seen_enrolled_uuids.insert(candidate.uuid.clone());
        }
        let enrolled_disk = cfg
            .backup_disks
            .iter()
            .find(|disk| disk.fs_uuid == candidate.uuid);
        let disk_id = enrolled_disk
            .map(|disk| disk.disk_id.as_str())
            .or_else(|| {
                candidate
                    .identity
                    .as_ref()
                    .map(|identity| identity.disk_id.as_str())
            })
            .unwrap_or("-");
        let enabled = enrolled_disk
            .map(|disk| if disk.disabled { "no" } else { "yes" })
            .unwrap_or("n/a");
        let registered = if enrolled_disk.is_some() { "yes" } else { "no" };
        let capacity = candidate
            .capacity_bytes
            .map(human_size)
            .unwrap_or_else(|| "-".to_string());
        if output == DiskListOutput::Columns {
            println!(
                "{:<20} {:<36} {:<8} {:<10} {:<7} {:<18} {:<12} {}",
                disk_id,
                candidate.uuid,
                "online",
                registered,
                enabled,
                candidate.serial.as_deref().unwrap_or("-"),
                capacity,
                candidate.device.display()
            );
        } else {
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}",
                disk_id,
                candidate.uuid,
                "online",
                registered,
                enabled,
                candidate.serial.as_deref().unwrap_or("-")
            );
        }
    }
    for disk in &cfg.backup_disks {
        if seen_enrolled_uuids.contains(&disk.fs_uuid) {
            continue;
        }
        let enabled = if disk.disabled { "no" } else { "yes" };
        if output == DiskListOutput::Columns {
            println!(
                "{:<20} {:<36} {:<8} {:<10} {:<7} {:<18} {:<12} -",
                disk.disk_id, disk.fs_uuid, "offline", "yes", enabled, "-", "-"
            );
        } else {
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}",
                disk.disk_id, disk.fs_uuid, "offline", "yes", enabled, "-"
            );
        }
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

fn warn_registered_identity_mismatches(
    cfg: &crate::config::model::RuntimeConfig,
    candidates: &[crate::disk::discovery::DiskCandidate],
) {
    let mut mismatches = find_registered_identity_mismatches(cfg, candidates);
    if mismatches.is_empty() {
        return;
    }
    mismatches.sort();
    println!();
    println!("WARNING: registered disk identity mismatch:");
    for mismatch in mismatches {
        println!("  {}", mismatch);
    }
    println!();
}

fn find_registered_identity_mismatches(
    cfg: &crate::config::model::RuntimeConfig,
    candidates: &[crate::disk::discovery::DiskCandidate],
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for candidate in candidates {
        let Some(config_disk) = cfg
            .backup_disks
            .iter()
            .find(|disk| disk.fs_uuid == candidate.uuid)
        else {
            continue;
        };
        let Some(identity) = &candidate.identity else {
            continue;
        };
        if identity.fs_uuid != config_disk.fs_uuid {
            mismatches.push(format!(
                "{}: actual fsUuid {}, identity has {}",
                config_disk.disk_id, config_disk.fs_uuid, identity.fs_uuid
            ));
        }
        if identity.disk_id != config_disk.disk_id {
            mismatches.push(format!(
                "{}: expected diskId {}, identity has {}",
                config_disk.fs_uuid, config_disk.disk_id, identity.disk_id
            ));
        }
    }
    mismatches
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
    let new_id_raw = args
        .new_id_arg
        .as_deref()
        .or(args.new_id.as_deref())
        .ok_or_else(|| {
            TimevaultError::message("disk rename requires <new-id> or --new-id".to_string())
        })?;
    let new_id = match new_id_raw.parse::<DiskId>() {
        Ok(id) => id,
        Err(_) => {
            return Err(TimevaultError::message(format!(
                "disk-id {} must use only letters, digits, '.', '-', '_'",
                new_id_raw
            )));
        }
    };
    let selector = selector_or_flag(args.selector.as_deref(), args.disk_id.as_deref());

    if selector.is_none() && args.fs_uuid.is_none() {
        return Err(TimevaultError::message(
            "disk rename requires <disk-id-or-fs-uuid>, --disk-id, or --fs-uuid".to_string(),
        ));
    }

    let mut contents = String::new();
    File::open(config_path)
        .map_err(|e| {
            TimevaultError::message(format!("open config {}: {}", config_path.display(), e))
        })?
        .read_to_string(&mut contents)
        .map_err(|e| {
            TimevaultError::message(format!("read config {}: {}", config_path.display(), e))
        })?;
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

    let idx = match resolve_disk_index(&cfg, selector, args.fs_uuid.as_deref(), "disk rename") {
        Ok(idx) => Some(idx),
        Err(err) if args.fs_uuid.is_some() => {
            if selector.is_some() {
                return Err(err);
            }
            None
        }
        Err(err) => return Err(err),
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

pub fn run_set_disabled(config_path: &Path, args: DiskStateArgs, disabled: bool) -> Result<()> {
    let action = if disabled {
        "disk disable"
    } else {
        "disk enable"
    };
    update_disk_state(config_path, args, action, |disk| disk.disabled = disabled)
}

pub fn run_set_rotated_out(
    config_path: &Path,
    args: DiskStateArgs,
    rotated_out: bool,
) -> Result<()> {
    let action = if rotated_out {
        "disk rotate-out"
    } else {
        "disk rotate-in"
    };
    update_disk_state(config_path, args, action, |disk| {
        disk.rotated_out = rotated_out
    })
}

pub fn run_unenroll(config_path: &Path, args: DiskUnenrollArgs) -> Result<()> {
    let selector = selector_or_flag(args.selector.as_deref(), args.disk_id.as_deref());

    if selector.is_none() && args.fs_uuid.is_none() {
        return Err(TimevaultError::message(
            "disk unenroll requires <disk-id-or-fs-uuid>, --disk-id, or --fs-uuid".to_string(),
        ));
    }

    let mut contents = String::new();
    File::open(config_path)
        .map_err(|e| {
            TimevaultError::message(format!("open config {}: {}", config_path.display(), e))
        })?
        .read_to_string(&mut contents)
        .map_err(|e| {
            TimevaultError::message(format!("read config {}: {}", config_path.display(), e))
        })?;
    let mut cfg: Config = serde_yaml::from_str(&contents)
        .map_err(|e| TimevaultError::message(format!("parse config: {}", e)))?;

    let idx = resolve_disk_index(&cfg, selector, args.fs_uuid.as_deref(), "disk unregister")?;

    cfg.backup_disks.remove(idx);
    save_config(config_path.to_string_lossy().as_ref(), &cfg)?;
    Ok(())
}

fn update_disk_state<F>(
    config_path: &Path,
    args: DiskStateArgs,
    action: &str,
    mut update: F,
) -> Result<()>
where
    F: FnMut(&mut BackupDiskConfig),
{
    let mut contents = String::new();
    File::open(config_path)
        .map_err(|e| {
            TimevaultError::message(format!("open config {}: {}", config_path.display(), e))
        })?
        .read_to_string(&mut contents)
        .map_err(|e| {
            TimevaultError::message(format!("read config {}: {}", config_path.display(), e))
        })?;
    let mut cfg: Config = serde_yaml::from_str(&contents)
        .map_err(|e| TimevaultError::message(format!("parse config: {}", e)))?;

    let selector = selector_or_flag(args.selector.as_deref(), args.disk_id.as_deref());
    let idx = resolve_disk_index(&cfg, selector, args.fs_uuid.as_deref(), action)?;
    update(&mut cfg.backup_disks[idx]);
    save_config(config_path.to_string_lossy().as_ref(), &cfg)?;
    Ok(())
}

fn selector_or_flag<'a>(selector: Option<&'a str>, flag: Option<&'a str>) -> Option<&'a str> {
    selector.or(flag)
}

fn resolve_disk_index(
    cfg: &Config,
    selector: Option<&str>,
    fs_uuid: Option<&str>,
    action: &str,
) -> Result<usize> {
    if selector.is_none() && fs_uuid.is_none() {
        return Err(TimevaultError::message(format!(
            "{} requires <disk-id-or-fs-uuid>, --disk-id, or --fs-uuid",
            action
        )));
    }

    if let Some(fs_uuid) = fs_uuid {
        return cfg
            .backup_disks
            .iter()
            .position(|disk| {
                disk.fs_uuid == fs_uuid
                    && selector
                        .map(|value| disk_matches_selector(disk, value))
                        .unwrap_or(true)
            })
            .ok_or_else(|| TimevaultError::message("disk not found in config".to_string()));
    }

    let matches: Vec<usize> = cfg
        .backup_disks
        .iter()
        .enumerate()
        .filter_map(|(idx, disk)| {
            if selector
                .map(|value| disk_matches_selector(disk, value))
                .unwrap_or(false)
            {
                Some(idx)
            } else {
                None
            }
        })
        .collect();
    if matches.is_empty() {
        return Err(TimevaultError::message(
            "disk selector not found in config; use --fs-uuid".to_string(),
        ));
    }
    if matches.len() > 1 {
        return Err(TimevaultError::message(
            "multiple disks match selector; use --fs-uuid to disambiguate".to_string(),
        ));
    }
    Ok(matches[0])
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
            serial: None,
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
                disabled: false,
                rotated_out: false,
            }],
            mount_base: "/run/timevault/mounts".to_string(),
            user_mount_base: "/run/timevault/user-mounts".to_string(),
            options: crate::config::model::ConfigOptions::default(),
        };
        let candidates = vec![candidate_with_identity("disk-a", "uuid-a")];
        let dupes = find_duplicate_disk_ids(&cfg, &candidates);
        assert!(dupes.is_empty());
        let mismatches = find_registered_identity_mismatches(&cfg, &candidates);
        assert!(mismatches.is_empty());
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
                disabled: false,
                rotated_out: false,
            }],
            mount_base: "/run/timevault/mounts".to_string(),
            user_mount_base: "/run/timevault/user-mounts".to_string(),
            options: crate::config::model::ConfigOptions::default(),
        };
        let candidates = vec![candidate_with_identity("disk-a", "uuid-b")];
        let dupes = find_duplicate_disk_ids(&cfg, &candidates);
        assert_eq!(dupes, vec!["disk-a".to_string()]);
    }

    #[test]
    fn registered_identity_mismatch_reports_wrong_disk_id() {
        let cfg = RuntimeConfig {
            jobs: Vec::new(),
            backup_disks: vec![BackupDiskConfig {
                disk_id: "disk-a".to_string(),
                fs_uuid: "uuid-a".to_string(),
                label: None,
                mount_options: None,
                disabled: false,
                rotated_out: false,
            }],
            mount_base: "/run/timevault/mounts".to_string(),
            user_mount_base: "/run/timevault/user-mounts".to_string(),
            options: crate::config::model::ConfigOptions::default(),
        };
        let candidates = vec![candidate_with_identity("old-name", "uuid-a")];
        let mismatches = find_registered_identity_mismatches(&cfg, &candidates);
        assert_eq!(
            mismatches,
            vec!["uuid-a: expected diskId disk-a, identity has old-name".to_string()]
        );
    }

    #[test]
    fn registered_identity_mismatch_reports_wrong_identity_uuid() {
        let cfg = RuntimeConfig {
            jobs: Vec::new(),
            backup_disks: vec![BackupDiskConfig {
                disk_id: "disk-a".to_string(),
                fs_uuid: "uuid-a".to_string(),
                label: None,
                mount_options: None,
                disabled: false,
                rotated_out: false,
            }],
            mount_base: "/run/timevault/mounts".to_string(),
            user_mount_base: "/run/timevault/user-mounts".to_string(),
            options: crate::config::model::ConfigOptions::default(),
        };
        let mut candidate = candidate_with_identity("disk-a", "uuid-b");
        candidate.uuid = "uuid-a".to_string();
        let candidates = vec![candidate];
        let mismatches = find_registered_identity_mismatches(&cfg, &candidates);
        assert_eq!(
            mismatches,
            vec!["disk-a: actual fsUuid uuid-a, identity has uuid-b".to_string()]
        );
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
                    disabled: false,
                    rotated_out: false,
                },
                BackupDiskConfig {
                    disk_id: "disk-a".to_string(),
                    fs_uuid: "uuid-b".to_string(),
                    label: None,
                    mount_options: None,
                    disabled: false,
                    rotated_out: false,
                },
            ],
            mount_base: "/run/timevault/mounts".to_string(),
            user_mount_base: "/run/timevault/user-mounts".to_string(),
            options: crate::config::model::ConfigOptions::default(),
        };
        let dupes = find_duplicate_disk_ids(&cfg, &[]);
        assert_eq!(dupes, vec!["disk-a".to_string()]);
    }

    #[test]
    fn resolves_disk_index_by_fs_uuid_selector() {
        let cfg = Config {
            jobs: Vec::new(),
            excludes: Vec::new(),
            options: crate::config::model::ConfigOptions::default(),
            backup_disks: vec![BackupDiskConfig {
                disk_id: "disk-a".to_string(),
                fs_uuid: "uuid-a".to_string(),
                label: None,
                mount_options: None,
                disabled: false,
                rotated_out: false,
            }],
            mount_base: None,
            user_mount_base: None,
        };
        let idx = resolve_disk_index(&cfg, Some("uuid-a"), None, "disk disable").expect("resolve");
        assert_eq!(idx, 0);
    }

    #[test]
    fn disk_unenroll_by_fs_uuid_removes_entry() {
        let dir = TempDir::new().expect("tempdir");
        let config_path = dir.path().join("timevault.yaml");
        let cfg = Config {
            jobs: Vec::new(),
            excludes: Vec::new(),
            options: crate::config::model::ConfigOptions::default(),
            backup_disks: vec![
                BackupDiskConfig {
                    disk_id: "disk-a".to_string(),
                    fs_uuid: "uuid-a".to_string(),
                    label: None,
                    mount_options: None,
                    disabled: false,
                    rotated_out: false,
                },
                BackupDiskConfig {
                    disk_id: "disk-b".to_string(),
                    fs_uuid: "uuid-b".to_string(),
                    label: None,
                    mount_options: None,
                    disabled: false,
                    rotated_out: false,
                },
            ],
            mount_base: None,
            user_mount_base: None,
        };
        save_config(config_path.to_string_lossy().as_ref(), &cfg).expect("save");

        let args = DiskUnenrollArgs {
            selector: None,
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
    fn disk_unenroll_by_positional_fs_uuid_removes_entry() {
        let dir = TempDir::new().expect("tempdir");
        let config_path = dir.path().join("timevault.yaml");
        let cfg = Config {
            jobs: Vec::new(),
            excludes: Vec::new(),
            options: crate::config::model::ConfigOptions::default(),
            backup_disks: vec![BackupDiskConfig {
                disk_id: "disk-a".to_string(),
                fs_uuid: "uuid-a".to_string(),
                label: None,
                mount_options: None,
                disabled: false,
                rotated_out: false,
            }],
            mount_base: None,
            user_mount_base: None,
        };
        save_config(config_path.to_string_lossy().as_ref(), &cfg).expect("save");

        let args = DiskUnenrollArgs {
            selector: Some("uuid-a".to_string()),
            disk_id: None,
            fs_uuid: None,
        };
        run_unenroll(&config_path, args).expect("unregister");

        let contents = std::fs::read_to_string(&config_path).expect("read");
        let updated: Config = serde_yaml::from_str(&contents).expect("parse");
        assert!(updated.backup_disks.is_empty());
    }

    #[test]
    fn disk_unenroll_by_disk_id_removes_entry() {
        let dir = TempDir::new().expect("tempdir");
        let config_path = dir.path().join("timevault.yaml");
        let cfg = Config {
            jobs: Vec::new(),
            excludes: Vec::new(),
            options: crate::config::model::ConfigOptions::default(),
            backup_disks: vec![
                BackupDiskConfig {
                    disk_id: "disk-a".to_string(),
                    fs_uuid: "uuid-a".to_string(),
                    label: None,
                    mount_options: None,
                    disabled: false,
                    rotated_out: false,
                },
                BackupDiskConfig {
                    disk_id: "disk-b".to_string(),
                    fs_uuid: "uuid-b".to_string(),
                    label: None,
                    mount_options: None,
                    disabled: false,
                    rotated_out: false,
                },
            ],
            mount_base: None,
            user_mount_base: None,
        };
        save_config(config_path.to_string_lossy().as_ref(), &cfg).expect("save");

        let args = DiskUnenrollArgs {
            selector: None,
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
            options: crate::config::model::ConfigOptions::default(),
            backup_disks: Vec::new(),
            mount_base: None,
            user_mount_base: None,
        };
        save_config(config_path.to_string_lossy().as_ref(), &cfg).expect("save");

        let args = DiskUnenrollArgs {
            selector: None,
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
            options: crate::config::model::ConfigOptions::default(),
            backup_disks: vec![
                BackupDiskConfig {
                    disk_id: "disk-a".to_string(),
                    fs_uuid: "uuid-a".to_string(),
                    label: None,
                    mount_options: None,
                    disabled: false,
                    rotated_out: false,
                },
                BackupDiskConfig {
                    disk_id: "disk-a".to_string(),
                    fs_uuid: "uuid-b".to_string(),
                    label: None,
                    mount_options: None,
                    disabled: false,
                    rotated_out: false,
                },
            ],
            mount_base: None,
            user_mount_base: None,
        };
        save_config(config_path.to_string_lossy().as_ref(), &cfg).expect("save");

        let args = DiskUnenrollArgs {
            selector: None,
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
            options: crate::config::model::ConfigOptions::default(),
            backup_disks: vec![BackupDiskConfig {
                disk_id: "disk-a".to_string(),
                fs_uuid: "uuid-a".to_string(),
                label: None,
                mount_options: None,
                disabled: false,
                rotated_out: false,
            }],
            mount_base: None,
            user_mount_base: None,
        };
        save_config(config_path.to_string_lossy().as_ref(), &cfg).expect("save");

        let args = DiskUnenrollArgs {
            selector: None,
            disk_id: Some("disk-b".to_string()),
            fs_uuid: None,
        };
        assert!(run_unenroll(&config_path, args).is_err());
    }

    #[test]
    fn disk_disable_updates_config() {
        let dir = TempDir::new().expect("tempdir");
        let config_path = dir.path().join("timevault.yaml");
        let cfg = Config {
            jobs: Vec::new(),
            excludes: Vec::new(),
            options: crate::config::model::ConfigOptions::default(),
            backup_disks: vec![BackupDiskConfig {
                disk_id: "disk-a".to_string(),
                fs_uuid: "uuid-a".to_string(),
                label: None,
                mount_options: None,
                disabled: false,
                rotated_out: false,
            }],
            mount_base: None,
            user_mount_base: None,
        };
        save_config(config_path.to_string_lossy().as_ref(), &cfg).expect("save");

        let args = DiskStateArgs {
            selector: None,
            disk_id: Some("disk-a".to_string()),
            fs_uuid: None,
        };
        run_set_disabled(&config_path, args, true).expect("disable");

        let contents = std::fs::read_to_string(&config_path).expect("read");
        let updated: Config = serde_yaml::from_str(&contents).expect("parse");
        assert!(updated.backup_disks[0].disabled);
    }

    #[test]
    fn disk_rotate_out_updates_config() {
        let dir = TempDir::new().expect("tempdir");
        let config_path = dir.path().join("timevault.yaml");
        let cfg = Config {
            jobs: Vec::new(),
            excludes: Vec::new(),
            options: crate::config::model::ConfigOptions::default(),
            backup_disks: vec![BackupDiskConfig {
                disk_id: "disk-a".to_string(),
                fs_uuid: "uuid-a".to_string(),
                label: None,
                mount_options: None,
                disabled: false,
                rotated_out: false,
            }],
            mount_base: None,
            user_mount_base: None,
        };
        save_config(config_path.to_string_lossy().as_ref(), &cfg).expect("save");

        let args = DiskStateArgs {
            selector: None,
            disk_id: None,
            fs_uuid: Some("uuid-a".to_string()),
        };
        run_set_rotated_out(&config_path, args, true).expect("rotate-out");

        let contents = std::fs::read_to_string(&config_path).expect("read");
        let updated: Config = serde_yaml::from_str(&contents).expect("parse");
        assert!(updated.backup_disks[0].rotated_out);
    }
}
