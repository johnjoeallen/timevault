use std::path::{Path, PathBuf};
use std::process::Command;

use crate::cli::args::DiskDfArgs;
use crate::config::load::load_config;
use crate::config::model::BackupDiskConfig;
use crate::disk::identity::{identity_path, read_identity, verify_identity};
use crate::disk::{device_path_for_uuid, disk_matches_selector, mount_options_for_restore};
use crate::error::{DiskError, Result, TimevaultError};
use crate::mount::guard::MountGuard;
use crate::mount::inspect::{find_device_mountpoint, mountpoint_is_mounted};
use crate::mount::ops::mount_device;
use crate::util::paths::{create_temp_dir, ensure_base_dir};

#[derive(Debug, PartialEq, Eq)]
struct DfStats {
    size_bytes: u64,
    used_bytes: u64,
    free_bytes: u64,
    used_percent: String,
}

pub fn run_df(config_path: &Path, args: DiskDfArgs, disk_id: Option<&str>) -> Result<()> {
    let cfg = load_config(config_path.to_string_lossy().as_ref())?;
    let requested_id = args
        .selector
        .as_deref()
        .or(args.fs_uuid.as_deref())
        .or(disk_id);
    let disks = match requested_id {
        Some(id) => vec![select_configured_disk(&cfg.backup_disks, id)?],
        None => {
            if cfg.backup_disks.is_empty() {
                return Err(DiskError::Other(
                    "no backup disks enrolled; run `timevault disk enroll ...`".to_string(),
                )
                .into());
            }
            cfg.backup_disks.clone()
        }
    };

    println!(
        "{:<20} {:<36} {:<9} {:<7} {:>10} {:>10} {:>10} {:>5}  MOUNTED",
        "DISK ID", "FS UUID", "STATUS", "ENABLED", "SIZE", "USED", "FREE", "USE%"
    );
    for disk in disks {
        let enabled = if disk.disabled { "no" } else { "yes" };
        let Some((mountpoint, _guard, mounted_label)) =
            mountpoint_for_df(&disk, Path::new(&cfg.user_mount_base))?
        else {
            println!(
                "{:<20} {:<36} {:<9} {:<7} {:>10} {:>10} {:>10} {:>5}  -",
                disk.disk_id, disk.fs_uuid, "offline", enabled, "-", "-", "-", "-"
            );
            continue;
        };
        verify_disk_identity(&disk, &mountpoint)?;
        let stats = df_stats(&mountpoint)?;
        println!(
            "{:<20} {:<36} {:<9} {:<7} {:>10} {:>10} {:>10} {:>5}  {}",
            disk.disk_id,
            disk.fs_uuid,
            "online",
            enabled,
            human_size(stats.size_bytes),
            human_size(stats.used_bytes),
            human_size(stats.free_bytes),
            stats.used_percent,
            mounted_label.display()
        );
    }

    Ok(())
}

fn select_configured_disk(disks: &[BackupDiskConfig], selector: &str) -> Result<BackupDiskConfig> {
    disks
        .iter()
        .find(|disk| disk_matches_selector(disk, selector))
        .cloned()
        .ok_or_else(|| {
            DiskError::Other(format!("disk selector {} not found in config", selector)).into()
        })
}

fn mountpoint_for_df(
    disk: &BackupDiskConfig,
    user_mount_base: &Path,
) -> Result<Option<(PathBuf, Option<MountGuard>, PathBuf)>> {
    let device = device_path_for_uuid(&disk.fs_uuid);
    if !device.exists() {
        return Ok(None);
    }
    if let Some(mountpoint) = find_device_mountpoint(&device)? {
        return Ok(Some((mountpoint.clone(), None, mountpoint)));
    }

    ensure_base_dir(user_mount_base)?;
    let mountpoint = create_temp_dir(user_mount_base, "tv-df")?;
    if mountpoint_is_mounted(&mountpoint)? {
        return Err(DiskError::Other(format!(
            "mountpoint {} is already in use",
            mountpoint.display()
        ))
        .into());
    }
    let options = mount_options_for_restore(disk);
    mount_device(&device, &mountpoint, &options)?;
    let guard = MountGuard::new(mountpoint.clone(), true);
    Ok(Some((mountpoint.clone(), Some(guard), mountpoint)))
}

fn verify_disk_identity(disk: &BackupDiskConfig, mountpoint: &Path) -> Result<()> {
    let path = identity_path(mountpoint);
    if !path.exists() {
        return Err(DiskError::IdentityMismatch(format!(
            "file missing at {}; expected diskId {} fsUuid {} (run `timevault disk enroll ...`)",
            path.display(),
            disk.disk_id,
            disk.fs_uuid
        ))
        .into());
    }
    let identity = read_identity(&path)?;
    verify_identity(&identity, &disk.disk_id, &disk.fs_uuid)
}

fn df_stats(mountpoint: &Path) -> Result<DfStats> {
    let output = Command::new("df")
        .arg("-B1")
        .arg("--output=size,used,avail,pcent")
        .arg(mountpoint)
        .output()
        .map_err(|e| TimevaultError::message(format!("df {}: {}", mountpoint.display(), e)))?;
    if !output.status.success() {
        return Err(TimevaultError::message(format!(
            "df {} failed with exit code {}",
            mountpoint.display(),
            output.status.code().unwrap_or(1)
        )));
    }
    let stdout = String::from_utf8(output.stdout)
        .map_err(|e| TimevaultError::message(format!("df output was not UTF-8: {}", e)))?;
    parse_df_output(&stdout)
}

fn parse_df_output(output: &str) -> Result<DfStats> {
    let line = output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .last()
        .ok_or_else(|| TimevaultError::message("df output was empty".to_string()))?;
    let fields: Vec<&str> = line.split_whitespace().collect();
    if fields.len() != 4 {
        return Err(TimevaultError::message(format!(
            "unexpected df output: {}",
            line
        )));
    }
    Ok(DfStats {
        size_bytes: parse_u64_field(fields[0], "size")?,
        used_bytes: parse_u64_field(fields[1], "used")?,
        free_bytes: parse_u64_field(fields[2], "free")?,
        used_percent: fields[3].to_string(),
    })
}

fn parse_u64_field(value: &str, name: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|e| TimevaultError::message(format!("invalid df {} value {}: {}", name, value, e)))
}

fn human_size(bytes: u64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut value = bytes as f64;
    let mut idx = 0usize;
    while value >= 1000.0 && idx + 1 < units.len() {
        value /= 1000.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{} {}", bytes, units[idx])
    } else if value < 10.0 {
        format!("{:.2} {}", value, units[idx])
    } else if value < 100.0 {
        format!("{:.1} {}", value, units[idx])
    } else {
        format!("{:.0} {}", value, units[idx])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_df_output() {
        let output =
            "  1B-blocks        Used   Available Use%\n100000000000 25000000000 75000000000  25%\n";
        let stats = parse_df_output(output).expect("parse df");
        assert_eq!(
            stats,
            DfStats {
                size_bytes: 100_000_000_000,
                used_bytes: 25_000_000_000,
                free_bytes: 75_000_000_000,
                used_percent: "25%".to_string(),
            }
        );
    }

    #[test]
    fn formats_human_size() {
        assert_eq!(human_size(999), "999 B");
        assert_eq!(human_size(1_500_000), "1.50 MB");
        assert_eq!(human_size(75_000_000_000), "75.0 GB");
    }

    #[test]
    fn selects_configured_disk_by_fs_uuid() {
        let disks = vec![BackupDiskConfig {
            disk_id: "primary".to_string(),
            fs_uuid: "uuid-primary".to_string(),
            label: None,
            mount_options: None,
            disabled: false,
            rotated_out: false,
        }];
        let selected = select_configured_disk(&disks, "uuid-primary").expect("select disk");
        assert_eq!(selected.disk_id, "primary");
    }
}
