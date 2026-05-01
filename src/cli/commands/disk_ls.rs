use std::fs;
use std::path::{Path, PathBuf};

use crate::cli::args::DiskLsArgs;
use crate::cli::commands::{disk_add, exit_for_disk_error};
use crate::config::load::load_config;
use crate::config::model::BackupDiskConfig;
use crate::disk::identity::{identity_path, read_identity, verify_identity};
use crate::disk::{device_path_for_uuid, mount_options_for_restore, select_disk};
use crate::error::{DiskError, Result, TimevaultError};
use crate::mount::guard::MountGuard;
use crate::mount::inspect::{find_device_mountpoint, mountpoint_is_mounted};
use crate::mount::ops::mount_device;
use crate::util::paths::{create_temp_dir, ensure_base_dir};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiskPathTarget {
    pub(crate) disk_id: String,
    pub(crate) path: String,
}

pub fn run_ls(config_path: &Path, args: DiskLsArgs) -> Result<()> {
    let Some(target) = args.target.as_deref() else {
        let output = if args.columns {
            disk_add::DiskListOutput::Columns
        } else if args.short {
            disk_add::DiskListOutput::Short
        } else {
            disk_add::DiskListOutput::Verbose
        };
        return disk_add::run_discover_with_output(config_path, output);
    };

    let target = parse_disk_path_target(target)?;
    let cfg = load_config(config_path.to_string_lossy().as_ref())?;
    let disk = match select_disk(&cfg.backup_disks, Some(&target.disk_id)) {
        Ok(disk) => disk,
        Err(TimevaultError::Disk(err)) => exit_for_disk_error(&err),
        Err(err) => return Err(err),
    };

    let (mountpoint, _guard) = mountpoint_for_disk_path(&disk, Path::new(&cfg.user_mount_base))?;
    verify_disk_identity(&disk, &mountpoint)?;
    let path = resolve_inside_mount(&mountpoint, &target.path)?;
    print_listing(&target, &path)
}

pub(crate) fn parse_disk_path_target(value: &str) -> Result<DiskPathTarget> {
    let Some((disk_id, path)) = value.split_once(':') else {
        return Err(TimevaultError::message(
            "disk ls path must use <disk-id>:/path".to_string(),
        ));
    };
    if disk_id.trim().is_empty() {
        return Err(TimevaultError::message(
            "disk ls path must include a disk id".to_string(),
        ));
    }
    if path.trim().is_empty() {
        return Err(TimevaultError::message(
            "disk ls path must include a path, for example disk-id:/".to_string(),
        ));
    }
    Ok(DiskPathTarget {
        disk_id: disk_id.to_string(),
        path: path.to_string(),
    })
}

pub(crate) fn mountpoint_for_disk_path(
    disk: &BackupDiskConfig,
    user_mount_base: &Path,
) -> Result<(PathBuf, Option<MountGuard>)> {
    let device = device_path_for_uuid(&disk.fs_uuid);
    if !device.exists() {
        return Err(DiskError::Other(format!("device {} not found", device.display())).into());
    }
    if let Some(mountpoint) = find_device_mountpoint(&device)? {
        return Ok((mountpoint, None));
    }

    ensure_base_dir(user_mount_base)?;
    let mountpoint = create_temp_dir(user_mount_base, "tv-ls")?;
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
    Ok((mountpoint, Some(guard)))
}

pub(crate) fn verify_disk_identity(disk: &BackupDiskConfig, mountpoint: &Path) -> Result<()> {
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

pub(crate) fn resolve_inside_mount(mountpoint: &Path, requested_path: &str) -> Result<PathBuf> {
    let relative = requested_path.trim_start_matches('/');
    let candidate = if relative.is_empty() {
        mountpoint.to_path_buf()
    } else {
        mountpoint.join(relative)
    };
    let mountpoint_real = mountpoint
        .canonicalize()
        .map_err(|e| TimevaultError::message(format!("resolve {}: {}", mountpoint.display(), e)))?;
    let candidate_real = candidate
        .canonicalize()
        .map_err(|e| TimevaultError::message(format!("resolve {}: {}", candidate.display(), e)))?;
    if !candidate_real.starts_with(&mountpoint_real) {
        return Err(TimevaultError::message(format!(
            "path {} escapes disk mount",
            requested_path
        )));
    }
    Ok(candidate_real)
}

fn print_listing(target: &DiskPathTarget, path: &Path) -> Result<()> {
    if path.is_dir() {
        println!("{}:{}", target.disk_id, target.path);
        let mut entries = fs::read_dir(path)
            .map_err(|e| TimevaultError::message(format!("read {}: {}", path.display(), e)))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| TimevaultError::message(format!("read {}: {}", path.display(), e)))?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let file_type = entry.file_type().map_err(|e| {
                TimevaultError::message(format!("stat {}: {}", entry.path().display(), e))
            })?;
            let mut name = entry.file_name().to_string_lossy().to_string();
            if file_type.is_dir() {
                name.push('/');
            }
            println!("{}", name);
        }
        return Ok(());
    }
    if path.is_file() {
        println!("{}:{}", target.disk_id, target.path);
        return Ok(());
    }
    Err(TimevaultError::message(format!(
        "{} is not a file or directory",
        path.display()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn parses_disk_path_target() {
        let target = parse_disk_path_target("primary:/snapshots").expect("parse");
        assert_eq!(
            target,
            DiskPathTarget {
                disk_id: "primary".to_string(),
                path: "/snapshots".to_string(),
            }
        );
    }

    #[test]
    fn rejects_target_without_colon() {
        assert!(parse_disk_path_target("primary").is_err());
    }

    #[test]
    fn resolves_path_inside_mount() {
        let dir = TempDir::new().expect("tempdir");
        fs::create_dir(dir.path().join("snapshots")).expect("mkdir");
        let resolved = resolve_inside_mount(dir.path(), "/snapshots").expect("resolve");
        assert!(resolved.ends_with("snapshots"));
    }
}
