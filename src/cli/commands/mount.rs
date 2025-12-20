use std::path::Path;

use crate::cli::args::MountArgs;
use crate::cli::commands::exit_for_disk_error;
use crate::config::load::load_config;
use crate::disk::{device_path_for_uuid, ensure_disk_not_mounted, mount_options_for_restore, select_disk};
use crate::disk::fs_type::detect_fs_type;
use crate::disk::identity::{identity_path, read_identity, verify_identity};
use crate::error::{DiskError, Result, TimevaultError};
use crate::mount::inspect::mountpoint_is_mounted;
use crate::mount::ops::mount_device;
use crate::util::paths::{create_temp_dir, ensure_base_dir};

pub fn run_mount(config_path: &Path, _args: MountArgs, disk_id: Option<&str>) -> Result<()> {
    let cfg = load_config(config_path.to_string_lossy().as_ref())?;
    let disk = match select_disk(&cfg.backup_disks, disk_id) {
        Ok(disk) => disk,
        Err(TimevaultError::Disk(err)) => exit_for_disk_error(&err),
        Err(err) => return Err(err),
    };

    let device = device_path_for_uuid(&disk.fs_uuid);
    if !device.exists() {
        return Err(DiskError::Other(format!("device {} not found", device.display())).into());
    }
    ensure_disk_not_mounted(&device)?;

    ensure_base_dir(Path::new(&cfg.user_mount_base))?;
    let mountpoint = create_temp_dir(Path::new(&cfg.user_mount_base), "tv")?;

    if mountpoint_is_mounted(&mountpoint)? {
        return Err(DiskError::Other(format!(
            "mountpoint {} is already in use",
            mountpoint.display()
        ))
        .into());
    }

    let options = mount_options_for_restore(&disk);
    mount_device(&device, &mountpoint, &options)?;
    let identity_path = identity_path(&mountpoint);
    if !identity_path.exists() {
        let _ = crate::mount::ops::unmount_path(&mountpoint);
        exit_for_disk_error(&DiskError::IdentityMismatch(format!(
            "file missing at {}; expected diskId {} fsUuid {} (run `timevault disk enroll ...`)",
            identity_path.display(),
            disk.disk_id,
            disk.fs_uuid
        )));
    }
    let identity = match read_identity(&identity_path) {
        Ok(identity) => identity,
        Err(err) => {
            let _ = crate::mount::ops::unmount_path(&mountpoint);
            return Err(TimevaultError::message(format!("identity file invalid: {}", err)));
        }
    };
    if let Err(err) = verify_identity(&identity, &disk.disk_id, &disk.fs_uuid) {
        let _ = crate::mount::ops::unmount_path(&mountpoint);
        if let TimevaultError::Disk(disk_err) = err {
            exit_for_disk_error(&disk_err);
        }
        return Err(err);
    }

    let fs_type = detect_fs_type(device.to_string_lossy().as_ref())?;
    if !fs_type.is_allowed() {
        let _ = crate::mount::ops::unmount_path(&mountpoint);
        return Err(DiskError::Other(format!(
            "unsupported filesystem type {}",
            fs_type
        ))
        .into());
    }
    if let Some(identity_type) = identity.fs_type {
        if identity_type != fs_type {
            let _ = crate::mount::ops::unmount_path(&mountpoint);
            return Err(DiskError::IdentityMismatch(format!(
                "fsType mismatch: expected {}, got {}",
                identity_type,
                fs_type
            ))
            .into());
        }
    }

    println!("{}", mountpoint.display());
    Ok(())
}
