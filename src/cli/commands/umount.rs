use std::path::Path;

use crate::cli::args::UmountArgs;
use crate::cli::commands::exit_for_disk_error;
use crate::config::load::load_config;
use crate::error::{DiskError, Result, TimevaultError};
use crate::mount::inspect::find_mounts_under;
use crate::mount::ops::unmount_path;

pub fn run_umount(config_path: &Path, args: UmountArgs) -> Result<()> {
    let cfg = load_config(config_path.to_string_lossy().as_ref())?;
    let mountpoint = if let Some(path) = args.mountpoint {
        path
    } else {
        let mounts = find_mounts_under(Path::new(&cfg.user_mount_base))?;
        if mounts.is_empty() {
            return Err(DiskError::Other("no timevault mounts found".to_string()).into());
        }
        if mounts.len() > 1 {
            return Err(DiskError::Other(
                "multiple timevault mounts found; unmount manually".to_string(),
            )
            .into());
        }
        mounts[0].clone()
    };

    if let Err(err) = unmount_path(&mountpoint) {
        if let TimevaultError::Disk(disk_err) = err {
            exit_for_disk_error(&disk_err);
        }
        return Err(err);
    }
    if mountpoint.starts_with(&cfg.user_mount_base) {
        let _ = std::fs::remove_dir(&mountpoint);
    }
    Ok(())
}
