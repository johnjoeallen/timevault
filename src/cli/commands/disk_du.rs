use std::path::Path;
use std::process::Command;

use crate::cli::args::DiskDuArgs;
use crate::cli::commands::disk_ls::{
    mountpoint_for_disk_path, parse_disk_path_target, resolve_inside_mount, verify_disk_identity,
};
use crate::cli::commands::exit_for_disk_error;
use crate::config::load::load_config;
use crate::disk::select_disk;
use crate::error::{Result, TimevaultError};
use crate::mount::guard::MountGuard;

pub fn run_du(config_path: &Path, args: DiskDuArgs) -> Result<()> {
    if args.args.is_empty() {
        return Err(TimevaultError::message(
            "disk du requires du options and at least one <disk-id>:/path target".to_string(),
        ));
    }

    let cfg = load_config(config_path.to_string_lossy().as_ref())?;
    let mut translated_args = Vec::with_capacity(args.args.len());
    let mut guards: Vec<MountGuard> = Vec::new();
    let mut translated_any = false;

    for arg in args.args {
        match parse_disk_path_target(&arg) {
            Ok(target) => {
                let disk = match select_disk(&cfg.backup_disks, Some(&target.disk_id)) {
                    Ok(disk) => disk,
                    Err(TimevaultError::Disk(err)) => exit_for_disk_error(&err),
                    Err(err) => return Err(err),
                };
                let (mountpoint, guard) =
                    mountpoint_for_disk_path(&disk, Path::new(&cfg.user_mount_base))?;
                verify_disk_identity(&disk, &mountpoint)?;
                let path = resolve_inside_mount(&mountpoint, &target.path)?;
                if let Some(guard) = guard {
                    guards.push(guard);
                }
                translated_args.push(path.to_string_lossy().to_string());
                translated_any = true;
            }
            Err(_) => translated_args.push(arg),
        }
    }

    if !translated_any {
        return Err(TimevaultError::message(
            "disk du requires at least one <disk-id>:/path target".to_string(),
        ));
    }

    let status = Command::new("du")
        .args(&translated_args)
        .status()
        .map_err(|e| TimevaultError::message(format!("du: {}", e)))?;
    drop(guards);
    if !status.success() {
        return Err(TimevaultError::message(format!(
            "du failed with exit code {}",
            status.code().unwrap_or(1)
        )));
    }

    Ok(())
}
