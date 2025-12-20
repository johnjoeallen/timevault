use std::path::Path;
use std::process::{Command, Stdio};

use crate::error::{DiskError, Result, TimevaultError};
use crate::types::RunMode;
use crate::util::command::run_command;

pub fn mount_device(device: &Path, mountpoint: &Path, options: &str) -> Result<()> {
    let mut cmd = Command::new("mount");
    cmd.arg("-o").arg(options).arg(device).arg(mountpoint);
    let rc = run_command(&mut cmd, RunMode { dry_run: false, safe_mode: false, verbose: false })
        .map_err(|e| TimevaultError::message(format!("mount {}: {}", device.display(), e)))?;
    if rc != 0 {
        return Err(DiskError::MountFailure(format!(
            "{} failed with exit code {}",
            device.display(),
            rc
        ))
        .into());
    }
    Ok(())
}

pub fn mount_device_silent(device: &Path, mountpoint: &Path, options: &str) -> Result<()> {
    let status = Command::new("mount")
        .arg("-o")
        .arg(options)
        .arg(device)
        .arg(mountpoint)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| TimevaultError::message(format!("mount {}: {}", device.display(), e)))?;
    if !status.success() {
        return Err(DiskError::MountFailure(format!(
            "{} failed with exit code {}",
            device.display(),
            status.code().unwrap_or(1)
        ))
        .into());
    }
    Ok(())
}

pub fn unmount_path(mountpoint: &Path) -> Result<()> {
    let mut cmd = Command::new("umount");
    cmd.arg(mountpoint);
    let rc = run_command(&mut cmd, RunMode { dry_run: false, safe_mode: false, verbose: false })
        .map_err(|e| TimevaultError::message(format!("umount {}: {}", mountpoint.display(), e)))?;
    if rc != 0 {
        return Err(DiskError::UmountFailure(format!(
            "{} failed with exit code {}",
            mountpoint.display(),
            rc
        ))
        .into());
    }
    Ok(())
}
