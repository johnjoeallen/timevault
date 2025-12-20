use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Result, TimevaultError};

fn read_mounts() -> Result<String> {
    fs::read_to_string("/proc/self/mounts")
        .map_err(|e| TimevaultError::message(format!("read /proc/self/mounts: {}", e)))
}

pub fn device_is_mounted(device: &Path) -> Result<bool> {
    let contents = read_mounts()?;
    let device_real = device
        .canonicalize()
        .map_err(|e| TimevaultError::message(format!("resolve {}: {}", device.display(), e)))?;
    for line in contents.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            continue;
        }
        let mounted_dev = Path::new(fields[0]);
        let mounted_real = mounted_dev
            .canonicalize()
            .unwrap_or_else(|_| mounted_dev.to_path_buf());
        if mounted_real == device_real {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn find_device_mountpoint(device: &Path) -> Result<Option<PathBuf>> {
    let contents = read_mounts()?;
    let device_real = device
        .canonicalize()
        .map_err(|e| TimevaultError::message(format!("resolve {}: {}", device.display(), e)))?;
    for line in contents.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            continue;
        }
        let mounted_dev = Path::new(fields[0]);
        let mounted_real = mounted_dev
            .canonicalize()
            .unwrap_or_else(|_| mounted_dev.to_path_buf());
        if mounted_real == device_real {
            return Ok(Some(PathBuf::from(fields[1])));
        }
    }
    Ok(None)
}

pub fn mountpoint_is_mounted(mountpoint: &Path) -> Result<bool> {
    let contents = read_mounts()?;
    for line in contents.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            continue;
        }
        if Path::new(fields[1]) == mountpoint {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn find_mounts_under(base: &Path) -> Result<Vec<PathBuf>> {
    let contents = read_mounts()?;
    let mut mounts = Vec::new();
    for line in contents.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            continue;
        }
        let mountpoint = Path::new(fields[1]);
        if mountpoint.starts_with(base) {
            mounts.push(mountpoint.to_path_buf());
        }
    }
    Ok(mounts)
}
