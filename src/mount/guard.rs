use std::path::PathBuf;

use crate::mount::ops::unmount_path;

pub struct MountGuard {
    mountpoint: PathBuf,
    remove_dir: bool,
}

impl MountGuard {
    pub fn new(mountpoint: PathBuf, remove_dir: bool) -> Self {
        Self { mountpoint, remove_dir }
    }

    pub fn mountpoint(&self) -> &PathBuf {
        &self.mountpoint
    }
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        let _ = unmount_path(&self.mountpoint);
        if self.remove_dir {
            let _ = std::fs::remove_dir(&self.mountpoint);
        }
    }
}
