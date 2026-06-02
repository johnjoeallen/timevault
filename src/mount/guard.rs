use std::path::PathBuf;

use crate::mount::ops::unmount_path;
use crate::mount::signals::{track_mount, untrack_mount};

pub struct MountGuard {
    mountpoint: PathBuf,
    remove_dir: bool,
}

impl MountGuard {
    pub fn new(mountpoint: PathBuf, remove_dir: bool) -> Self {
        track_mount(mountpoint.clone(), remove_dir);
        Self {
            mountpoint,
            remove_dir,
        }
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
        untrack_mount(&self.mountpoint);
    }
}
