use std::path::PathBuf;
use std::sync::{Mutex, Once, OnceLock};
use std::thread;

use signal_hook::consts::signal::{SIGHUP, SIGINT, SIGQUIT, SIGTERM};
use signal_hook::iterator::Signals;

use crate::error::Result;
use crate::mount::ops::unmount_path;

#[derive(Clone)]
struct TrackedMount {
    mountpoint: PathBuf,
    remove_dir: bool,
}

static ACTIVE_MOUNTS: OnceLock<Mutex<Vec<TrackedMount>>> = OnceLock::new();
static INSTALL_SIGNALS: Once = Once::new();

pub fn install_signal_handlers() -> Result<()> {
    let mut install_result = Ok(());
    INSTALL_SIGNALS.call_once(|| {
        let signals = Signals::new([SIGINT, SIGTERM, SIGHUP, SIGQUIT]);
        let mut signals = match signals {
            Ok(signals) => signals,
            Err(err) => {
                install_result = Err(err.into());
                return;
            }
        };
        thread::spawn(move || {
            if let Some(signal) = signals.forever().next() {
                cleanup_active_mounts();
                std::process::exit(128 + signal);
            }
        });
    });
    install_result
}

pub fn track_mount(mountpoint: PathBuf, remove_dir: bool) {
    active_mounts()
        .lock()
        .expect("active mount registry poisoned")
        .push(TrackedMount {
            mountpoint,
            remove_dir,
        });
}

pub fn untrack_mount(mountpoint: &PathBuf) {
    let mut mounts = active_mounts()
        .lock()
        .expect("active mount registry poisoned");
    if let Some(index) = mounts
        .iter()
        .position(|tracked| tracked.mountpoint == *mountpoint)
    {
        mounts.swap_remove(index);
    }
}

fn cleanup_active_mounts() {
    let mounts = {
        let mut mounts = active_mounts()
            .lock()
            .expect("active mount registry poisoned");
        mounts.drain(..).collect::<Vec<_>>()
    };

    for tracked in mounts.into_iter().rev() {
        let _ = unmount_path(&tracked.mountpoint);
        if tracked.remove_dir {
            let _ = std::fs::remove_dir(&tracked.mountpoint);
        }
    }
}

fn active_mounts() -> &'static Mutex<Vec<TrackedMount>> {
    ACTIVE_MOUNTS.get_or_init(|| Mutex::new(Vec::new()))
}
