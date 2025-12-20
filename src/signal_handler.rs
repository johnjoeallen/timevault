use std::{process::Command, thread};

use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
};

use crate::data::MountTracker;

pub fn signal_handler(mounts: &MountTracker) {
    let mounts = mounts.clone();
    thread::spawn(move || {
        let mut signals = match Signals::new([SIGINT, SIGTERM]) {
            Ok(signals) => signals,
            Err(err) => {
                eprintln!("signal handler setup failed: {}", err);
                return;
            }
        };
        if signals.forever().next().is_some() {
            let mut list: Vec<_> = match mounts.lock() {
                Ok(set) => set.iter().cloned().collect(),
                Err(_) => Vec::new(),
            };
            list.sort_by_key(|m| std::cmp::Reverse(m.as_os_str().len()));
            for mount in list {
                let mut cmd = Command::new("umount");
                cmd.arg(&mount);
                let _ = cmd.status();
            }
            std::process::exit(1);
        }
    });
}
