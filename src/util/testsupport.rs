//! Helpers shared by tests that need more than directories: a genuinely small
//! filesystem.

use std::path::PathBuf;
use std::process::Command;

/// Run `test_path` of this test binary inside a new mount namespace with a
/// small tmpfs mounted at `$TIMEVAULT_SMALL_FS`, so a test can fill a real
/// filesystem and see a genuine `ENOSPC`. Needs unprivileged user namespaces;
/// returns `None` when they are unavailable so the caller can skip.
pub fn run_with_small_fs(test_path: &str, size: &str) -> Option<bool> {
    let probe = Command::new("unshare")
        .args(["-r", "-m", "--", "true"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !probe {
        eprintln!("unprivileged mount namespaces are unavailable: skipping");
        return None;
    }
    let mountpoint: PathBuf = tempfile::tempdir().expect("tempdir").keep();
    let exe = std::env::current_exe().expect("current exe");
    let script = format!(
        "mount -t tmpfs -o size={size} tmpfs \"$TIMEVAULT_SMALL_FS\" && \
         exec \"$0\" {test_path} --exact --ignored --nocapture"
    );
    let status = Command::new("unshare")
        .args(["-r", "-m", "--", "sh", "-c", &script])
        .arg(&exe)
        .env("TIMEVAULT_SMALL_FS", &mountpoint)
        .status()
        .expect("unshare");
    let _ = std::fs::remove_dir_all(&mountpoint);
    Some(status.success())
}
