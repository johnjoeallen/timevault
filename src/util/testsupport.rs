//! Helpers shared by tests that need more than directories: real SSH to this
//! machine, and a genuinely small filesystem.

use std::path::PathBuf;
use std::process::Command;

/// SSH to `localhost` as the current user, for tests of remote sources.
///
/// Opt in with `TIMEVAULT_TEST_SSH=1`; it needs key-based login to this
/// machine and is never run by default. A wrapper `ssh` placed first on `PATH`
/// adds `BatchMode` and a throw-away known-hosts file, so nothing in the
/// user's `~/.ssh` is read for trust decisions or modified. `PATH` is
/// process-wide, so run such tests with `--test-threads=1`.
pub struct SshLoopback {
    _dir: tempfile::TempDir,
    old_path: String,
    pub user: String,
}

impl SshLoopback {
    pub fn enabled() -> Option<SshLoopback> {
        if std::env::var("TIMEVAULT_TEST_SSH").as_deref() != Ok("1") {
            eprintln!("TIMEVAULT_TEST_SSH is not 1: skipping the real-SSH test");
            return None;
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let real = ["/usr/bin/ssh", "/bin/ssh"]
            .iter()
            .find(|p| std::path::Path::new(p).exists())
            .expect("ssh binary");
        let wrapper = dir.path().join("ssh");
        std::fs::write(
            &wrapper,
            format!(
                "#!/bin/sh\nexec {real} -o BatchMode=yes -o ConnectTimeout=5 \
                 -o UserKnownHostsFile={known} -o StrictHostKeyChecking=accept-new \"$@\"\n",
                known = dir.path().join("known_hosts").display()
            ),
        )
        .expect("wrapper");
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let user = String::from_utf8(Command::new("id").arg("-un").output().expect("id").stdout)
            .unwrap()
            .trim()
            .to_string();
        let old_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", dir.path().display(), old_path));
        let probe = Command::new("ssh")
            .args(["localhost", "true"])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !probe {
            std::env::set_var("PATH", &old_path);
            eprintln!("cannot ssh to localhost without a password: skipping the real-SSH test");
            return None;
        }
        Some(SshLoopback {
            _dir: dir,
            old_path,
            user,
        })
    }

    /// An rsync-style remote source for a directory on this machine.
    pub fn source(&self, path: &std::path::Path) -> String {
        format!("{}@localhost:{}", self.user, path.display())
    }
}

impl Drop for SshLoopback {
    fn drop(&mut self) {
        std::env::set_var("PATH", &self.old_path);
    }
}

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
