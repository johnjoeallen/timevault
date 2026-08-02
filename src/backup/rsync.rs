use std::io::{self, Write};
use std::path::Path;
use std::process::{Command, Stdio};

use crate::error::{Result, TimevaultError};
use crate::types::RunMode;
use crate::util::command::maybe_print_command;

pub struct RsyncResult {
    pub exit_code: i32,
    pub stderr: String,
}

pub fn run_rsync(
    source: &str,
    backup_dir: &Path,
    excludes_file: &Path,
    extra: &[String],
    run_mode: RunMode,
) -> Result<RsyncResult> {
    let source = normalize_rsync_source(source);
    let backup_dir = ensure_trailing_slash(&backup_dir.to_string_lossy());
    let mut args = vec![
        "rsync".to_string(),
        "-ar".to_string(),
        "--stats".to_string(),
        format!("--exclude-from={}", excludes_file.display()),
    ];
    if !run_mode.safe_mode {
        args.push("--delete-after".to_string());
        args.push("--delete-excluded".to_string());
    }
    args.extend(extra.iter().cloned());
    args.push(source);
    args.push(backup_dir);
    let mut cmd = Command::new("nice");
    cmd.arg("-n")
        .arg("19")
        .arg("ionice")
        .arg("-c")
        .arg("3")
        .arg("-n7");
    cmd.args(&args);

    if run_mode.dry_run {
        maybe_print_command(&cmd, run_mode);
        return Ok(RsyncResult {
            exit_code: 0,
            stderr: String::new(),
        });
    }

    maybe_print_command(&cmd, run_mode);
    let output = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| TimevaultError::message(format!("rsync: {}", err)))?;
    io::stdout().write_all(&output.stdout)?;
    io::stderr().write_all(&output.stderr)?;

    Ok(RsyncResult {
        exit_code: output.status.code().unwrap_or(1),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

fn normalize_rsync_source(source: &str) -> String {
    if source.ends_with('/') {
        return source.to_string();
    }
    if source.contains(':') || Path::new(source).exists() || is_symlink(source) {
        return ensure_trailing_slash(source);
    }
    ensure_trailing_slash(source)
}

fn ensure_trailing_slash(path: &str) -> String {
    if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{}/", path)
    }
}

fn is_symlink(path: &str) -> bool {
    std::fs::symlink_metadata(path)
        .map(|meta| meta.file_type().is_symlink())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use tempfile::TempDir;

    #[test]
    fn normalize_rsync_source_always_trailing_slash() {
        assert_eq!(normalize_rsync_source("/"), "/");
        assert_eq!(normalize_rsync_source("/tmp"), "/tmp/");
        assert_eq!(normalize_rsync_source("host:/var"), "host:/var/");
        assert_eq!(normalize_rsync_source("relative/path"), "relative/path/");
    }

    #[cfg(unix)]
    #[test]
    fn normalize_rsync_source_symlink_trailing_slash() {
        let dir = TempDir::new().expect("tempdir");
        let target = dir.path().join("target");
        fs::create_dir_all(&target).expect("mkdir");
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&target, &link).expect("symlink");
        assert_eq!(
            normalize_rsync_source(link.to_string_lossy().as_ref()),
            format!("{}/", link.to_string_lossy())
        );
    }

    #[test]
    fn ensure_trailing_slash_adds_when_missing() {
        assert_eq!(ensure_trailing_slash("/tmp"), "/tmp/");
        assert_eq!(ensure_trailing_slash("/tmp/"), "/tmp/");
    }

    #[test]
    fn normalize_rsync_source_file_path() {
        let dir = TempDir::new().expect("tempdir");
        let file = dir.path().join("file.txt");
        File::create(&file).expect("create");
        assert_eq!(
            normalize_rsync_source(file.to_string_lossy().as_ref()),
            format!("{}/", file.to_string_lossy())
        );
    }
}
