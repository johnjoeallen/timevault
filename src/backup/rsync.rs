use std::io::{self, BufReader, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};

use crate::error::{Result, TimevaultError};
use crate::progress;
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
    label: &str,
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
    let streamed = progress::enabled();
    if streamed {
        args.push("--info=progress2".to_string());
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
    if streamed {
        return run_rsync_streamed(&mut cmd, label);
    }

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

/// Spawn rsync with piped output and feed its `--info=progress2` line to the
/// progress spinner. rsync emits progress updates terminated with `\r` and the
/// `--stats` block terminated with `\n`, so we split on both.
fn run_rsync_streamed(cmd: &mut Command, label: &str) -> Result<RsyncResult> {
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| TimevaultError::message(format!("rsync: {}", err)))?;

    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let stderr_reader = std::thread::spawn(move || {
        let mut buf = String::new();
        let _ = BufReader::new(stderr).read_to_string(&mut buf);
        buf
    });

    let mut reader = BufReader::new(stdout);
    let mut captured = String::new();
    let mut segment = String::new();
    let mut byte = [0u8; 1];
    while let Ok(1) = reader.read(&mut byte) {
        let ch = byte[0] as char;
        if ch == '\r' || ch == '\n' {
            handle_rsync_line(&segment, label, &mut captured);
            segment.clear();
        } else {
            segment.push(ch);
        }
    }
    handle_rsync_line(&segment, label, &mut captured);

    let status = child
        .wait()
        .map_err(|err| TimevaultError::message(format!("rsync: {}", err)))?;
    let stderr_text = stderr_reader.join().unwrap_or_default();
    if !stderr_text.trim().is_empty() {
        progress::note(stderr_text.trim_end());
    }
    if let Some(summary) = rsync_summary(&captured, label) {
        progress::note(summary);
    }

    Ok(RsyncResult {
        exit_code: status.code().unwrap_or(1),
        stderr: stderr_text.trim().to_string(),
    })
}

fn handle_rsync_line(segment: &str, label: &str, captured: &mut String) {
    let line = segment.trim_end_matches(['\r', '\n']);
    if line.is_empty() {
        return;
    }
    captured.push_str(line);
    captured.push('\n');
    if let Some((pct, rate)) = parse_rsync_progress(line) {
        if rate.is_empty() {
            progress::status(format!("{}: {}", label, pct));
        } else {
            progress::status(format!("{}: {}  {}", label, pct, rate));
        }
    }
}

/// From an rsync `--info=progress2` line like
/// `        1,234,567  45%   12.34MB/s    0:00:07` return `("45%", "12.34MB/s")`.
pub fn parse_rsync_progress(line: &str) -> Option<(String, String)> {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    let idx = tokens
        .iter()
        .position(|tok| tok.len() > 1 && tok.ends_with('%'))?;
    let pct = tokens[idx];
    // A progress2 line is `<size> <pct> <rate> <elapsed>`: the % must parse and
    // be followed by a transfer rate. This rejects stats lines like
    // `speedup is 27.14%`.
    pct.trim_end_matches('%')
        .replace(',', ".")
        .parse::<f64>()
        .ok()?;
    let rate = tokens.get(idx + 1).filter(|tok| tok.contains("/s"))?;
    Some((pct.to_string(), rate.to_string()))
}

/// A one-line note from rsync's `--stats` block, e.g.
/// `spitfire-primary: 42 files, 1.23G transferred`.
fn rsync_summary(captured: &str, label: &str) -> Option<String> {
    let field = |name: &str| {
        captured
            .lines()
            .find_map(|line| line.trim().strip_prefix(name))
            .map(|rest| rest.trim().trim_end_matches(" bytes").trim().to_string())
    };
    let files = field("Number of regular files transferred:")
        .or_else(|| field("Number of files transferred:"))?;
    let size = field("Total transferred file size:").unwrap_or_default();
    if size.is_empty() {
        Some(format!("{}: {} files transferred", label, files))
    } else {
        Some(format!("{}: {} files, {} transferred", label, files, size))
    }
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
    fn parses_rsync_progress2_lines() {
        assert_eq!(
            parse_rsync_progress("      1,234,567  45%   12.34MB/s    0:00:07"),
            Some(("45%".to_string(), "12.34MB/s".to_string()))
        );
        assert_eq!(
            parse_rsync_progress(
                "        163.88M 100%   21.50MB/s    0:00:07 (xfr#12, to-chk=0/345)"
            ),
            Some(("100%".to_string(), "21.50MB/s".to_string()))
        );
        assert_eq!(
            parse_rsync_progress("              0   0%    0.00kB/s    0:00:00"),
            Some(("0%".to_string(), "0.00kB/s".to_string()))
        );
    }

    #[test]
    fn ignores_non_progress_lines() {
        assert_eq!(parse_rsync_progress("Number of files: 1,234"), None);
        assert_eq!(parse_rsync_progress("sending incremental file list"), None);
        assert_eq!(parse_rsync_progress(""), None);
        assert_eq!(parse_rsync_progress("speedup is 27.14%"), None);
    }

    #[test]
    fn rsync_summary_from_stats_block() {
        let stats = "\
Number of files: 10
Number of regular files transferred: 3
Total file size: 100 bytes
Total transferred file size: 1.23G bytes
";
        assert_eq!(
            rsync_summary(stats, "primary"),
            Some("primary: 3 files, 1.23G transferred".to_string())
        );
        assert_eq!(rsync_summary("no stats here", "primary"), None);
    }

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
