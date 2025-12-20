use std::path::Path;

use crate::error::Result;
use crate::types::RunMode;
use crate::util::command::run_nice_ionice;

pub fn run_rsync(
    source: &str,
    backup_dir: &Path,
    excludes_file: &Path,
    extra: &[String],
    run_mode: RunMode,
) -> Result<i32> {
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
    args.push(source.to_string());
    args.push(backup_dir.to_string_lossy().to_string());
    run_nice_ionice(&args, run_mode)
}
