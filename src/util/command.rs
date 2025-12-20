use std::process::Command;

use crate::error::{Result, TimevaultError};
use crate::types::RunMode;

pub fn maybe_print_command(cmd: &Command, run_mode: RunMode) {
    if !run_mode.dry_run && !run_mode.verbose {
        return;
    }
    let program = cmd.get_program().to_string_lossy();
    let args: Vec<String> = cmd
        .get_args()
        .map(|a| a.to_string_lossy().to_string())
        .collect();
    println!("{} {}", program, args.join(" "));
}

pub fn run_command(cmd: &mut Command, run_mode: RunMode) -> Result<i32> {
    maybe_print_command(cmd, run_mode);
    let status = cmd
        .status()
        .map_err(|e| TimevaultError::message(format!("{}: {}", cmd.get_program().to_string_lossy(), e)))?;
    Ok(status.code().unwrap_or(1))
}

pub fn run_nice_ionice(args: &[String], run_mode: RunMode) -> Result<i32> {
    let mut cmd = Command::new("nice");
    cmd.arg("-n")
        .arg("19")
        .arg("ionice")
        .arg("-c")
        .arg("3")
        .arg("-n7");
    for arg in args {
        cmd.arg(arg);
    }
    if run_mode.dry_run {
        maybe_print_command(&cmd, run_mode);
        Ok(0)
    } else {
        run_command(&mut cmd, run_mode)
    }
}
