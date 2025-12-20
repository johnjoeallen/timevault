use std::{io, path::Path, process::Command};

use crate::RunMode;

pub struct Mount<'a>(pub &'a Path);
pub struct ReMount<'a>(pub &'a Path);

pub struct UnMount<'a>(pub &'a Path);
pub struct SyncCommand;
pub struct IoNice<'a>(pub &'a [String]);

impl<'a> From<Mount<'a>> for Command {
    fn from(mount: Mount) -> Self {
        let mut cmd = Command::new("mount");
        cmd.arg("-oremount,ro").arg(mount.0);
        cmd
    }
}

impl<'a> From<ReMount<'a>> for Command {
    fn from(mount: ReMount) -> Self {
        let mut cmd = Command::new("mount");
        cmd.arg(mount.0);
        cmd
    }
}

impl<'a> From<UnMount<'a>> for Command {
    fn from(mount: UnMount) -> Self {
        let mut cmd = Command::new("unmount");
        cmd.arg(mount.0);
        cmd
    }
}

impl From<SyncCommand> for Command {
    fn from(_: SyncCommand) -> Self {
        Command::new("sync")
    }
}

impl<'a> From<IoNice<'a>> for Command {
    fn from(c: IoNice) -> Self {
        let mut cmd = Command::new("nice");
        cmd.arg("-n")
            .arg("19")
            .arg("ionice")
            .arg("-c")
            .arg("3")
            .arg("-n7");
        for arg in c.0 {
            cmd.arg(arg);
        }
        cmd
    }
}

fn print_command(cmd: &Command) {
    let program = cmd.get_program().to_string_lossy();
    let args: Vec<String> = cmd
        .get_args()
        .map(|a| a.to_string_lossy().to_string())
        .collect();
    println!("{} {}", program, args.join(" "));
}

fn run_and_print<T: Into<Command>>(command: T, run_mode: &RunMode) -> io::Result<i32> {
    let mut cmd: Command = command.into();
    if run_mode.verbose {
        print_command(&cmd);
    }
    if run_mode.dry_run {
        Ok(0)
    } else {
        let status = cmd.status()?;
        Ok(status.code().unwrap_or(1))
    }
}

impl<'a> Mount<'a> {
    pub fn run(self, run_mode: &RunMode) -> io::Result<i32> {
        run_and_print::<Command>(self.into(), run_mode)
    }
}

impl<'a> ReMount<'a> {
    pub fn run(self, run_mode: &RunMode) -> io::Result<i32> {
        run_and_print::<Command>(self.into(), run_mode)
    }
}

impl<'a> UnMount<'a> {
    pub fn run(self, run_mode: &RunMode) -> io::Result<i32> {
        run_and_print::<Command>(self.into(), run_mode)
    }
}

impl SyncCommand {
    pub fn run(self, run_mode: &RunMode) -> io::Result<i32> {
        run_and_print::<Command>(self.into(), run_mode)
    }
}

impl<'a> IoNice<'a> {
    pub fn run(self, run_mode: &RunMode) -> io::Result<i32> {
        run_and_print::<Command>(self.into(), run_mode)
    }
}
