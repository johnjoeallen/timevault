use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "timevault", disable_help_flag = true, disable_version_flag = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    #[arg(long, global = true)]
    pub dry_run: bool,
    #[arg(long, global = true)]
    pub safe: bool,
    #[arg(long, short = 'v', global = true)]
    pub verbose: bool,

    #[arg(long, global = true)]
    pub config: Option<PathBuf>,
    #[arg(long, global = true)]
    pub job: Vec<String>,
    #[arg(long, global = true)]
    pub print_order: bool,

    #[arg(long, global = true)]
    pub disk_id: Option<String>,

    #[arg(long, short = 'h')]
    pub help: bool,
    #[arg(long)]
    pub version: bool,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    Backup,
    Disk {
        #[command(subcommand)]
        command: DiskCommand,
    },
}

#[derive(Subcommand, Debug, Clone)]
pub enum DiskCommand {
    Enroll(DiskAddArgs),
    Discover,
    Mount(MountArgs),
    #[command(alias = "unmount", alias = "umount")]
    Umount(UmountArgs),
}

#[derive(Args, Debug, Clone)]
pub struct DiskAddArgs {
    #[arg(long)]
    pub fs_uuid: Option<String>,
    #[arg(long, alias = "block-id")]
    pub device: Option<String>,
    #[arg(long)]
    pub label: Option<String>,
    #[arg(long)]
    pub mount_options: Option<String>,
    #[arg(long)]
    pub force: bool,
}

impl Default for DiskAddArgs {
    fn default() -> Self {
        Self {
            fs_uuid: None,
            device: None,
            label: None,
            mount_options: None,
            force: false,
        }
    }
}

#[derive(Args, Debug, Clone)]
pub struct MountArgs {
}

#[derive(Args, Debug, Clone)]
pub struct UmountArgs {
}
