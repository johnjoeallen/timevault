use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "timevault",
    disable_help_flag = true,
    disable_version_flag = true
)]
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
    pub exclude_pristine: bool,
    #[arg(long, global = true)]
    pub exclude_pristine_only: bool,

    #[arg(long, global = true)]
    pub disk_id: Option<String>,
    #[arg(long, global = true)]
    pub cascade: bool,

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
    #[command(alias = "register", alias = "add", alias = "create")]
    Enroll(DiskAddArgs),
    #[command(alias = "ls", alias = "list")]
    Discover(DiskLsArgs),
    Mount(MountArgs),
    Df(DiskDfArgs),
    Du(DiskDuArgs),
    #[command(alias = "unmount")]
    Umount(UmountArgs),
    Enable(DiskStateArgs),
    Disable(DiskStateArgs),
    RotateIn(DiskStateArgs),
    RotateOut(DiskStateArgs),
    #[command(
        alias = "deregister",
        alias = "de-register",
        alias = "unregister",
        alias = "rm",
        alias = "remove"
    )]
    Unenroll(DiskUnenrollArgs),
    Inspect(DiskInspectArgs),
    Rename(DiskRenameArgs),
}

#[derive(Args, Debug, Clone)]
pub struct DiskAddArgs {
    pub disk_id: Option<String>,
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
            disk_id: None,
            fs_uuid: None,
            device: None,
            label: None,
            mount_options: None,
            force: false,
        }
    }
}

#[derive(Args, Debug, Clone)]
pub struct DiskLsArgs {
    pub target: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct MountArgs {
    pub selector: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct DiskDfArgs {
    pub selector: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct DiskDuArgs {
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}

#[derive(Args, Debug, Clone)]
pub struct UmountArgs {}

#[derive(Args, Debug, Clone)]
pub struct DiskUnenrollArgs {
    pub selector: Option<String>,
    #[arg(long)]
    pub disk_id: Option<String>,
    #[arg(long)]
    pub fs_uuid: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct DiskStateArgs {
    pub selector: Option<String>,
    #[arg(long)]
    pub disk_id: Option<String>,
    #[arg(long)]
    pub fs_uuid: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct DiskInspectArgs {
    pub selector: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct DiskRenameArgs {
    pub selector: Option<String>,
    pub new_id_arg: Option<String>,
    #[arg(long)]
    pub disk_id: Option<String>,
    #[arg(long)]
    pub fs_uuid: Option<String>,
    #[arg(long)]
    pub new_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_disk_ls_alias() {
        let cli = Cli::parse_from(["timevault", "disk", "ls"]);
        let Some(Command::Disk {
            command: DiskCommand::Discover(args),
        }) = cli.command
        else {
            panic!("expected disk discover");
        };
        assert!(args.target.is_none());
    }

    #[test]
    fn parses_disk_ls_target() {
        let cli = Cli::parse_from(["timevault", "disk", "ls", "primary:/snapshots"]);
        let Some(Command::Disk {
            command: DiskCommand::Discover(args),
        }) = cli.command
        else {
            panic!("expected disk discover");
        };
        assert_eq!(args.target.as_deref(), Some("primary:/snapshots"));
    }

    #[test]
    fn parses_disk_register_with_positional_id() {
        let cli = Cli::parse_from([
            "timevault",
            "disk",
            "register",
            "primary",
            "--fs-uuid",
            "uuid-a",
        ]);
        let Some(Command::Disk {
            command: DiskCommand::Enroll(args),
        }) = cli.command
        else {
            panic!("expected disk enroll");
        };
        assert_eq!(args.disk_id.as_deref(), Some("primary"));
        assert_eq!(args.fs_uuid.as_deref(), Some("uuid-a"));
    }

    #[test]
    fn parses_disk_de_register_with_positional_id() {
        let cli = Cli::parse_from(["timevault", "disk", "de-register", "primary"]);
        let Some(Command::Disk {
            command: DiskCommand::Unenroll(args),
        }) = cli.command
        else {
            panic!("expected disk unenroll");
        };
        assert_eq!(args.selector.as_deref(), Some("primary"));
    }

    #[test]
    fn parses_disk_rename_with_positional_ids() {
        let cli = Cli::parse_from(["timevault", "disk", "rename", "primary", "archive"]);
        let Some(Command::Disk {
            command: DiskCommand::Rename(args),
        }) = cli.command
        else {
            panic!("expected disk rename");
        };
        assert_eq!(args.selector.as_deref(), Some("primary"));
        assert_eq!(args.new_id_arg.as_deref(), Some("archive"));
    }

    #[test]
    fn parses_disk_mount_with_positional_id() {
        let cli = Cli::parse_from(["timevault", "disk", "mount", "primary"]);
        let Some(Command::Disk {
            command: DiskCommand::Mount(args),
        }) = cli.command
        else {
            panic!("expected disk mount");
        };
        assert_eq!(args.selector.as_deref(), Some("primary"));
    }

    #[test]
    fn parses_disk_inspect_with_positional_id() {
        let cli = Cli::parse_from(["timevault", "disk", "inspect", "primary"]);
        let Some(Command::Disk {
            command: DiskCommand::Inspect(args),
        }) = cli.command
        else {
            panic!("expected disk inspect");
        };
        assert_eq!(args.selector.as_deref(), Some("primary"));
    }

    #[test]
    fn parses_disk_df_with_positional_id() {
        let cli = Cli::parse_from(["timevault", "disk", "df", "primary"]);
        let Some(Command::Disk {
            command: DiskCommand::Df(args),
        }) = cli.command
        else {
            panic!("expected disk df");
        };
        assert_eq!(args.selector.as_deref(), Some("primary"));
    }

    #[test]
    fn parses_disk_du_options_and_target() {
        let cli = Cli::parse_from(["timevault", "disk", "du", "-sh", "primary:/backups"]);
        let Some(Command::Disk {
            command: DiskCommand::Du(args),
        }) = cli.command
        else {
            panic!("expected disk du");
        };
        assert_eq!(args.args, vec!["-sh", "primary:/backups"]);
    }
}
