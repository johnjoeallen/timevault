use std::env;
use std::path::PathBuf;

use anyhow::Result;
use clap::error::ErrorKind;
use clap::Parser;

use crate::cli::args::{Cli, Command, DiskCommand};
use crate::cli::commands::{backup, disk_add, disk_inspect, exit_for_error, mount, umount};
use crate::types::RunMode;
use crate::backup::BackupOptions;

const CONFIG_FILE: &str = "/etc/timevault.yaml";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const LICENSE_NAME: &str = "GNU GPL v3 or later";
const COPYRIGHT: &str = "Copyright (C) 2025 John Allen (john.joe.allen@gmail.com)";
const PROJECT_URL: &str = "https://github.com/johnjoeallen/timevault";

pub mod args;
pub mod commands;

pub fn run() -> Result<()> {
    init_tracing();
    let (cli, rsync_extra) = parse_cli()?;

    print_banner();
    if cli.help {
        print_help();
        return Ok(());
    }
    if cli.version {
        print_copyright();
        println!("Project: {}", PROJECT_URL);
        println!("License: {}", LICENSE_NAME);
        return Ok(());
    }

    let config_path = cli
        .config
        .unwrap_or_else(|| PathBuf::from(CONFIG_FILE));
    let run_mode = RunMode {
        dry_run: cli.dry_run,
        safe_mode: cli.safe,
        verbose: cli.verbose,
    };
    let options = BackupOptions {
        exclude_pristine: cli.exclude_pristine || cli.exclude_pristine_only,
        exclude_pristine_only: cli.exclude_pristine_only,
    };

    let command = cli.command.clone().unwrap_or(Command::Backup);
    match command {
        Command::Backup => backup::run_backup_command(
            &config_path,
            &cli.job,
            cli.print_order,
            cli.disk_id.as_deref(),
            cli.cascade,
            run_mode,
            &rsync_extra,
            options,
        )?,
        Command::Disk { command } => match command {
            DiskCommand::Enroll(args) => {
                if let Err(err) = disk_add::run_enroll(&config_path, cli.disk_id.as_deref(), args)
                {
                    exit_for_error(&err);
                }
            }
            DiskCommand::Discover => {
                if let Err(err) = disk_add::run_discover(&config_path) {
                    exit_for_error(&err);
                }
            }
            DiskCommand::Mount(args) => {
                if let Err(err) = mount::run_mount(&config_path, args, cli.disk_id.as_deref()) {
                    exit_for_error(&err);
                }
            }
            DiskCommand::Umount(args) => {
                if let Err(err) = umount::run_umount(&config_path, args) {
                    exit_for_error(&err);
                }
            }
            DiskCommand::Inspect(args) => {
                if let Err(err) = disk_inspect::run_inspect(&config_path, args, cli.disk_id.as_deref()) {
                    exit_for_error(&err);
                }
            }
            DiskCommand::Unenroll(args) => {
                if let Err(err) = disk_add::run_unenroll(&config_path, args) {
                    exit_for_error(&err);
                }
            }
            DiskCommand::Rename(args) => {
                if let Err(err) = disk_add::run_rename(&config_path, args) {
                    exit_for_error(&err);
                }
            }
        },
    }

    Ok(())
}

fn parse_cli() -> Result<(Cli, Vec<String>)> {
    let raw: Vec<String> = env::args().collect();
    let (args, rsync_extra) = split_rsync_args(raw);
    match Cli::try_parse_from(args) {
        Ok(cli) => Ok((cli, rsync_extra)),
        Err(err) => {
            if err.kind() == ErrorKind::DisplayHelp {
                print_banner();
                print_help();
                std::process::exit(0);
            }
            if err.kind() == ErrorKind::DisplayVersion {
                print_banner();
                print_copyright();
                println!("Project: {}", PROJECT_URL);
                println!("License: {}", LICENSE_NAME);
                std::process::exit(0);
            }
            if err.kind() == ErrorKind::UnknownArgument {
                if let Some(arg) = err.context().find_map(|c| {
                    if let clap::error::ContextKind::InvalidArg = c.0 {
                        Some(c.1.to_string())
                    } else {
                        None
                    }
                }) {
                    println!("unknown option {}", arg);
                    std::process::exit(2);
                }
            }
            println!("{}", err);
            std::process::exit(2);
        }
    }
}

fn split_rsync_args(raw: Vec<String>) -> (Vec<String>, Vec<String>) {
    let mut args = Vec::new();
    let mut rsync_extra = Vec::new();
    let mut iter = raw.into_iter();
    if let Some(bin) = iter.next() {
        args.push(bin);
    }
    let mut in_rsync = false;
    for arg in iter {
        if in_rsync {
            rsync_extra.push(arg);
            continue;
        }
        if arg == "--rsync" {
            in_rsync = true;
            continue;
        }
        args.push(arg);
    }
    (args, rsync_extra)
}

fn print_banner() {
    println!("Timevault {}", VERSION);
}

fn print_copyright() {
    println!("{}", COPYRIGHT);
}

fn print_help() {
    println!("Usage:");
    println!("  timevault [backup] [options]");
    println!("  timevault disk enroll --disk-id <id> [--fs-uuid <uuid> | --device <path>] [--label <label>] [--mount-options <opts>] [--force]");
    println!("  timevault disk discover");
    println!("  timevault disk mount [--disk-id <id>]");
    println!("  timevault disk umount");
    println!("  timevault disk inspect [--disk-id <id>]");
    println!("  timevault disk unenroll [--disk-id <id> | --fs-uuid <uuid>]");
    println!("  timevault disk rename [--disk-id <id> | --fs-uuid <uuid>] --new-id <id>");
    println!("  timevault --version");
    println!();
    println!("Options:");
    println!("  --config <path>        Config file path");
    println!("  --job <name>           Run only selected job(s)");
    println!("  --dry-run              Do not write data");
    println!("  --safe                 Do not delete files");
    println!("  --verbose              Verbose logging");
    println!("  --exclude-pristine     Exclude pristine package-managed files");
    println!("  --exclude-pristine-only  Generate pristine excludes and exit");
    println!("  --print-order          Print resolved job order and exit");
    println!("  --rsync <args...>      Pass remaining args to rsync");
    println!("  --disk-id <id>         Select enrolled backup disk");
    println!("  --cascade              Run backup across all connected disks");
    println!("  --fs-uuid <uuid>       Filesystem UUID (disk enroll)");
    println!("  --device <path>        Block device path (disk enroll)");
    println!("  --label <label>        Optional disk label (disk enroll)");
    println!("  --mount-options <opt>  Mount options (disk enroll)");
    println!("  --force                Force disk enroll on non-empty root or existing identity");
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();
}
