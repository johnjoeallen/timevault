use std::env;
use std::path::PathBuf;

use anyhow::Result;
use clap::error::ErrorKind;
use clap::Parser;

use crate::cli::args::{Cli, Command, DiskCommand};
use crate::cli::commands::{backup, disk_add, exit_for_error, mount, umount};
use crate::types::RunMode;

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

    let command = cli.command.clone().unwrap_or(Command::Backup);
    match command {
        Command::Backup => backup::run_backup_command(
            &config_path,
            &cli.job,
            cli.print_order,
            cli.disk_id.as_deref(),
            run_mode,
            &rsync_extra,
        )?,
        Command::Disk { command } => match command {
            DiskCommand::Add(args) | DiskCommand::Enroll(args) => {
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
        },
        Command::Mount(args) => {
            if let Err(err) = mount::run_mount(&config_path, args, cli.disk_id.as_deref()) {
                exit_for_error(&err);
            }
        }
        Command::Umount(args) => {
            if let Err(err) = umount::run_umount(&config_path, args) {
                exit_for_error(&err);
            }
        }
    }

    Ok(())
}

fn parse_cli() -> Result<(Cli, Vec<String>)> {
    let raw: Vec<String> = env::args().collect();
    let preprocessed = preprocess_args(raw);
    let (args, rsync_extra) = split_rsync_args(preprocessed);
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

fn preprocess_args(raw: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    let mut iter = raw.into_iter();
    if let Some(bin) = iter.next() {
        out.push(bin);
    }
    for arg in iter {
        if arg == "--disk-enroll" {
            out.push("disk".to_string());
            out.push("enroll".to_string());
            continue;
        }
        if arg == "--disk-discover" {
            out.push("disk".to_string());
            out.push("discover".to_string());
            continue;
        }
        if arg == "--backup" {
            continue;
        }
        out.push(arg);
    }
    out
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
    println!("TimeVault {}", VERSION);
}

fn print_copyright() {
    println!("{}", COPYRIGHT);
}

fn print_help() {
    println!("Usage:");
    println!("  timevault [backup] [options]");
    println!("  timevault --disk-discover");
    println!("  timevault --disk-enroll --disk-id <id> [--fs-uuid <uuid> | --device <path>] [--label <label>] [--mount-options <opts>] [--force]");
    println!("  timevault disk add --disk-id <id> [--fs-uuid <uuid> | --device <path>] [--label <label>] [--mount-options <opts>] [--force]");
    println!("  timevault disk enroll --disk-id <id> [--fs-uuid <uuid> | --device <path>] [--label <label>] [--mount-options <opts>] [--force]");
    println!("  timevault disk discover");
    println!("  timevault mount [--disk-id <id>] [--mountpoint <path>] [--read-write]");
    println!("  timevault umount [--mountpoint <path>]");
    println!("  timevault --version");
    println!();
    println!("Options:");
    println!("  --config <path>        Config file path");
    println!("  --job <name>           Run only selected job(s)");
    println!("  --dry-run              Do not write data");
    println!("  --safe                 Do not delete files");
    println!("  --verbose              Verbose logging");
    println!("  --print-order          Print resolved job order and exit");
    println!("  --rsync <args...>      Pass remaining args to rsync");
    println!("  --disk-id <id>         Select enrolled backup disk");
    println!("  --fs-uuid <uuid>       Filesystem UUID (disk add/enroll)");
    println!("  --device <path>        Block device path (disk add/enroll)");
    println!("  --label <label>        Optional disk label (disk add/enroll)");
    println!("  --mount-options <opt>  Mount options (disk add/enroll)");
    println!("  --force                Force disk add/enroll on non-empty root or existing identity");
    println!("  --mountpoint <path>    Mountpoint for mount/umount");
    println!("  --read-write           Mount disk read/write (restore)");
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();
}
