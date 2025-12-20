use std::path::PathBuf;

use clap::*;

#[derive(Parser, Debug)]
#[command(about="TimeVault Copyright (C) 2025 John Allen (john.joe.allen@gmail.com)", long_about = None)]
pub struct Configuration {
    #[arg(short, long)]
    pub version: bool,
    #[command(flatten)]
    pub run_mode: Option<RunMode>,
    #[arg(short, long)]
    pub safe: bool,
    #[arg(group = "init_group")]
    pub init: Option<PathBuf>,
    #[arg(group = "init_group")]
    pub force_init: Option<PathBuf>,
    #[arg(short, long)]
    pub job: Option<String>,
    #[arg(short, long)]
    pub config: Option<PathBuf>,
    #[arg(short, long)]
    pub print_order: bool,
}

#[derive(Args, Debug, Default, Clone)]
#[group(required = false, multiple = true)]
pub struct RunMode {
    #[arg(long)]
    pub dry_run: bool,
    #[arg(long)]
    pub safe_mode: bool,
    #[arg(long)]
    pub verbose: bool,
}

pub fn get_configuration() -> Configuration {
    Configuration::parse()
}

const COPYRIGHT: &str = "Copyright (C) 2025 John Allen (john.joe.allen@gmail.com)";
const PROJECT_URL: &str = "https://github.com/johnjoeallen/timevault";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const LICENSE_NAME: &str = "GNU GPL v3 or later";

pub fn print_banner() -> String {
    format!("TimeVault {VERSION}")
}

pub fn print_copyright() {
    println!("{}", COPYRIGHT);
    println!("Project: {}", PROJECT_URL);
    println!("License: {}", LICENSE_NAME);
}
