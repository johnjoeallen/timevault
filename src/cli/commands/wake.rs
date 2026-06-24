use std::path::Path;

use crate::backup::wake_remote_job;
use crate::cli::args::WakeArgs;
use crate::config::load::load_config;
use crate::error::{Result, TimevaultError};
use crate::types::RunMode;

pub fn run_wake(config_path: &Path, args: WakeArgs, run_mode: RunMode) -> Result<()> {
    let cfg = load_config(&config_path.to_string_lossy())?;
    let Some(job) = cfg.jobs.iter().find(|job| job.name == args.job) else {
        return Err(TimevaultError::message(format!(
            "job {} not found",
            args.job
        )));
    };
    println!("wake job: {}", job.name);
    wake_remote_job(job, run_mode)
}
