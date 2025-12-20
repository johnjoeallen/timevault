use serde::Deserialize;
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub const LOCK_FILE: &str = "/var/run/timevault.pid";

pub type MountTracker = Arc<Mutex<HashSet<PathBuf>>>;

pub struct LockGuard;

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = unlock();
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub name: String,
    pub source: String,
    pub dest: String,
    pub copies: usize,
    pub mount: Option<PathBuf>,
    pub run_policy: RunPolicy,
    pub excludes: Vec<String>,
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunPolicy {
    Auto,
    Demand,
    Off,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub jobs: Vec<JobConfig>,
    #[serde(default)]
    pub excludes: Vec<String>,
    #[serde(default)]
    pub mount_prefix: Option<String>,
}

fn default_run_policy() -> String {
    "auto".to_string()
}

#[derive(Debug, Deserialize)]
pub struct JobConfig {
    pub name: String,
    pub source: String,
    pub dest: String,
    pub copies: usize,
    #[serde(default)]
    pub mount: Option<PathBuf>,
    #[serde(default = "default_run_policy")]
    pub run: String,
    #[serde(default)]
    pub excludes: Vec<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

pub fn lock() -> io::Result<bool> {
    for _ in 0..3 {
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(LOCK_FILE)
        {
            Ok(mut f) => {
                writeln!(f, "{}", std::process::id())?;
                return Ok(true);
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                let pid = match fs::read_to_string(LOCK_FILE) {
                    Ok(text) => text.trim().parse::<u32>().ok(),
                    Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                    Err(err) => return Err(err),
                };
                if let Some(pid) = pid
                    && Path::new("/proc").join(pid.to_string()).exists()
                {
                    return Ok(false);
                }
                match fs::remove_file(LOCK_FILE) {
                    Ok(()) => continue,
                    Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                    Err(err) => return Err(err),
                }
            }
            Err(err) => return Err(err),
        }
    }
    Ok(false)
}

pub fn unlock() -> io::Result<()> {
    let pid = fs::read_to_string(LOCK_FILE).ok();
    if let Some(pid) = pid {
        let pid = pid.trim();
        if !pid.is_empty()
            && pid == std::process::id().to_string()
            && Path::new("/proc").join(pid).exists()
        {
            let _ = fs::remove_file(LOCK_FILE);
        }
    }
    Ok(())
}
