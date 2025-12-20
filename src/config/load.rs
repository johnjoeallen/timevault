use std::fs::File;
use std::io::Read;

use crate::config::model::{Config, Job, RuntimeConfig};
use crate::error::{ConfigError, Result, TimevaultError};
use crate::types::RunPolicy;
use crate::util::paths::is_safe_name;

const DEFAULT_MOUNT_BASE: &str = "/run/timevault/mounts";
const DEFAULT_USER_MOUNT_BASE: &str = "/run/timevault/user-mounts";

pub fn load_config(path: &str) -> Result<RuntimeConfig> {
    let mut contents = String::new();
    File::open(path)
        .map_err(|e| TimevaultError::Io(e))?
        .read_to_string(&mut contents)
        .map_err(|e| TimevaultError::Io(e))?;
    let cfg: Config = serde_yaml::from_str(&contents)
        .map_err(|e| ConfigError::Parse(e.to_string()))?;
    parse_runtime(cfg)
}

fn parse_runtime(cfg: Config) -> Result<RuntimeConfig> {
    let global_excludes = cfg.excludes;
    let mut jobs = Vec::new();
    let mut names = std::collections::HashSet::new();

    for job in cfg.jobs {
        let run_policy = RunPolicy::parse(&job.run)
            .map_err(|e| ConfigError::Invalid(format!("job {}: {}", job.name, e)))?;
        if job.source.trim().is_empty() {
            return Err(ConfigError::Invalid(format!("job {}: source path is empty", job.name)).into());
        }
        if job.name.trim().is_empty() {
            return Err(ConfigError::Invalid("job name is required".to_string()).into());
        }
        if !is_safe_name(&job.name) {
            return Err(ConfigError::Invalid(format!(
                "job {} name must use only letters, digits, '.', '-', '_'",
                job.name
            ))
            .into());
        }
        if !names.insert(job.name.clone()) {
            return Err(ConfigError::Invalid(format!("duplicate job name {}", job.name)).into());
        }
        let mut excludes = global_excludes.clone();
        excludes.extend(job.excludes);
        jobs.push(Job {
            name: job.name,
            source: job.source,
            copies: job.copies,
            run_policy,
            excludes,
        });
    }

    Ok(RuntimeConfig {
        jobs,
        backup_disks: cfg.backup_disks,
        mount_base: cfg.mount_base.unwrap_or_else(|| DEFAULT_MOUNT_BASE.to_string()),
        user_mount_base: cfg
            .user_mount_base
            .unwrap_or_else(|| DEFAULT_USER_MOUNT_BASE.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn load_config_with_backup_disks() {
        let mut file = NamedTempFile::new().expect("tempfile");
        let yaml = r#"
mountBase: "/run/timevault/mounts"
userMountBase: "/run/timevault/user-mounts"
backupDisks:
  - diskId: "primary"
    fsUuid: "uuid-1"
jobs:
  - name: "job1"
    source: "/"
    copies: 2
    run: "auto"
    excludes: []
"#;
        file.write_all(yaml.as_bytes()).expect("write");
        let cfg = load_config(file.path().to_string_lossy().as_ref()).expect("load");
        assert_eq!(cfg.backup_disks.len(), 1);
        assert_eq!(cfg.jobs.len(), 1);
    }
}
