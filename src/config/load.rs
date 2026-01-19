use std::fs::File;
use std::io::Read;

use crate::config::model::{Config, Job, RuntimeConfig};
use crate::error::{ConfigError, Result, TimevaultError};
use crate::types::{DiskId, RunPolicy};
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

    let mut disk_ids = std::collections::HashSet::new();
    let mut duplicate_disk_ids = std::collections::HashSet::new();
    let mut fs_uuids = std::collections::HashSet::new();
    for disk in &cfg.backup_disks {
        if !disk_ids.insert(disk.disk_id.clone()) {
            duplicate_disk_ids.insert(disk.disk_id.clone());
        }
        if !fs_uuids.insert(disk.fs_uuid.clone()) {
            return Err(ConfigError::Invalid(format!(
                "duplicate fs-uuid {}; remove or fix duplicates",
                disk.fs_uuid
            ))
            .into());
        }
    }
    if !duplicate_disk_ids.is_empty() {
        let mut list: Vec<String> = duplicate_disk_ids.iter().cloned().collect();
        list.sort();
        println!();
        println!(
            "WARNING: duplicate disk-id(s) found: {} (rename with `timevault disk rename --fs-uuid <uuid> --new-id <id>`)",
            list.join(", ")
        );
        println!();
    }

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
        let disk_ids_for_job = if let Some(raw_ids) = job.disk_ids {
            let mut seen = std::collections::HashSet::new();
            let mut parsed = Vec::new();
            for raw_id in raw_ids {
                let id = raw_id.parse::<DiskId>().map_err(|e| {
                    ConfigError::Invalid(format!("job {}: disk-id {}: {}", job.name, raw_id, e))
                })?;
                if duplicate_disk_ids.contains(id.as_str()) {
                    return Err(ConfigError::Invalid(format!(
                        "job {}: disk-id {} is duplicated in config",
                        job.name,
                        id.as_str()
                    ))
                    .into());
                }
                if !disk_ids.contains(id.as_str()) {
                    return Err(ConfigError::Invalid(format!(
                        "job {}: disk-id {} not found in backupDisks",
                        job.name,
                        id.as_str()
                    ))
                    .into());
                }
                if seen.insert(id.as_str().to_string()) {
                    parsed.push(id.as_str().to_string());
                }
            }
            Some(parsed)
        } else {
            None
        };
        let mut excludes = global_excludes.clone();
        excludes.extend(job.excludes);
        jobs.push(Job {
            name: job.name,
            source: job.source,
            copies: job.copies,
            run_policy,
            excludes,
            disk_ids: disk_ids_for_job,
        });
    }

    Ok(RuntimeConfig {
        jobs,
        backup_disks: cfg.backup_disks,
        mount_base: cfg.mount_base.unwrap_or_else(|| DEFAULT_MOUNT_BASE.to_string()),
        user_mount_base: cfg
            .user_mount_base
            .unwrap_or_else(|| DEFAULT_USER_MOUNT_BASE.to_string()),
        options: cfg.options,
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

    #[test]
    fn load_config_with_job_disk_ids() {
        let mut file = NamedTempFile::new().expect("tempfile");
        let yaml = r#"
backupDisks:
  - diskId: "primary"
    fsUuid: "uuid-1"
jobs:
  - name: "job1"
    source: "/"
    copies: 2
    run: "auto"
    diskIds: ["primary"]
    excludes: []
"#;
        file.write_all(yaml.as_bytes()).expect("write");
        let cfg = load_config(file.path().to_string_lossy().as_ref()).expect("load");
        assert_eq!(cfg.jobs.len(), 1);
        assert_eq!(
            cfg.jobs[0].disk_ids,
            Some(vec!["primary".to_string()])
        );
    }

    #[test]
    fn load_config_rejects_unknown_job_disk_id() {
        let mut file = NamedTempFile::new().expect("tempfile");
        let yaml = r#"
backupDisks:
  - diskId: "primary"
    fsUuid: "uuid-1"
jobs:
  - name: "job1"
    source: "/"
    copies: 2
    run: "auto"
    diskIds: ["missing"]
    excludes: []
"#;
        file.write_all(yaml.as_bytes()).expect("write");
        assert!(load_config(file.path().to_string_lossy().as_ref()).is_err());
    }

    #[test]
    fn load_config_rejects_invalid_job_disk_id() {
        let mut file = NamedTempFile::new().expect("tempfile");
        let yaml = r#"
backupDisks:
  - diskId: "primary"
    fsUuid: "uuid-1"
jobs:
  - name: "job1"
    source: "/"
    copies: 2
    run: "auto"
    diskIds: ["bad id"]
    excludes: []
"#;
        file.write_all(yaml.as_bytes()).expect("write");
        assert!(load_config(file.path().to_string_lossy().as_ref()).is_err());
    }
}
