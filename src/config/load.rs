use std::fs::File;
use std::io::Read;
use std::net::Ipv4Addr;

use crate::config::model::{Config, Job, RuntimeConfig};
use crate::error::{ConfigError, Result, TimevaultError};
use crate::types::{DiskId, RunPolicy};
use crate::util::paths::is_safe_name;

const DEFAULT_MOUNT_BASE: &str = "/run/timevault/mounts";
const DEFAULT_USER_MOUNT_BASE: &str = "/run/timevault/user-mounts";

pub fn load_config(path: &str) -> Result<RuntimeConfig> {
    let mut contents = String::new();
    File::open(path)
        .map_err(|e| TimevaultError::message(format!("open config {}: {}", path, e)))?
        .read_to_string(&mut contents)
        .map_err(|e| TimevaultError::message(format!("read config {}: {}", path, e)))?;
    let cfg: Config =
        serde_yaml::from_str(&contents).map_err(|e| ConfigError::Parse(e.to_string()))?;
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
            return Err(
                ConfigError::Invalid(format!("job {}: source path is empty", job.name)).into(),
            );
        }
        if job.remote.is_some() && !is_ssh_rsync_source(&job.source) {
            return Err(ConfigError::Invalid(format!(
                "job {}: remote options require an SSH-style source like user@host:/path",
                job.name
            ))
            .into());
        }
        if let Some(remote) = &job.remote {
            if remote.inhibit_suspend == Some(true) && remote.wake.is_none() {
                return Err(ConfigError::Invalid(format!(
                    "job {}: remote.inhibitSuspend requires remote.wake",
                    job.name
                ))
                .into());
            }
            if let Some(wake) = &remote.wake {
                if wake.mac.trim().is_empty() {
                    return Err(ConfigError::Invalid(format!(
                        "job {}: remote.wake.mac is empty",
                        job.name
                    ))
                    .into());
                }
                if parse_mac_address(&wake.mac).is_none() {
                    return Err(ConfigError::Invalid(format!(
                        "job {}: remote.wake.mac is invalid",
                        job.name
                    ))
                    .into());
                }
                if matches!(&wake.host, Some(host) if host.trim().is_empty()) {
                    return Err(ConfigError::Invalid(format!(
                        "job {}: remote.wake.host is empty",
                        job.name
                    ))
                    .into());
                }
                if let Some(broadcast) = &wake.broadcast {
                    if broadcast.parse::<Ipv4Addr>().is_err() {
                        return Err(ConfigError::Invalid(format!(
                            "job {}: remote.wake.broadcast must be an IPv4 address",
                            job.name
                        ))
                        .into());
                    }
                }
                if matches!(wake.port, Some(0)) {
                    return Err(ConfigError::Invalid(format!(
                        "job {}: remote.wake.port must be greater than zero",
                        job.name
                    ))
                    .into());
                }
                if matches!(wake.keepalive_seconds, Some(0)) {
                    return Err(ConfigError::Invalid(format!(
                        "job {}: remote.wake.keepaliveSeconds must be greater than zero",
                        job.name
                    ))
                    .into());
                }
                if matches!(wake.wait_seconds, Some(0)) {
                    return Err(ConfigError::Invalid(format!(
                        "job {}: remote.wake.waitSeconds must be greater than zero",
                        job.name
                    ))
                    .into());
                }
            }
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
            description: job.description.filter(|value| !value.trim().is_empty()),
            source: job.source,
            copies: job.copies,
            run_policy,
            excludes,
            disk_ids: disk_ids_for_job,
            remote: job.remote,
        });
    }

    Ok(RuntimeConfig {
        jobs,
        backup_disks: cfg.backup_disks,
        mount_base: cfg
            .mount_base
            .unwrap_or_else(|| DEFAULT_MOUNT_BASE.to_string()),
        user_mount_base: cfg
            .user_mount_base
            .unwrap_or_else(|| DEFAULT_USER_MOUNT_BASE.to_string()),
        options: cfg.options,
    })
}

fn is_ssh_rsync_source(source: &str) -> bool {
    let source = source.trim();
    if source.starts_with('/') || source.starts_with("rsync://") {
        return false;
    }
    let Some((host, path)) = source.split_once(':') else {
        return false;
    };
    !host.is_empty() && path.starts_with('/')
}

fn parse_mac_address(value: &str) -> Option<[u8; 6]> {
    let mut mac = [0_u8; 6];
    let mut count = 0;
    for (index, part) in value.split(':').enumerate() {
        if index >= mac.len() || part.len() != 2 {
            return None;
        }
        mac[index] = u8::from_str_radix(part, 16).ok()?;
        count += 1;
    }
    if count == mac.len() {
        Some(mac)
    } else {
        None
    }
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
    description: "Primary filesystem"
    source: "/"
    copies: 2
    run: "auto"
    excludes: []
"#;
        file.write_all(yaml.as_bytes()).expect("write");
        let cfg = load_config(file.path().to_string_lossy().as_ref()).expect("load");
        assert_eq!(cfg.backup_disks.len(), 1);
        assert_eq!(cfg.jobs.len(), 1);
        assert_eq!(
            cfg.jobs[0].description.as_deref(),
            Some("Primary filesystem")
        );
    }

    #[test]
    fn load_config_with_remote_power_options() {
        let mut file = NamedTempFile::new().expect("tempfile");
        let yaml = r#"
backupDisks:
  - diskId: "primary"
    fsUuid: "uuid-1"
jobs:
  - name: "remote"
    source: "root@example.com:/"
    copies: 2
    run: "auto"
    remote:
      inhibitSuspend: true
      wake:
        mac: "aa:bb:cc:dd:ee:ff"
        host: "example.com"
        broadcast: "192.0.2.255"
        port: 9
        keepaliveSeconds: 60
        waitSeconds: 15
"#;
        file.write_all(yaml.as_bytes()).expect("write");
        let cfg = load_config(file.path().to_string_lossy().as_ref()).expect("load");
        let remote = cfg.jobs[0].remote.as_ref().expect("remote options");
        assert_eq!(remote.inhibit_suspend, Some(true));
        let wake = remote.wake.as_ref().expect("wake options");
        assert_eq!(wake.mac, "aa:bb:cc:dd:ee:ff");
        assert_eq!(wake.host.as_deref(), Some("example.com"));
        assert_eq!(wake.broadcast.as_deref(), Some("192.0.2.255"));
        assert_eq!(wake.port, Some(9));
        assert_eq!(wake.keepalive_seconds, Some(60));
        assert_eq!(wake.wait_seconds, Some(15));
    }

    #[test]
    fn remote_power_options_require_remote_source() {
        let mut file = NamedTempFile::new().expect("tempfile");
        let yaml = r#"
backupDisks:
  - diskId: "primary"
    fsUuid: "uuid-1"
jobs:
  - name: "local"
    source: "/"
    copies: 2
    remote:
      inhibitSuspend: true
"#;
        file.write_all(yaml.as_bytes()).expect("write");
        let err = load_config(file.path().to_string_lossy().as_ref()).expect_err("invalid config");
        assert!(err
            .to_string()
            .contains("remote options require an SSH-style source"));
    }

    #[test]
    fn remote_suspend_inhibit_requires_wake() {
        let mut file = NamedTempFile::new().expect("tempfile");
        let yaml = r#"
backupDisks:
  - diskId: "primary"
    fsUuid: "uuid-1"
jobs:
  - name: "remote"
    source: "root@example.com:/"
    copies: 2
    remote:
      inhibitSuspend: true
"#;
        file.write_all(yaml.as_bytes()).expect("write");
        let err = load_config(file.path().to_string_lossy().as_ref()).expect_err("invalid config");
        assert!(err
            .to_string()
            .contains("remote.inhibitSuspend requires remote.wake"));
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
        assert_eq!(cfg.jobs[0].disk_ids, Some(vec!["primary".to_string()]));
    }

    #[test]
    fn load_config_with_report_options() {
        let mut file = NamedTempFile::new().expect("tempfile");
        let yaml = r#"
options:
  report:
    emailTo: "admin@example.com"
    emailFrom: "timevault@example.com"
    sendmail: "/usr/sbin/sendmail"
backupDisks:
  - diskId: "primary"
    fsUuid: "uuid-1"
jobs:
  - name: "job1"
    source: "/"
    copies: 2
    run: "auto"
"#;
        file.write_all(yaml.as_bytes()).expect("write");
        let cfg = load_config(file.path().to_string_lossy().as_ref()).expect("load");
        let report = cfg.options.report.expect("report options");
        assert_eq!(report.email_to, "admin@example.com");
        assert_eq!(report.email_from.as_deref(), Some("timevault@example.com"));
        assert_eq!(report.sendmail.as_deref(), Some("/usr/sbin/sendmail"));
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

    #[test]
    fn load_config_reports_path_when_missing() {
        let missing = "/tmp/timevault-missing-config.yaml";
        let err = load_config(missing).expect_err("missing config should fail");
        assert_eq!(
            err.to_string(),
            format!(
                "open config {}: No such file or directory (os error 2)",
                missing
            )
        );
    }
}
