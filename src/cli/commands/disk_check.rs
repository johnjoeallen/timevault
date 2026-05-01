use std::path::Path;

use crate::cli::args::DiskCheckArgs;
use crate::config::load::load_config;
use crate::config::model::BackupDiskConfig;
use crate::disk::discovery::{list_candidates, DiskCandidate};
use crate::disk::disk_matches_selector;
use crate::disk::identity::verify_identity;
use crate::error::{DiskError, Result, TimevaultError};

#[derive(Debug, PartialEq, Eq)]
struct CheckRow {
    disk_id: String,
    fs_uuid: String,
    status: String,
    identity: String,
    details: String,
    ok: bool,
}

pub fn run_check(config_path: &Path, args: DiskCheckArgs, disk_id: Option<&str>) -> Result<()> {
    let cfg = load_config(config_path.to_string_lossy().as_ref())?;
    let candidates = list_candidates(&cfg.backup_disks, Path::new(&cfg.user_mount_base))?;
    let selector = args
        .selector
        .as_deref()
        .or(args.fs_uuid.as_deref())
        .or(disk_id);
    let rows = check_rows(&cfg.backup_disks, &candidates, selector)?;

    println!(
        "{:<20} {:<36} {:<12} {:<10} DETAILS",
        "DISK ID", "FS UUID", "STATUS", "IDENTITY"
    );
    for row in &rows {
        println!(
            "{:<20} {:<36} {:<12} {:<10} {}",
            row.disk_id, row.fs_uuid, row.status, row.identity, row.details
        );
    }

    let failures = rows.iter().filter(|row| !row.ok).count();
    if failures > 0 {
        return Err(DiskError::IdentityMismatch(format!(
            "disk check failed: {} issue(s)",
            failures
        ))
        .into());
    }
    Ok(())
}

fn check_rows(
    disks: &[BackupDiskConfig],
    candidates: &[DiskCandidate],
    selector: Option<&str>,
) -> Result<Vec<CheckRow>> {
    let selected: Vec<&BackupDiskConfig> = disks
        .iter()
        .filter(|disk| {
            selector
                .map(|value| disk_matches_selector(disk, value))
                .unwrap_or(true)
        })
        .collect();
    if selected.is_empty() {
        let message = selector
            .map(|value| format!("disk selector {} not found in config", value))
            .unwrap_or_else(|| {
                "no backup disks enrolled; run `timevault disk enroll ...`".to_string()
            });
        return Err(TimevaultError::message(message));
    }

    let mut rows = Vec::new();
    for disk in selected {
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.uuid == disk.fs_uuid);
        rows.push(check_registered_disk(disk, candidate));
    }

    if selector.is_none() {
        for candidate in candidates {
            if disks.iter().any(|disk| disk.fs_uuid == candidate.uuid) {
                continue;
            }
            if let Some(identity) = &candidate.identity {
                let (identity_status, details, ok) =
                    actual_fs_uuid_check(candidate, &identity.fs_uuid)
                        .map(|details| ("mismatch".to_string(), details, false))
                        .unwrap_or_else(|| {
                            (
                                "present".to_string(),
                                "identity found but disk is not in config".to_string(),
                                true,
                            )
                        });
                rows.push(CheckRow {
                    disk_id: identity.disk_id.clone(),
                    fs_uuid: candidate.uuid.clone(),
                    status: "unregistered".to_string(),
                    identity: identity_status,
                    details,
                    ok,
                });
            }
        }
    }

    Ok(rows)
}

fn check_registered_disk(disk: &BackupDiskConfig, candidate: Option<&DiskCandidate>) -> CheckRow {
    let Some(candidate) = candidate else {
        return CheckRow {
            disk_id: disk.disk_id.clone(),
            fs_uuid: disk.fs_uuid.clone(),
            status: "offline".to_string(),
            identity: "unknown".to_string(),
            details: "not connected".to_string(),
            ok: true,
        };
    };

    let Some(identity) = &candidate.identity else {
        return CheckRow {
            disk_id: disk.disk_id.clone(),
            fs_uuid: disk.fs_uuid.clone(),
            status: "online".to_string(),
            identity: "missing".to_string(),
            details: "expected .timevault identity".to_string(),
            ok: false,
        };
    };

    if let Some(details) = actual_fs_uuid_check(candidate, &identity.fs_uuid) {
        return CheckRow {
            disk_id: disk.disk_id.clone(),
            fs_uuid: disk.fs_uuid.clone(),
            status: "online".to_string(),
            identity: "mismatch".to_string(),
            details,
            ok: false,
        };
    }

    match verify_identity(identity, &disk.disk_id, &disk.fs_uuid) {
        Ok(()) => CheckRow {
            disk_id: disk.disk_id.clone(),
            fs_uuid: disk.fs_uuid.clone(),
            status: "online".to_string(),
            identity: "ok".to_string(),
            details: "matches config".to_string(),
            ok: true,
        },
        Err(err) => CheckRow {
            disk_id: disk.disk_id.clone(),
            fs_uuid: disk.fs_uuid.clone(),
            status: "online".to_string(),
            identity: "mismatch".to_string(),
            details: err.to_string(),
            ok: false,
        },
    }
}

fn actual_fs_uuid_check(candidate: &DiskCandidate, identity_fs_uuid: &str) -> Option<String> {
    if identity_fs_uuid == candidate.uuid {
        return None;
    }
    Some(format!(
        "actual fsUuid mismatch: device is {}, identity has {}",
        candidate.uuid, identity_fs_uuid
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::identity::{DiskIdentity, IDENTITY_VERSION};
    use std::path::PathBuf;

    fn disk() -> BackupDiskConfig {
        BackupDiskConfig {
            disk_id: "primary".to_string(),
            fs_uuid: "uuid-primary".to_string(),
            label: None,
            mount_options: None,
            disabled: false,
            rotated_out: false,
        }
    }

    fn candidate(disk_id: &str, fs_uuid: &str) -> DiskCandidate {
        DiskCandidate {
            uuid: "uuid-primary".to_string(),
            device: PathBuf::from("/dev/sdz1"),
            mounted_at: None,
            capacity_bytes: None,
            serial: None,
            empty: None,
            removable: None,
            reasons: Vec::new(),
            identity: Some(DiskIdentity {
                version: IDENTITY_VERSION,
                disk_id: disk_id.to_string(),
                fs_uuid: fs_uuid.to_string(),
                fs_type: None,
                created: "2026-01-01T00:00:00Z".to_string(),
            }),
            enrolled: true,
            fs_type: None,
        }
    }

    #[test]
    fn check_rows_accepts_matching_identity() {
        let rows = check_rows(&[disk()], &[candidate("primary", "uuid-primary")], None)
            .expect("check rows");
        assert_eq!(rows[0].identity, "ok");
        assert!(rows[0].ok);
    }

    #[test]
    fn check_rows_reports_wrong_disk_id() {
        let rows = check_rows(&[disk()], &[candidate("old-name", "uuid-primary")], None)
            .expect("check rows");
        assert_eq!(rows[0].identity, "mismatch");
        assert!(!rows[0].ok);
        assert!(rows[0].details.contains("diskId mismatch"));
    }

    #[test]
    fn check_rows_reports_identity_uuid_that_differs_from_actual_uuid() {
        let rows =
            check_rows(&[disk()], &[candidate("primary", "old-uuid")], None).expect("check rows");
        assert_eq!(rows[0].identity, "mismatch");
        assert!(!rows[0].ok);
        assert!(rows[0].details.contains("actual fsUuid mismatch"));
    }

    #[test]
    fn check_rows_reports_unregistered_identity_uuid_that_differs_from_actual_uuid() {
        let mut unregistered = candidate("archive", "old-uuid");
        unregistered.uuid = "uuid-archive".to_string();
        let rows = check_rows(&[disk()], &[unregistered], None).expect("check rows");
        let row = rows
            .iter()
            .find(|row| row.status == "unregistered")
            .expect("unregistered row");
        assert_eq!(row.identity, "mismatch");
        assert!(!row.ok);
        assert!(row.details.contains("actual fsUuid mismatch"));
    }

    #[test]
    fn check_rows_can_select_by_fs_uuid() {
        let rows = check_rows(
            &[disk()],
            &[candidate("primary", "uuid-primary")],
            Some("uuid-primary"),
        )
        .expect("check rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].disk_id, "primary");
    }
}
