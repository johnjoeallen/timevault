use std::path::Path;
use std::process::Command;

use crate::cli::args::DiskFsckArgs;
use crate::config::load::load_config;
use crate::disk::discovery::list_candidates;
use crate::disk::disk_matches_selector;
use crate::disk::fs_type::FsType;
use crate::error::{DiskError, Result, TimevaultError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Read-only: report problems, never modify the disk.
    Check,
    /// Automatically repair problems.
    Fix,
}

/// Filesystem checker command for `fs_type`. In `Check` mode it never repairs.
fn fsck_command(fs_type: &FsType, device: &Path, mode: Mode) -> Option<Command> {
    let fix = mode == Mode::Fix;
    let mut cmd = match fs_type {
        FsType::Ext2 | FsType::Ext3 | FsType::Ext4 => {
            let mut cmd = Command::new("e2fsck");
            cmd.arg(if fix { "-y" } else { "-n" }).arg("-f");
            cmd
        }
        FsType::Xfs => {
            let mut cmd = Command::new("xfs_repair");
            if !fix {
                cmd.arg("-n");
            }
            cmd
        }
        FsType::Btrfs => {
            let mut cmd = Command::new("btrfs");
            cmd.arg("check")
                .arg(if fix { "--repair" } else { "--readonly" });
            cmd
        }
        FsType::Jfs => {
            let mut cmd = Command::new("jfs_fsck");
            cmd.arg(if fix { "-f" } else { "-n" });
            cmd
        }
        // Without -a, fsck.f2fs only inspects the filesystem.
        FsType::F2fs => {
            let mut cmd = Command::new("fsck.f2fs");
            if fix {
                cmd.arg("-a").arg("-f");
            }
            cmd
        }
        FsType::Zfs | FsType::Other(_) => return None,
    };
    cmd.arg(device);
    Some(cmd)
}

/// Whether the checker's exit code means success. e2fsck exits 1 when it
/// corrected errors, which is a successful fix.
fn exit_ok(fs_type: &FsType, mode: Mode, code: Option<i32>) -> bool {
    match code {
        Some(0) => true,
        Some(1) => {
            mode == Mode::Fix && matches!(fs_type, FsType::Ext2 | FsType::Ext3 | FsType::Ext4)
        }
        _ => false,
    }
}

pub fn run_fsck(
    config_path: &Path,
    args: DiskFsckArgs,
    disk_id: Option<&str>,
    mode: Mode,
) -> Result<()> {
    let cfg = load_config(config_path.to_string_lossy().as_ref())?;
    let candidates = list_candidates(&cfg.backup_disks, Path::new(&cfg.user_mount_base))?;
    let selector = args
        .selector
        .as_deref()
        .or(args.fs_uuid.as_deref())
        .or(disk_id);
    let label = if mode == Mode::Fix { "fix" } else { "check" };

    let selected: Vec<_> = cfg
        .backup_disks
        .iter()
        .filter(|disk| {
            selector
                .map(|value| disk_matches_selector(disk, value))
                .unwrap_or(true)
        })
        .collect();
    if selected.is_empty() {
        return Err(TimevaultError::message(match selector {
            Some(value) => format!("disk selector {} not found in config", value),
            None => "no backup disks enrolled; run `timevault disk enroll ...`".to_string(),
        }));
    }

    let mut failures = 0;
    for disk in selected {
        let Some(candidate) = candidates.iter().find(|c| c.uuid == disk.fs_uuid) else {
            println!("{} {}: skipped (not connected)", label, disk.disk_id);
            continue;
        };
        if let Some(mount) = &candidate.mounted_at {
            println!(
                "{} {}: skipped (mounted at {}; unmount first)",
                label,
                disk.disk_id,
                mount.display()
            );
            continue;
        }
        let Some(fs_type) = &candidate.fs_type else {
            println!(
                "{} {}: skipped (unknown filesystem type)",
                label, disk.disk_id
            );
            continue;
        };
        let Some(mut cmd) = fsck_command(fs_type, &candidate.device, mode) else {
            println!(
                "{} {}: skipped (no checker for {})",
                label, disk.disk_id, fs_type
            );
            continue;
        };
        println!(
            "{} {}: running on {} ({}){}",
            label,
            disk.disk_id,
            candidate.device.display(),
            fs_type,
            if mode == Mode::Fix {
                " with automatic repair"
            } else {
                " read-only"
            }
        );
        match cmd.status() {
            Ok(status) if exit_ok(fs_type, mode, status.code()) => {
                println!("{} {}: ok", label, disk.disk_id)
            }
            Ok(status) => {
                println!(
                    "{} {}: FAILED (exit code {})",
                    label,
                    disk.disk_id,
                    status.code().unwrap_or(1)
                );
                failures += 1;
            }
            Err(err) => {
                println!(
                    "{} {}: FAILED to run {}: {}",
                    label,
                    disk.disk_id,
                    cmd.get_program().to_string_lossy(),
                    err
                );
                failures += 1;
            }
        }
    }

    if failures > 0 {
        return Err(DiskError::IdentityMismatch(format!(
            "disk {} failed: {} issue(s)",
            label, failures
        ))
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(fs: FsType, mode: Mode) -> Vec<String> {
        let cmd = fsck_command(&fs, Path::new("/dev/sdz1"), mode).expect("command");
        std::iter::once(cmd.get_program())
            .chain(cmd.get_args())
            .map(|a| a.to_string_lossy().to_string())
            .collect()
    }

    #[test]
    fn check_is_read_only_per_filesystem() {
        assert_eq!(
            args(FsType::Ext4, Mode::Check),
            ["e2fsck", "-n", "-f", "/dev/sdz1"]
        );
        assert_eq!(
            args(FsType::Xfs, Mode::Check),
            ["xfs_repair", "-n", "/dev/sdz1"]
        );
        assert_eq!(
            args(FsType::Btrfs, Mode::Check),
            ["btrfs", "check", "--readonly", "/dev/sdz1"]
        );
        assert!(fsck_command(&FsType::Zfs, Path::new("/dev/x"), Mode::Check).is_none());
    }

    #[test]
    fn fix_repairs_per_filesystem() {
        assert_eq!(
            args(FsType::Ext4, Mode::Fix),
            ["e2fsck", "-y", "-f", "/dev/sdz1"]
        );
        assert_eq!(args(FsType::Xfs, Mode::Fix), ["xfs_repair", "/dev/sdz1"]);
        assert_eq!(
            args(FsType::Btrfs, Mode::Fix),
            ["btrfs", "check", "--repair", "/dev/sdz1"]
        );
    }

    #[test]
    fn e2fsck_corrected_exit_is_ok_only_when_fixing() {
        assert!(exit_ok(&FsType::Ext4, Mode::Fix, Some(1)));
        assert!(!exit_ok(&FsType::Ext4, Mode::Check, Some(1)));
        assert!(!exit_ok(&FsType::Xfs, Mode::Fix, Some(1)));
        assert!(exit_ok(&FsType::Xfs, Mode::Check, Some(0)));
        assert!(!exit_ok(&FsType::Ext4, Mode::Fix, None));
    }
}
