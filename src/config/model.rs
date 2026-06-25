use serde::{Deserialize, Serialize};

use crate::types::RunPolicy;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub jobs: Vec<JobConfig>,
    #[serde(default)]
    pub excludes: Vec<String>,
    #[serde(default)]
    pub options: ConfigOptions,
    #[serde(default, rename = "backupDisks")]
    pub backup_disks: Vec<BackupDiskConfig>,
    #[serde(default, rename = "mountBase", skip_serializing_if = "Option::is_none")]
    pub mount_base: Option<String>,
    #[serde(
        default,
        rename = "userMountBase",
        skip_serializing_if = "Option::is_none"
    )]
    pub user_mount_base: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct ConfigOptions {
    #[serde(rename = "exclude-pristine", skip_serializing_if = "Option::is_none")]
    pub exclude_pristine: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cascade: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verbose: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safe: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rsync: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<ReportOptions>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ReportOptions {
    #[serde(rename = "emailTo")]
    pub email_to: String,
    #[serde(default, rename = "emailFrom", skip_serializing_if = "Option::is_none")]
    pub email_from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sendmail: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct JobConfig {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub source: String,
    pub copies: usize,
    #[serde(default = "default_run_policy")]
    pub run: String,
    #[serde(default)]
    pub excludes: Vec<String>,
    #[serde(default, rename = "diskIds", skip_serializing_if = "Option::is_none")]
    pub disk_ids: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<RemoteJobOptions>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq, Eq)]
pub struct RemoteJobOptions {
    #[serde(
        default,
        rename = "inhibitSuspend",
        skip_serializing_if = "Option::is_none"
    )]
    pub inhibit_suspend: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wake: Option<RemoteWakeOptions>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct RemoteWakeOptions {
    pub mac: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broadcast: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface: Option<String>,
    #[serde(
        default,
        rename = "keepaliveSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub keepalive_seconds: Option<u64>,
    #[serde(
        default,
        rename = "waitSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub wait_seconds: Option<u64>,
    #[serde(
        default,
        rename = "suspendAfterBackup",
        skip_serializing_if = "Option::is_none"
    )]
    pub suspend_after_backup: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BackupDiskConfig {
    #[serde(rename = "diskId")]
    pub disk_id: String,
    #[serde(rename = "fsUuid")]
    pub fs_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(
        default,
        rename = "mountOptions",
        skip_serializing_if = "Option::is_none"
    )]
    pub mount_options: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub disabled: bool,
    #[serde(default, rename = "rotatedOut", skip_serializing_if = "is_false")]
    pub rotated_out: bool,
}

#[derive(Debug, Clone)]
pub struct Job {
    pub name: String,
    pub description: Option<String>,
    pub source: String,
    pub copies: usize,
    pub run_policy: RunPolicy,
    pub excludes: Vec<String>,
    pub disk_ids: Option<Vec<String>>,
    pub remote: Option<RemoteJobOptions>,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub jobs: Vec<Job>,
    pub backup_disks: Vec<BackupDiskConfig>,
    pub mount_base: String,
    pub user_mount_base: String,
    pub options: ConfigOptions,
}

fn default_run_policy() -> String {
    "auto".to_string()
}

fn is_false(value: &bool) -> bool {
    !*value
}
