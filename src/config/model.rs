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
    #[serde(default, rename = "userMountBase", skip_serializing_if = "Option::is_none")]
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
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct JobConfig {
    pub name: String,
    pub source: String,
    pub copies: usize,
    #[serde(default = "default_run_policy")]
    pub run: String,
    #[serde(default)]
    pub excludes: Vec<String>,
    #[serde(default, rename = "diskIds", skip_serializing_if = "Option::is_none")]
    pub disk_ids: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BackupDiskConfig {
    #[serde(rename = "diskId")]
    pub disk_id: String,
    #[serde(rename = "fsUuid")]
    pub fs_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, rename = "mountOptions", skip_serializing_if = "Option::is_none")]
    pub mount_options: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Job {
    pub name: String,
    pub source: String,
    pub copies: usize,
    pub run_policy: RunPolicy,
    pub excludes: Vec<String>,
    pub disk_ids: Option<Vec<String>>,
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
