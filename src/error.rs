use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TimevaultError {
    #[error("{0}")]
    Message(String),
    #[error("{0}")]
    Disk(DiskError),
    #[error("{0}")]
    Config(ConfigError),
    #[error("{0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("parse config: {0}")]
    Parse(String),
    #[error("{0}")]
    Invalid(String),
}

#[derive(Debug, Error)]
pub enum DiskError {
    #[error("no enrolled backup disk connected")]
    NoDiskConnected,
    #[error("multiple enrolled backup disks connected; specify --disk-id")]
    MultipleDisksConnected,
    #[error("identity {0}")]
    IdentityMismatch(String),
    #[error("disk not empty; unexpected entries: {0} (use --force to override)")]
    DiskNotEmpty(String),
    #[error("mount {0}")]
    MountFailure(String),
    #[error("umount {0}")]
    UmountFailure(String),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, TimevaultError>;

impl TimevaultError {
    pub fn message(msg: impl Into<String>) -> Self {
        TimevaultError::Message(msg.into())
    }
}

impl From<DiskError> for TimevaultError {
    fn from(err: DiskError) -> Self {
        TimevaultError::Disk(err)
    }
}

impl From<ConfigError> for TimevaultError {
    fn from(err: ConfigError) -> Self {
        TimevaultError::Config(err)
    }
}
