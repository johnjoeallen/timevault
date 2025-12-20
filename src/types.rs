use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiskId(String);

impl DiskId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for DiskId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() || s == "." || s == ".." {
            return Err("disk-id is empty".to_string());
        }
        if !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.') {
            return Err("disk-id must use only letters, digits, '.', '-', '_'".to_string());
        }
        Ok(DiskId(s.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FsUuid(String);

impl FsUuid {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for FsUuid {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().is_empty() {
            return Err("fs-uuid is empty".to_string());
        }
        Ok(FsUuid(s.to_string()))
    }
}

impl fmt::Display for FsUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunPolicy {
    Auto,
    Demand,
    Off,
}

#[derive(Debug, Clone, Copy)]
pub struct RunMode {
    pub dry_run: bool,
    pub safe_mode: bool,
    pub verbose: bool,
}

impl RunPolicy {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(RunPolicy::Auto),
            "demand" => Ok(RunPolicy::Demand),
            "off" => Ok(RunPolicy::Off),
            _ => Err(format!(
                "invalid run policy {}; expected auto, demand, or off",
                value
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            RunPolicy::Auto => "auto",
            RunPolicy::Demand => "demand",
            RunPolicy::Off => "off",
        }
    }
}
