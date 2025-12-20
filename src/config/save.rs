use std::fs::File;
use std::io::Write;

use crate::config::model::Config;
use crate::error::{Result, TimevaultError};

pub fn save_config(path: &str, cfg: &Config) -> Result<()> {
    let data = serde_yaml::to_string(cfg)
        .map_err(|e| TimevaultError::message(format!("encode config: {}", e)))?;
    let mut file = File::create(path)
        .map_err(|e| TimevaultError::message(format!("write config {}: {}", path, e)))?;
    file.write_all(data.as_bytes())
        .map_err(|e| TimevaultError::message(format!("write config {}: {}", path, e)))?;
    Ok(())
}
