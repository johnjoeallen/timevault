use std::{fs, path::Path};

pub struct MountEntry<'a> {
    path: &'a Path,
    contents: String,
}

impl<'a> MountEntry<'a> {
    pub fn new(path: &'a Path) -> Result<Self, String> {
        let contents =
            fs::read_to_string("/proc/mounts").map_err(|e| format!("read /proc/mounts: {e}"))?;
        Ok(Self { path, contents })
    }
    pub fn is_mounted(&self) -> Result<bool, String> {
        for line in self.contents.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 2 {
                continue;
            }
            if Path::new(fields[1]) == self.path {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub fn is_readonly(&self) -> Result<bool, String> {
        for line in self.contents.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 4 {
                continue;
            }
            if Path::new(fields[1]) == self.path {
                let mut opts = fields[3].split(',');
                return Ok(opts.any(|opt| opt == "ro"));
            }
        }
        Err(format!("mount {} is not mounted", self.path.display()))
    }
}
