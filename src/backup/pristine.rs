use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, BufRead, BufReader, Read};
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const OS_RELEASE_PATH: &str = "/etc/os-release";
const PRISTINE_CACHE_REL: &str = ".cache/timevault/pristine-cache.json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OsFamily {
    Linux,
    Macos,
    Windows,
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OsInfo {
    pub family: OsFamily,
    pub id: Option<String>,
    pub id_like: Vec<String>,
    pub name: Option<String>,
    pub version_id: Option<String>,
}

impl OsInfo {
    fn linux() -> Self {
        Self {
            family: OsFamily::Linux,
            id: None,
            id_like: Vec::new(),
            name: None,
            version_id: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageManager {
    Dpkg,
    Rpm,
    Pacman,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheFile {
    version: u32,
    entries: HashMap<String, CacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    mtime: u64,
    hash: String,
    dirty: bool,
}

pub fn build_pristine_excludes(verbose: bool) -> Result<Vec<String>> {
    if verbose {
        println!("pristine: detect operating system");
    }
    let os = detect_os()?;
    if verbose {
        println!("pristine: os {}", format_os_info(&os));
    }
    let manager = detect_package_manager(&os);
    if verbose {
        match manager {
            Some(pm) => println!("pristine: package manager {}", format_package_manager(pm)),
            None => println!("pristine: package manager unknown"),
        }
    }
    let Some(manager) = manager else {
        return Ok(Vec::new());
    };
    let cache_path = pristine_cache_path();
    if verbose {
        println!("pristine: cache {}", cache_path.display());
    }
    let mut cache = load_cache(&cache_path, verbose);
    let files = list_package_files(manager, verbose)?;
    let mut new_entries = HashMap::new();
    let mut reused = 0usize;
    let mut hashed = 0usize;
    let mut dirty = 0usize;
    let mut pristine = 0usize;
    for path in files {
        let meta = match fs::symlink_metadata(&path) {
            Ok(meta) => meta,
            Err(_) => continue,
        };
        if !meta.is_file() {
            continue;
        }
        let mtime = match to_unix_mtime(meta.modified()) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if let Some(entry) = cache.entries.get(&path) {
            if entry.mtime == mtime {
                reused += 1;
                if entry.dirty {
                    dirty += 1;
                } else {
                    pristine += 1;
                }
                new_entries.insert(path, entry.clone());
                continue;
            }
            let current_hash = match hash_file(Path::new(&path)) {
                Ok(hash) => hash,
                Err(err) => {
                    if verbose {
                        println!("pristine: hash failed {} ({})", path, err);
                    }
                    continue;
                }
            };
            hashed += 1;
            let is_dirty = current_hash != entry.hash;
            if is_dirty {
                dirty += 1;
            } else {
                pristine += 1;
            }
            new_entries.insert(
                path,
                CacheEntry {
                    mtime,
                    hash: entry.hash.clone(),
                    dirty: is_dirty,
                },
            );
            continue;
        }
        let current_hash = match hash_file(Path::new(&path)) {
            Ok(hash) => hash,
            Err(err) => {
                if verbose {
                    println!("pristine: hash failed {} ({})", path, err);
                }
                continue;
            }
        };
        hashed += 1;
        pristine += 1;
        new_entries.insert(
            path,
            CacheEntry {
                mtime,
                hash: current_hash,
                dirty: false,
            },
        );
    }
    cache.entries = new_entries;
    save_cache(&cache_path, &cache, verbose)?;
    if verbose {
        println!(
            "pristine: cache stats reused={} hashed={} pristine={} dirty={}",
            reused, hashed, pristine, dirty
        );
    }
    let mut excludes = cache
        .entries
        .iter()
        .filter_map(|(path, entry)| if entry.dirty { None } else { Some(path.clone()) })
        .collect::<Vec<String>>();
    excludes.sort();
    Ok(excludes)
}

pub fn detect_os() -> Result<OsInfo> {
    if cfg!(target_os = "linux") {
        return read_os_release(Path::new(OS_RELEASE_PATH));
    }
    let family = if cfg!(target_os = "macos") {
        OsFamily::Macos
    } else if cfg!(target_os = "windows") {
        OsFamily::Windows
    } else {
        OsFamily::Other(std::env::consts::OS.to_string())
    };
    Ok(OsInfo {
        family,
        id: None,
        id_like: Vec::new(),
        name: None,
        version_id: None,
    })
}

pub fn detect_package_manager(os: &OsInfo) -> Option<PackageManager> {
    if !matches!(os.family, OsFamily::Linux) {
        return None;
    }
    if matches_id(os, &["debian", "ubuntu", "linuxmint"]) {
        return Some(PackageManager::Dpkg);
    }
    if matches_id(os, &["rhel", "fedora", "centos", "rocky", "almalinux", "amzn"]) {
        return Some(PackageManager::Rpm);
    }
    if matches_id(os, &["arch", "manjaro", "endeavouros"]) {
        return Some(PackageManager::Pacman);
    }
    None
}

fn read_os_release(path: &Path) -> Result<OsInfo> {
    let content = fs::read_to_string(path)?;
    Ok(parse_os_release(&content))
}

pub fn pristine_cache_path() -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    Path::new(&home).join(PRISTINE_CACHE_REL)
}

fn list_package_files(manager: PackageManager, verbose: bool) -> Result<Vec<String>> {
    if verbose {
        println!("pristine: enumerate package-managed files");
    }
    let mut files = match manager {
        PackageManager::Dpkg => list_dpkg_files(verbose)?,
        PackageManager::Rpm => list_command_files("rpm", &["-qal"], verbose)?,
        PackageManager::Pacman => list_command_files("pacman", &["-Qlq"], verbose)?,
    };
    files.sort();
    Ok(files)
}

fn list_dpkg_files(verbose: bool) -> Result<Vec<String>> {
    let info_dir = Path::new("/var/lib/dpkg/info");
    let mut files = HashSet::new();
    let entries = match fs::read_dir(info_dir) {
        Ok(entries) => entries,
        Err(err) => {
            return Err(crate::error::TimevaultError::message(format!(
                "read dpkg info dir failed: {}",
                err
            )));
        }
    };
    let mut list_count = 0usize;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("list") {
            continue;
        }
        let file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(err) => {
                if verbose {
                    println!("pristine: skip {} ({})", path.display(), err);
                }
                continue;
            }
        };
        list_count += 1;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = match line {
                Ok(line) => line.trim().to_string(),
                Err(_) => continue,
            };
            if line.starts_with('/') {
                files.insert(line);
            }
        }
    }
    if verbose {
        println!("pristine: dpkg lists {}", list_count);
        println!("pristine: dpkg files {}", files.len());
    }
    Ok(files.into_iter().collect())
}

fn list_command_files(cmd: &str, args: &[&str], verbose: bool) -> Result<Vec<String>> {
    let output = Command::new(cmd).args(args).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(crate::error::TimevaultError::message(format!(
            "{} failed: {}",
            cmd,
            stderr.trim()
        )));
    }
    let mut files = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let line = line.trim();
        if line.starts_with('/') {
            files.push(line.to_string());
        }
    }
    if verbose {
        println!("pristine: {} files {}", cmd, files.len());
    }
    Ok(files)
}

fn load_cache(path: &Path, verbose: bool) -> CacheFile {
    let data = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(_) => {
            return CacheFile {
                version: 1,
                entries: HashMap::new(),
            }
        }
    };
    match serde_json::from_str::<CacheFile>(&data) {
        Ok(mut cache) => {
            if cache.version == 0 {
                cache.version = 1;
            }
            cache
        }
        Err(err) => {
            if verbose {
                println!("pristine: cache read failed ({})", err);
            }
            CacheFile {
                version: 1,
                entries: HashMap::new(),
            }
        }
    }
}

fn save_cache(path: &Path, cache: &CacheFile, verbose: bool) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_string_pretty(cache)
        .map_err(|err| crate::error::TimevaultError::message(err.to_string()))?;
    fs::write(path, data)?;
    if verbose {
        println!("pristine: cache updated");
    }
    Ok(())
}

fn to_unix_mtime(time: io::Result<SystemTime>) -> io::Result<u64> {
    let time = time?;
    let duration = time
        .duration_since(UNIX_EPOCH)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    Ok(duration.as_secs())
}

fn hash_file(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn parse_os_release(content: &str) -> OsInfo {
    let mut info = OsInfo::linux();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, '=');
        let key = parts.next().unwrap_or("").trim();
        let raw = parts.next().unwrap_or("").trim();
        if key.is_empty() {
            continue;
        }
        let value = raw
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();
        match key {
            "ID" => info.id = Some(value),
            "ID_LIKE" => info.id_like = value.split_whitespace().map(|s| s.to_string()).collect(),
            "NAME" => info.name = Some(value),
            "VERSION_ID" => info.version_id = Some(value),
            _ => {}
        }
    }
    info
}

fn matches_id(os: &OsInfo, ids: &[&str]) -> bool {
    let id = os.id.as_deref();
    ids.iter().any(|needle| {
        id == Some(*needle) || os.id_like.iter().any(|like| like == needle)
    })
}

fn format_package_manager(manager: PackageManager) -> &'static str {
    match manager {
        PackageManager::Dpkg => "dpkg",
        PackageManager::Rpm => "rpm",
        PackageManager::Pacman => "pacman",
    }
}

fn format_os_info(os: &OsInfo) -> String {
    let mut parts = Vec::new();
    match &os.family {
        OsFamily::Linux => parts.push("linux".to_string()),
        OsFamily::Macos => parts.push("macos".to_string()),
        OsFamily::Windows => parts.push("windows".to_string()),
        OsFamily::Other(name) => parts.push(name.to_string()),
    }
    if let Some(id) = &os.id {
        parts.push(format!("id={}", id));
    }
    if !os.id_like.is_empty() {
        parts.push(format!("id_like={}", os.id_like.join(",")));
    }
    if let Some(version) = &os.version_id {
        parts.push(format!("version_id={}", version));
    }
    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_os_release_fields() {
        let content = r#"
NAME="Ubuntu"
VERSION_ID="22.04"
ID=ubuntu
ID_LIKE=debian
"#;
        let info = parse_os_release(content);
        assert_eq!(info.family, OsFamily::Linux);
        assert_eq!(info.name.as_deref(), Some("Ubuntu"));
        assert_eq!(info.version_id.as_deref(), Some("22.04"));
        assert_eq!(info.id.as_deref(), Some("ubuntu"));
        assert_eq!(info.id_like, vec!["debian".to_string()]);
    }

    #[test]
    fn detect_package_manager_by_id_like() {
        let info = OsInfo {
            family: OsFamily::Linux,
            id: Some("custom".to_string()),
            id_like: vec!["arch".to_string()],
            name: None,
            version_id: None,
        };
        assert_eq!(detect_package_manager(&info), Some(PackageManager::Pacman));
    }
}
