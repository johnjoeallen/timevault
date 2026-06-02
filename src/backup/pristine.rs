use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::error::Result;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

const OS_RELEASE_PATH: &str = "/etc/os-release";
const PRISTINE_CACHE_REL: &str = ".cache/timevault/pristine-cache.json";
const REMOTE_HELPER_PATH: &str = "/root/tmp/timevault-pristine-hash.sh";
const REMOTE_CACHE_INPUT_PATH: &str = "/root/tmp/timevault-pristine-cache.tsv";

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PristineSource {
    Local,
    RemoteSsh { host: String },
}

pub fn build_pristine_excludes(verbose: bool) -> Result<Vec<String>> {
    build_pristine_excludes_for_source(&PristineSource::Local, verbose)
}

pub fn build_pristine_excludes_for_source(
    source: &PristineSource,
    verbose: bool,
) -> Result<Vec<String>> {
    if verbose {
        println!("pristine: detect operating system");
    }
    let os = detect_os_for_source(source)?;
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
    let cache_path = pristine_cache_path_for_source(source);
    if verbose {
        println!("pristine: cache {}", cache_path.display());
    }
    let mut cache = load_cache(&cache_path, verbose);
    let stats = match source {
        PristineSource::Local => analyze_local_files(manager, &cache, verbose)?,
        PristineSource::RemoteSsh { host } => analyze_remote_files(host, manager, &cache, verbose)?,
    };
    cache.entries = stats.entries;
    save_cache(&cache_path, &cache, verbose)?;
    if verbose {
        println!(
            "pristine: cache stats reused={} hashed={} pristine={} dirty={}",
            stats.reused, stats.hashed, stats.pristine, stats.dirty
        );
    }
    let mut excludes = cache
        .entries
        .iter()
        .filter_map(|(path, entry)| {
            if entry.dirty {
                None
            } else {
                Some(path.clone())
            }
        })
        .collect::<Vec<String>>();
    excludes.sort();
    Ok(excludes)
}

#[derive(Debug, Default)]
struct AnalyzeStats {
    entries: HashMap<String, CacheEntry>,
    reused: usize,
    hashed: usize,
    dirty: usize,
    pristine: usize,
}

pub fn detect_os() -> Result<OsInfo> {
    detect_os_for_source(&PristineSource::Local)
}

fn detect_os_for_source(source: &PristineSource) -> Result<OsInfo> {
    if let PristineSource::RemoteSsh { host } = source {
        let content = ssh_output(host, "cat /etc/os-release")?;
        return Ok(parse_os_release(&content));
    }
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
    if matches_id(
        os,
        &["rhel", "fedora", "centos", "rocky", "almalinux", "amzn"],
    ) {
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

pub fn pristine_cache_path() -> PathBuf {
    pristine_cache_path_for_source(&PristineSource::Local)
}

pub fn pristine_cache_path_for_source(source: &PristineSource) -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    let base = Path::new(&home).join(PRISTINE_CACHE_REL);
    match source {
        PristineSource::Local => base,
        PristineSource::RemoteSsh { host } => {
            base.with_file_name(format!("pristine-cache-{}.json", sanitize_cache_key(host)))
        }
    }
}

fn analyze_local_files(
    manager: PackageManager,
    cache: &CacheFile,
    verbose: bool,
) -> Result<AnalyzeStats> {
    let output = execute_local_helper(manager, cache, verbose)?;
    parse_helper_output(&output, cache, "local", verbose)
}

fn analyze_remote_files(
    host: &str,
    manager: PackageManager,
    cache: &CacheFile,
    verbose: bool,
) -> Result<AnalyzeStats> {
    upload_remote_helper(host, pristine_helper_script(), verbose)?;
    upload_remote_cache_input(host, cache, verbose)?;
    let output = execute_remote_helper(host, manager, verbose)?;
    parse_helper_output(&output, cache, "remote", verbose)
}

fn pristine_helper_script() -> &'static str {
    r#"#!/bin/sh
tab="$(printf '\t')"
manager="${1:?package manager required}"
cache_file="${2:-/root/tmp/timevault-pristine-cache.tsv}"
package_files="$(mktemp)"
cache_sorted=""
joined_records=""
trap 'rm -f "$package_files" ${cache_sorted:+"$cache_sorted"} ${joined_records:+"$joined_records"}' EXIT

case "$manager" in
    dpkg)
        find /var/lib/dpkg/info -type f -name '*.list' -exec cat {} + 2>/dev/null
        ;;
    rpm)
        rpm -qal
        ;;
    pacman)
        pacman -Qlq
        ;;
    *)
        echo "unsupported package manager: $manager" >&2
        exit 2
        ;;
esac | awk '/^\//' | sort -u > "$package_files"

count="$(wc -l < "$package_files" | tr -d '[:space:]')"
printf 'C\t%s\n' "$count"

if [ -s "$cache_file" ]; then
    cache_sorted="$(mktemp)"
    joined_records="$(mktemp)"
    sort -t "$tab" -k1,1 "$cache_file" > "$cache_sorted"
    awk -v tab="$tab" '{print $0 tab}' "$package_files" \
        | join -t "$tab" -a 1 -e '' -o '1.1 2.2 2.3' - "$cache_sorted" \
        > "$joined_records"
    input_file="$joined_records"
else
    input_file="$package_files"
fi

while IFS="$tab" read -r path cached_mtime cached_hash; do
    [ -f "$path" ] || continue
    mtime=$(stat -c %Y -- "$path" 2>/dev/null) || continue
    if [ "$mtime" = "$cached_mtime" ] && [ -n "$cached_hash" ]; then
        printf 'R\t%s\t%s\t%s\n' "$mtime" "$cached_hash" "$path"
        continue
    fi
    hash=$(sha256sum -- "$path" 2>/dev/null | awk '{print $1}') || continue
    [ -n "$hash" ] || continue
    printf 'H\t%s\t%s\t%s\n' "$mtime" "$hash" "$path"
done < "$input_file"
"#
}

fn parse_helper_output(
    output: &str,
    cache: &CacheFile,
    source_label: &str,
    verbose: bool,
) -> Result<AnalyzeStats> {
    let mut stats = AnalyzeStats::default();
    for line in output.lines() {
        let mut parts = line.splitn(4, '\t');
        let state = parts.next().unwrap_or("");
        if state == "C" {
            if verbose {
                println!(
                    "pristine: {} files {}",
                    source_label,
                    parts.next().unwrap_or("0")
                );
            }
            continue;
        }
        let mtime = parts
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let current_hash = parts.next().unwrap_or("");
        let path = parts.next().unwrap_or("").to_string();
        if path.is_empty() || current_hash.is_empty() {
            continue;
        }

        if state == "R" {
            if let Some(entry) = cache.entries.get(&path) {
                stats.reused += 1;
                if entry.dirty {
                    stats.dirty += 1;
                } else {
                    stats.pristine += 1;
                }
                stats.entries.insert(path, entry.clone());
            }
            continue;
        }

        if state != "H" {
            continue;
        }
        stats.hashed += 1;
        let (hash, dirty) = match cache.entries.get(&path) {
            Some(entry) => (entry.hash.clone(), current_hash != entry.hash),
            None => (current_hash.to_string(), false),
        };
        if dirty {
            stats.dirty += 1;
        } else {
            stats.pristine += 1;
        }
        stats
            .entries
            .insert(path, CacheEntry { mtime, hash, dirty });
    }
    if verbose {
        println!(
            "pristine: {} file states {}",
            source_label,
            stats.entries.len()
        );
    }
    Ok(stats)
}

fn ssh_output(host: &str, command: &str) -> Result<String> {
    let output = Command::new("ssh").arg(host).arg(command).output()?;
    command_output_to_string("ssh", host, output)
}

fn execute_local_helper(
    manager: PackageManager,
    cache: &CacheFile,
    verbose: bool,
) -> Result<String> {
    let mut cache_input = NamedTempFile::new()?;
    cache_input.write_all(cache_input_tsv(cache).as_bytes())?;
    if verbose {
        println!("pristine: execute local helper");
    }
    let mut child = Command::new("sh")
        .arg("-s")
        .arg(remote_package_manager_name(manager))
        .arg(cache_input.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(pristine_helper_script().as_bytes())?;
    }
    let output = child.wait_with_output()?;
    command_output_to_string("sh", "local pristine helper", output)
}

fn upload_remote_helper(host: &str, script: &str, verbose: bool) -> Result<()> {
    if verbose {
        println!(
            "pristine: upload remote helper {}:{}",
            host, REMOTE_HELPER_PATH
        );
    }
    let mut child = Command::new("ssh")
        .arg(host)
        .arg(format!(
            "mkdir -p /root/tmp && cat > {}",
            REMOTE_HELPER_PATH
        ))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(script.as_bytes())?;
    }
    let output = child.wait_with_output()?;
    command_output_to_string("ssh", host, output).map(|_| ())
}

fn upload_remote_cache_input(host: &str, cache: &CacheFile, verbose: bool) -> Result<()> {
    let input = cache_input_tsv(cache);
    if verbose {
        println!(
            "pristine: upload remote cache input {}:{}",
            host, REMOTE_CACHE_INPUT_PATH
        );
    }
    let mut child = Command::new("ssh")
        .arg(host)
        .arg(format!(
            "mkdir -p /root/tmp && cat > {}",
            REMOTE_CACHE_INPUT_PATH
        ))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(input.as_bytes())?;
    }
    let output = child.wait_with_output()?;
    command_output_to_string("ssh", host, output).map(|_| ())
}

fn cache_input_tsv(cache: &CacheFile) -> String {
    let mut paths = cache.entries.keys().cloned().collect::<Vec<_>>();
    paths.sort();
    let mut input = String::new();
    for path in paths {
        if let Some(entry) = cache.entries.get(&path) {
            input.push_str(&path);
            input.push('\t');
            input.push_str(&entry.mtime.to_string());
            input.push('\t');
            input.push_str(&entry.hash);
            input.push('\n');
        }
    }
    input
}

fn execute_remote_helper(host: &str, manager: PackageManager, verbose: bool) -> Result<String> {
    if verbose {
        println!(
            "pristine: execute remote helper {}:{}",
            host, REMOTE_HELPER_PATH
        );
    }
    let child = Command::new("ssh")
        .arg(host)
        .arg(format!(
            "sh {} {} {}",
            REMOTE_HELPER_PATH,
            remote_package_manager_name(manager),
            REMOTE_CACHE_INPUT_PATH
        ))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let output = child.wait_with_output()?;
    command_output_to_string("ssh", host, output)
}

fn remote_package_manager_name(manager: PackageManager) -> &'static str {
    match manager {
        PackageManager::Dpkg => "dpkg",
        PackageManager::Rpm => "rpm",
        PackageManager::Pacman => "pacman",
    }
}

fn command_output_to_string(
    command: &str,
    target: &str,
    output: std::process::Output,
) -> Result<String> {
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(crate::error::TimevaultError::message(format!(
            "{} {} failed: {}",
            command,
            target,
            stderr.trim()
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn sanitize_cache_key(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "unknown".to_string()
    } else {
        sanitized
    }
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
        let value = raw.trim_matches('"').trim_matches('\'').to_string();
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
    ids.iter()
        .any(|needle| id == Some(*needle) || os.id_like.iter().any(|like| like == needle))
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
