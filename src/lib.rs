pub mod backup;
pub mod cli;
pub mod config;
pub mod disk;
pub mod error;
pub mod mount;
pub mod types;
pub mod util;

#[cfg(test)]
mod tests {
    #[test]
    fn deb_revision_matches_build_number() {
        let manifest = include_str!("../Cargo.toml");
        let revision = manifest
            .lines()
            .find_map(|line| line.trim().strip_prefix("revision = "))
            .map(|value| value.trim_matches('"'))
            .expect("package.metadata.deb revision");
        assert_eq!(revision, crate::cli::BUILD_NUMBER.to_string());
    }
}
