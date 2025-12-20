# Packaging TimeVault (.deb)

## Requirements
- Rust toolchain
- `cargo-deb` (`cargo install cargo-deb`)

## Build and package

```bash
cargo build --release
cargo deb
```

The package will be created under:

```
target/debian/*.deb
```

## Install

```bash
sudo dpkg -i target/debian/*.deb
```

## Notes
- The package installs the binary to `/usr/bin/timevault`.
- Example config and docs are installed under `/usr/share/doc/timevault/`.
- Systemd service + timer are installed under `/lib/systemd/system/`.
- User config is not overwritten on upgrade.
