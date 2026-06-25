# Packaging Timevault (.deb)

## Requirements
- Rust toolchain
- `cargo-deb` (`cargo install cargo-deb`)

## Other docs
- [README](README.md)
- [User Guide](USER_GUIDE.md)

## Build and package

```bash
scripts/build-deb.sh
```

The build script builds the current tree first, then increments the build number
and commits that packaging-only bump for the next package build. Semantic version
changes are made when source changes are committed, before packaging.

The package will be created under:

```
target/debian/*.deb
```

## Install

```bash
sudo dpkg -i target/debian/*.deb
```

You can also use apt with a local file (supports install and reinstall):

```bash
sudo apt install ./target/debian/*.deb
```

## Notes
- The package installs the binary to `/usr/bin/timevault`.
- Example config and docs are installed under `/usr/share/doc/timevault/`.
- Systemd service + timer are installed under `/lib/systemd/system/`.
- User config is not overwritten on upgrade.
- Pristine exclude cache lives at `~/.cache/timevault/pristine-cache.json`.
