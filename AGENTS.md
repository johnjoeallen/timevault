# Agent Instructions

## Project Rules

- When asked to commit bug fixes, bump the patch version and reset the build number to 0 as part of the commit.
- When asked to commit features, bump the minor version and reset the build number to 0 as part of the commit.
- Before applying a commit-time version bump, inspect the diff. If the only changes are packaging build-number changes from `scripts/build-deb.sh`, commit those as-is without changing the semantic version or resetting the build number.
- Major version changes are managed manually.
