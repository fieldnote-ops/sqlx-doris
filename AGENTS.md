# Project conventions

- Preserve public API, native SQLx MySql identity, and Doris native semantics.
- Use Doris public protocols only; do not modify/build/deploy server source.
- Preserve typed/tokenized safe binding and never replay uncertain writes.
- Require an explicitly configured SQLX_DORIS_EXTERNAL_VOLUME; never embed machine-specific storage paths. Keep .cargo/ ignored.
- Run all tooling via ./scripts/dev; keep builds, caches, downloads, logs and temporary files on the external mounted volume. Stop if unavailable.
- Before starting/pulling fixtures, verify the Docker VM disk and database volumes.
- Maintain user-facing README, usage, compatibility and testing documentation. Keep development logs and historical research outside this repository.
- Missing live-test prerequisites must fail; do not count ignored or unexecuted tests as passing.
