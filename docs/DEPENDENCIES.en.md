# Dependencies, provenance, and licenses

[简体中文](DEPENDENCIES.md) | English

The SDK uses Apache-2.0. The supplied sqlx-mysql retains its upstream name, version 0.9.0, and Apache-2.0 OR
MIT dual license. Full license texts are included in the source and compatibility bundle.
It lives at `crates/sqlx-mysql` and arrives through the SDK's path dependency, so applications need no patch
entry. See [installation](INSTALLATION.en.md).

## Driver provenance

SHA256 of the original sqlx-mysql 0.9.0 crate:

```text
90b8020fe17c5f2c245bfa2505d7ef59c5604839527c740266ad2214acebea27
```

patches/sqlx-mysql-provenance.json records the source, file hashes, and licenses. Three patches reconstruct
the current driver.
`check-sqlx-patch` checks current files; `verify-delivery` checks reconstruction and bundle contents. See
[testing](TESTING.en.md) for command prerequisites.

## Status of crates.io publication

0.1.0 ships through GitHub and the compatibility bundle only. The driver builds through a path dependency,
and publishing rewrites path dependencies to registry ones, so `cargo publish --dry-run` falls back to the
upstream stock driver and fails with a missing `doris_transaction_compatibility`.
Publishing therefore needs the driver released as its own crate first (the `sqlx-mysql` name is taken
upstream, so it must be renamed and referenced through a dependency rename), followed by the SDK; that
occupies two package names and versions cannot be withdrawn.
The current install path uses path dependencies; see [installation](INSTALLATION.en.md).

## Security notes for locked dependencies

These notes refer to the source repository's Cargo.lock. Consumer applications must assess their own
Cargo.lock.
The locked block-buffer 0.10.4 falls within the version range listed by GHSA-qwgh-2vcv-g2f7.
The reviewed locked source stores pos separately and has no ReadBuffer, unlike the implementation described in
the advisory. This review does not replace a complete dependency audit.
The upstream fix is on the 0.12.1 line; this version still uses the dependencies recorded in Cargo.lock.

References: [upstream advisory](https://github.com/RustCrypto/utils/security/advisories/GHSA-qwgh-2vcv-g2f7)
and [upstream fix](https://github.com/RustCrypto/utils/pull/1487). When upgrading or distributing an
application, reassess licenses and advisories against that application's locked versions.
