# Building and testing

[简体中文](TESTING.md) | English

Run these commands from the source root. To use the SDK, follow [installation](INSTALLATION.en.md); you do
not need to run the entire test suite.

## 1. Prepare the build environment

You need Rust 1.94+ (including Cargo, Rustfmt, and Clippy), a C toolchain, Python 3.12+, and patch (only for
reconstructing the patch chain).
The scripts run in POSIX environments and find tools through PATH. The minimum Rust version check also needs
rustup; TLS certificate generation needs OpenSSL with -addext support.

Select a mounted external build volume. The working directory within it is configurable; no drive path is
hardcoded:

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<absolute path to a mounted build volume>"
export SQLX_DORIS_BUILD_ROOT="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris"
./scripts/dev --print-env
```

`dev` sets locations for Cargo caches, target output, temporary files, and logs. It fails before writing if
the volume is unmounted or a directory escapes it.
Git ignores `.cargo/`; it is only for local configuration and does not define this repository's build output
location.

Use an existing Black 25.1.0 installation, or prepare a Python environment on the build volume:

```sh
./scripts/dev python3 -m venv "$SQLX_DORIS_BUILD_ROOT/runtime/dev-python"
./scripts/dev "$SQLX_DORIS_BUILD_ROOT/runtime/dev-python/bin/python" -m pip install \
  --disable-pip-version-check \
  --cache-dir "$SQLX_DORIS_BUILD_ROOT/cache/pip" \
  --index-url https://pypi.org/simple black==25.1.0
export PATH="$SQLX_DORIS_BUILD_ROOT/runtime/dev-python/bin:$PATH"
```

Fetch both sets of locked dependencies:

```sh
./scripts/dev cargo fetch --locked
./scripts/dev cargo fetch --locked --manifest-path tests/mysql-upstream/Cargo.toml
```

The first is for this project; the second supports the unpatched SQLx comparison and provenance checks. Run
both when using a fresh cache.

## 2. Checks without database connections

```sh
./scripts/dev ./scripts/check
./scripts/dev ./scripts/test-consumer
```

check runs bilingual documentation checks, formatting, Clippy, Python tool tests, ordinary Rust tests, driver
tests, feature builds, API documentation, and package verification.
test-consumer extracts complete Cargo.toml and main.rs files from the Chinese and English installation guides,
compiles them in independent applications, checks native MySql type interchangeability, and confirms the
dependency graph contains only the bundled sqlx-mysql.
Full database tests appear as ignored at this stage. They have not run and do not count as passing.

To check formatting or the minimum Rust version separately:

```sh
./scripts/dev ./scripts/format --check
./scripts/dev ./scripts/test-msrv
```

format covers workspace Rust, standalone Rust consumers, and Python scripts. Editor indentation follows
.editorconfig.
test-msrv installs Rust 1.94.0 and stores its output on the build volume without changing the global default
toolchain.

## 3. Run the examples first

Prepare existing databases and accounts with the necessary permissions. Set MYSQL_URL, DORIS_URL, and the HTTP
variables as shown in [usage](USAGE.en.md#run-the-source-examples).
Run the three ordinary examples separately, or run all five examples with online/offline macro checks:

```sh
export MYSQL_TEST_URL="$MYSQL_URL"
export DORIS_TEST_URL="$DORIS_URL"
./scripts/dev ./scripts/test-examples
```

This command requires DORIS_HTTP_URL and, if the returned BE address is unreachable, DORIS_BE_HTTP_URL.
It creates and drops temporary demonstration tables. The accounts need permissions to create, write, query,
drop, and import. It does not need the authentication users or TLS failure setup described below.

## 4. Full regression against both databases

The full suite creates/drops tables and test users and injects cancellation and network failures. Use
dedicated, disposable instances.
Scripts only connect to instances; they do not install databases or manage VMs. Before pulling or starting
containers, verify that image layers, the Docker VM disk, and database volumes are on the build volume.

Create a database named sqlx_doris_test on both servers:

```sql
CREATE DATABASE IF NOT EXISTS sqlx_doris_test;
```

Accounts must be able to create tables, create users, and grant permissions. The full suite's HTTP TLS proxy
uses the Doris root account with an empty password, only on isolated test instances.
SQL and HTTP endpoints must be accessible through local ports or forwarding for certificate and hostname
checks. The examples use the default service ports; substitute your local forwarded ports if needed:

```sh
export MYSQL_TEST_URL='mysql://root@127.0.0.1:3306/sqlx_doris_test'
export DORIS_TEST_URL='mysql://root@127.0.0.1:9030/sqlx_doris_test'
export DORIS_HTTP_URL='http://127.0.0.1:8030'
export DORIS_BE_HTTP_URL='http://127.0.0.1:8040'
export DORIS_HTTP_USER='root'
export DORIS_HTTP_PASSWORD=''
```

All four endpoint variables are required for the full suite. Use the root HTTP account with an empty password
as set above. The proxy only accepts local http endpoints.
Ordinary applications and the preceding examples do not have this forwarding restriction; they can use remote
SQL/HTTPS endpoints appropriate to the deployment.

### Prepare files and certificates

```sh
./scripts/dev ./scripts/prepare-load-fixtures
./scripts/dev ./scripts/prepare-test-certificates
```

The first script installs PyArrow 22.0.0 in an external Python environment and generates Parquet/ORC files.
The second generates test certificates with OpenSSL.
Output goes under SQLX_DORIS_BUILD_ROOT/fixtures; the scripts print the actual paths. Private keys are not
written to Git.
The certificates contain a DNS subject alternative name (SAN) for localhost but no IP SAN, so the tests
can check hostname mismatches.

Copy the generated certificates into the two dedicated server deployments and configure TLS.
Replace <server certificate directory> with the actual server-side path.

MySQL configuration:

```ini
[mysqld]
ssl_ca = <server certificate directory>/ca.pem
ssl_cert = <server certificate directory>/server-cert.pem
ssl_key = <server certificate directory>/server-key.pem
```

Doris FE configuration:

```ini
enable_ssl = true
mysql_ssl_default_ca_certificate = <server certificate directory>/ca_certificate.p12
mysql_ssl_default_ca_certificate_password = fixture
mysql_ssl_default_server_certificate = <server certificate directory>/server_certificate.p12
mysql_ssl_default_server_certificate_password = fixture
```

Restart the services according to your deployment method to apply the configuration. The certificate script
itself changes no server configuration and restarts no services.
The SQL ports must be accessible through both localhost and 127.0.0.1. Keep the local http endpoints
configured above.

### Run the tests

```sh
./scripts/dev ./scripts/test-live
./scripts/dev ./scripts/test-mysql-regression
./scripts/dev ./scripts/test-consumer --live
```

test-live runs the full database tests serially, then the five examples and online/offline macros.
test-mysql-regression applies the same 14 tests to upstream SQLx and the companion driver.
test-consumer --live verifies source/bundle installation in independent applications and calls against both
databases.
Missing connections, certificates, or data files cause failure, not a successful result with skipped tests.

## Packaging

check generates and verifies the SDK and compatibility packages. To rebuild or verify them separately:

```sh
./scripts/dev ./scripts/package-compat
./scripts/dev ./scripts/verify-delivery
```

These commands use the SDK .crate already built in CARGO_TARGET_DIR/package. If the source has changed, run
check first.
SHA256SUMS.json covers every file in the compatibility bundle. See [installation](INSTALLATION.en.md) for
paths.

## CI configuration

The workflow uses a POSIX self-hosted runner labeled self-hosted and external-storage.
Set SQLX_DORIS_EXTERNAL_VOLUME and SQLX_DORIS_BUILD_ROOT in repository or organization Variables.
Place the runner workspace, temporary directory, and tool cache on the build volume. The workflow checks these
locations before checkout.
When manually enabling live tests, the workflow reads the four database/HTTP variables from Secrets of the
same names. Instances, certificates, and files must already be prepared.

## Validation results

On 2026-10-30, this version was tested with a macOS ARM64 client, MySQL 8.4.13, and official Doris 4.1.5.
Tests also check Doris errors for unsupported operations. See [compatibility](COMPATIBILITY.en.md) for
semantics.

| Check | Actual result |
| --- | --- |
| Full live suite against both databases | 231/231: SDK 195 + protocol 22 + MySQL core 14; 0 failed, 0 skipped |
| Upstream SQLx/companion MySQL comparison | 14/14 on each; the companion's 14 are also included in the 231 above |
| Ordinary Rust and documentation tests | 29 ordinary + 3 doctests passed; ignored live tests do not count as database passes |
| Companion driver | 115/115 |
| Build and packaging tool tests | 21/21, covering explicit build volume configuration, custom directories, path containment, package provenance/content, and MySQL execution counts |
| Rust 1.94.0 | Ordinary, documentation, and driver tests plus minimal/all-feature builds passed |
| fmt/clippy/API docs | Passed, with no Clippy or strict documentation warnings |
| Examples and macros | Five examples and online/offline macros for both databases passed; the missing-cache error matched expectations |
| Independent installation | Source and compatibility bundle applications connected to both databases; native types were interchangeable; the dependency graph held a single sqlx-mysql |
| Complete programs in the guides | All four combinations (Chinese/English × source/bundle) used the documented Cargo.toml/main.rs, connected to both databases, and printed id=42, name=hello |
| Bilingual documentation | Language navigation, local links, matching examples, and syntax of 40 shell blocks passed for 9 guide pairs; changed examples, broken links, and missing English files were rejected |
| Usage guide code | MySQL/Doris table creation and transactions, Doris TLS, and JSON import passed against real databases |
| Source and compatibility bundle | Original SQLx patch reconstruction, file lists, and SDK/driver pairing passed |
| Formatting and workflow | Rustfmt/Black and actionlint static checks passed; static validation is not reported as a remote CI run |

Command logs and summary.json files are under SQLX_DORIS_ARTIFACTS_DIR. Run
`./scripts/dev --print-env` to see the path.
The 231 tests do not include external Catalogs, Broker, Kafka, multiple FE deployments, or performance tests.

HTTPS/mTLS clients and redirects were verified against real databases through a local TLS proxy. Native server
HTTPS deployment requires separate verification.
