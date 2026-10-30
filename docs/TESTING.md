# 构建与测试

简体中文 | [English](TESTING.en.md)

以下命令均在源码根目录执行。业务项目接入见[安装指南](INSTALLATION.md)，不需要先运行整个测试集。

## 1. 准备构建环境

需要 Rust 1.94+（含 Cargo、Rustfmt、Clippy）、C 编译工具、Python 3.12+，以及 patch（仅用于重建补丁链）。
测试脚本适用于 POSIX 环境，工具从 PATH 查找。最低 Rust 版本检查另需 rustup；TLS 测试证书生成另需支持 -addext 的 OpenSSL。

先选择已挂载的外置构建盘。可以更改卷内工作目录，代码里没有固定盘符：

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<已挂载构建盘的绝对路径>"
export SQLX_DORIS_BUILD_ROOT="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris"
./scripts/dev --print-env
```

`dev` 设置 Cargo 缓存、target、临时目录和日志位置。盘未挂载或目录越界时会在写入前失败。
`.cargo/` 被 Git 忽略，只用于本机配置，不负责设定仓库的构建输出路径。

已有 Black 25.1.0 可直接使用；否则在构建盘上准备 Python 环境：

```sh
./scripts/dev python3 -m venv "$SQLX_DORIS_BUILD_ROOT/runtime/dev-python"
./scripts/dev "$SQLX_DORIS_BUILD_ROOT/runtime/dev-python/bin/python" -m pip install \
  --disable-pip-version-check \
  --cache-dir "$SQLX_DORIS_BUILD_ROOT/cache/pip" \
  --index-url https://pypi.org/simple black==25.1.0
export PATH="$SQLX_DORIS_BUILD_ROOT/runtime/dev-python/bin:$PATH"
```

准备两套锁定依赖：

```sh
./scripts/dev cargo fetch --locked
./scripts/dev cargo fetch --locked --manifest-path tests/mysql-upstream/Cargo.toml
```

第一条用于本项目，第二条用于未打补丁的 SQLx 对照和来源校验。第一次使用新缓存时两条都要执行。

## 2. 不连接数据库的检查

```sh
./scripts/dev ./scripts/check
./scripts/dev ./scripts/test-consumer
```

check 依次执行双语文档、格式检查、Clippy、Python 工具测试、Rust 普通测试、驱动测试、feature 构建、API 文档和打包校验。
test-consumer 从中英文安装文档中提取完整 Cargo.toml 和 main.rs，在独立应用中编译，检查原生 MySql 类型能否互换，
并确认依赖图里只有随包提供的一份 sqlx-mysql。
完整数据库用例此时显示 ignored，表示尚未执行，不算通过。

单独检查格式或最低 Rust 版本：

```sh
./scripts/dev ./scripts/format --check
./scripts/dev ./scripts/test-msrv
```

format 覆盖工作区 Rust、独立 Rust 消费者及 Python 脚本，编辑器缩进遵循 .editorconfig。
test-msrv 把 Rust 1.94.0 工具链和产物放在构建盘，不修改全局默认版本。

## 3. 先运行示例

准备好已有数据库和有相应权限的账号，按[使用指南](USAGE.md#运行源码示例)设置 MYSQL_URL、DORIS_URL 及 HTTP 变量。
三个普通示例可分别运行；五个示例连同在线/离线宏可一次执行：

```sh
export MYSQL_TEST_URL="$MYSQL_URL"
export DORIS_TEST_URL="$DORIS_URL"
./scripts/dev ./scripts/test-examples
```

该命令需要 DORIS_HTTP_URL，BE 地址不可直达时还需 DORIS_BE_HTTP_URL。
它会创建并删除临时演示表，账号需要建表、写入、查询、删表及导入权限；不需要下节的认证用户和 TLS 故障测试环境。

## 4. 完整双库回归

完整回归会创建/删除表和测试用户，并注入取消、断网等故障，使用专用、可销毁的实例。
脚本只连接实例，不安装数据库或管理虚拟机。若用容器准备实例，拉取/启动前需确认镜像层、Docker VM 磁盘和数据库卷都在构建盘。

在两库分别建好固定名称的测试库：

```sql
CREATE DATABASE IF NOT EXISTS sqlx_doris_test;
```

账号要能建表、建用户和授权。完整套件的 HTTP TLS 代理用 Doris root 空密码账号，只用于隔离测试实例。
SQL 和 HTTP 端点需要通过本地端口或转发访问，以便测试证书和主机名校验。下面的端口只是原生端口示例，可换成本地转发端口：

```sh
export MYSQL_TEST_URL='mysql://root@127.0.0.1:3306/sqlx_doris_test'
export DORIS_TEST_URL='mysql://root@127.0.0.1:9030/sqlx_doris_test'
export DORIS_HTTP_URL='http://127.0.0.1:8030'
export DORIS_BE_HTTP_URL='http://127.0.0.1:8040'
export DORIS_HTTP_USER='root'
export DORIS_HTTP_PASSWORD=''
```

完整套件同时需要这四个端点变量；HTTP 账号使用上面设置的 root 空密码。代理仅接受本地 http 地址。
普通应用和前一节的示例没有这一转发限制，可以按部署使用远端 SQL/HTTPS 地址。

### 准备文件和证书

```sh
./scripts/dev ./scripts/prepare-load-fixtures
./scripts/dev ./scripts/prepare-test-certificates
```

前者在外置 Python 环境中安装 PyArrow 22.0.0 并生成 Parquet/ORC；后者用 OpenSSL 生成测试证书。
输出在 SQLX_DORIS_BUILD_ROOT/fixtures，脚本会打印实际路径。私钥不写入 Git。
证书只包含 DNS localhost，不包含 IP SAN，供主机名错误用例使用。

把生成的证书复制到两个专用实例的部署目录，并配置 TLS。
将下面的 <服务端证书目录> 换成服务器上的实际路径：

MySQL 配置中：

```ini
[mysqld]
ssl_ca = <服务端证书目录>/ca.pem
ssl_cert = <服务端证书目录>/server-cert.pem
ssl_key = <服务端证书目录>/server-key.pem
```

Doris FE 配置中：

```ini
enable_ssl = true
mysql_ssl_default_ca_certificate = <服务端证书目录>/ca_certificate.p12
mysql_ssl_default_ca_certificate_password = fixture
mysql_ssl_default_server_certificate = <服务端证书目录>/server_certificate.p12
mysql_ssl_default_server_certificate_password = fixture
```

按实例的部署方式重启服务使配置生效。证书脚本本身不修改配置或重启服务。
SQL 端口要能从 localhost 和 127.0.0.1 访问，HTTP 地址保持上一节的本地 http 端点。

### 执行

```sh
./scripts/dev ./scripts/test-live
./scripts/dev ./scripts/test-mysql-regression
./scripts/dev ./scripts/test-consumer --live
```

test-live 串行执行完整数据库测试，再运行五个示例和在线/离线宏。
test-mysql-regression 用同一组 14 项测试分别对照原版 SQLx 和配套驱动。
test-consumer --live 验证独立应用的源码/配套包接入和双库调用。
缺少连接、证书或数据文件时命令会失败，不会跳过后报成功。

## 打包

check 已生成并验证 SDK 和 compat 包。单独重建或复核时：

```sh
./scripts/dev ./scripts/package-compat
./scripts/dev ./scripts/verify-delivery
```

它们使用 CARGO_TARGET_DIR/package 中已构建的 SDK .crate；若源码已改，先重新运行 check。
compat 包内的 SHA256SUMS.json 覆盖所有文件。安装路径见[安装指南](INSTALLATION.md)。

## CI 配置

工作流使用 POSIX 自托管 runner，标签为 self-hosted 和 external-storage。
在仓库或组织 Variables 设置 SQLX_DORIS_EXTERNAL_VOLUME、SQLX_DORIS_BUILD_ROOT，
并把 runner 工作区、临时目录、工具缓存放在构建盘。工作流在 checkout 前检查这些目录。
手动开启 live 时，从同名 Secrets 读取四个数据库/HTTP 变量；实例、证书和文件需要事先准备好。

## 当前验证

2026-10-30，本版本的测试环境为 macOS ARM64 客户端、MySQL 8.4.13 与官方 Doris 4.1.5。
测试也检查 Doris 对不支持操作的报错；具体语义见[兼容文档](COMPATIBILITY.md)。

| 检查 | 实际结果 |
| --- | --- |
| 完整双库 live | 231/231：SDK195 + 协议测试 22 + MySQL 核心 14，失败 0、跳过 0 |
| 原版 SQLx/配套驱动 MySQL 对照 | 两侧各 14/14；配套驱动这 14 项也包含在 231 项中 |
| Rust 普通与文档测试 | 29 普通 + 3 doctest 通过；live 的 ignored 不算真库通过 |
| 配套驱动 | 115/115 |
| 构建与打包工具测试 | 21/21，覆盖显式构建盘配置、自定义目录、路径越界、包来源、文件内容及 MySQL 执行计数 |
| Rust 1.94.0 | 普通/文档/驱动测试与最小/全部 feature 构建通过 |
| fmt/clippy/API 文档 | 通过，clippy 与严格文档检查无警告 |
| 示例与宏 | 五个示例、双库在线/离线宏通过；缺缓存时报错符合预期 |
| 独立安装 | 源码和 compat 包实际连接双库，原生类型可互换；依赖图中只有一份 sqlx-mysql |
| 文档中的完整程序 | 中英文 × 源码/compat 四种组合均使用文档中的 Cargo.toml/main.rs，连接双库并输出 id=42、name=hello |
| 中英文文档 | 9 组文档的语言切换、本地链接、示例一致性和 40 段 shell 语法通过；示例偏移、失效链接和缺英文文件时明确失败 |
| 使用指南代码 | MySQL/Doris 建表与事务、Doris TLS 和 JSON 导入连接真库通过 |
| 源码与配套包 | 原始 SQLx 补丁重建、文件清单和 SDK/驱动配对校验通过 |
| 统一排版与工作流 | Rustfmt/Black 检查及 actionlint 静态检查通过；没有把静态检查当作远端 CI 执行 |

命令日志和 summary.json 保存在 `./scripts/dev --print-env` 显示的 SQLX_DORIS_ARTIFACTS_DIR。
这 231 项不包含外部 Catalog、Broker、Kafka、多 FE 部署或性能测试。

HTTPS/mTLS 客户端和重定向通过本地 TLS 代理连接真库验证，服务器原生 HTTPS 部署需另行验证。
