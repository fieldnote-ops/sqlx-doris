# 测试与复现

包括纯 Rust、协议模型、双库功能和故障注入、原版 SQLx 对照、安装消费者及宏。
编译检查与真实执行分开；限制负例通过不等于数据库支持相应功能。

开发脚本需要 Python 3.12+；统一排版可用 Black25.1.0 和 `./scripts/dev ./scripts/format --check`。

## 本机构建路径

先在 shell 中设置 `SQLX_DORIS_EXTERNAL_VOLUME`，值为实际挂载的构建盘绝对路径。
可选的 `SQLX_DORIS_BUILD_ROOT` 指定该卷内工作目录；未设置时使用卷内的 sqlx-doris 子目录。
`./scripts/dev --print-env` 显示 target、缓存、临时目录和日志位置。未配置或路径越界时会在写入前失败。
`.cargo/` 仅保存本机配置并被 Git 忽略，不能依靠仓库里的绝对路径配置构建。

CI 的同名路径使用仓库或组织 Variables 配置，不在工作流中写入本机盘符。
CI 使用预装 Rust、Python 与 Black 的 POSIX 自托管 runner，不限定 macOS 或 ARM64。
runner 的工作区、临时目录和工具缓存也须先配置在所选构建盘，checkout 前会检查这些位置。
手动启用 live 时从 Secrets 读取同名连接变量；数据库和 TLS 由运行者事先配置，工作流不管理虚拟机或容器。

## 检查

```sh
./scripts/dev cargo fetch --locked
./scripts/dev cargo fetch --locked --manifest-path tests/mysql-upstream/Cargo.toml
./scripts/dev ./scripts/check
./scripts/dev ./scripts/test-msrv
```

第二条 fetch 准备未打补丁的 SQLx 对照依赖及原始 crate，供来源重建校验使用；新缓存也需要执行。
check 在离线模式下运行 fmt/clippy、Python 存储/包门禁、Rust 测试、驱动测试、feature 矩阵、严格 API 文档和配套包校验。
普通 cargo test 将 live 用例标为 ignored，不代表真库通过。MSRV 使用外置独立 Rust 1.94，不改全局默认版本。

## 专用双库

完整 live 集合需配置 MYSQL_TEST_URL、DORIS_TEST_URL、DORIS_HTTP_URL 和 DORIS_BE_HTTP_URL。
其中 BE 地址供 HTTP TLS/重定向案例直接连接使用。
不要提交账号密码或业务数据。TLS/HTTP 和文件格式案例还需要外置测试夹具。

```sh
./scripts/dev ./scripts/test-live
./scripts/dev ./scripts/test-mysql-regression
./scripts/dev ./scripts/test-consumer --live
```

缺前提时显式失败。test-live 串行运行完整 live 集合、五示例及在线/离线宏，日志与 summary.json 写到外置 artifacts/clean/live。
MySQL 回归将同一 14 项合同分别编译到隔离 registry SQLx 和 companion 图，核验原包/展开源及实际逐项执行。
消费者检查直接 SQLx 类型互换、源码/精确包安装及缺 patch 编译负例。

## 测试数据与 TLS

仓库中的脚本都是可审查的 Python 源码，需要 Python 3.12+ 和 POSIX 环境；不是编译后的程序，也不是 SDK 的运行依赖。
构建与打包需要 Rust/Cargo 和所在平台的 C 编译工具链；源码重建校验还需要 patch。
排版使用 Black25.1.0；证书生成需要支持 -addext 的 OpenSSL；MSRV 脚本需要 rustup。
这些命令从 PATH 查找，不指定某台机器的安装位置。此仓库不安装虚拟机、不选择 Docker socket，也不启动数据库。
如自行用容器准备实例，拉取或启动前需同时确认镜像层、Docker VM 磁盘及数据库卷实际位于所选构建盘。

完整 live 集合需要专用、可销毁的 `sqlx_doris_test` 数据库，以及能建表、建用户和授权的测试账号。
部分安全负例要求本地转发：SQL 端口同时能从 localhost 和 127.0.0.1 访问。
HTTP TLS 代理案例当前使用 Doris 的 root 空密码测试账号；它只适用于隔离的测试实例。

```sh
./scripts/dev ./scripts/prepare-load-fixtures
./scripts/dev ./scripts/prepare-test-certificates
```

前者用外置 Python 环境中的 PyArrow22.0.0 生成 Parquet/ORC；后者用 OpenSSL 生成测试 CA、错误 CA、服务端及客户端证书。
所有数据和私钥写在 `SQLX_DORIS_BUILD_ROOT/fixtures`，不写入 Git。服务端证书仅含 DNS localhost，不含 IP SAN，供主机名校验负例使用。
证书生成脚本不更改数据库配置，也不重启服务。测试前应把生成的证书配置到两个专用实例：

- MySQL：ssl_ca、ssl_cert、ssl_key 分别指向 ca.pem、server-cert.pem、server-key.pem，并启用 TLS。
- Doris FE：enable_ssl=true；mysql_ssl_default_ca_certificate 指向 ca_certificate.p12；mysql_ssl_default_server_certificate 指向 server_certificate.p12；两项对应的 *_password 均为 fixture。

上述路径由实例部署决定，证书目录以生成脚本输出为准。不要把这些公开用途的测试口令和证书用于生产。
配置完后运行 test-live；证书、端点或权限缺失都会失败，不会跳过后仍报成功。

## 当前验证

2026-10-30，本版本受测环境为 macOS ARM64 客户端、MySQL 8.4.13 与官方 Doris 4.1.5。
测试包含原生限制的明确断言，不将已知不支持能力视为等价支持。

| 检查 | 实际结果 |
| --- | --- |
| 完整双库 live | 231/231：SDK195 + 协议/后端合同 22 + companion MySQL 核心 14，失败 0、跳过 0 |
| 原版 SQLx/companion 同源 MySQL 对照 | 两侧各 14/14；companion14 与上行重复，不计新增产品用例 |
| Rust 普通与文档测试 | 29 普通 + 3 doctest 通过；live 的 ignored 不算真库通过 |
| companion 驱动 | 115/115 |
| 工具策略测试 | 21/21，覆盖显式构建盘配置、自定义目录、路径越界、包来源/负载和 MySQL 执行计数 |
| Rust 1.94.0 | 普通/文档/驱动测试与最小/全部 feature 构建通过 |
| fmt/clippy/API 文档 | 通过，clippy 与严格文档检查无警告 |
| 示例与宏 | 五示例、双库在线/离线宏及缺缓存编译负例通过 |
| 独立安装 | 源码/compat 包实际调用双库及类型身份通过；缺 patch 编译拒绝符合预期 |
| 源码与配套包 | 原始 SQLx 重建、完整 manifest 和 SDK/driver 精确配对通过 |
| 统一排版与工作流 | Rustfmt/Black 检查及 actionlint 静态检查通过；没有把静态检查当作远端 CI 执行 |
| 全新依赖缓存 | HOME 外的空 Cargo 缓存按准备命令下载后，来源重建通过；缺原始 crate 时明确失败 |
| 异址源码与新工作目录 | 两处路径均含空格，复用外置编译缓存；包重建、安装消费者、双库在线/离线宏及缺 patch 负例通过，未预建旧 tmp 目录 |

原始日志和机器可读结果位于 scripts/dev 输出的 SQLX_DORIS_ARTIFACTS_DIR，不写入源码仓库。
上述 231 项不包含 Catalog/Broker/Kafka/多 FE 部署认证或性能长测。

完整双库、MySQL 对照、示例和宏通过连接配置执行，不依赖固定容器名或某个虚拟机实例。
