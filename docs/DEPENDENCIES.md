# 依赖、来源与许可证

简体中文 | [English](DEPENDENCIES.en.md)

SDK 使用 Apache-2.0。随附的 sqlx-mysql 保留上游包名、0.9.0 版本号及 Apache-2.0 OR MIT 双许可证，全文随源码和配套包提供。
应用根的 `[patch.crates-io]` 让应用、宏和 SDK 使用同一个 MySql 类型；安装方式见[安装指南](INSTALLATION.md)。

## 驱动来源

原始 sqlx-mysql 0.9.0 crate 的 SHA256 为：

```text
90b8020fe17c5f2c245bfa2505d7ef59c5604839527c740266ad2214acebea27
```

patches/sqlx-mysql-provenance.json 记录来源、文件哈希和许可证，三份 patch 可重建当前驱动。
`check-sqlx-patch` 检查当前文件，`verify-delivery` 检查补丁重建和配套包内容，命令前提见[测试说明](TESTING.md)。

## 锁定依赖的安全说明

本文对应源码仓库的 Cargo.lock；应用接入后以自己的 Cargo.lock 为准。
其中 block-buffer 0.10.4 命中 GHSA-qwgh-2vcv-g2f7 公布的版本范围。
已核对的锁定源码将 pos 独立存储，且没有 ReadBuffer，与公告描述的实现不同；这项复核不能代替完整依赖审计。
上游修复线为 0.12.1，本版本仍使用 Cargo.lock 记录的依赖。

依据：[上游公告](https://github.com/RustCrypto/utils/security/advisories/GHSA-qwgh-2vcv-g2f7)、
[上游修复](https://github.com/RustCrypto/utils/pull/1487)。升级或分发应用时，需要按应用自己的锁定版本重新核对许可证和公告。
