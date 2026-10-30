# 依赖、来源与许可证

SDK 为 Apache-2.0，LICENSE 随源码和包交付。SQLx 固定=0.9.0；companion 保持 sqlx-mysql 原名/版本及 Apache-2.0 OR MIT 许可，保留两份上游许可证。
应用根 patch 统一应用、宏和 SDK 的 MySql 身份，不自动升级或替换其他加密依赖。

## Companion 来源

上游 sqlx-mysql0.9.0 原始 crate SHA256 为 90b8020fe17c5f2c245bfa2505d7ef59c5604839527c740266ad2214acebea27。
patches/sqlx-mysql-provenance.json 列出原始来源、全部上游/修改源哈希及许可，三份 patch 可重建当前源。
check-sqlx-patch 和 verify-delivery 拒绝未登记变化、负载遗漏或混入服务器源/二进制。
修正范围为短 PrepareOk、事务与受检绑定/时间/分页，只使用 Doris 公开协议。

## 锁定依赖的公告说明

block-buffer0.10.4 会命中 GHSA-qwgh-2vcv-g2f7 公布的版本范围。当前锁定源的 pos 独立存储、没有 ReadBuffer，
与公告描述的易受影响实现不同。原始命中不能写成零命中，局部源码复核也不是所有依赖的安全保证。
上游修复线为 0.12.1；本版本的依赖版本以 Cargo.lock 为准。
依据：[上游公告](https://github.com/RustCrypto/utils/security/advisories/GHSA-qwgh-2vcv-g2f7)、[上游修复](https://github.com/RustCrypto/utils/pull/1487)。

分发或升级时根据当前 Cargo.lock 重新清点许可证和公告，不将这个说明当作未来公告或任意下游依赖图的认证。
