/* Canonical documentation model. Identifiers and relations follow the manifests. */
globalThis.DORIS_ATLAS_DATA = {
    "version": 1,
    "packages": [
        {
            "id": "facade",
            "name": "sqlx-doris",
            "role": {
                "en": "Public Doris entry point",
                "zh": "Doris 公开入口"
            }
        },
        {
            "id": "core",
            "name": "sqlx-doris-core",
            "role": {
                "en": "Identity · wire · values · migration traits",
                "zh": "身份 · 协议 · 类型 · 迁移 traits"
            }
        },
        {
            "id": "macros",
            "name": "sqlx-doris-macros",
            "role": {
                "en": "Checked queries · offline metadata",
                "zh": "编译期查询 · 离线元数据"
            }
        },
        {
            "id": "ops",
            "name": "sqlx-doris-ops",
            "role": {
                "en": "Helpers · ingestion · guarded operations",
                "zh": "应用助手 · 导入 · 受控运维"
            }
        },
        {
            "id": "cli",
            "name": "sqlx-doris-cli",
            "role": {
                "en": "Cargo integration · commands · rendering",
                "zh": "Cargo 集成 · 命令 · 输出"
            }
        }
    ],
    "dependencies": [
        {
            "from": "facade",
            "to": "core",
            "optional": false
        },
        {
            "from": "facade",
            "to": "macros",
            "optional": true
        },
        {
            "from": "facade",
            "to": "ops",
            "optional": true
        },
        {
            "from": "facade",
            "to": "cli",
            "optional": true
        },
        {
            "from": "macros",
            "to": "core",
            "optional": true
        },
        {
            "from": "ops",
            "to": "core",
            "optional": false
        },
        {
            "from": "cli",
            "to": "core",
            "optional": false
        },
        {
            "from": "cli",
            "to": "ops",
            "optional": false
        }
    ],
    "features": {
        "default": [],
        "macros": [
            "dep:sqlx-doris-macros",
            "sqlx-doris-macros/query"
        ],
        "migrate": [
            "sqlx-doris-core/migrate"
        ],
        "cli": [
            "migrate",
            "dep:sqlx-doris-cli"
        ],
        "cli-admin": [
            "cli",
            "sqlx-doris-cli/admin"
        ],
        "cli-admin-kafka": [
            "cli-admin",
            "sqlx-doris-cli/kafka-admin"
        ],
        "operations": [
            "dep:sqlx-doris-ops",
            "sqlx-doris-ops/helpers"
        ],
        "stream-load": [
            "operations",
            "sqlx-doris-ops/stream-load"
        ]
    },
    "tasks": [
        {
            "id": "query",
            "number": "01",
            "tier": "application",
            "features": [],
            "packages": [
                "facade",
                "core"
            ],
            "title": {
                "en": "Query & stream",
                "zh": "查询与流式读取"
            },
            "description": {
                "en": "Bind values, map rows and stream results through a native Doris pool.",
                "zh": "通过原生 Doris 连接池绑定参数、映射行并流式读取结果。"
            },
            "api": "sqlx::query* + DorisPool",
            "upstream": {
                "en": "sqlx; derive for FromRow",
                "zh": "sqlx；FromRow 需要 derive"
            },
            "flow": {
                "en": "Application → SQLx Executor → Doris wire → FE",
                "zh": "应用 → SQLx Executor → Doris 协议 → FE"
            },
            "boundary": {
                "en": "Runtime queries need no build-time database. Use SQLx pool hooks for session settings.",
                "zh": "运行时查询无需编译期数据库。会话设置使用 SQLx 连接池钩子。"
            },
            "guides": [
                {
                    "name": "USAGE",
                    "anchor": "doc-queries",
                    "label": {
                        "en": "Queries & pools",
                        "zh": "查询与连接池"
                    }
                },
                {
                    "name": "USAGE",
                    "anchor": "doc-types",
                    "label": {
                        "en": "Exact Doris types",
                        "zh": "精确 Doris 类型"
                    }
                }
            ]
        },
        {
            "id": "ingest",
            "number": "02",
            "tier": "application",
            "features": [
                "stream-load"
            ],
            "packages": [
                "facade",
                "ops",
                "core"
            ],
            "title": {
                "en": "Ingest a batch",
                "zh": "批量导入"
            },
            "description": {
                "en": "Send JSON, bytes or immutable files to an existing table; retain identity and inspect the receipt.",
                "zh": "向已有表发送 JSON、字节或不可变文件；保留批次身份并核对回执。"
            },
            "api": "StreamLoadClient → LoadReceipt",
            "upstream": {
                "en": "Only as required by query code",
                "zh": "按查询代码需要选择"
            },
            "flow": {
                "en": "FE routing → client validation → BE upload → receipt checks",
                "zh": "FE 路由 → 客户端验证 → BE 上传 → 回执核对"
            },
            "boundary": {
                "en": "Accepted, committed and visible differ. Unknown outcomes do not authorize replay; label does not prove equal payloads.",
                "zh": "已接受、已提交与已可见含义不同。结果未知不能据此重发；label 不证明 payload 相同。"
            },
            "guides": [
                {
                    "name": "INGESTION",
                    "anchor": "doc-json",
                    "label": {
                        "en": "Import an existing table",
                        "zh": "向已有表导入"
                    }
                },
                {
                    "name": "INGESTION",
                    "anchor": "doc-result",
                    "label": {
                        "en": "Interpret outcomes",
                        "zh": "理解导入结果"
                    }
                }
            ]
        },
        {
            "id": "checked",
            "number": "03",
            "tier": "development",
            "features": [
                "macros"
            ],
            "packages": [
                "facade",
                "macros",
                "core"
            ],
            "title": {
                "en": "Check SQL at build time",
                "zh": "编译期检查 SQL"
            },
            "description": {
                "en": "Describe Doris queries online, prepare metadata and build offline with checked output types.",
                "zh": "在线描述 Doris 查询、准备元数据，并以受检查的输出类型离线构建。"
            },
            "api": "sqlx_doris::query!",
            "upstream": {
                "en": "Direct sqlx; derive or macros",
                "zh": "直接依赖 sqlx；derive 或 macros"
            },
            "flow": {
                "en": "prepare → Cargo build → macro → describe / offline cache",
                "zh": "prepare → Cargo 构建 → 宏 → describe / 离线缓存"
            },
            "boundary": {
                "en": "The macro describes SQL; it does not execute business SQL. The compiled query later uses the normal runtime driver.",
                "zh": "宏描述 SQL，不执行业务 SQL。编译后的查询仍通过正常运行时驱动执行。"
            },
            "guides": [
                {
                    "name": "USAGE",
                    "anchor": "doc-macros",
                    "label": {
                        "en": "Checked queries",
                        "zh": "编译期查询"
                    }
                },
                {
                    "name": "USAGE",
                    "anchor": "doc-offline",
                    "label": {
                        "en": "Prepare & offline builds",
                        "zh": "准备元数据与离线构建"
                    }
                }
            ]
        },
        {
            "id": "migrate",
            "number": "04",
            "tier": "development",
            "features": [
                "migrate"
            ],
            "packages": [
                "facade",
                "core"
            ],
            "title": {
                "en": "Ship schema changes",
                "zh": "交付结构变更"
            },
            "description": {
                "en": "Use the SQLx migrator in your application, or install the basic CLI for migration files and commands.",
                "zh": "在应用中使用 SQLx Migrator，或安装基础 CLI 管理迁移文件并执行命令。"
            },
            "api": "sqlx::migrate! / Migrator",
            "upstream": {
                "en": "migrate; macros for migrate!",
                "zh": "migrate；migrate! 另需 macros"
            },
            "flow": {
                "en": "Migrator → owner / history checks → DDL → job completion",
                "zh": "Migrator → owner / 历史检查 → DDL → 作业完成"
            },
            "boundary": {
                "en": "Embedded migrations also need upstream macros + migrate. CLI is a separate feature; recovery is explicit and never automatic.",
                "zh": "嵌入迁移还需上游 macros + migrate。CLI 是独立特性；恢复必须显式执行，不自动接管。"
            },
            "guides": [
                {
                    "name": "MIGRATIONS",
                    "anchor": "doc-api",
                    "label": {
                        "en": "Migration API",
                        "zh": "迁移 API"
                    }
                },
                {
                    "name": "MIGRATIONS",
                    "anchor": "doc-commands",
                    "label": {
                        "en": "Migration commands",
                        "zh": "迁移命令"
                    }
                }
            ]
        },
        {
            "id": "helpers",
            "number": "05",
            "tier": "application",
            "features": [
                "operations"
            ],
            "packages": [
                "facade",
                "ops",
                "core"
            ],
            "title": {
                "en": "Use Doris helpers",
                "zh": "使用 Doris 助手"
            },
            "description": {
                "en": "Use explicit schema, complex-value, MV and visibility helpers alongside native SQL.",
                "zh": "在原生 SQL 旁按需使用 schema、复杂值、MV 和可见性助手。"
            },
            "api": "sqlx_doris::operations",
            "upstream": {
                "en": "As required by query code",
                "zh": "按查询代码需要选择"
            },
            "flow": {
                "en": "Explicit helper → native SQL → observe the caller-verified task",
                "zh": "显式助手 → 原生 SQL → 观察调用方已核验的任务"
            },
            "boundary": {
                "en": "Submitting a refresh is not task completion. Helpers do not supply an ORM, scheduler or automatic orchestration.",
                "zh": "提交刷新不等于任务完成。助手不提供 ORM、调度器或自动编排。"
            },
            "guides": [
                {
                    "name": "USAGE",
                    "anchor": "doc-types",
                    "label": {
                        "en": "Values & helpers",
                        "zh": "类型与助手"
                    }
                },
                {
                    "name": "USAGE",
                    "anchor": "doc-mv",
                    "label": {
                        "en": "Observe MV refresh",
                        "zh": "观察 MV 刷新"
                    }
                }
            ]
        },
        {
            "id": "diagnose",
            "number": "06",
            "tier": "specialist",
            "features": [
                "cli-admin"
            ],
            "packages": [
                "facade",
                "cli",
                "ops",
                "core"
            ],
            "title": {
                "en": "Diagnose an incident",
                "zh": "诊断异常"
            },
            "description": {
                "en": "Start with read-only diagnosis, exact owners and retained evidence; choose maintenance actions explicitly.",
                "zh": "先只读诊断、核对精确 owner 并保留证据，再显式选择维护操作。"
            },
            "api": "cargo sqlx-doris admin diagnose",
            "upstream": {
                "en": "Managed by the CLI",
                "zh": "由 CLI 管理"
            },
            "flow": {
                "en": "Operator → CLI → read-only ops → scoped Doris observations",
                "zh": "操作人员 → CLI → 只读 ops → 限定范围 Doris 观察"
            },
            "boundary": {
                "en": "Diagnosis does not certify takeover. Independent Kafka preflight additionally requires cli-admin-kafka; no offset transfer/reset.",
                "zh": "诊断不认证安全接管。独立 Kafka 预检另需 cli-admin-kafka；不转移或重置 offset。"
            },
            "guides": [
                {
                    "name": "CLI",
                    "anchor": "doc-diagnose",
                    "label": {
                        "en": "Read-only diagnosis",
                        "zh": "只读诊断"
                    }
                },
                {
                    "name": "RECOVERY",
                    "anchor": "doc-sequence",
                    "label": {
                        "en": "Recovery boundaries",
                        "zh": "恢复步骤与边界"
                    }
                }
            ]
        }
    ]
};
