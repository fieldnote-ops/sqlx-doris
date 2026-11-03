/* User documentation only. No network calls, telemetry or runtime dependencies. */
(function () {
    "use strict";

    const model = globalThis.DORIS_ATLAS_DATA;
    const text = (lang, en, zh) => lang === "zh" ? zh : en;
    const escape = (value) => String(value).replace(/[&<>"']/g, (character) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    }[character]));
    const local = (value, lang) => value[lang];
    const guide = (item, lang) => `${item.name}${lang === "zh" ? ".zh-CN" : ""}.md#${item.anchor}`;
    const svgText = (x, y, value, className = "body", extra = "") =>
        `<text x="${x}" y="${y}" class="${className}" ${extra}>${escape(value)}</text>`;

    const svgStyle = `
        svg { --paper:#0b111a; --panel:#111b28; --raised:#192638; --edge:#34465b;
            --ink:#edf3fa; --muted:#a4b4c8; --faint:#668097; --cyan:#62dbef;
            --amber:#edc17d; --violet:#b7a1f5; --green:#8ed7ba; background:var(--paper); }
        @media (prefers-color-scheme:light) {
            svg { --paper:#f5f8fc; --panel:#fff; --raised:#e6eef7; --edge:#abbdd1;
                --ink:#15273c; --muted:#425c74; --faint:#60788c; --cyan:#086780;
                --amber:#855109; --violet:#654397; --green:#236c51; }
        }
        svg[data-theme=light] { --paper:#f5f8fc; --panel:#fff; --raised:#e6eef7; --edge:#abbdd1;
            --ink:#15273c; --muted:#425c74; --faint:#60788c; --cyan:#086780;
            --amber:#855109; --violet:#654397; --green:#236c51; }
        svg[data-theme=dark] { --paper:#0b111a; --panel:#111b28; --raised:#192638; --edge:#34465b;
            --ink:#edf3fa; --muted:#a4b4c8; --faint:#668097; --cyan:#62dbef;
            --amber:#edc17d; --violet:#b7a1f5; --green:#8ed7ba; }
        text { font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC",sans-serif; fill:var(--ink); }
        .title { font-size:27px; font-weight:650; letter-spacing:-.7px; }
        .node-title { font-size:17px; font-weight:600; letter-spacing:-.2px; }
        .body { font-size:14px; fill:var(--muted); }
        .small { font-size:12px; fill:var(--muted); }
        .micro { font-size:11px; fill:var(--faint); letter-spacing:1.3px; }
        .mono { font:13px "SFMono-Regular",Consolas,"Liberation Mono",monospace; fill:var(--muted); }
        .accent { fill:var(--cyan); }
        .number { font:15px "SFMono-Regular",Consolas,monospace; fill:var(--cyan); }
        .panel { fill:var(--panel); stroke:var(--edge); stroke-width:1; }
        .raised { fill:var(--raised); stroke:var(--edge); stroke-width:1; }
        .frame { fill:var(--paper); stroke:var(--edge); stroke-width:1; }
        .rule { stroke:var(--edge); stroke-width:1; fill:none; }
        .flow { fill:none; stroke-width:1.8; stroke-linecap:round; stroke-linejoin:round; }
        .execute { stroke:var(--cyan); }
        .control { stroke:var(--amber); stroke-dasharray:7 5; }
        .metadata { stroke:var(--violet); stroke-dasharray:1 5; }
        .data { stroke:var(--green); stroke-width:3; }
        .dependency { stroke:var(--muted); }
        .optional { stroke-dasharray:5 5; }
        .relation-label { paint-order:stroke; stroke:var(--paper); stroke-width:7px; stroke-linejoin:round; }
        [data-task], [data-package] { cursor:pointer; outline:none; }
        [data-task]:focus-visible .panel, [data-package]:focus-visible .panel { stroke:var(--cyan); stroke-width:3; }
        .selected .panel { stroke:var(--cyan); stroke-width:1.8; }
        .dim .flow { stroke-width:1.3; }
        .hot { stroke:var(--cyan); stroke-width:2.4; }
        .selected .node-title { fill:var(--cyan); }
    `;

    function frame(lang, id, height, title, subtitle, body) {
        const defs = ["execute", "control", "metadata", "data", "dependency"].map((kind) =>
            `<marker id="${id}-${kind}" viewBox="0 0 10 8" refX="9" refY="4" markerWidth="9" markerHeight="8" markerUnits="userSpaceOnUse" orient="auto-start-reverse"><path d="M1 1L9 4L1 7" fill="none" class="flow ${kind}" style="stroke-dasharray:none"/></marker>`
        ).join("");
        return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 ${height}" role="img" aria-labelledby="${id}-title ${id}-desc" data-view="${id}">
            <title id="${id}-title">${escape(title)}</title>
            <desc id="${id}-desc">${escape(subtitle)}</desc>
            <defs><style>${svgStyle}</style>${defs}</defs>
            <rect x="1" y="1" width="1198" height="${height - 2}" rx="16" class="frame"/>
            ${svgText(32, 35, `SQLX–DORIS / ${id.toUpperCase()}`, "micro")}
            ${svgText(32, 75, title, "title")}
            ${svgText(1168, 35, "0.2.0 / UNRELEASED", "micro", 'text-anchor="end"')}
            ${svgText(32, 102, subtitle, "body")}
            ${body}
        </svg>`;
    }

    function card(x, y, width, height, title, detail, options = {}) {
        const { eyebrow = "", attributes = "", selected = false } = options;
        const budget = Math.floor((width - 32) / 7.8);
        const words = detail.split(" ");
        const detailLines = [""];
        for (const word of words) {
            const last = detailLines.length - 1;
            const next = `${detailLines[last]} ${word}`.trim();
            if (next.length > budget && detailLines[last]) detailLines.push(word);
            else detailLines[last] = next;
        }
        const titleY = eyebrow ? 43 : height < 62 ? 22 : 27;
        const detailY = height - (eyebrow ? 12 : height < 62 ? 11 : 16) - (detailLines.length - 1) * 15;
        return `<g class="node${selected ? " selected" : ""}" ${attributes}>
            <rect x="${x}" y="${y + 4}" width="${width}" height="${height}" rx="9" class="raised"/>
            <rect x="${x}" y="${y}" width="${width}" height="${height}" rx="9" class="panel"/>
            <path d="M${x + 12} ${y + 1}H${x + width - 12}" class="rule"/>
            ${eyebrow ? svgText(x + 16, y + 23, eyebrow, "micro") : ""}
            ${svgText(x + 16, y + titleY, title, "node-title")}
            ${detailLines.map((value, index) => svgText(x + 16, y + detailY + index * 15, value, "mono")).join("")}
        </g>`;
    }

    function line(view, kind, path, label = "", x = 0, y = 0, extra = "") {
        return `<path d="${path}" class="flow ${kind}" marker-end="url(#${view}-${kind})" ${extra}/>` +
            (label ? svgText(x, y, label, "small relation-label", 'text-anchor="middle"') : "");
    }

    function legend(view, x, y, kind, label) {
        return line(view, kind, `M${x} ${y - 4}H${x + 34}`) + svgText(x + 46, y, label, "small");
    }

    function taskDiagram(lang = "en", selected = "") {
        const tiers = {
            application: text(lang, "APPLICATION", "应用运行期"),
            development: text(lang, "DEVELOPMENT", "开发与交付"),
            specialist: text(lang, "SPECIALIST", "专项运维")
        };
        const cards = model.tasks.map((task, index) => {
            const x = 32 + (index % 2) * 580;
            const y = 130 + Math.floor(index / 2) * 127;
            const feature = task.features.join(" + ") || text(lang, "none / default=[]", "无需特性 / default=[]");
            return `<g data-task="${task.id}" role="button" tabindex="0" aria-label="${escape(local(task.title, lang))}" class="${selected === task.id ? "selected" : ""}">
                <rect x="${x}" y="${y + 3}" width="556" height="111" rx="10" class="raised"/>
                <rect x="${x}" y="${y}" width="556" height="111" rx="10" class="panel"/>
                ${svgText(x + 18, y + 23, task.number, "number")}
                ${svgText(x + 58, y + 23, tiers[task.tier], "micro")}
                ${svgText(x + 18, y + 48, local(task.title, lang), "node-title")}
                ${svgText(x + 18, y + 71, task.api, "mono")}
                <path d="M${x + 18} ${y + 84}H${x + 538}" class="rule"/>
                ${svgText(x + 18, y + 102, feature, "mono accent")}
                ${svgText(x + 530, y + 49, "↗", "node-title accent")}
            </g>`;
        }).join("");
        return frame(lang, "tasks", 542,
            text(lang, "Choose a task. Follow one clear path.", "按任务选路，沿清晰路径开始。"),
            text(lang, "One public Doris extension · native SQLx APIs · features selected by the application", "一个 Doris 公开扩展 · 原生 SQLx API · 应用按需选择特性"),
            cards + svgText(32, 522,
                text(lang, "Features name the Doris side. Upstream SQLx features remain explicit; see the task detail and guides.", "这里列出 Doris 特性；上游 SQLx 特性仍需显式选择，详见任务说明与指南。"), "small"));
    }

    function integrationDiagram(lang = "en") {
        const id = "integration";
        const band = (y, height, number, title, note) =>
            `<rect x="22" y="${y}" width="1156" height="${height}" rx="10" class="panel"/>` +
            svgText(38, y + 26, `${number} / ${title}`, "micro") +
            svgText(1160, y + 26, note, "small", 'text-anchor="end"');
        let body = band(124, 192, "A", text(lang, "BUILD TIME", "编译期"),
            text(lang, "Describe only; business SQL executes later at runtime", "只描述查询；业务 SQL 稍后在运行期执行"));
        body += card(38, 170, 218, 67, "prepare (optional)", "cargo sqlx-doris", { attributes: 'data-for="checked"' });
        body += card(300, 170, 184, 67, "Cargo build", "application crate", { attributes: 'data-for="checked"' });
        body += card(544, 170, 246, 67, "sqlx_doris::query!", "proc-macro", { attributes: 'data-for="checked"' });
        body += card(872, 163, 288, 59, "Doris FE", "online: describe", { attributes: 'data-for="checked"' });
        body += card(872, 244, 288, 51, text(lang, "Offline metadata", "离线元数据"), "SQLX_OFFLINE_DIR", { attributes: 'data-for="checked"' });
        body += line(id, "control", "M256 201H300");
        body += line(id, "control", "M484 201H544");
        body += line(id, "metadata", "M790 192H872", "online", 830, 181);
        body += line(id, "metadata", "M790 213H825V270H872", "or offline", 816, 296);
        body += svgText(38, 279, text(lang,
            "prepare invokes the application build; it does not enable the application’s macros feature.",
            "prepare 调用应用构建；不会代替应用开启 macros 特性。"), "small");

        body += band(332, 292, "B", text(lang, "APPLICATION RUNTIME", "应用运行期"),
            text(lang, "Queries return rows; ingestion returns a business receipt", "查询返回结果行；导入返回业务回执"));
        body += card(38, 384, 218, 72, text(lang, "Application", "应用"), "queries / binds", { attributes: 'data-for="query migrate helpers checked"' });
        body += card(300, 384, 218, 72, "sqlx::query*", "DorisPool / Executor", { attributes: 'data-for="query migrate helpers checked"' });
        body += card(578, 384, 234, 72, "Doris core", "wire / types / values", { attributes: 'data-for="query migrate helpers checked"' });
        body += card(900, 384, 260, 72, "Doris FE", "SQL endpoint", { attributes: 'data-for="query migrate helpers checked"' });
        for (const [from, to] of [[256, 300], [518, 578], [812, 900]]) {
            body += line(id, "execute", `M${from} 412H${to}`);
            body += line(id, "execute", `M${to} 438H${from}`);
        }
        body += svgText(856, 400, "SQL →", "small", 'text-anchor="middle"');
        body += svgText(856, 455, "← rows", "small", 'text-anchor="middle"');
        body += card(38, 512, 218, 72, text(lang, "Application batch", "应用批次"), "label + stable bytes", { attributes: 'data-for="ingest"' });
        body += card(300, 512, 218, 72, "StreamLoadClient", "HTTP ingestion", { attributes: 'data-for="ingest"' });
        body += card(578, 512, 234, 72, "Doris FE", "HTTP routing", { attributes: 'data-for="ingest"' });
        body += card(900, 512, 260, 72, "Doris BE", "validated load target", { attributes: 'data-for="ingest"' });
        body += line(id, "data", "M256 541H300");
        body += line(id, "control", "M518 541H578", "PUT", 548, 529);
        body += line(id, "control", "M578 565H518", "307", 548, 582);
        body += line(id, "data", "M409 584V604H1030V584", text(lang,
            "client upload → verified BE; FE does not relay the payload",
            "客户端上传 → 已验证 BE；FE 不转发 payload"), 734, 618);

        body += band(640, 184, "C", text(lang, "EXPLICIT TOOL CONTROL", "显式工具控制"),
            text(lang, "No automatic takeover or offset movement", "不自动接管或移动 offset"));
        body += card(38, 685, 266, 72, "cargo sqlx-doris", "migrate / admin", { attributes: 'data-for="migrate diagnose"' });
        body += card(390, 685, 310, 72, text(lang, "Runner / guarded ops", "Runner / 受控 ops"), "owner / history / scope", { attributes: 'data-for="migrate diagnose"' });
        body += card(820, 685, 340, 72, "Doris endpoints", "SQL + scoped maintenance HTTP", { attributes: 'data-for="migrate diagnose"' });
        body += line(id, "control", "M304 721H390");
        body += line(id, "control", "M700 721H820", "explicit", 760, 707);
        body += svgText(38, 798, text(lang,
            "Kafka preflight is a separate direct broker observation (cli-admin-kafka); it does not relay through Doris or reset offsets.",
            "Kafka 预检独立直连 broker（cli-admin-kafka）；不经 Doris 转发，也不重置 offset。"), "small");
        body += legend(id, 32, 858, "execute", text(lang, "SQL execution / rows", "SQL 执行 / 结果行"));
        body += legend(id, 328, 858, "data", text(lang, "HTTP batch data", "HTTP 批次数据"));
        body += legend(id, 608, 858, "control", text(lang, "Control / orchestration", "控制 / 编排"));
        body += legend(id, 928, 858, "metadata", text(lang, "Build metadata", "编译元数据"));
        return frame(lang, id, 888,
            text(lang, "Separate control, execution and data.", "控制、执行与数据，各走其路。"),
            text(lang, "Logical request paths; server internals and task duration are intentionally not simulated", "逻辑请求路径；不模拟服务端内部执行或任务耗时"), body);
    }

    function packageDiagram(lang = "en", selected = "") {
        const id = "packages";
        const places = {
            facade: [450, 146, 300, 88], macros: [52, 338, 310, 92],
            cli: [838, 338, 310, 92], ops: [450, 492, 300, 92], core: [450, 690, 300, 92]
        };
        const paths = [
            ["facade", "core", "M530 234V278H410V717H450", "default", 409, 642],
            ["facade", "macros", "M450 190H207V338", "macros", 207, 291],
            ["facade", "ops", "M640 234V492", "operations / stream-load", 640, 466],
            ["facade", "cli", "M750 190H993V338", "cli / cli-admin / cli-admin-kafka", 971, 291],
            ["macros", "core", "M207 430V748H450", "query", 275, 748],
            ["ops", "core", "M600 584V690", "always", 641, 644],
            ["cli", "core", "M993 430V748H750", "always", 899, 748],
            ["cli", "ops", "M838 384H798V536H750", "cli-support", 798, 466]
        ];
        const active = model.tasks.find((task) => task.id === selected)?.packages || [];
        let body = `<rect x="24" y="124" width="1152" height="690" rx="12" class="panel"/>`;
        body += svgText(44, 154, text(lang, "FIVE DORIS PACKAGES", "五个 Doris 包"), "micro");
        body += svgText(1152, 154, text(lang, "Arrow = depends on", "箭头 = 依赖于"), "small", 'text-anchor="end"');
        for (const [from, to, path, label, x, y] of paths) {
            const dependency = model.dependencies.find((edge) => edge.from === from && edge.to === to);
            const hot = active.includes(from) && active.includes(to);
            body += `<g data-edge="${from}:${to}" data-optional="${dependency.optional}" class="${active.length && !hot ? "dim" : ""}">`;
            body += line(id, "dependency", path, label, x, y,
                `style="${dependency.optional ? "stroke-dasharray:5 5;" : ""}${hot ? "stroke:var(--cyan);stroke-width:2.4" : ""}"`);
            body += "</g>";
        }
        for (const item of model.packages) {
            const [x, y, width, height] = places[item.id];
            const role = local(item.role, lang);
            body += card(x, y, width, height, item.name, role, {
                eyebrow: item.id === "facade" ? text(lang, "APPLICATION ENTRY", "应用公开入口") : item.id.toUpperCase(),
                attributes: `data-package="${item.id}" role="button" tabindex="0" aria-label="${escape(item.name)}"`,
                selected: active.includes(item.id)
            });
            // Seated module connectors: each contact ends at its socket outline.
            for (let pin = 0; pin < 5; pin++) {
                body += `<rect x="${x + 20 + pin * 11}" y="${y + height + 4}" width="5" height="8" rx="1" fill="var(--faint)"/>`;
            }
        }
        body += svgText(600, 805, text(lang,
            "Core owns the Doris identity. Internal packages never depend on the facade.",
            "core 拥有唯一 Doris 身份，内部包不反向依赖公开入口。"), "small", 'text-anchor="middle"');
        body += legend(id, 32, 852, "dependency", text(lang, "Required package dependency", "必需的包依赖"));
        body += `<path d="M416 848H450" class="flow dependency optional" marker-end="url(#${id}-dependency)"/>`;
        body += svgText(462, 852, text(lang, "Optional dependency (feature gated)", "可选依赖（受 feature 门控）"), "small");
        body += svgText(32, 886, text(lang,
            "Potential package topology ≠ execution order. Shared upstream sqlx-core supplies generic traits; it is not a sixth Doris package.",
            "潜在包依赖图 ≠ 执行顺序。共用上游 sqlx-core 提供通用 traits，不是第六个 Doris 包。"), "small");
        return frame(lang, id, 916,
            text(lang, "One public entry. Explicit internal boundaries.", "一个公开入口，明确的内部边界。"),
            text(lang, "The highlighted packages support the selected task; the full graph shows potential dependencies, not a Cargo build trace", "高亮包服务于所选任务；全图展示潜在依赖关系，不代表一次 Cargo 构建轨迹"), body);
    }

    const diagrams = { tasks: taskDiagram, integration: integrationDiagram, packages: packageDiagram };
    globalThis.DORIS_ATLAS = { diagrams, guide };
    if (typeof document === "undefined") return;

    const lang = document.documentElement.lang.startsWith("zh") ? "zh" : "en";
    const $ = (selector) => document.querySelector(selector);
    const words = {
        en: {
            none: "No Doris feature needed", feature: "Doris feature", upstream: "Upstream SQLx",
            path: "Execution / control path", boundary: "Contract to keep", copy: "Copy feature list",
            copied: "Copied", copyFailed: "Select and copy the feature list below", diagram: "Diagram",
            package: "Package responsibility", topology: "All edges remain visible. Highlighting is task context, not a resolved Cargo graph."
        },
        zh: {
            none: "无需开启 Doris 特性", feature: "Doris 特性", upstream: "上游 SQLx",
            path: "执行 / 控制路径", boundary: "需要保留的契约", copy: "复制特性列表",
            copied: "已复制", copyFailed: "请选中下方特性列表复制", diagram: "图示",
            package: "包的职责", topology: "保留全部依赖边。高亮仅表达任务上下文，不是 Cargo 已解析依赖图。"
        }
    }[lang];
    const params = new URLSearchParams(location.hash.slice(1));
    let taskId = model.tasks.some((item) => item.id === params.get("task")) ? params.get("task") : "query";
    let view = Object.hasOwn(diagrams, params.get("view")) ? params.get("view") : "tasks";
    let focusedPackage = "";
    let theme = "auto";
    let fit = false;

    function updateLocation() {
        const hash = new URLSearchParams({ view, task: taskId }).toString();
        // Replace only the fragment; file:// and privacy-restricted contexts also work.
        try { history.replaceState(null, "", `#${hash}`); } catch { location.hash = hash; }
        $("[data-language]").href = `${lang === "zh" ? "ATLAS.html" : "ATLAS.zh-CN.html"}#${hash}`;
    }

    function render() {
        const task = model.tasks.find((item) => item.id === taskId);
        $("[data-canvas]").innerHTML = diagrams[view](lang, taskId);
        if (view === "tasks") {
            $("[data-canvas]").insertAdjacentHTML("beforeend", `<div class="mobile-map">${model.tasks.map((item) =>
                `<button type="button" data-task="${item.id}" aria-pressed="${item.id === taskId}">
                    <strong><span>${item.number}</span>${escape(local(item.title, lang))}</strong>
                    <code>${escape(item.api)}</code>
                    <span class="mobile-feature">${escape(item.features.join(" + ") || words.none)}</span>
                </button>`
            ).join("")}</div>`);
        }
        $("[data-canvas]").dataset.currentView = view;
        $("[data-canvas]").dataset.fit = String(fit);
        $("[data-fit]").hidden = view === "tasks";
        $("[data-fit]").setAttribute("aria-pressed", String(fit));
        $("[data-fit]").textContent = fit ? text(lang, "Reading size", "清晰阅读") : text(lang, "Fit diagram", "全图总览");
        $("#diagram").setAttribute("aria-labelledby", `tab-${view}`);
        $("[data-canvas] svg").setAttribute("data-theme", theme);
        if (view !== "integration") $("[data-canvas] svg").setAttribute("role", "group");
        document.querySelectorAll("[data-for]").forEach((node) => {
            node.classList.toggle("selected", node.dataset.for.split(" ").includes(taskId));
        });
        document.querySelectorAll("[data-view-choice]").forEach((button) => {
            const active = button.dataset.viewChoice === view;
            button.setAttribute("aria-selected", String(active));
            button.tabIndex = active ? 0 : -1;
        });
        document.querySelectorAll("[data-task-choice]").forEach((button) => {
            button.setAttribute("aria-pressed", String(button.dataset.taskChoice === taskId));
        });
        $("[data-current-number]").textContent = task.number;
        $("[data-current-title]").textContent = local(task.title, lang);
        $("[data-current-description]").textContent = local(task.description, lang);
        $("[data-current-api]").textContent = task.api;
        $("[data-current-feature]").textContent = task.features.length ? task.features.join(" + ") : words.none;
        $("[data-copy-value]").textContent = `features = ${JSON.stringify(task.features)}`;
        $("[data-current-upstream]").textContent = local(task.upstream, lang);
        $("[data-current-flow]").textContent = local(task.flow, lang);
        $("[data-current-boundary]").textContent = local(task.boundary, lang);
        $("[data-guide-links]").innerHTML = task.guides.map((item) =>
            `<a href="${guide(item, lang)}">${escape(local(item.label, lang))}<span aria-hidden="true"> ↗</span></a>`
        ).join("");
        $("[data-topology-note]").hidden = view !== "packages";
        $("[data-copy]").textContent = words.copy;
        $("[data-copy-feedback]").textContent = "";
        $("[data-package-detail]").hidden = !focusedPackage;
        if (focusedPackage) {
            const item = model.packages.find((entry) => entry.id === focusedPackage);
            $("[data-package-detail]").textContent = `${item.name} — ${local(item.role, lang)}`;
        }
        $("[data-announcement]").textContent = `${local(task.title, lang)} · ${$("[aria-selected=true]").textContent.trim()}`;
        updateLocation();
    }

    $("[data-task-list]").innerHTML = model.tasks.map((task) =>
        `<button type="button" data-task-choice="${task.id}" aria-pressed="false"><span class="task-number">${task.number}</span><span>${escape(local(task.title, lang))}</span><span class="task-chevron" aria-hidden="true">↗</span></button>`
    ).join("");
    $("[data-atlas]").addEventListener("click", (event) => {
        const choice = event.target.closest("[data-task-choice], [data-task]");
        const tab = event.target.closest("[data-view-choice]");
        const item = event.target.closest("[data-package]");
        if (choice) {
            taskId = choice.dataset.taskChoice || choice.dataset.task;
            focusedPackage = "";
            render();
            if (choice.hasAttribute("data-task")) $("[data-current-title]").focus({ preventScroll: true });
        } else if (tab) {
            view = tab.dataset.viewChoice;
            focusedPackage = "";
            render();
        } else if (item) {
            focusedPackage = item.dataset.package;
            const pack = model.packages.find((entry) => entry.id === focusedPackage);
            $("[data-package-detail]").hidden = false;
            $("[data-package-detail]").textContent = `${pack.name} — ${local(pack.role, lang)}`;
        }
    });
    $("[data-tabs]").addEventListener("keydown", (event) => {
        if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
        event.preventDefault();
        const keys = Object.keys(diagrams);
        const delta = event.key === "ArrowLeft" ? -1 : 1;
        const next = event.key === "Home" ? 0 : event.key === "End" ? keys.length - 1 : (keys.indexOf(view) + delta + keys.length) % keys.length;
        view = keys[next];
        focusedPackage = "";
        render();
        $(`[data-view-choice="${view}"]`).focus();
    });
    $("[data-canvas]").addEventListener("keydown", (event) => {
        if ((event.key === "Enter" || event.key === " ") && event.target.matches("[data-task], [data-package]")) {
            event.preventDefault();
            event.target.dispatchEvent(new MouseEvent("click", { bubbles: true }));
        }
    });
    $("[data-theme-choice]").addEventListener("click", (event) => {
        theme = theme === "dark" ? "light" : theme === "light" ? "auto" : "dark";
        document.documentElement.dataset.theme = theme;
        event.currentTarget.textContent = text(lang, `Theme: ${theme}`, `主题：${{ auto: "跟随系统", dark: "深色", light: "浅色" }[theme]}`);
        $("[data-canvas] svg").setAttribute("data-theme", theme);
    });
    $("[data-fit]").addEventListener("click", () => {
        fit = !fit;
        render();
    });
    $("[data-copy]").addEventListener("click", async () => {
        const value = $("[data-copy-value]").textContent;
        try {
            await navigator.clipboard.writeText(value);
            $("[data-copy-feedback]").textContent = words.copied;
        } catch {
            const range = document.createRange();
            range.selectNodeContents($("[data-copy-value]"));
            const selection = getSelection();
            selection.removeAllRanges();
            selection.addRange(range);
            $("[data-copy-feedback]").textContent = words.copyFailed;
        }
    });
    window.addEventListener("hashchange", () => {
        const next = new URLSearchParams(location.hash.slice(1));
        if (model.tasks.some((item) => item.id === next.get("task"))) taskId = next.get("task");
        if (Object.hasOwn(diagrams, next.get("view"))) view = next.get("view");
        focusedPackage = "";
        render();
    });
    $("[data-atlas]").hidden = false;
    $("[data-loading]").hidden = true;
    render();
}());
