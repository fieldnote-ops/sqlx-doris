"""Contracts for the English and Simplified Chinese user guides."""

from dataclasses import dataclass
from pathlib import Path
import re
import tomllib
from urllib.parse import unquote, urlsplit

GUIDES = (
    "README",
    "INSTALLATION",
    "USAGE",
    "INGESTION",
    "TROUBLESHOOTING",
    "MIGRATIONS",
    "RECOVERY",
    "ROUTINE_LOAD",
    "KAFKA",
    "CLI",
    "COMPATIBILITY",
    "TESTING",
    "DEPENDENCIES",
    "PERFORMANCE",
    "ARCHITECTURE",
)
PACKAGES = (
    "sqlx-doris",
    "sqlx-doris-core",
    "sqlx-doris-macros",
    "sqlx-doris-ops",
    "sqlx-doris-cli",
)


class DocumentationError(ValueError):
    """An actionable documentation contract failure."""


@dataclass
class Page:
    text: str
    sections: list[tuple[int, str | None]]
    anchors: set[str]
    code: list[tuple[str, str]]
    links: list[str]
    tables: list[list[str]]


def parse(text: str) -> Page:
    sections = []
    anchors = set()
    code = []
    prose = []
    pending_anchor = None
    language = None
    body = []
    tables = []
    table = []
    for line in text.splitlines():
        if line.startswith("```"):
            if language is None:
                language = line[3:].strip()
                if not language:
                    raise DocumentationError("Code fences must declare a language")
            else:
                if line.strip() != "```":
                    raise DocumentationError("Unexpected nested code fence")
                code.append((language, "\n".join(body)))
                language = None
                body = []
            continue
        if language is not None:
            body.append(line)
            continue
        prose.append(line)
        if match := re.fullmatch(r'<a name="(doc-[a-z0-9-]+)"></a>', line):
            anchor = match[1]
            if anchor in anchors:
                raise DocumentationError(f"Duplicate anchor: {anchor}")
            anchors.add(anchor)
            pending_anchor = anchor
        elif match := re.match(r"^(#{1,6})\s+", line):
            level = len(match[1])
            if level >= 2 and pending_anchor is None:
                raise DocumentationError(f"Heading needs a stable doc- anchor: {line}")
            sections.append((level, pending_anchor))
            pending_anchor = None
        elif line.strip():
            pending_anchor = None
        if line.startswith("|"):
            table.append(line)
        elif table:
            tables.append(table)
            table = []
    if language is not None:
        raise DocumentationError("Unclosed code fence")
    if table:
        tables.append(table)
    links = re.findall(r"!?\[[^\]\n]*\]\(([^)\n]+)\)", "\n".join(prose))
    return Page(text, sections, anchors, code, links, tables)


def pairs(root: Path) -> list[tuple[Path, Path]]:
    bases = [root / "README.md", root / "CHANGELOG.md"]
    bases.extend(root / "docs" / f"{name}.md" for name in GUIDES)
    bases.extend(root / "crates" / package / "README.md" for package in PACKAGES)
    return [(base, base.with_name(base.stem + ".zh-CN.md")) for base in bases]


def validate_pair(english: Path, chinese: Path) -> tuple[Page, Page]:
    if not english.is_file() or not chinese.is_file():
        raise DocumentationError(f"Missing bilingual pair: {english}, {chinese}")
    en, zh = parse(english.read_text()), parse(chinese.read_text())
    switch = f"[English]({english.name}) | [简体中文]({chinese.name})"
    for path, page in ((english, en), (chinese, zh)):
        if switch not in "\n".join(page.text.splitlines()[:8]):
            raise DocumentationError(f"Missing reciprocal language switch: {path}")
    if en.sections != zh.sections:
        raise DocumentationError(f"Bilingual section IDs/order differ: {english}")
    if en.code != zh.code:
        raise DocumentationError(f"Bilingual code/commands differ: {english}")
    if [len(table) for table in en.tables] != [len(table) for table in zh.tables]:
        raise DocumentationError(f"Bilingual table coverage differs: {english}")
    for en_table, zh_table in zip(en.tables, zh.tables):
        numeric = lambda table: [
            re.findall(r"(?<![A-Za-z_])\d+(?:\.\d+)?", row) for row in table
        ]
        if numeric(en_table) != numeric(zh_table):
            raise DocumentationError(
                f"Bilingual numeric table values differ: {english}"
            )
    return en, zh


def validate_link(root: Path, source: Path, target: str, pages: dict[Path, Page]):
    url = urlsplit(target)
    if url.scheme:
        if url.scheme not in ("https", "http", "mailto"):
            raise DocumentationError(
                f"Unsupported documentation URL: {source}: {target}"
            )
        return
    destination = (source.parent / unquote(url.path)).resolve() if url.path else source
    if not destination.is_relative_to(root) or not destination.is_file():
        raise DocumentationError(f"Broken/escaping local link: {source}: {target}")
    if destination.suffix == ".md" and destination not in pages:
        raise DocumentationError(f"Link is outside the user guides: {source}: {target}")
    if url.fragment:
        if (
            destination not in pages
            or unquote(url.fragment) not in pages[destination].anchors
        ):
            raise DocumentationError(f"Broken stable anchor: {source}: {target}")
    # A language switch is the only link allowed to cross document locales.
    if destination in pages and source.name.endswith(
        ".zh-CN.md"
    ) != destination.name.endswith(".zh-CN.md"):
        counterpart = (
            source.with_name(source.name.replace(".zh-CN.md", ".md"))
            if source.name.endswith(".zh-CN.md")
            else source.with_name(source.stem + ".zh-CN.md")
        )
        if destination != counterpart:
            raise DocumentationError(f"Cross-language navigation: {source}: {target}")


def validate(root: Path) -> dict:
    root = root.resolve()
    expected_pairs = pairs(root)
    expected = {p for pair in expected_pairs for p in pair}
    actual = {p for p in root.rglob("*.md") if ".git" not in p.parts}
    if extras := actual - expected - {root / "AGENTS.md"}:
        raise DocumentationError(f"Unexpected/non-user documentation: {sorted(extras)}")
    pages = {}
    for en_path, zh_path in expected_pairs:
        en, zh = validate_pair(en_path, zh_path)
        pages[en_path] = en
        pages[zh_path] = zh
    for source, page in pages.items():
        for link in page.links:
            validate_link(root, source, link, pages)
    manifest = tomllib.loads((root / "Cargo.toml").read_text())
    sqlx_version = manifest["workspace"]["dependencies"]["sqlx"]["version"]
    facade = tomllib.loads((root / "crates/sqlx-doris/Cargo.toml").read_text())
    for package in PACKAGES:
        metadata = tomllib.loads((root / "crates" / package / "Cargo.toml").read_text())
        for filename in ("README.md", "README.zh-CN.md"):
            if (
                metadata["package"]["version"]
                not in pages[root / "crates" / package / filename].text
            ):
                raise DocumentationError(
                    f"README version differs from manifest: {package}/{filename}"
                )
        if metadata["package"].get("readme") != "README.md":
            raise DocumentationError(
                f"Package README must default to English: {package}"
            )
        if not {"README.md", "README.zh-CN.md"}.issubset(
            metadata["package"].get("include", [])
        ):
            raise DocumentationError(
                f"Package must include both README languages: {package}"
            )
    for path, page in pages.items():
        for language, code in page.code:
            if language != "toml":
                continue
            dependencies = tomllib.loads(code).get("dependencies", {})
            if "sqlx" not in dependencies or "sqlx-doris" not in dependencies:
                raise DocumentationError(
                    f"Cargo example must have an explicit dependencies section: {path}"
                )
            upstream = dependencies["sqlx"]
            if (
                upstream.get("version") != sqlx_version
                or upstream.get("default-features") is not False
            ):
                raise DocumentationError(
                    f"SQLx example version/default features differ: {path}"
                )
            if set(upstream.get("features", [])).intersection(
                ("mysql", "postgres", "sqlite")
            ):
                raise DocumentationError(f"Other database features in example: {path}")
            if (
                set(dependencies["sqlx-doris"].get("features", []))
                - facade["features"].keys()
            ):
                raise DocumentationError(f"Unknown Doris feature in example: {path}")
    quick = (root / "crates/sqlx-doris/examples/query.rs").read_text().strip()
    for name in (
        "README.md",
        "README.zh-CN.md",
        "crates/sqlx-doris/README.md",
        "crates/sqlx-doris/README.zh-CN.md",
    ):
        if ("rust", quick) not in pages[root / name].code:
            raise DocumentationError(f"Quickstart drift from compiled example: {name}")
    from _atlas import validate_atlas

    validate_atlas(root, pages)
    return dict(
        pairs=len(expected_pairs),
        pages=len(pages),
        code_blocks=sum(len(p.code) for p in pages.values()),
    )
