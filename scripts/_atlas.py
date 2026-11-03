"""Validate documentation topology against Cargo, and local bilingual assets."""

import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import tomllib
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET


class HtmlLinks(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self.ids = set()
        self.lang = None
        self.structure = []
        self.references = []

    def handle_starttag(self, tag, attributes):
        values = dict(attributes)
        stable = {
            key: (value.replace(".zh-CN", "") if value is not None else None)
            for key, value in attributes
            if key
            in (
                "id",
                "class",
                "role",
                "aria-controls",
                "aria-labelledby",
                "aria-selected",
                "tabindex",
                "hidden",
                "type",
                "rel",
                "href",
                "src",
            )
            or key.startswith("data-")
        }
        self.structure.append((tag, stable))
        for key in ("aria-controls", "aria-labelledby"):
            self.references.extend(values.get(key, "").split())
        if tag == "html":
            self.lang = values.get("lang")
        if "id" in values:
            if values["id"] in self.ids:
                raise ValueError(f"Duplicate atlas HTML id: {values['id']}")
            self.ids.add(values["id"])
        for attribute in ("href", "src"):
            if attribute in values:
                self.links.append(values[attribute])


def validate_atlas(root: Path, pages):
    assets = root / "docs/assets"
    data = (assets / "atlas-data.js").read_text()
    match = re.search(r"globalThis\.DORIS_ATLAS_DATA\s*=\s*(\{.*\});\s*$", data, re.S)
    if not match:
        raise ValueError("Atlas model must be one JSON-compatible assignment")
    model = json.loads(match[1])
    packages = {item["id"]: item["name"] for item in model["packages"]}
    expected_members = {f"crates/{name}" for name in packages.values()}
    workspace = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]
    if expected_members != set(workspace["members"]) or len(packages) != 5:
        raise ValueError("Atlas five-package model differs from the workspace")
    manifests = {
        key: tomllib.loads((root / "crates" / name / "Cargo.toml").read_text())
        for key, name in packages.items()
    }
    if model["features"] != manifests["facade"]["features"]:
        raise ValueError("Atlas feature activation differs from the public manifest")
    actual = {
        (source, target, specification.get("optional", False))
        for source, manifest in manifests.items()
        for target, package in packages.items()
        if (specification := manifest.get("dependencies", {}).get(package))
    }
    documented = {
        (edge["from"], edge["to"], edge["optional"]) for edge in model["dependencies"]
    }
    if actual != documented or len(documented) != len(model["dependencies"]):
        raise ValueError("Atlas required/optional dependency arrows differ from Cargo")
    tasks = model["tasks"]
    if len(tasks) != 6 or len({task["id"] for task in tasks}) != 6:
        raise ValueError("Atlas needs six distinct task routes")
    for task in tasks:
        if set(task["features"]) - model["features"].keys():
            raise ValueError(f"Unknown atlas feature: {task['id']}")
        if set(task["packages"]) - packages.keys():
            raise ValueError(f"Unknown atlas package: {task['id']}")
        for locale, suffix in (("en", ""), ("zh", ".zh-CN")):
            for field in ("title", "description", "upstream", "flow", "boundary"):
                if not task[field].get(locale):
                    raise ValueError(f"Missing atlas translation: {task['id']}/{field}")
            for link in task["guides"]:
                destination = root / "docs" / f"{link['name']}{suffix}.md"
                if (
                    destination not in pages
                    or link["anchor"] not in pages[destination].anchors
                ):
                    raise ValueError(
                        f"Broken atlas guide: {destination}#{link['anchor']}"
                    )
    digest = hashlib.sha256(
        (assets / "atlas-data.js").read_bytes() + (assets / "atlas.js").read_bytes()
    ).hexdigest()
    structures = []
    for suffix, locale in (("", "en"), (".zh-CN", "zh")):
        html = root / "docs" / f"ATLAS{suffix}.html"
        parsed = HtmlLinks()
        parsed.feed(html.read_text())
        structures.append(parsed.structure)
        if set(parsed.references) - parsed.ids:
            raise ValueError(f"Broken atlas accessibility relationship: {html}")
        if parsed.lang != ("en" if locale == "en" else "zh-CN"):
            raise ValueError(f"Wrong atlas language: {html}")
        for link in parsed.links:
            url = urlsplit(link)
            if url.scheme or url.netloc:
                raise ValueError(f"Atlas must be self-contained: {link}")
            destination = (
                (html.parent / unquote(url.path)).resolve() if url.path else html
            )
            if not destination.is_relative_to(root) or not destination.is_file():
                raise ValueError(f"Broken/escaping atlas link: {link}")
            if url.fragment and destination == html and url.fragment not in parsed.ids:
                raise ValueError(f"Broken atlas anchor: {link}")
            if destination in pages and destination.name.endswith(".zh-CN.md") != bool(
                suffix
            ):
                raise ValueError(f"Cross-language atlas guide: {link}")
        for name in ("task-map", "integration", "packages"):
            svg = ET.parse(assets / f"{name}{suffix}.svg").getroot()
            if (
                svg.get("data-source-sha256") != digest
                or svg.get("data-locale") != locale
            ):
                raise ValueError(f"Stale paired SVG asset: {name}{suffix}")
            if svg.get("role") != "img" or not svg.get("viewBox"):
                raise ValueError(f"Missing accessible scalable SVG: {name}{suffix}")
            if name == "packages":
                rendered = {
                    (
                        *element.get("data-edge").split(":"),
                        element.get("data-optional") == "true",
                    )
                    for element in svg.iter()
                    if element.get("data-edge")
                }
                if rendered != actual:
                    raise ValueError(
                        f"Rendered dependency arrows differ from Cargo: {suffix}"
                    )
                rendered_packages = {
                    element.get("data-package")
                    for element in svg.iter()
                    if element.get("data-package")
                }
                if rendered_packages != packages.keys():
                    raise ValueError(
                        f"Rendered package nodes differ from Cargo: {suffix}"
                    )
    if structures[0] != structures[1]:
        raise ValueError("English/Chinese atlas controls or document structure differ")
    return {"views": 3, "svg_assets": 6, "tasks": 6, "dependency_arrows": 8}
