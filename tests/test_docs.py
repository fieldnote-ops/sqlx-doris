"""Guard bilingual navigation, executable examples and contractual table values."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from _docs import DocumentationError, parse, validate_link, validate_pair


class DocumentationContracts(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="doris-doc-contract-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.en = self.root / "README.md"
        self.zh = self.root / "README.zh-CN.md"
        self.text = """# Guide

[English](README.md) | [简体中文](README.zh-CN.md)

<a name="doc-query"></a>

## Query

```rust
fn main() {}
```

| Exit | Meaning |
| --- | --- |
| 0 | Completed within scope |
"""
        self.en.write_text(self.text)
        self.zh.write_text(self.text.replace("## Query", "## 查询"))

    def test_translated_headings_share_stable_sections_and_examples(self):
        validate_pair(self.en, self.zh)

    def test_missing_translation_fails(self):
        self.zh.unlink()
        with self.assertRaisesRegex(DocumentationError, "Missing bilingual pair"):
            validate_pair(self.en, self.zh)

    def test_changed_command_or_sql_in_translation_fails(self):
        self.zh.write_text(self.zh.read_text().replace("fn main() {}", "fn other() {}"))
        with self.assertRaisesRegex(DocumentationError, "code/commands differ"):
            validate_pair(self.en, self.zh)

    def test_missing_section_or_anchor_drift_fails(self):
        self.zh.write_text(self.zh.read_text().replace("doc-query", "doc-other"))
        with self.assertRaisesRegex(DocumentationError, "section IDs/order differ"):
            validate_pair(self.en, self.zh)

    def test_duplicate_anchor_and_unclosed_code_fence_fail(self):
        for text, message in [
            (self.text + '<a name="doc-query"></a>\n', "Duplicate anchor"),
            (self.text + "```sql\nSELECT 1\n", "Unclosed code fence"),
        ]:
            with self.subTest(message=message):
                with self.assertRaisesRegex(DocumentationError, message):
                    parse(text)

    def test_numeric_contract_or_measurement_drift_fails(self):
        self.zh.write_text(self.zh.read_text().replace("| 0 |", "| 7 |"))
        with self.assertRaisesRegex(DocumentationError, "numeric table values differ"):
            validate_pair(self.en, self.zh)

    def test_language_switch_is_the_only_cross_locale_link(self):
        other = self.root / "OTHER.md"
        other.write_text(self.text)
        pages = {p: parse(p.read_text()) for p in (self.en, self.zh, other)}
        validate_link(self.root, self.zh, "README.md", pages)
        with self.assertRaisesRegex(DocumentationError, "Cross-language navigation"):
            validate_link(self.root, self.zh, "OTHER.md", pages)

    def test_broken_fragment_and_escape_fail(self):
        pages = {self.en: parse(self.text)}
        validate_link(self.root, self.en, "#doc-query", pages)
        for target in ("#missing", "missing.md", "../README.md"):
            with self.subTest(target=target):
                with self.assertRaises(DocumentationError):
                    validate_link(self.root, self.en, target, pages)

    def test_code_samples_are_not_interpreted_as_markdown_links(self):
        text = self.text.replace(
            "fn main() {}", 'fn main() { let _ = "[x](missing.md)"; }'
        )
        self.assertNotIn("missing.md", parse(text).links)
