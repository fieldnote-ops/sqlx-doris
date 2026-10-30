import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from mysql_regression_audit import verify_log


class MySqlRegressionAudit(unittest.TestCase):
    def sample(self):
        return (
            "test one ... ok\ntest two ... ok\ntest result: ok. 2 passed; 0 failed; 0 "
            "ignored; 0 measured; 0 filtered out; finished in "
            "1s\nMYSQL_REGRESSION_IDENTITY official=true\n"
        )

    def test_complete_two_case_execution(self):
        self.assertEqual(
            verify_log(self.sample(), ["one", "two"], True), {"one": "ok", "two": "ok"}
        )

    def test_missing_duplicate_unexpected_cases(self):
        for text in [
            self.sample().replace("test two ... ok\n", ""),
            self.sample() + "test one ... ok\n",
            self.sample().replace("test two", "test three"),
        ]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                verify_log(text, ["one", "two"], True)

    def test_failed_or_ignored_is_not_success(self):
        for status in ["FAILED", "ignored"]:
            with self.subTest(status=status), self.assertRaises(ValueError):
                verify_log(
                    self.sample().replace("test two ... ok", "test two ... " + status),
                    ["one", "two"],
                    True,
                )

    def test_summary_must_prove_full_execution(self):
        for text in [
            self.sample().replace("2 passed", "1 passed"),
            self.sample().replace("0 filtered out", "1 filtered out"),
            self.sample().replace("0 failed", "1 failed"),
        ]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                verify_log(text, ["one", "two"], True)

    def test_identity_is_unique_and_source_specific(self):
        for text in [
            self.sample().replace("official=true", "official=false"),
            self.sample().replace("MYSQL_REGRESSION_IDENTITY official=true\n", ""),
            self.sample() + "MYSQL_REGRESSION_IDENTITY official=true\n",
        ]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                verify_log(text, ["one", "two"], True)
