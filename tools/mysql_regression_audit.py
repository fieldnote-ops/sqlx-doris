"""Fail closed when either independent MySQL test executable omits a contract."""

import re


def verify_log(text, expected, official):
    cases = re.findall(r"^test ([a-zA-Z0-9_]+) \.\.\. (ok|FAILED|ignored)$", text, re.M)
    names = [name for name, _ in cases]
    if len(set(names)) != len(names) or set(names) != set(expected):
        raise ValueError("missing, duplicate or unexpected MySQL regression cases")
    if any(status != "ok" for _, status in cases):
        raise ValueError("MySQL contract failed or was ignored")
    summaries = re.findall(
        r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;",
        text,
        re.M,
    )
    if summaries != [(str(len(expected)), "0", "0", "0", "0")]:
        raise ValueError("full MySQL execution counts required")
    markers = re.findall(
        r"^MYSQL_REGRESSION_IDENTITY official=(true|false)$", text, re.M
    )
    if markers != ["true" if official else "false"]:
        raise ValueError("compiled MySQL regression identity differs")
    return {name: status for name, status in cases}
