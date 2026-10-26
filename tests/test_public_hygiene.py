"""The repository addresses a stranger who cloned it: no real home path, no real account id, and none of the names a
maintainer keeps out of it.

The names themselves are not in the repository, since listing them would publish them. A maintainer keeps them in
`.private-names` at the repository root (Git ignores it): one regular expression per line, `#` starts a comment.
Without that file only the generic rules apply. Every file Git tracks is scanned; binary files are skipped.
"""

import re
import subprocess  # nosec B404
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# Account ids the AWS documentation uses as placeholders.
PLACEHOLDER_ACCOUNTS = {"111122223333", "444455556666", "123456789012"}
GENERIC = {
    "home path": re.compile(r"/(?:home|Users)/(?!me\b|user\b|ec2-user\b)[a-z][\w.-]*", re.IGNORECASE),
    "account id": re.compile(r"(?<![\w-])\d{12}(?![\w-])"),
}
# Files that must match a rule, and why.
ALLOWED = {"tests/test_public_hygiene.py": "its own examples of what the rules match"}


def private_names(path: Path) -> re.Pattern | None:
    """The maintainer's names in `path`, as one case-insensitive pattern; None when there is no such file."""
    if not path.exists():
        return None
    parts = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    parts = [p for p in parts if p and not p.startswith("#")]
    return re.compile("|".join(f"(?:{p})" for p in parts), re.IGNORECASE) if parts else None


def tracked_files() -> list[str]:
    """Every file Git tracks (and would track: new files not yet added, outside .gitignore)."""
    out = subprocess.run(  # nosec B603 B607
        ["git", "-C", str(ROOT), "ls-files", "--cached", "--others", "--exclude-standard"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [f for f in out.stdout.splitlines() if (ROOT / f).is_file()]


def hits(rule: re.Pattern, ignore: set[str] = frozenset()) -> list[str]:
    """`file:line: text` for each line of a scanned file that matches `rule` (with matches in `ignore` left out)."""
    found = []
    for name in tracked_files():
        if name in ALLOWED:
            continue
        try:
            text = (ROOT / name).read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue  # binary (the architecture diagram)
        for number, line in enumerate(text.splitlines(), 1):
            if any(m.group(0) not in ignore for m in rule.finditer(line)):
                found.append(f"{name}:{number}: {line.strip()}")
    return found


class PublicHygieneTest(unittest.TestCase):
    def test_no_real_home_path(self):
        self.assertEqual(hits(GENERIC["home path"]), [])

    def test_no_real_account_id(self):
        self.assertEqual(hits(GENERIC["account id"], PLACEHOLDER_ACCOUNTS), [])

    def test_none_of_the_maintainers_private_names(self):
        names = private_names(ROOT / ".private-names")
        if names is None:
            self.skipTest("no .private-names list")
        self.assertEqual(hits(names), [])

    def test_the_rules_match_what_they_should(self):
        for line in ("/home/alice/repo", "/Users/Bob/x"):
            self.assertRegex(line, GENERIC["home path"])
        for line in ("/home/ec2-user/x", "/Users/me/x", "arn:aws:sns:us-east-1:111122223333:t"):
            self.assertNotRegex(line, GENERIC["home path"])
        self.assertRegex("account 210987654321 here", GENERIC["account id"])
        self.assertNotRegex("sha 0123456789abcdef0123", GENERIC["account id"])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, ".private-names")
            self.assertIsNone(private_names(path))
            path.write_text("# a maintainer's names\n\nbox-[0-9]+\nmy-tool\n")
            names = private_names(path)
            self.assertRegex("ssh BOX-7", names)
            self.assertNotRegex("my tool", names)


if __name__ == "__main__":
    unittest.main()
