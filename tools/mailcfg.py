#!/usr/bin/env python3
"""Recipient configuration as code: a reviewed YAML file is the source, DynamoDB (mail-config) the runtime store.

  mailcfg.py validate FILE
  mailcfg.py plan  FILE --table T                       dry-run: what would change against the table's CURRENT items
  mailcfg.py apply FILE --table T [--from-git] --execute
  mailcfg.py rollback --table T --key TOPIC#<arn>|GROUP#<name> --to-version N --execute
  mailcfg.py history --table T --key ...

Every change writes an immutable V#n item and CURRENT in one transaction, conditional on the version it was
planned against, so concurrent publishers cannot overwrite each other. A rollback writes the old content as a
new version (history stays linear). Items are never deleted: switching a topic off is `mode: off`.
"""

import argparse
import datetime
import getpass
import io
import json
import re
import subprocess  # nosec B404
import sys
from collections.abc import Hashable
from pathlib import Path
from typing import Any

import boto3
import yaml
from botocore.exceptions import ClientError

# Used with fullmatch: `$` would also accept a trailing line feed, so the anchors are left out.
ARN = re.compile(r"arn:(aws|aws-cn|aws-us-gov):sns:[a-z]{2}(-[a-z]+)+-\d:\d{12}:[A-Za-z0-9_-]{1,256}")
EMAIL = re.compile(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+")
NAME = re.compile(r"[A-Za-z0-9_.-]{1,64}")
MODES, FORMATS = ("off", "shadow", "live"), ("text", "json")
MAX_RECIPIENTS = 50
FIELDS = ("mode", "recipients", "groups", "format", "defaultSubject", "ownerTeam", "shard", "members")


def load(path: str) -> tuple[dict, list[str]]:
    """Parse and validate a configuration file: ({config key: values}, [errors]). Nothing is read from AWS."""
    return parse(Path(path).read_bytes(), path)


class DuplicateKeyError(yaml.constructor.ConstructorError):
    """A mapping repeats a key (YAML itself would silently keep the last one)."""


class UniqueKeyLoader(yaml.SafeLoader):
    """safe_load that refuses duplicate mapping keys at any level (a group or topic defined twice is a mistake)."""

    def construct_mapping(self, node: Any, deep: bool = False) -> dict:
        """Build the mapping, naming the first key that appears twice."""
        seen = set()
        for key_node, _ in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                continue  # `<<` merges are resolved by the base class and may be overridden on purpose
            key = self.construct_object(key_node, deep=True)
            if isinstance(key, Hashable):
                if key in seen:
                    raise DuplicateKeyError(None, None, f"duplicate key {key!r}", key_node.start_mark)
                seen.add(key)
        return super().construct_mapping(node, deep)


def parse(data: bytes, name: str = "<config>") -> tuple[dict, list[str]]:
    """Parse and validate configuration given as bytes: ({config key: values}, [errors]); `name` appears in
    YAML error positions."""
    stream = io.StringIO(data.decode("utf-8"))
    stream.name = name  # PyYAML reports it in error marks instead of "<unicode string>"
    try:
        doc = yaml.load(stream, Loader=UniqueKeyLoader) or {}  # nosec B506 (SafeLoader subclass)
    except DuplicateKeyError as error:
        return {}, [f"{error.problem} ({error.problem_mark})"]
    errors, items = [], {}
    if doc.get("schema") != 1:
        errors.append("schema must be 1")
    allowed = [d.lower() for d in doc.get("allowedDomains", [])]

    def addresses(where: str, values: Any) -> list:
        if not isinstance(values, list):
            errors.append(f"{where}: must be a list")
            return []
        for a in values:
            if not isinstance(a, str) or not EMAIL.fullmatch(a):
                errors.append(f"{where}: invalid address {a!r}")
            elif allowed and a.rsplit("@", 1)[1].lower() not in allowed:
                errors.append(f"{where}: domain of {a!r} is not in allowedDomains")
        if len(values) > MAX_RECIPIENTS:
            errors.append(f"{where}: more than {MAX_RECIPIENTS} addresses (use a group)")
        if len({a.lower() for a in values if isinstance(a, str)}) != len(values):
            errors.append(f"{where}: duplicate address")
        return values

    for name, g in (doc.get("groups") or {}).items():
        if not isinstance(name, str):
            errors.append(f'group {name!r}: the name must be a string (quote it, for example "{name}")')
            continue
        if not NAME.fullmatch(name) or not isinstance(g, dict):
            errors.append(f"group {name!r}: invalid")
            continue
        items["GROUP#" + name] = {
            "members": addresses(f"group {name}", g.get("members", [])),
            "ownerTeam": str(g.get("ownerTeam", "")),
        }
    for t in doc.get("topics") or []:
        if not isinstance(t, dict):
            errors.append(f"topic entry {t!r}: must be a mapping")
            continue
        arn = t.get("topicArn", "")
        where = f"topic {arn}"
        if not isinstance(arn, str) or not ARN.fullmatch(arn):
            errors.append(f"{where}: invalid topic ARN")
            continue
        if "TOPIC#" + arn in items:
            errors.append(f"{where}: listed twice")
        item = {
            "mode": t.get("mode"),
            "recipients": addresses(where, t.get("recipients", [])),
            "groups": t.get("groups", []),
            "format": t.get("format", "text"),
            "defaultSubject": str(t.get("defaultSubject", "")),
            "ownerTeam": str(t.get("ownerTeam", "")),
            "shard": str(t.get("shard", "")),
        }
        if item["mode"] not in MODES:
            hint = ' (write "off" in quotes: YAML reads a bare off as false)' if item["mode"] is False else ""
            errors.append(f"{where}: mode must be one of {MODES}{hint}")
        if item["format"] not in FORMATS:
            errors.append(f"{where}: format must be one of {FORMATS}")
        if re.search(r"[\r\n]", item["defaultSubject"]):
            errors.append(f"{where}: defaultSubject has a line break")
        if not isinstance(item["groups"], list) or not all(isinstance(g, str) for g in item["groups"]):
            errors.append(f"{where}: groups must be a list of group names")
            item["groups"] = []
        for g in item["groups"]:
            if "GROUP#" + g not in items:
                errors.append(f"{where}: unknown group {g!r}")
        if item["mode"] != "off" and not item["recipients"] and not item["groups"]:
            errors.append(f"{where}: no recipients")
        items["TOPIC#" + arn] = item
    return items, errors


def to_ddb(values: dict) -> dict:
    """Plain values (str, int, list of str) as DynamoDB attribute values."""
    out = {}
    for k, v in values.items():
        if isinstance(v, list):
            out[k] = {"L": [{"S": s} for s in v]}
        elif isinstance(v, int):
            out[k] = {"N": str(v)}
        else:
            out[k] = {"S": v}
    return out


def from_ddb(item: dict) -> dict:
    """The inverse of to_ddb."""
    out = {}
    for k, v in item.items():
        out[k] = [s["S"] for s in v["L"]] if "L" in v else int(v["N"]) if "N" in v else v["S"]
    return out


def content(values: dict) -> dict:
    """The configuration fields of an item, without version metadata."""
    return {k: values[k] for k in FIELDS if k in values}


def current(client: Any, table: str, key: str) -> dict | None:
    """The CURRENT item of `key`, read consistently, or None."""
    item = client.get_item(TableName=table, Key={"pk": {"S": key}, "sk": {"S": "CURRENT"}}, ConsistentRead=True).get(
        "Item"
    )
    return from_ddb(item) if item else None


def write(client: Any, table: str, key: str, values: dict, version_before: int, change_id: str) -> int:
    """CURRENT and V#n in one transaction, only if CURRENT is still at version_before."""
    version = version_before + 1
    meta = {
        "version": version,
        "changeId": change_id,
        "updatedBy": f"{getpass.getuser()}",
        "updatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
    }
    body = to_ddb({**content(values), **meta})
    cond = (
        {"ConditionExpression": "attribute_not_exists(pk)"}
        if version_before == 0
        else {"ConditionExpression": "version = :v", "ExpressionAttributeValues": {":v": {"N": str(version_before)}}}
    )
    client.transact_write_items(
        TransactItems=[
            {
                "Put": {
                    "TableName": table,
                    "Item": {"pk": {"S": key}, "sk": {"S": f"V#{version:06d}"}, **body},
                    "ConditionExpression": "attribute_not_exists(pk)",
                }
            },
            {"Put": {"TableName": table, "Item": {"pk": {"S": key}, "sk": {"S": "CURRENT"}, **body}, **cond}},
        ]
    )
    return version


def diff(client: Any, table: str, items: dict) -> list[tuple[str, int, dict, dict]]:
    """(key, version before, content before, content after) for every item the file changes."""
    changes = []
    for key, values in items.items():
        old = current(client, table, key)
        before = content(old) if old else {}
        after = content(values)
        if before != after:
            changes.append((key, old["version"] if old else 0, before, after))
    return changes


def describe(before: dict, after: dict) -> list[str]:
    """Readable lines for one item's change."""
    lines = []
    for k in FIELDS:
        b, a = before.get(k), after.get(k)
        if b == a:
            continue
        if isinstance(a, list) or isinstance(b, list):
            b, a = set(b or []), set(a or [])
            lines += [f"    + {k}: {x}" for x in sorted(a - b)] + [f"    - {k}: {x}" for x in sorted(b - a)]
        else:
            lines.append(f"    {k}: {b!r} -> {a!r}")
    return lines


def committed(path: str) -> tuple[str, bytes]:
    """(commit, content) of a tracked, committed and unchanged file; exits otherwise. The content is the committed
    blob itself, read from Git: what reviewers approved is exactly what the caller parses and publishes."""

    def git(*args: str, text: bool = True) -> subprocess.CompletedProcess:
        # bandit B404/B603/B607: git from PATH, as the operator runs it; arguments are a list (no shell) and only
        # the file path is user input
        return subprocess.run(["git", *args], capture_output=True, text=text)  # nosec B603 B607

    tracked = git("ls-files", "--full-name", "--error-unmatch", "--", path)
    if tracked.returncode != 0 or not tracked.stdout.strip():
        sys.exit(f"{path} is not tracked by Git: publish only reviewed, committed configuration")
    if git("status", "--porcelain", "--", path).stdout.strip():
        sys.exit(f"{path} has uncommitted changes: publish only reviewed, committed configuration")
    top = git("rev-parse", "--show-toplevel").stdout.strip()
    head = git("-C", top, "rev-parse", "HEAD").stdout.strip()  # resolved once: blob and changeId come from this commit
    blob = git("-C", top, "rev-parse", f"{head}:{tracked.stdout.strip()}").stdout.strip() if head else ""
    if not blob:
        sys.exit(f"{path} has no committed content at HEAD")
    data = git("-C", top, "cat-file", "blob", blob, text=False)
    commit = git("-C", top, "log", "-1", "--format=%H", head, "--", tracked.stdout.strip()).stdout.strip()
    if data.returncode != 0 or not commit:
        sys.exit(f"{path} has no commit")
    return commit, data.stdout


def git_change_id(path: str) -> str:
    """The commit of a tracked, committed and unchanged file (the changeId of a --from-git publish)."""
    return committed(path)[0]


def main() -> None:
    """Command-line entry point."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile")
    ap.add_argument("--region")
    sub = ap.add_subparsers(dest="cmd", required=True)
    table, execute = ("--table", {"required": True}), ("--execute", {"action": "store_true"})
    key = ("--key", {"required": True})
    commands = {
        "validate": [("file", {})],
        "plan": [("file", {}), table],
        "apply": [
            ("file", {}),
            table,
            ("--from-git", {"action": "store_true"}),
            ("--change-id", {"default": "manual"}),
            execute,
        ],
        "rollback": [table, key, ("--to-version", {"type": int, "required": True}), execute],
        "history": [table, key],
    }
    for name, arguments in commands.items():
        p = sub.add_parser(name)
        for flag, options in arguments:
            p.add_argument(flag, **options)
    a = ap.parse_args()

    if a.cmd == "validate":
        items, errors = load(a.file)
        print("\n".join(errors) or f"ok: {len(items)} items")
        sys.exit(1 if errors else 0)
    client = boto3.Session(profile_name=a.profile, region_name=a.region).client("dynamodb")

    if a.cmd in ("plan", "apply"):
        change_id = a.change_id if a.cmd == "apply" else "manual"
        if a.cmd == "apply" and a.from_git:
            change_id, data = committed(a.file)  # the verified blob is what is parsed, planned and published
            items, errors = parse(data, a.file)
        else:
            items, errors = load(a.file)
        if errors:
            sys.exit("invalid configuration:\n" + "\n".join(errors))
        changes = diff(client, a.table, items)
        known = {
            i["pk"]["S"]
            for page in client.get_paginator("scan").paginate(
                TableName=a.table,
                FilterExpression="sk = :c",
                ExpressionAttributeValues={":c": {"S": "CURRENT"}},
                ProjectionExpression="pk",
            )
            for i in page["Items"]
        }
        for key in sorted(known - set(items)):
            print(f"! {key} is in the table but not in {a.file}: left unchanged (set mode: off to switch it off)")
        for key, version, before, after in changes:
            print(f"{'~' if version else '+'} {key} (version {version} -> {version + 1})")
            print("\n".join(describe(before, after)))
        if not changes:
            print("no changes")
        if a.cmd == "plan" or not changes:
            return
        if not a.execute:
            print(f"dry run: {len(changes)} change(s) not written (add --execute); changeId would be {change_id}")
            return
        for key, version, _, after in changes:
            try:
                print(f"wrote {key} version {write(client, a.table, key, after, version, change_id)}")
            except ClientError as error:
                if error.response["Error"]["Code"] == "TransactionCanceledException":
                    sys.exit(f"{key} changed since it was planned (concurrent publish): plan again")
                raise

    elif a.cmd == "rollback":
        old = client.get_item(
            TableName=a.table, Key={"pk": {"S": a.key}, "sk": {"S": f"V#{a.to_version:06d}"}}, ConsistentRead=True
        ).get("Item")
        now = current(client, a.table, a.key)
        if not old or not now:
            sys.exit("no such version")
        print(f"{a.key}: version {now['version']} -> {now['version'] + 1} = content of version {a.to_version}")
        print("\n".join(describe(content(now), content(from_ddb(old)))) or "    (same content)")
        if not a.execute:
            print("dry run (add --execute)")
            return
        print(
            "wrote version "
            + str(write(client, a.table, a.key, from_ddb(old), now["version"], f"rollback-to-v{a.to_version}"))
        )

    else:
        pages = client.get_paginator("query").paginate(
            TableName=a.table,
            KeyConditionExpression="pk = :k AND begins_with(sk, :v)",
            ExpressionAttributeValues={":k": {"S": a.key}, ":v": {"S": "V#"}},
        )
        for page in pages:
            for item in page["Items"]:
                i = from_ddb(item)
                print(json.dumps({k: i.get(k) for k in ("version", "mode", "changeId", "updatedBy", "updatedAt")}))


if __name__ == "__main__":
    main()
