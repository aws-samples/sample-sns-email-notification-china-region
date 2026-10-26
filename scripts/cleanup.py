#!/usr/bin/env python3
"""Delete what this sample deployed in one region. Dry run by default (prints the plan); --execute deletes.

  cleanup.py --region R [--central-profile P] [--member-profile Q] [--prefix P] [--execute]

The stacks are found by name (<prefix>-test-smtp, <prefix>-central, <prefix>-member). The tables and the artifact
bucket are taken from the central stack itself (its table resources and its CodeBucket parameter), not derived
from a name. Every stack, table and bucket must carry the tag project=sample-sns-email; this is checked when the
plan is made and again right before each change (deletion protection off, delete, each batch of objects removed
from the bucket, and the bucket's own deletion). Anything else is refused, and so is any lookup that fails for a
reason other than "does not exist". The checks and the changes are separate calls, so a tag removed in between is
not caught. SES identities, an SES configuration set and DKIM records made for a global test are not touched
(see README, Clean up).
"""

import argparse
import sys
from typing import Any, Callable, NoReturn

import boto3
from botocore.exceptions import ClientError

TAG = ("project", "sample-sns-email")
TABLES = ("ConfigTable", "MigrationTable", "DeliveryTable")  # logical ids in templates/central.yaml
RETAINED = ("ConfigTable", "MigrationTable")  # DeletionPolicy: Retain


def owned(tags: list[dict]) -> bool:
    """Whether a resource's tags include this sample's project tag."""
    return any(t["Key"] == TAG[0] and t["Value"] == TAG[1] for t in tags)


def refuse(what: str) -> NoReturn:
    """Stop: `what` does not belong to this sample."""
    sys.exit(f"refused: {what} is not tagged {TAG[0]}={TAG[1]}; nothing more is changed")


def stack(cfn: Any, name: str) -> dict | None:
    """The stack, or None when it does not exist; refuses one without the tag. Any other error (access denied,
    throttling, ...) is raised: a stack that could not be looked up must not be treated as gone."""
    try:
        s = cfn.describe_stacks(StackName=name)["Stacks"][0]
    except ClientError as error:
        details = error.response.get("Error", {})
        if details.get("Code") == "ValidationError" and "does not exist" in details.get("Message", ""):
            return None
        raise
    if not owned(s.get("Tags", [])):
        refuse(f"stack {name}")
    return s


def check_table(ddb: Any, name: str) -> dict:
    """The table's description, after checking its tag."""
    table = ddb.describe_table(TableName=name)["Table"]
    if not owned(ddb.list_tags_of_resource(ResourceArn=table["TableArn"]).get("Tags", [])):
        refuse(f"table {name}")
    return table


def check_bucket(s3: Any, bucket: str) -> None:
    """Refuse a bucket without this sample's tag (also when its tags cannot be read)."""
    try:
        tags = s3.get_bucket_tagging(Bucket=bucket)["TagSet"]
    except ClientError:
        tags = []
    if not owned(tags):
        refuse(f"bucket {bucket}")


def main() -> None:
    """Command-line entry point."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--region", required=True)
    ap.add_argument("--central-profile")
    ap.add_argument("--member-profile")
    ap.add_argument("--prefix", default="sample-sns-email", help="prefix of the stack names")
    ap.add_argument("--execute", action="store_true")
    a = ap.parse_args()
    central = boto3.Session(profile_name=a.central_profile, region_name=a.region)
    member = boto3.Session(profile_name=a.member_profile, region_name=a.region)
    ddb, s3 = central.client("dynamodb"), central.client("s3")
    plan, tables, bucket = [], {}, None

    # Each action returns True when it changed something and False when there was nothing to do (reported as skipped).
    def protection_off(name: str) -> Callable[[], bool]:
        def run() -> bool:
            if check_table(ddb, name).get("DeletionProtectionEnabled"):
                ddb.update_table(TableName=name, DeletionProtectionEnabled=False)
                return True
            return False

        return run

    def delete_stack(cfn: Any, name: str) -> Callable[[], bool]:
        def run() -> bool:
            if not stack(cfn, name):
                return False
            cfn.delete_stack(StackName=name)
            cfn.get_waiter("stack_delete_complete").wait(StackName=name, WaiterConfig={"Delay": 15, "MaxAttempts": 120})
            return True

        return run

    def delete_table(name: str) -> Callable[[], bool]:
        def run() -> bool:
            check_table(ddb, name)
            ddb.delete_table(TableName=name)
            return True

        return run

    def delete_bucket(name: str) -> Callable[[], bool]:
        def run() -> bool:
            check_bucket(s3, name)
            for page in s3.get_paginator("list_object_versions").paginate(Bucket=name):
                objects = [
                    {"Key": o["Key"], "VersionId": o["VersionId"]}
                    for o in page.get("Versions", []) + page.get("DeleteMarkers", [])
                ]
                if objects:
                    check_bucket(s3, name)  # before every delete batch: stop as soon as the tag is gone
                    s3.delete_objects(Bucket=name, Delete={"Objects": objects})
            check_bucket(s3, name)
            s3.delete_bucket(Bucket=name)
            return True

        return run

    for session, suffix in ((central, "test-smtp"), (central, "central"), (member, "member")):
        cfn, name = session.client("cloudformation"), f"{a.prefix}-{suffix}"
        s = stack(cfn, name)
        if not s:
            print(f"no stack {name}")
            continue
        if suffix == "central":
            resources = cfn.describe_stack_resources(StackName=name)["StackResources"]
            tables = {
                r["LogicalResourceId"]: r["PhysicalResourceId"] for r in resources if r["LogicalResourceId"] in TABLES
            }
            bucket = next(
                (p["ParameterValue"] for p in s.get("Parameters", []) if p["ParameterKey"] == "CodeBucket"), None
            )
            for table in tables.values():  # the stack's tables have deletion protection
                check_table(ddb, table)
                plan.append((f"remove deletion protection {table}", protection_off(table)))
        plan.append((f"delete stack {name}", delete_stack(cfn, name)))

    if not tables:
        print(
            f"no {a.prefix}-central stack: its retained tables and artifact bucket cannot be identified;"
            " delete them by hand if they remain"
        )
    for logical in RETAINED:
        if logical in tables:
            plan.append((f"delete retained table {tables[logical]}", delete_table(tables[logical])))
    if bucket:
        check_bucket(s3, bucket)
        plan.append((f"empty and delete bucket {bucket}", delete_bucket(bucket)))

    for what, _ in plan:
        print(("" if a.execute else "would ") + what)
    if not a.execute:
        print("dry run (add --execute)")
        return
    for what, action in plan:
        print(("done: " if action() else "skipped (nothing to do): ") + what)


if __name__ == "__main__":
    main()
