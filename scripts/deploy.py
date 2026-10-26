#!/usr/bin/env python3
"""Package the mailer and create/update the sample stacks (tagged project=sample-sns-email).

  deploy.py --profile P --region R package [--prefix NamePrefix]  -> uploads src/mailer, prints CodeBucket/CodeKey
  deploy.py --profile P --region R stack NAME TEMPLATE [K=V ...]  -> create or update, wait, print outputs
  deploy.py --profile P --region R outputs NAME
Parameters not given on an update keep their previous values; unknown parameter names are refused.
DynamoDbPrefixListId (central template) is looked up in the target region when not given.
"""

import argparse
import hashlib
import io
import json
import sys
import zipfile
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError, WaiterError

ROOT = Path(__file__).resolve().parents[1]
TAGS = [{"Key": "project", "Value": "sample-sns-email"}]


def bucket_name(prefix: str, account: str, region: str) -> str:
    """One bucket per prefix, account and region; the central stack records it as CodeBucket."""
    return f"{prefix}-artifacts-{account}-{region}"


def dynamodb_prefix_list_id(session: boto3.Session) -> str:
    """ID of the AWS managed prefix list of the DynamoDB gateway endpoint (aws and aws-cn use the same name pattern)."""
    name = f"com.amazonaws.{session.region_name}.dynamodb"
    lists = session.client("ec2").describe_managed_prefix_lists(
        Filters=[{"Name": "prefix-list-name", "Values": [name]}]
    )
    if len(lists["PrefixLists"]) != 1:
        sys.exit(f"expected exactly one managed prefix list named {name}, found {len(lists['PrefixLists'])}")
    return lists["PrefixLists"][0]["PrefixListId"]


def package(session: boto3.Session, prefix: str) -> None:
    """Zip src/mailer reproducibly, upload it to the artifact bucket (created and tagged on first use) and print
    its location."""
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w", zipfile.ZIP_DEFLATED) as z:
        for f in sorted((ROOT / "src/mailer").glob("*.py")):
            info = zipfile.ZipInfo(f.name, (2024, 1, 1, 0, 0, 0))  # reproducible: the key changes only with the code
            info.external_attr = 0o644 << 16
            z.writestr(info, f.read_bytes())
    body = data.getvalue()
    account = session.client("sts").get_caller_identity()["Account"]
    bucket = bucket_name(prefix, account, session.region_name)
    s3 = session.client("s3")
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        kwargs = (
            {}
            if session.region_name == "us-east-1"
            else {"CreateBucketConfiguration": {"LocationConstraint": session.region_name}}
        )
        s3.create_bucket(Bucket=bucket, **kwargs)
        s3.put_public_access_block(
            Bucket=bucket,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        s3.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": TAGS})
    key = f"mailer/{hashlib.sha256(body).hexdigest()[:16]}.zip"
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    print(json.dumps({"CodeBucket": bucket, "CodeKey": key}))


def outputs(cfn: Any, name: str) -> dict[str, str]:
    """A stack's outputs."""
    stack = cfn.describe_stacks(StackName=name)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


def stack(session: boto3.Session, name: str, template: str, pairs: list[str]) -> None:
    """Create or update a stack from KEY=VALUE parameters, wait for it and print its outputs."""
    cfn = session.client("cloudformation")
    given = dict(p.split("=", 1) for p in pairs)
    body = (ROOT / template).read_text()
    declared = cfn.validate_template(TemplateBody=body)["Parameters"]
    if "DynamoDbPrefixListId" in {p["ParameterKey"] for p in declared}:
        if "DynamoDbPrefixListId" not in given:  # looked up only when it was not passed
            given["DynamoDbPrefixListId"] = dynamodb_prefix_list_id(session)
    unknown = set(given) - {p["ParameterKey"] for p in declared}
    if unknown:
        sys.exit(f"unknown parameter(s) for {template}: {', '.join(sorted(unknown))}")
    try:
        existing = cfn.describe_stacks(StackName=name)["Stacks"][0]
        if existing["StackStatus"] == "ROLLBACK_COMPLETE":
            sys.exit(f"{name} is ROLLBACK_COMPLETE: inspect and delete it first")
    except ClientError:
        existing = None
    params = []
    for p in declared:
        k = p["ParameterKey"]
        if k in given:
            params.append({"ParameterKey": k, "ParameterValue": given[k]})
        elif existing and any(e["ParameterKey"] == k for e in existing.get("Parameters", [])):
            params.append({"ParameterKey": k, "UsePreviousValue": True})
    args = dict(
        StackName=name,
        TemplateBody=body,
        Parameters=params,
        Tags=TAGS,
        Capabilities=["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"],
    )
    try:
        if existing:
            cfn.update_stack(**args)
            waiter = "stack_update_complete"
        else:
            cfn.create_stack(DisableRollback=True, **args)  # keep a failed stack for diagnosis
            waiter = "stack_create_complete"
    except ClientError as error:
        if "No updates are to be performed" not in str(error):
            raise
        print(json.dumps(outputs(cfn, name), indent=2))
        return
    try:
        cfn.get_waiter(waiter).wait(StackName=name, WaiterConfig={"Delay": 10, "MaxAttempts": 180})
    except WaiterError:
        failed = [
            e
            for e in cfn.describe_stack_events(StackName=name)["StackEvents"]
            if e["ResourceStatus"].endswith("FAILED")
        ][:5]
        for e in failed:
            print(e["LogicalResourceId"], e.get("ResourceStatusReason", ""), file=sys.stderr)
        sys.exit(1)
    print(json.dumps(outputs(cfn, name), indent=2))


def main() -> None:
    """Command-line entry point."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile")
    ap.add_argument("--region", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    pk = sub.add_parser("package")
    pk.add_argument("--prefix", default="sample-sns-email", help="the stacks' NamePrefix (names the artifact bucket)")
    s = sub.add_parser("stack")
    s.add_argument("name")
    s.add_argument("template")
    s.add_argument("params", nargs="*")
    o = sub.add_parser("outputs")
    o.add_argument("name")
    a = ap.parse_args()
    session = boto3.Session(profile_name=a.profile, region_name=a.region)
    if a.cmd == "package":
        package(session, a.prefix)
    elif a.cmd == "stack":
        stack(session, a.name, a.template, a.params)
    else:
        print(json.dumps(outputs(session.client("cloudformation"), a.name), indent=2))


if __name__ == "__main__":
    main()
