#!/usr/bin/env python3
"""Move member-account topics from email / email-json subscriptions to the central mail queues.

Run with central-account credentials; member accounts are read through their inventory role and changed
(unsubscribe only) through their migration role. Every write to AWS is a dry run unless --execute is given
(`plan` has no such switch and always writes, or overwrites, its local --out file), and every step can be
repeated safely.

  inventory  --table MIG --inventory-role ARN [--topic-prefix P] [--execute]    email subscriptions -> mail-migration
  plan       --table MIG --out FILE                    mail-config YAML (mode: shadow) from confirmed subscriptions
  subscribe  --table MIG --inventory-role ARN --queue-arn Q --topic T [--execute]   SQS subscription by the queue owner
  verify     --table MIG --delivery-table DEL --config-table CFG --topic T --since ISO
             [--mark shadow-verified|live --execute]
  remove-email --table MIG --config-table CFG --delivery-table DEL --migration-role ARN --topic T [--execute]
  status     --table MIG

Order per topic: inventory -> plan (review, publish with mailcfg) -> subscribe -> shadow run + verify ->
mode live (mailcfg) -> verify --mark live (every legacy recipient mailed by the new path under the live
configuration version, and still routed by the topic or its groups) -> remove-email (re-resolves the topic and
group versions and recipients, and re-checks those deliveries). Before remove-email, going back is `mode: shadow`
(the email subscription still delivers). After it, a removed email subscription cannot be recreated in a
region that no longer accepts email subscriptions: roll back inside the new path only.
"""

import argparse
import datetime
import hashlib
import json
import sys
from typing import Any, Iterator

import boto3
import yaml
from botocore.exceptions import ClientError

EMAIL = ("email", "email-json")


def now() -> str:
    """The current UTC time, ISO 8601 to the second."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def region_of(arn: str) -> str:
    """The Region of an ARN."""
    return arn.split(":")[3]


def account_of(arn: str) -> str:
    """The account id of an ARN."""
    return arn.split(":")[4]


def assumed(session: boto3.Session, role_arn: str, region: str) -> Any:
    """An SNS client in `region` with the credentials of `role_arn` (a member account's inventory or migration role)."""
    creds = session.client("sts").assume_role(RoleArn=role_arn, RoleSessionName="sample-sns-email-migrate")[
        "Credentials"
    ]
    return boto3.client(
        "sns",
        region_name=region,
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )


def subscriptions(sns: Any, topic: str) -> Iterator[dict]:
    """All subscriptions of a topic."""
    for page in sns.get_paginator("list_subscriptions_by_topic").paginate(TopicArn=topic):
        yield from page["Subscriptions"]


def items(ddb: Any, table: str, account: str | None = None) -> Iterator[dict]:
    """The mail-migration items (of one member account when given), as plain values."""
    kwargs = {"TableName": table}
    for page in ddb.get_paginator("scan").paginate(**kwargs):
        for i in page["Items"]:
            if account is None or i["pk"]["S"] == "ACCOUNT#" + account:
                yield {
                    k: (v.get("S") if "S" in v else v.get("BOOL") if "BOOL" in v else v.get("N")) for k, v in i.items()
                }


def recipient_key(address: str) -> str:
    """Same key as the mailer's delivery records (src/mailer/handler.py)."""
    return "R#" + hashlib.sha256(address.strip().lower().encode()).hexdigest()[:32]


def cmd_inventory(a: argparse.Namespace, session: boto3.Session) -> None:
    """Record a member account's email / email-json subscriptions in mail-migration."""
    ddb = session.client("dynamodb")
    region = a.region or session.region_name
    sns = assumed(session, a.inventory_role, region)
    account = a.inventory_role.split(":")[4]
    found = []
    for page in sns.get_paginator("list_topics").paginate():
        for t in page["Topics"]:
            topic = t["TopicArn"]
            if a.topic_prefix and not topic.split(":")[5].startswith(a.topic_prefix):
                continue
            for s in subscriptions(sns, topic):
                if s["Protocol"] not in EMAIL:
                    continue
                pending = s["SubscriptionArn"] == "PendingConfirmation"
                attrs = (
                    {}
                    if pending
                    else sns.get_subscription_attributes(SubscriptionArn=s["SubscriptionArn"])["Attributes"]
                )
                sk = (
                    ("PENDING#" + topic + "#" + hashlib.sha256(s["Endpoint"].lower().encode()).hexdigest()[:16])
                    if pending
                    else "SUB#" + s["SubscriptionArn"]
                )
                policy = attrs.get("FilterPolicy", "")
                record = {
                    "topicArn": topic,
                    "protocol": s["Protocol"],
                    "endpoint": s["Endpoint"],
                    "subState": "pending" if pending else "confirmed",
                    "filterPolicy": policy,
                    # SNS applies a filter policy to message attributes unless the scope says MessageBody
                    "filterPolicyScope": attrs.get("FilterPolicyScope", "MessageAttributes") if policy else "",
                }
                found.append({"sk": sk, **{k: v for k, v in record.items() if k != "endpoint"}})
                if a.execute:
                    ddb.update_item(
                        TableName=a.table,
                        Key={"pk": {"S": "ACCOUNT#" + account}, "sk": {"S": sk}},
                        UpdateExpression="SET "
                        + ", ".join(f"{k} = :{k}" for k in record)
                        + ", discoveredAt = if_not_exists(discoveredAt, :t), "
                        "updatedAt = :t, #s = if_not_exists(#s, :d)",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={
                            **{f":{k}": {"S": v} for k, v in record.items()},
                            ":t": {"S": now()},
                            ":d": {"S": "discovered"},
                        },
                    )
    print(
        json.dumps(
            {
                "account": account,
                "emailSubscriptions": len(found),
                "pending": sum(f["subState"] == "pending" for f in found),
                "written": a.execute,
                "items": found,
            },
            indent=2,
        )
    )
    if not a.execute:
        print("dry run: nothing written (add --execute)")


def legacy(ddb: Any, table: str, topic: str) -> list[dict]:
    """The confirmed email subscriptions of a topic that still have to be migrated."""
    return [
        i
        for i in items(ddb, table, account_of(topic))
        if i["topicArn"] == topic
        and i.get("subState") == "confirmed"
        and i.get("status") not in ("excluded", "email-removed")
    ]


def faithful(subs: list[dict]) -> str | None:
    """Why the new path cannot reproduce these subscriptions of one topic exactly, or None. One topic maps to one
    SQS subscription (one filter policy and scope) and one body format, so mixed subscriptions must be split first."""
    if len({i["protocol"] for i in subs}) > 1:
        return "both email and email-json subscriptions"
    if len({(i.get("filterPolicy", ""), i.get("filterPolicyScope", "")) for i in subs}) > 1:
        return "subscriptions with different filter policies or scopes"
    return None


def cmd_plan(a: argparse.Namespace, session: boto3.Session) -> None:
    """Write a mail-config YAML (mode: shadow) from the confirmed subscriptions; refuses topics the new path
    cannot reproduce."""
    ddb = session.client("dynamodb")
    topics = {}
    for i in items(ddb, a.table):
        if i.get("subState") == "confirmed" and i.get("status") not in ("excluded", "email-removed"):
            topics.setdefault(i["topicArn"], []).append(i)
    blocked = {arn: why for arn, subs in topics.items() if (why := faithful(subs))}
    if blocked:
        for arn, why in sorted(blocked.items()):
            print(
                f"! {arn}: {why}; the new path cannot reproduce it. Split the topic or exclude subscriptions first.",
                file=sys.stderr,
            )
        sys.exit("no plan written")
    doc = {"schema": 1, "topics": []}
    for arn, subs in sorted(topics.items()):
        doc["topics"].append(
            {
                "topicArn": arn,
                "mode": "shadow",
                "recipients": sorted({i["endpoint"] for i in subs}),
                "format": "json" if subs[0]["protocol"] == "email-json" else "text",
                "ownerTeam": "",
                "shard": "s1",
            }
        )
    with open(a.out, "w", encoding="utf-8") as f:
        f.write(
            "# Generated by migrate.py plan from confirmed email subscriptions."
            " Review, commit, then publish with mailcfg.py.\n"
        )
        yaml.safe_dump(doc, f, sort_keys=False, allow_unicode=True)
    print(f"wrote {a.out}: {len(doc['topics'])} topics (pending subscriptions are not included)")


def set_status(ddb: Any, table: str, topic: str, status: str, extra: dict | None = None) -> None:
    """Set the migration status (and `extra` attributes) of a topic's legacy subscriptions."""
    values = {":s": {"S": status}, ":t": {"S": now()}, **{f":{k}": {"S": str(v)} for k, v in (extra or {}).items()}}
    for i in legacy(ddb, table, topic):
        ddb.update_item(
            TableName=table,
            Key={"pk": {"S": i["pk"]}, "sk": {"S": i["sk"]}},
            UpdateExpression="SET #s = :s, updatedAt = :t" + "".join(f", {k} = :{k}" for k in (extra or {})),
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues=values,
        )


def expected_attributes(subs: list[dict]) -> dict[str, str]:
    """The SQS subscription attributes that reproduce the email subscriptions: envelope delivery, filter policy
    and scope."""
    attributes = {"RawMessageDelivery": "false"}  # keep the SNS envelope: TopicArn is the routing key
    policy = subs[0].get("filterPolicy", "") if subs else ""
    if policy:
        attributes["FilterPolicy"] = policy
        attributes["FilterPolicyScope"] = subs[0].get("filterPolicyScope") or "MessageAttributes"
    return attributes


def cmd_subscribe(a: argparse.Namespace, session: boto3.Session) -> None:
    """Subscribe the central queue to a topic as the queue owner, or check an existing subscription's attributes."""
    ddb = session.client("dynamodb")
    subs = legacy(ddb, a.table, a.topic)
    why = faithful(subs)
    if why:
        sys.exit(f"refused: {why}; split the topic or exclude subscriptions first")
    attributes = expected_attributes(subs)
    reader = assumed(session, a.inventory_role, region_of(a.topic))
    existing = [s for s in subscriptions(reader, a.topic) if s["Protocol"] == "sqs" and s["Endpoint"] == a.queue_arn]
    if existing:
        actual = reader.get_subscription_attributes(SubscriptionArn=existing[0]["SubscriptionArn"])["Attributes"]
        wrong = {k: (actual.get(k, ""), v) for k, v in attributes.items() if actual.get(k, "") != v}
        if "FilterPolicy" not in attributes and actual.get("FilterPolicy"):
            wrong["FilterPolicy"] = (actual["FilterPolicy"], "")
        if wrong:
            sys.exit(
                f"refused: {existing[0]['SubscriptionArn']} exists with different attributes"
                f" (actual, expected): {wrong}"
            )
        print(
            f"{a.topic}: already subscribed to {a.queue_arn} with the expected attributes"
            f" ({existing[0]['SubscriptionArn']})"
        )
        return
    print(f"{a.topic}: subscribe {a.queue_arn} as the queue owner with {attributes}")
    if not a.execute:
        print("dry run (add --execute)")
        return
    # the queue owner subscribes: no confirmation step; the topic policy must allow this account sns:Subscribe
    arn = session.client("sns", region_name=region_of(a.topic)).subscribe(
        TopicArn=a.topic, Protocol="sqs", Endpoint=a.queue_arn, Attributes=attributes, ReturnSubscriptionArn=True
    )["SubscriptionArn"]
    set_status(ddb, a.table, a.topic, "sqs-subscribed")
    print(f"subscribed: {arn}")


def deliveries(ddb: Any, table: str, topic: str, since: int) -> dict[str, set[tuple[str, int]]]:
    """{recipient key: {(status, config version), ...}} of a topic's delivery records finished since `since`."""
    out = {}
    for page in ddb.get_paginator("scan").paginate(
        TableName=table, FilterExpression="topicArn = :t", ExpressionAttributeValues={":t": {"S": topic}}
    ):
        for i in page["Items"]:
            if int(i.get("finishedAt", {}).get("N", "0")) >= since:
                out.setdefault(i["sk"]["S"], set()).add(
                    (i.get("status", {}).get("S"), int(i.get("configVersion", {}).get("N", "0")))
                )
    return out


def effective(ddb: Any, table: str, topic: str) -> tuple[int, dict[str, int], set[str]] | None:
    """What the mailer routes for a live topic now, read consistently: (topic version, {config key: version} of
    the topic and every group it references, lower-case recipients). None when the topic is not live."""

    def current(pk: str) -> dict | None:
        return ddb.get_item(TableName=table, Key={"pk": {"S": pk}, "sk": {"S": "CURRENT"}}, ConsistentRead=True).get(
            "Item"
        )

    item = current("TOPIC#" + topic)
    if not item or item.get("mode", {}).get("S") != "live":
        return None
    versions = {"TOPIC#" + topic: int(item["version"]["N"])}
    recipients = {r["S"].lower() for r in item.get("recipients", {}).get("L", [])}
    for group in (g["S"] for g in item.get("groups", {}).get("L", [])):
        g = current("GROUP#" + group)
        if not g:
            sys.exit(f"refused: {topic} references group {group}, which is missing from mail-config")
        versions["GROUP#" + group] = int(g["version"]["N"])
        recipients |= {m["S"].lower() for m in g.get("members", {}).get("L", [])}
    return versions["TOPIC#" + topic], versions, recipients


def unrouted(subs: list[dict], recipients: set[str]) -> list[str]:
    """Legacy recipients the live configuration no longer sends to."""
    return [i["endpoint"] for i in subs if i["endpoint"].strip().lower() not in recipients]


def uncovered(subs: list[dict], records: dict, status: str, version: int | None = None) -> list[str]:
    """Legacy recipients without a delivery record of `status` (under config `version`, when given)."""
    return [
        i["endpoint"]
        for i in subs
        if not any(
            s == status and (version is None or v == version) for s, v in records.get(recipient_key(i["endpoint"]), ())
        )
    ]


def cmd_verify(a: argparse.Namespace, session: boto3.Session) -> None:
    """Report what the new path did for a topic since --since and, with --mark, advance its migration status."""
    ddb = session.client("dynamodb")
    since = int(datetime.datetime.fromisoformat(a.since).timestamp())
    subs, records = legacy(ddb, a.table, a.topic), deliveries(ddb, a.delivery_table, a.topic, since)
    live = effective(ddb, a.config_table, a.topic)
    version, versions, routed = live or (None, {}, set())
    shadow_missing = [r for r in uncovered(subs, records, "shadow") if r in uncovered(subs, records, "sent")]
    sent_missing = uncovered(subs, records, "sent", version) if live else [i["endpoint"] for i in subs]
    not_routed = unrouted(subs, routed) if live else [i["endpoint"] for i in subs]
    report = {
        "topic": a.topic,
        "since": a.since,
        "legacyRecipients": len(subs),
        "liveConfigVersions": versions,
        "resolvedByNewPath": len(subs) - len(shadow_missing),
        "sentByNewPathUnderLiveVersion": len(subs) - len(sent_missing),
        "routedByLiveConfig": len(subs) - len(not_routed),
    }
    print(json.dumps(report, indent=2))
    if not a.mark:
        return
    if not subs:
        sys.exit("no legacy subscriptions left to mark")
    if a.mark == "shadow-verified" and shadow_missing:
        sys.exit(
            f"not marked: {len(shadow_missing)} legacy recipient(s) were not resolved by the new path since {a.since}"
        )
    if a.mark == "live":
        # every legacy recipient must still be routed by the live topic and group configuration, and have been
        # mailed by the new path under the topic version now published
        if not live:
            sys.exit("not marked: the topic is not live in mail-config")
        if not_routed:
            sys.exit(
                f"not marked: {len(not_routed)} legacy recipient(s) are not in the live topic or group configuration"
            )
        if sent_missing:
            sys.exit(
                f"not marked: {len(sent_missing)} legacy recipient(s) have no mail sent under config version"
                f" {version} since {a.since}"
            )
    if not a.execute:
        print(f"dry run: would mark {a.mark} (add --execute)")
        return
    extra = (
        {"liveConfigVersion": version, "liveConfigVersions": json.dumps(versions, sort_keys=True), "liveSince": since}
        if a.mark == "live"
        else None
    )
    set_status(ddb, a.table, a.topic, a.mark, extra)
    print(f"marked {a.mark}")


def cmd_remove_email(a: argparse.Namespace, session: boto3.Session) -> None:
    """Unsubscribe a live topic's email subscriptions after re-checking its configuration and deliveries."""
    ddb = session.client("dynamodb")
    targets = legacy(ddb, a.table, a.topic)
    if not targets:
        sys.exit("nothing to remove")
    if any(i.get("status") != "live" for i in targets):
        sys.exit("refused: mark the topic live first (verify --mark live --execute)")
    # re-check now: the topic and every group it references must still be at the versions that were verified,
    # every legacy recipient must still be routed, and each must have been mailed by the new path
    live = effective(ddb, a.config_table, a.topic)
    marked = {i.get("liveConfigVersions") for i in targets}
    if not live or marked != {json.dumps(live[1], sort_keys=True)}:
        sys.exit(
            f"refused: mail-config is now {live[1] if live else 'not live'},"
            f" verified was {sorted(m or '' for m in marked)}; verify again"
        )
    version, _, routed = live
    if unrouted(targets, routed):
        sys.exit("refused: a legacy recipient is no longer in the live topic or group configuration")
    since = min(int(i.get("liveSince", "0")) for i in targets)
    missing = uncovered(targets, deliveries(ddb, a.delivery_table, a.topic, since), "sent", version)
    if missing:
        sys.exit(f"refused: {len(missing)} legacy recipient(s) have no mail sent under config version {version}")
    for i in targets:
        print(f"unsubscribe {i['protocol']} {i['sk'][4:]}")
    if not a.execute:
        print("dry run (add --execute). This cannot be undone where email subscriptions can no longer be created.")
        return
    sns = assumed(session, a.migration_role, region_of(a.topic))
    for i in targets:
        try:
            sns.unsubscribe(SubscriptionArn=i["sk"][4:])
        except ClientError as error:
            if error.response["Error"]["Code"] != "NotFound":
                raise
        ddb.update_item(
            TableName=a.table,
            Key={"pk": {"S": i["pk"]}, "sk": {"S": i["sk"]}},
            UpdateExpression="SET #s = :s, removedAt = :t, updatedAt = :t",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": {"S": "email-removed"}, ":t": {"S": now()}},
        )
    print(f"removed {len(targets)} email subscription(s)")


def cmd_status(a: argparse.Namespace, session: boto3.Session) -> None:
    """Print the migration status of every recorded subscription."""
    for i in items(session.client("dynamodb"), a.table):
        print(json.dumps({k: i.get(k) for k in ("pk", "topicArn", "protocol", "subState", "status", "updatedAt")}))


def main() -> None:
    """Command-line entry point."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile")
    ap.add_argument("--region")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def required(*flags: str) -> list[tuple[str, dict]]:
        return [(f, {"required": True}) for f in flags]

    execute = [("--execute", {"action": "store_true"})]
    commands = {
        "inventory": required("--table", "--inventory-role") + [("--topic-prefix", {})] + execute,
        "plan": required("--table", "--out"),
        "subscribe": required("--table", "--inventory-role", "--queue-arn", "--topic") + execute,
        "verify": required("--table", "--delivery-table", "--config-table", "--topic", "--since")
        + [("--mark", {"choices": ["shadow-verified", "live"]})]
        + execute,
        "remove-email": required("--table", "--config-table", "--delivery-table", "--migration-role", "--topic")
        + execute,
        "status": required("--table"),
    }
    for name, arguments in commands.items():
        p = sub.add_parser(name)
        for flag, options in arguments:
            p.add_argument(flag, **options)
    a = ap.parse_args()
    session = boto3.Session(profile_name=a.profile, region_name=a.region)
    {
        "inventory": cmd_inventory,
        "plan": cmd_plan,
        "subscribe": cmd_subscribe,
        "verify": cmd_verify,
        "remove-email": cmd_remove_email,
        "status": cmd_status,
    }[a.cmd](a, session)


if __name__ == "__main__":
    main()
