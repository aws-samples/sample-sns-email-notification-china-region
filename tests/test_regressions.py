"""Tests for migration gates, filter fidelity, dry runs, claim races,
the time budget and --from-git. In-memory fakes only; no AWS access."""

import contextlib
import io
import json
import os
import re
import smtplib
import subprocess
import sys
import tempfile
import unittest
from email.message import EmailMessage
from pathlib import Path
from types import SimpleNamespace as NS
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_core as t  # noqa: E402  (sets the mailer environment and import paths)
import handler  # noqa: E402
import mailcfg  # noqa: E402
import migrate  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import cleanup  # noqa: E402

TOPIC = t.TOPIC
LEGACY = "legacy@example.com"


def sub(endpoint=LEGACY, protocol="email", status="shadow-verified", policy="", scope="", **extra):
    return {
        "pk": "ACCOUNT#111122223333",
        "sk": f"SUB#{TOPIC}:{endpoint}",
        "topicArn": TOPIC,
        "protocol": protocol,
        "endpoint": endpoint,
        "subState": "confirmed",
        "status": status,
        "filterPolicy": policy,
        "filterPolicyScope": scope,
        **extra,
    }


class Recorder:
    """A DynamoDB / SNS client that records writes; get_item answers CURRENT config items from `config`."""

    def __init__(self, config_version=7, mode="live"):
        self.writes, self.subscribed, self.unsubscribed = [], [], []
        self.config = {
            "TOPIC#"
            + TOPIC: {"mode": {"S": mode}, "version": {"N": str(config_version)}, "recipients": {"L": [{"S": LEGACY}]}}
        }

    def get_item(self, **kw):
        item = self.config.get(kw["Key"]["pk"]["S"])
        return {"Item": item} if item else {}

    def update_item(self, **kw):
        self.writes.append(kw)

    def subscribe(self, **kw):
        self.subscribed.append(kw)
        return {"SubscriptionArn": TOPIC + ":sqs"}

    def unsubscribe(self, **kw):
        self.unsubscribed.append(kw)

    def get_paginator(self, name):
        return NS(paginate=lambda **kw: [{"Topics": [{"TopicArn": TOPIC}]}])

    def get_subscription_attributes(self, **kw):
        return {"Attributes": self.attributes}


def verified(topic=7, **groups):
    """The liveConfigVersions that verify --mark live stores."""
    return json.dumps({"TOPIC#" + TOPIC: topic, **{"GROUP#" + g: v for g, v in groups.items()}}, sort_keys=True)


def args(**kw):
    base = dict(
        table="mig",
        delivery_table="del",
        config_table="cfg",
        topic=TOPIC,
        since="2026-10-18T00:00:00+00:00",
        mark=None,
        execute=False,
        inventory_role="arn:aws-cn:iam::111122223333:role/inv",
        migration_role="arn:aws-cn:iam::111122223333:role/mig",
        queue_arn="arn:aws-cn:sqs:cn-north-1:444455556666:q",
        region="cn-north-1",
        topic_prefix=None,
        out=None,
    )
    return NS(**{**base, **kw})


class MigrationTest(unittest.TestCase):
    def run_cmd(self, cmd, a, rows, records=None, subs=(), client=None):
        client = client or Recorder()
        session = NS(region_name="cn-north-1", client=lambda name, **kw: client)
        with mock.patch.object(migrate, "items", side_effect=lambda *x: [dict(r) for r in rows]), mock.patch.object(
            migrate, "deliveries", return_value=records or {}
        ), mock.patch.object(migrate, "assumed", return_value=client), mock.patch.object(
            migrate, "subscriptions", return_value=list(subs)
        ):
            cmd(a, session)
        return client

    def test_shadow_alone_or_another_recipient_sent_never_allows_removal(self):
        # the legacy recipient only has shadow evidence; an unrelated recipient was mailed under the live version
        records = {
            handler.recipient_key(LEGACY): {("shadow", 6)},
            handler.recipient_key("other@example.com"): {("sent", 7)},
        }
        with self.assertRaises(SystemExit):
            self.run_cmd(migrate.cmd_verify, args(mark="live", execute=True), [sub()], records)
        # even if an item is marked live by hand, removal re-checks deliveries and the configuration version
        with self.assertRaises(SystemExit):
            self.run_cmd(
                migrate.cmd_remove_email,
                args(execute=True),
                [sub(status="live", liveConfigVersions=verified(), liveSince="0")],
                records,
            )
        # mailed under an older version, and the configuration has changed since verification
        with self.assertRaises(SystemExit):
            self.run_cmd(
                migrate.cmd_remove_email,
                args(execute=True),
                [sub(status="live", liveConfigVersions=verified(6), liveSince="0")],
                {handler.recipient_key(LEGACY): {("sent", 6)}},
            )

    def test_removal_goes_ahead_only_with_the_legacy_recipient_mailed_under_the_verified_version(self):
        client = self.run_cmd(
            migrate.cmd_remove_email,
            args(execute=True),
            [sub(status="live", liveConfigVersions=verified(), liveSince="0")],
            {handler.recipient_key(LEGACY): {("sent", 7)}},
        )
        self.assertEqual(len(client.unsubscribed), 1)

    def test_a_group_change_after_live_verification_blocks_removal(self):
        # the legacy recipient is routed through group ops only; a group update does not change the topic version
        client = Recorder()
        client.config["TOPIC#" + TOPIC].update(recipients={"L": []}, groups={"L": [{"S": "ops"}]})

        def group(version, members):
            return {"version": {"N": str(version)}, "members": {"L": [{"S": m} for m in members]}}

        client.config["GROUP#ops"] = group(1, [LEGACY])
        sent = {handler.recipient_key(LEGACY): {("sent", 7)}}
        self.run_cmd(migrate.cmd_verify, args(mark="live", execute=True), [sub()], sent, client=client)
        stored = client.writes[0]["ExpressionAttributeValues"][":liveConfigVersions"]["S"]
        self.assertEqual(stored, verified(ops=1))
        row = sub(status="live", liveConfigVersions=stored, liveSince="0")
        for members in (
            [LEGACY, "new@example.com"],
            ["new@example.com"],
        ):  # any later group version, and one dropping legacy
            client.config["GROUP#ops"] = group(2, members)
            with self.assertRaises(SystemExit):
                self.run_cmd(migrate.cmd_remove_email, args(execute=True), [row], sent, client=client)
        # verification itself refuses while the legacy recipient is no longer routed, despite older sent evidence
        with self.assertRaises(SystemExit):
            self.run_cmd(migrate.cmd_verify, args(mark="live", execute=True), [sub()], sent, client=client)
        self.assertEqual(client.unsubscribed, [])

    def test_filter_policy_and_scope_are_copied_and_an_existing_mismatch_is_refused(self):
        rows = [sub(policy='{"severity":["high"]}', scope="MessageBody")]
        client = self.run_cmd(migrate.cmd_subscribe, args(execute=True), rows)
        self.assertEqual(
            client.subscribed[0]["Attributes"],
            {
                "RawMessageDelivery": "false",
                "FilterPolicy": '{"severity":["high"]}',
                "FilterPolicyScope": "MessageBody",
            },
        )
        existing = Recorder()
        existing.attributes = {"RawMessageDelivery": "true", "FilterPolicy": '{"severity":["high"]}'}
        with self.assertRaises(SystemExit):
            self.run_cmd(
                migrate.cmd_subscribe,
                args(execute=True),
                rows,
                subs=[{"Protocol": "sqs", "Endpoint": args().queue_arn, "SubscriptionArn": TOPIC + ":sqs"}],
                client=existing,
            )

    def test_subscriptions_the_new_path_cannot_reproduce_block_the_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            out = os.path.join(directory, "plan.yaml")
            for rows in (
                [sub(), sub("b@example.com", protocol="email-json")],
                [
                    sub(policy='{"a":["1"]}', scope="MessageAttributes"),
                    sub("b@example.com", policy='{"a":["1"]}', scope="MessageBody"),
                ],
            ):
                with self.assertRaises(SystemExit):
                    self.run_cmd(migrate.cmd_plan, args(out=out), rows)
                self.assertFalse(os.path.exists(out))
                with self.assertRaises(SystemExit):
                    self.run_cmd(migrate.cmd_subscribe, args(execute=True), rows)

    def test_inventory_and_verify_mark_write_only_with_execute(self):
        client = Recorder()
        client.attributes = {"FilterPolicy": '{"a":["1"]}', "FilterPolicyScope": "MessageBody"}
        legacy_sub = [{"Protocol": "email", "SubscriptionArn": TOPIC + ":legacy", "Endpoint": LEGACY}]
        self.run_cmd(migrate.cmd_inventory, args(), [], subs=legacy_sub, client=client)
        self.run_cmd(
            migrate.cmd_verify,
            args(mark="shadow-verified"),
            [sub(status="sqs-subscribed")],
            {handler.recipient_key(LEGACY): {("shadow", 6)}},
            client=client,
        )
        self.assertEqual(client.writes, [])
        self.run_cmd(migrate.cmd_inventory, args(execute=True), [], subs=legacy_sub, client=client)
        self.assertIn(":filterPolicyScope", client.writes[0]["ExpressionAttributeValues"])
        self.assertEqual(client.writes[0]["ExpressionAttributeValues"][":filterPolicyScope"], {"S": "MessageBody"})


class MailerRaceTest(unittest.TestCase):
    def test_a_claim_released_by_another_attempt_is_retried_not_acknowledged(self):
        class Vanished(t.FakeDynamo):
            def put_item(self, **kw):
                raise t.failed()

            def get_item(self, **kw):
                return {}

        handler._ddb = Vanished()
        self.assertEqual(handler.claim("race", LEGACY, TOPIC, 1, "live")[0], "busy")

    def test_the_minimum_function_timeout_leaves_time_to_send(self):
        handler._ddb, handler._cache = t.FakeDynamo(), {}
        t.topic(handler._ddb)
        smtp = t.FakeSmtp()
        with mock.patch.object(handler, "Smtp", return_value=smtp):
            reply = handler.lambda_handler({"Records": [t.record()]}, NS(get_remaining_time_in_millis=lambda: 29000))
        self.assertEqual(reply["batchItemFailures"], [])
        self.assertEqual(len(smtp.sent), 2)
        # the template cannot be given a timeout that leaves no time after the reserve (it allowed 15 s before)
        import re

        template = (Path(__file__).resolve().parents[1] / "templates/central.yaml").read_text()
        minimum = int(re.search(r"FunctionTimeoutSeconds: \{[^}]*MinValue: (\d+)", template).group(1))
        self.assertGreaterEqual(minimum * 1000 - handler.RESERVE_MS, 15000)


class DeployPrefixListTest(unittest.TestCase):
    """deploy.py resolves the DynamoDB managed prefix list of the target region (aws and aws-cn alike), and the central
    template only opens HTTPS to that list and to the endpoint security group."""

    def test_lookup_uses_the_regional_name(self):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
        import deploy

        for region in ("us-west-2", "cn-north-1"):
            filters = []
            ec2 = NS(
                describe_managed_prefix_lists=lambda Filters: filters.append(Filters)
                or {"PrefixLists": [{"PrefixListId": "pl-0123abcd"}]}
            )
            session = NS(region_name=region, client=lambda name: ec2)
            self.assertEqual(deploy.dynamodb_prefix_list_id(session), "pl-0123abcd")
            self.assertEqual(filters[0][0]["Values"], [f"com.amazonaws.{region}.dynamodb"])
        ec2 = NS(describe_managed_prefix_lists=lambda Filters: {"PrefixLists": []})
        with self.assertRaises(SystemExit):
            deploy.dynamodb_prefix_list_id(NS(region_name="us-west-2", client=lambda name: ec2))

    def test_lambda_security_group_has_no_open_https_rule(self):
        template = (Path(__file__).resolve().parents[1] / "templates/central.yaml").read_text()
        group = template.split("  LambdaSecurityGroup:")[1].split("  LambdaToSecretsManagerEgress:")[0]
        self.assertNotIn("0.0.0.0/0", group)
        self.assertNotIn("443", group)
        self.assertIn("DestinationSecurityGroupId: !Ref EndpointSecurityGroup", template)
        self.assertIn("DestinationPrefixListId: !Ref DynamoDbPrefixListId", template)


class CleanupTest(unittest.TestCase):
    """cleanup.py changes only the tables and bucket the central stack names, and only while they carry the tag."""

    def run_cleanup(self, table_tags):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
        import cleanup

        tag = [{"Key": "project", "Value": "sample-sns-email"}]
        calls = []
        tables = {
            "ConfigTable": "actual-mail-config",
            "MigrationTable": "actual-mail-migration",
            "DeliveryTable": "actual-mail-delivery",
        }
        fake = NS(
            describe_stacks=lambda StackName: (
                {
                    "Stacks": [
                        {"Tags": tag, "Parameters": [{"ParameterKey": "CodeBucket", "ParameterValue": "actual-bucket"}]}
                    ]
                }
                if StackName == "custom-central"
                else (_ for _ in ()).throw(
                    cleanup.ClientError(
                        {"Error": {"Code": "ValidationError", "Message": "Stack with id x does not exist"}},
                        "DescribeStacks",
                    )
                )
            ),
            describe_stack_resources=lambda StackName: {
                "StackResources": [{"LogicalResourceId": k, "PhysicalResourceId": v} for k, v in tables.items()]
            },
            describe_table=lambda TableName: {
                "Table": {"TableArn": "arn:" + TableName, "DeletionProtectionEnabled": True}
            },
            list_tags_of_resource=lambda ResourceArn: {"Tags": table_tags(ResourceArn[4:])},
            get_bucket_tagging=lambda Bucket: {"TagSet": tag},
            get_paginator=lambda name: NS(paginate=lambda **kw: []),
            get_waiter=lambda name: NS(wait=lambda **kw: None),
            get_caller_identity=lambda: {"Account": "444455556666"},
            **{
                op: (lambda op: lambda **kw: calls.append((op, kw)))(op)
                for op in ("update_table", "delete_table", "delete_stack", "delete_objects", "delete_bucket")
            },
        )
        session = NS(client=lambda name, **kw: fake)
        with mock.patch.object(cleanup.boto3, "Session", return_value=session), mock.patch.object(
            sys, "argv", ["cleanup.py", "--region", "us-west-2", "--prefix", "custom", "--execute"]
        ):
            try:
                cleanup.main()
            except SystemExit:
                pass
        return calls

    def test_untagged_tables_named_by_the_stack_are_never_changed(self):
        self.assertEqual(self.run_cleanup(lambda table: []), [])
        # tables tagged until the plan is made, then not: the next change is refused
        seen = []
        calls = self.run_cleanup(
            lambda table: (
                [] if table in seen else seen.append(table) or [{"Key": "project", "Value": "sample-sns-email"}]
            )
        )
        self.assertEqual(calls, [])

    def test_the_identifiers_come_from_the_stack(self):
        calls = self.run_cleanup(lambda table: [{"Key": "project", "Value": "sample-sns-email"}])
        changed = {kw.get("TableName") or kw.get("Bucket") or kw.get("StackName") for _, kw in calls}
        self.assertEqual(
            changed,
            {"actual-mail-config", "actual-mail-migration", "actual-mail-delivery", "actual-bucket", "custom-central"},
        )


class RejectedAlarmTest(unittest.TestCase):
    """A permanent refusal is logged (without address or SMTP reply) as soon as it is recorded, even when a later
    recipient of the same message fails temporarily and ends the invocation."""

    def test_alarm_event_is_not_lost_when_a_later_recipient_fails_temporarily(self):
        ddb = t.FakeDynamo()
        handler._ddb, handler._cache = ddb, {}
        t.topic(ddb, recipients=("a@example.com", "b@example.com"))

        class Smtp:
            def send(self, message):
                if message["To"] == "a@example.com":
                    raise smtplib.SMTPRecipientsRefused({"a@example.com": (550, b"no such user a@example.com")})
                raise smtplib.SMTPRecipientsRefused({"b@example.com": (450, b"try later")})

            def close(self):
                pass

        with mock.patch.object(handler, "Smtp", return_value=Smtp()), self.assertLogs(level="ERROR") as logs:
            failures = handler.lambda_handler({"Records": [t.record()]}, None)["batchItemFailures"]
        self.assertEqual(failures, [{"itemIdentifier": "sqs-m-1"}])  # b is retried
        events = [json.loads(line.split(":", 2)[2])["event"] for line in logs.output]
        self.assertEqual(events.count("mail_rejected"), 1)
        text = "\n".join(logs.output)
        self.assertNotIn("example.com", text)
        self.assertNotIn("no such user", text)
        # the retry skips the recorded rejection and does not log it a second time
        with mock.patch.object(handler, "Smtp", return_value=t.FakeSmtp()), self.assertNoLogs(level="ERROR"):
            self.assertEqual(handler.lambda_handler({"Records": [t.record(receive=2)]}, None)["batchItemFailures"], [])


class SendRateTest(unittest.TestCase):
    """The send rate is kept per execution environment and must be a positive finite number."""

    def test_consecutive_invocations_share_the_rate_limit(self):
        handler._last_send = 0.0
        clock = iter([100.0, 100.0, 100.05, 100.05])  # wait check and timestamp of two sends, 50 ms apart
        slept = []
        with mock.patch.object(handler, "MAX_PER_SECOND", 5.0), mock.patch.object(
            handler.time, "monotonic", side_effect=lambda: next(clock)
        ), mock.patch.object(handler.time, "sleep", side_effect=slept.append):
            for _ in range(2):  # two invocations, each with its own Smtp object and session
                smtp = handler.Smtp()
                smtp.conn = mock.MagicMock()
                smtp.send(EmailMessage())
        self.assertEqual(len(slept), 1)
        self.assertAlmostEqual(slept[0], 0.2 - 0.05, places=6)  # 5 sends per second

    def test_non_positive_or_non_finite_rates_are_refused(self):
        for bad in ("0", "-1", "nan", "inf", "-inf"):
            with self.assertRaises(ValueError):
                handler.send_rate(bad)
        self.assertEqual(handler.send_rate("2.5"), 2.5)

    def test_the_template_has_a_positive_minimum(self):
        template = (Path(__file__).resolve().parents[1] / "templates/central.yaml").read_text()
        self.assertGreater(float(re.search(r"MaxSendsPerSecond: \{[^}]*MinValue: ([0-9.]+)", template).group(1)), 0)


class ConfigValidationTest(unittest.TestCase):
    """The validator accepts only what the runtime can route: full-match ARNs and addresses, groups as a list."""

    def check(self, text):
        return mailcfg.parse(text.encode())[1]

    TOPIC = "arn:aws:sns:us-west-2:111122223333:orders"

    def test_trailing_line_feed_is_refused(self):
        arn_lf = json.dumps(self.TOPIC + "\n")
        mail_lf = json.dumps("a@example.com\n")
        self.assertTrue(
            self.check(f"schema: 1\ntopics:\n  - {{topicArn: {arn_lf}, mode: live, recipients: [a@example.com]}}\n")
        )
        self.assertTrue(
            self.check(f"schema: 1\ntopics:\n  - {{topicArn: {self.TOPIC}, mode: live, recipients: [{mail_lf}]}}\n")
        )
        self.assertEqual(
            self.check(
                f"schema: 1\ntopics:\n  - {{topicArn: {self.TOPIC}, mode: live, recipients: [a@example.com]}}\n"
            ),
            [],
        )

    def test_group_names_must_be_strings_so_they_cannot_collide(self):
        topic = "topics:\n  - {topicArn: " + self.TOPIC + ", mode: live, groups: ['1']}\n"
        numeric = self.check("schema: 1\ngroups:\n  1: {members: [a@example.com]}\n" + topic)
        self.assertTrue(any("must be a string" in e for e in numeric))  # a number key is refused, not turned into "1"
        both = self.check(
            "schema: 1\ngroups:\n  1: {members: [a@example.com]}\n  '1': {members: [b@example.com]}\n" + topic
        )
        self.assertTrue(any("must be a string" in e for e in both))  # no silent overwrite of one by the other
        self.assertEqual(self.check("schema: 1\ngroups:\n  '1': {members: [b@example.com]}\n" + topic), [])

    def test_duplicate_keys_are_refused_at_any_level(self):
        topic = "topics:\n  - {topicArn: " + self.TOPIC + ", mode: live, groups: [g]}\n"
        group_twice = "schema: 1\ngroups:\n  g: {members: [a@example.com]}\n  g: {members: [b@example.com]}\n" + topic
        errors = self.check(group_twice)
        self.assertEqual(len(errors), 1)
        self.assertIn("duplicate key 'g'", errors[0])
        self.assertIn("line 4", errors[0])  # the second occurrence
        named = mailcfg.parse(group_twice.encode(), "config/real.yaml")[1][0]
        self.assertIn("config/real.yaml", named)
        self.assertNotIn("<unicode string>", named)
        topic_twice = (
            "schema: 1\ngroups:\n  g: {members: [a@example.com]}\ntopics:\n  - {topicArn: %s, mode: live, mode: off}\n"
            % self.TOPIC
        )
        self.assertTrue(any("duplicate key 'mode'" in e for e in self.check(topic_twice)))
        top_level = "schema: 1\nschema: 1\n"
        self.assertTrue(any("duplicate key 'schema'" in e for e in self.check(top_level)))
        ok = "schema: 1\ngroups:\n  g: {members: [a@example.com]}\n" + topic
        self.assertEqual(self.check(ok), [])

    def test_a_bare_off_gets_a_quoting_hint(self):
        base = "schema: 1\ntopics:\n  - {topicArn: %s, recipients: [a@example.com], mode: %s}\n" % (self.TOPIC, "%s")
        bare = self.check(base % "off")  # YAML reads it as the boolean false
        self.assertTrue(any("mode must be one of" in e and 'write "off" in quotes' in e for e in bare))
        self.assertEqual(self.check(base % '"off"'), [])
        self.assertTrue(any("mode must be one of" in e and "quotes" not in e for e in self.check(base % "nope")))

    def test_the_example_config_is_valid_and_its_comment_quotes_off(self):
        example = Path(__file__).resolve().parents[1] / "config/examples/mail-config.example.yaml"
        self.assertEqual(mailcfg.load(str(example))[1], [])
        self.assertIn('"off" | "shadow"', example.read_text())

    def test_groups_must_be_a_list_of_names(self):
        base = (
            "schema: 1\ngroups:\n  g: {members: [a@example.com]}\ntopics:\n  - topicArn: "
            + self.TOPIC
            + "\n    mode: live\n"
        )
        self.assertTrue(self.check(base + "    groups: g\n"))  # groups must be a list, not a scalar
        self.assertTrue(self.check(base + "    groups: [1]\n"))
        self.assertEqual(self.check(base + "    groups: [g]\n"), [])


class CommittedBlobTest(unittest.TestCase):
    """--from-git parses and publishes the committed blob, even if the working file changes after the checks."""

    def test_content_is_the_committed_blob_not_a_later_working_tree(self):
        repo = tempfile.mkdtemp()

        def git(*a):
            return subprocess.run(["git", "-C", repo, *a], check=True, capture_output=True)

        git("init", "-q")
        git("config", "user.email", "t@example.com")
        git("config", "user.name", "t")
        Path(repo, "cfg.yaml").write_bytes(b"schema: 1\n# committed\n")
        git("add", ".")
        git("commit", "-qm", "c")
        real_run = subprocess.run

        def run(cmd, *args, **kwargs):
            result = real_run(cmd, *args, **kwargs)
            if cmd[:2] == ["git", "status"]:  # the file changes right after the clean check
                Path(repo, "cfg.yaml").write_bytes(b"schema: 1\n# changed after the check\n")
            return result

        cwd = os.getcwd()
        os.chdir(repo)
        try:
            with mock.patch.object(mailcfg.subprocess, "run", side_effect=run):
                commit, data = mailcfg.committed("cfg.yaml")
        finally:
            os.chdir(cwd)
        self.assertRegex(commit, r"^[0-9a-f]{40}$")
        self.assertEqual(data, b"schema: 1\n# committed\n")

    def test_blob_and_change_id_come_from_the_same_head(self):
        repo = tempfile.mkdtemp()

        def git(*a):
            return subprocess.run(["git", "-C", repo, *a], check=True, capture_output=True, text=True).stdout.strip()

        git("init", "-q")
        git("config", "user.email", "t@example.com")
        git("config", "user.name", "t")
        Path(repo, "cfg.yaml").write_text("schema: 1\n# first\n")
        git("add", ".")
        git("commit", "-qm", "first")
        first = git("rev-parse", "HEAD")
        real_run = subprocess.run
        moved = []

        def run(cmd, *args, **kwargs):
            result = real_run(cmd, *args, **kwargs)
            if cmd[-2:] == ["rev-parse", "HEAD"] and not moved:  # HEAD moves right after it was resolved
                moved.append(True)
                Path(repo, "cfg.yaml").write_text("schema: 1\n# second\n")
                git("commit", "-qam", "second")
            return result

        cwd = os.getcwd()
        os.chdir(repo)
        try:
            with mock.patch.object(mailcfg.subprocess, "run", side_effect=run):
                commit, data = mailcfg.committed("cfg.yaml")
        finally:
            os.chdir(cwd)
        self.assertTrue(moved)
        self.assertNotEqual(git("rev-parse", "HEAD"), first)
        self.assertEqual((commit, data), (first, b"schema: 1\n# first\n"))


class DeployParameterTest(unittest.TestCase):
    """The prefix-list lookup happens only when DynamoDbPrefixListId was not passed."""

    def run_stack(self, pairs):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
        import deploy

        created, ec2_calls = [], []
        missing = deploy.ClientError(
            {"Error": {"Code": "ValidationError", "Message": "does not exist"}}, "DescribeStacks"
        )
        state = {"n": 0}

        def describe_stacks(StackName):
            state["n"] += 1
            if state["n"] == 1:
                raise missing
            return {"Stacks": [{"Outputs": []}]}

        cfn = NS(
            validate_template=lambda TemplateBody: {"Parameters": [{"ParameterKey": "DynamoDbPrefixListId"}]},
            describe_stacks=describe_stacks,
            create_stack=lambda **kw: created.append(kw["Parameters"]),
            get_waiter=lambda name: NS(wait=lambda **kw: None),
        )
        ec2 = NS(
            describe_managed_prefix_lists=lambda Filters: ec2_calls.append(Filters)
            or {"PrefixLists": [{"PrefixListId": "pl-looked-up"}]}
        )
        session = NS(region_name="us-west-2", client=lambda name: cfn if name == "cloudformation" else ec2)
        with contextlib.redirect_stdout(io.StringIO()):
            deploy.stack(session, "s", "templates/central.yaml", pairs)
        return created[0], ec2_calls

    def test_no_lookup_when_the_parameter_is_given(self):
        params, ec2_calls = self.run_stack(["DynamoDbPrefixListId=pl-given"])
        self.assertEqual(ec2_calls, [])
        self.assertEqual(params, [{"ParameterKey": "DynamoDbPrefixListId", "ParameterValue": "pl-given"}])

    def test_lookup_when_it_is_absent(self):
        params, ec2_calls = self.run_stack([])
        self.assertEqual(len(ec2_calls), 1)
        self.assertEqual(params[0]["ParameterValue"], "pl-looked-up")


class TestSmtpKeyParameterTest(unittest.TestCase):
    """The first CMK upgrade supplies the key ARN; later updates may retain it."""

    def run_update(self, pairs, existing_parameters):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
        import deploy

        updates = []
        cfn = NS(
            validate_template=lambda TemplateBody: {"Parameters": [{"ParameterKey": "DataKeyArn"}]},
            describe_stacks=lambda **kw: {
                "Stacks": [{"StackStatus": "UPDATE_COMPLETE", "Parameters": existing_parameters, "Outputs": []}]
            },
            update_stack=lambda **kw: updates.append(kw["Parameters"]),
            get_waiter=lambda name: NS(wait=lambda **kw: None),
        )
        session = NS(client=lambda name: cfn)
        with contextlib.redirect_stdout(io.StringIO()):
            deploy.stack(session, "s", "templates/test-smtp.yaml", pairs)
        return updates[0]

    def test_first_upgrade_passes_the_new_key_parameter(self):
        self.assertEqual(
            self.run_update(["DataKeyArn=key-arn-placeholder"], []),
            [{"ParameterKey": "DataKeyArn", "ParameterValue": "key-arn-placeholder"}],
        )

    def test_later_updates_can_retain_the_key_parameter(self):
        self.assertEqual(
            self.run_update([], [{"ParameterKey": "DataKeyArn", "ParameterValue": "key-arn-placeholder"}]),
            [{"ParameterKey": "DataKeyArn", "UsePreviousValue": True}],
        )


class CleanupFailClosedTest(unittest.TestCase):
    """cleanup.py stops on lookup errors that are not "does not exist", re-checks the bucket tag before every delete
    batch and before the bucket itself, and reports skipped actions as skipped."""

    TAG = [{"Key": "project", "Value": "sample-sns-email"}]

    def fake(self, describe_error=None, bucket_tag_calls=99, pages=2):
        calls, tagging = [], []
        resources = {"ConfigTable": "t-config", "MigrationTable": "t-migration", "DeliveryTable": "t-delivery"}

        def describe_stacks(StackName):
            if StackName != "p-central":
                raise cleanup.ClientError(
                    {"Error": {"Code": "ValidationError", "Message": f"Stack with id {StackName} does not exist"}},
                    "DescribeStacks",
                )
            if describe_error:
                raise describe_error
            return {
                "Stacks": [
                    {"Tags": self.TAG, "Parameters": [{"ParameterKey": "CodeBucket", "ParameterValue": "bucket"}]}
                ]
            }

        def get_bucket_tagging(Bucket):
            tagging.append(Bucket)
            return {"TagSet": self.TAG if len(tagging) <= bucket_tag_calls else []}

        page = {"Versions": [{"Key": "k", "VersionId": "v"}]}
        fake = NS(
            describe_stacks=describe_stacks,
            describe_stack_resources=lambda StackName: {
                "StackResources": [{"LogicalResourceId": k, "PhysicalResourceId": v} for k, v in resources.items()]
            },
            describe_table=lambda TableName: {
                "Table": {"TableArn": "arn:" + TableName, "DeletionProtectionEnabled": False}
            },
            list_tags_of_resource=lambda ResourceArn: {"Tags": self.TAG},
            get_bucket_tagging=get_bucket_tagging,
            get_paginator=lambda name: NS(paginate=lambda **kw: [page] * pages),
            get_waiter=lambda name: NS(wait=lambda **kw: None),
            **{
                op: (lambda op: lambda **kw: calls.append(op))(op)
                for op in ("update_table", "delete_table", "delete_stack", "delete_objects", "delete_bucket")
            },
        )
        return fake, calls

    def run_main(self, fake, out=None):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
        session = NS(client=lambda name, **kw: fake)
        with mock.patch.object(cleanup.boto3, "Session", return_value=session), mock.patch.object(
            sys, "argv", ["cleanup.py", "--region", "us-west-2", "--prefix", "p", "--execute"]
        ):
            cleanup.main()

    def test_a_failed_stack_lookup_is_not_a_missing_stack(self):
        denied = cleanup.ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "DescribeStacks")
        fake, calls = self.fake(describe_error=denied)
        with self.assertRaises(cleanup.ClientError):
            self.run_main(fake)
        self.assertEqual(calls, [])

    def test_a_bucket_retagged_between_batches_stops_the_deletion(self):
        # plan check, delete_bucket start, first batch pass; the check before the second batch no longer sees the tag
        fake, calls = self.fake(bucket_tag_calls=3)
        with self.assertRaises(SystemExit):
            self.run_main(fake)
        self.assertEqual(calls.count("delete_objects"), 1)
        self.assertNotIn("delete_bucket", calls)

    def test_a_bucket_retagged_before_its_own_deletion_is_kept(self):
        fake, calls = self.fake(bucket_tag_calls=4)  # both batches pass, the check before delete_bucket does not
        with self.assertRaises(SystemExit):
            self.run_main(fake)
        self.assertEqual(calls.count("delete_objects"), 2)
        self.assertNotIn("delete_bucket", calls)

    def test_skipped_actions_are_not_reported_as_done(self):
        fake, calls = self.fake()
        gone = {"Error": {"Code": "ValidationError", "Message": "Stack with id p-central does not exist"}}
        state = {"n": 0}
        original = fake.describe_stacks

        def vanishing(StackName):  # present while the plan is made, gone when the deletion runs
            state["n"] += 1
            if StackName == "p-central" and state["n"] > 3:
                raise cleanup.ClientError(gone, "DescribeStacks")
            return original(StackName)

        fake.describe_stacks = vanishing
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.run_main(fake)
        self.assertNotIn("delete_stack", calls)
        self.assertIn("skipped (nothing to do): delete stack p-central", out.getvalue())
        self.assertNotIn("done: delete stack p-central", out.getvalue())


class FromGitTest(unittest.TestCase):
    def test_only_a_tracked_unchanged_committed_file_is_accepted(self):
        repo = tempfile.mkdtemp()

        def git(*a):
            return subprocess.run(["git", "-C", repo, *a], check=True, capture_output=True)

        git("init", "-q")
        git("config", "user.email", "t@example.com")
        git("config", "user.name", "t")
        Path(repo, ".gitignore").write_text("ignored.yaml\n")
        Path(repo, "cfg.yaml").write_text("schema: 1\n")
        git("add", ".")
        git("commit", "-qm", "c")
        Path(repo, "ignored.yaml").write_text("schema: 1\n")
        Path(repo, "untracked.yaml").write_text("schema: 1\n")
        cwd = os.getcwd()
        os.chdir(repo)
        try:
            self.assertRegex(mailcfg.git_change_id("cfg.yaml"), r"^[0-9a-f]{40}$")
            for path in ("ignored.yaml", "untracked.yaml"):
                with self.assertRaises(SystemExit):
                    mailcfg.git_change_id(path)
            Path(repo, "cfg.yaml").write_text("schema: 1\ntopics: []\n")
            with self.assertRaises(SystemExit):
                mailcfg.git_change_id("cfg.yaml")
        finally:
            os.chdir(cwd)


class TestSmtpTemplateRulesTest(unittest.TestCase):
    def test_ses_delivery_mode_is_rejected_in_the_china_partition(self):
        template = (Path(__file__).resolve().parents[1] / "templates/test-smtp.yaml").read_text()
        rules = template.split("\nRules:\n", 1)[1].split("\nConditions:", 1)[0]
        self.assertIn('RuleCondition: !Equals [!Ref "AWS::Partition", aws-cn]', rules)
        self.assertIn("Assert: !Equals [!Ref DeliveryMode, store]", rules)


if __name__ == "__main__":
    unittest.main()
