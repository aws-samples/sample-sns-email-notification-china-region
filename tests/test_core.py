"""Core invariants only: no duplicate or lost mail, shadow sends nothing, configuration errors are never silent,
and published configuration versions cannot overwrite each other. No AWS access (in-memory fakes)."""

import json
import os
import smtplib
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src/mailer"), str(ROOT / "tools")]
os.environ.update(
    CONFIG_TABLE="cfg",
    DELIVERY_TABLE="del",
    SMTP_SECRET_ARN="s",
    SMTP_HOST="smtp.example.com",
    MAIL_FROM="n@example.com",
    MAX_SENDS_PER_SECOND="1000",
)
import handler  # noqa: E402
import mailcfg  # noqa: E402

TOPIC = "arn:aws-cn:sns:cn-north-1:111122223333:orders"


def failed():
    return ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "op")


class FakeDynamo:
    """Just enough of DynamoDB for the conditions the code relies on."""

    def __init__(self):
        self.tables = {}

    def rows(self, table):
        return self.tables.setdefault(table, {})

    def get_item(self, TableName, Key, **_):
        item = self.rows(TableName).get((Key["pk"]["S"], Key["sk"]["S"]))
        return {"Item": dict(item)} if item else {}

    def put_item(self, TableName, Item, ConditionExpression=None, ExpressionAttributeValues=None, **_):
        key = (Item["pk"]["S"], Item["sk"]["S"])
        old = self.rows(TableName).get(key)
        if ConditionExpression and old is not None:
            # handler.claim: attribute_not_exists(pk) OR (status = claimed AND leaseUntil < now)
            expired = old["status"]["S"] == "claimed" and int(old["leaseUntil"]["N"]) < int(
                ExpressionAttributeValues[":now"]["N"]
            )
            if not expired:
                raise failed()
        self.rows(TableName)[key] = dict(Item)

    def update_item(self, TableName, Key, UpdateExpression, ConditionExpression, ExpressionAttributeValues, **_):
        key = (Key["pk"]["S"], Key["sk"]["S"])
        item = self.rows(TableName).get(key)
        if not item or item.get("lease", {}).get("S") != ExpressionAttributeValues[":t"]["S"]:
            raise failed()
        item["status"] = ExpressionAttributeValues[":s"]
        item.pop("lease", None)

    def delete_item(self, TableName, Key, ExpressionAttributeValues, **_):
        key = (Key["pk"]["S"], Key["sk"]["S"])
        if self.rows(TableName).get(key, {}).get("lease", {}).get("S") == ExpressionAttributeValues[":t"]["S"]:
            del self.rows(TableName)[key]

    def transact_write_items(self, TransactItems):
        for t in TransactItems:  # all conditions first, then all writes
            p = t["Put"]
            old = self.rows(p["TableName"]).get((p["Item"]["pk"]["S"], p["Item"]["sk"]["S"]))
            cond = p.get("ConditionExpression")
            if (
                cond == "attribute_not_exists(pk)"
                and old is not None
                or cond == "version = :v"
                and (old is None or old["version"]["N"] != p["ExpressionAttributeValues"][":v"]["N"])
            ):
                raise ClientError({"Error": {"Code": "TransactionCanceledException"}}, "TransactWriteItems")
        for t in TransactItems:
            p = t["Put"]
            self.rows(p["TableName"])[(p["Item"]["pk"]["S"], p["Item"]["sk"]["S"])] = dict(p["Item"])


class FakeSmtp:
    def __init__(self, fail=None):
        self.sent, self.fail = [], fail

    def send(self, message):
        if self.fail:
            raise self.fail
        self.sent.append(message["To"])

    def close(self):
        pass


def topic(ddb, mode="live", recipients=("a@example.com", "b@example.com"), groups=()):
    ddb.rows("cfg")[("TOPIC#" + TOPIC, "CURRENT")] = mailcfg.to_ddb(
        {"mode": mode, "recipients": list(recipients), "groups": list(groups), "format": "text", "version": 1}
    )


def record(message_id="m-1", receive=1):
    body = {"Type": "Notification", "MessageId": message_id, "TopicArn": TOPIC, "Subject": "Hello", "Message": "body"}
    return {
        "messageId": "sqs-" + message_id,
        "body": json.dumps(body),
        "attributes": {"ApproximateReceiveCount": str(receive)},
    }


class MailerTest(unittest.TestCase):
    def setUp(self):
        self.ddb = FakeDynamo()
        handler._ddb, handler._cache = self.ddb, {}

    def run_records(self, smtp, *records):
        with mock.patch.object(handler, "Smtp", return_value=smtp):
            return handler.lambda_handler({"Records": list(records)}, None)["batchItemFailures"]

    def test_redelivered_message_mails_each_recipient_once(self):
        topic(self.ddb)
        smtp = FakeSmtp()
        self.assertEqual(self.run_records(smtp, record(), record()), [])
        self.assertEqual(self.run_records(smtp, record(receive=2)), [])
        self.assertEqual(sorted(smtp.sent), ["a@example.com", "b@example.com"])

    def test_temporary_failure_retries_only_unsent_recipients(self):
        topic(self.ddb)
        calls = []

        class FlakySecond(FakeSmtp):
            def send(self, message):
                calls.append(message["To"])
                if message["To"] == "b@example.com" and calls.count("b@example.com") == 1:
                    raise smtplib.SMTPServerDisconnected("down")
                self.sent.append(message["To"])

        smtp = FlakySecond()
        self.assertEqual(self.run_records(smtp, record()), [{"itemIdentifier": "sqs-m-1"}])  # back to SQS
        self.assertEqual(self.run_records(smtp, record(receive=2)), [])
        self.assertEqual(smtp.sent, ["a@example.com", "b@example.com"])  # a once, b after the retry

    def test_permanent_refusal_is_recorded_not_retried(self):
        topic(self.ddb, recipients=("a@example.com",))
        smtp = FakeSmtp(fail=smtplib.SMTPRecipientsRefused({"a@example.com": (550, b"no such user")}))
        self.assertEqual(self.run_records(smtp, record()), [])
        status = self.ddb.rows("del")[("MSG#m-1", handler.recipient_key("a@example.com"))]["status"]["S"]
        self.assertEqual(status, "rejected")

    def test_shadow_sends_nothing_and_records_recipients(self):
        topic(self.ddb, mode="shadow")
        smtp = FakeSmtp()
        self.assertEqual(self.run_records(smtp, record()), [])
        self.assertEqual(smtp.sent, [])
        self.assertEqual({v["status"]["S"] for v in self.ddb.rows("del").values()}, {"shadow"})

    def test_unconfigured_or_off_topic_goes_back_to_sqs(self):
        smtp = FakeSmtp()
        self.assertEqual(self.run_records(smtp, record()), [{"itemIdentifier": "sqs-m-1"}])
        handler._cache = {}
        topic(self.ddb, mode="off")
        self.assertEqual(self.run_records(smtp, record()), [{"itemIdentifier": "sqs-m-1"}])
        self.assertEqual(smtp.sent, [])

    def test_missing_group_never_sends_a_partial_list(self):
        topic(self.ddb, groups=("ops",))
        smtp = FakeSmtp()
        self.assertEqual(self.run_records(smtp, record()), [{"itemIdentifier": "sqs-m-1"}])
        self.assertEqual(smtp.sent, [])

    def test_raw_delivery_or_header_injection_is_refused(self):
        topic(self.ddb)
        smtp = FakeSmtp()
        raw = {"messageId": "x", "body": "plain text, not an SNS envelope"}
        bad = record()
        bad["body"] = json.dumps({**json.loads(bad["body"]), "Subject": "a\r\nBcc: evil@example.org"})
        self.assertEqual(len(self.run_records(smtp, raw, bad)), 2)
        self.assertEqual(smtp.sent, [])
        self.assertEqual(self.ddb.rows("del"), {})  # refused before any recipient was claimed

    def test_expired_lease_can_be_reclaimed(self):
        topic(self.ddb, recipients=("a@example.com",))
        state, _ = handler.claim("m-2", "a@example.com", TOPIC, 1, "live")
        self.assertEqual(state, "claimed")
        self.assertEqual(handler.claim("m-2", "a@example.com", TOPIC, 1, "live")[0], "busy")
        self.ddb.rows("del")[("MSG#m-2", handler.recipient_key("a@example.com"))]["leaseUntil"] = {
            "N": str(int(time.time()) - 1)
        }
        self.assertEqual(handler.claim("m-2", "a@example.com", TOPIC, 1, "live")[0], "claimed")


class ConfigTest(unittest.TestCase):
    def config(self, text):
        f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
        f.write(text)
        f.close()
        return mailcfg.load(f.name)

    def test_validation_rejects_bad_input(self):
        _, errors = self.config("""schema: 1
allowedDomains: [example.com]
topics:
  - {topicArn: "arn:aws:sns:us-west-2:111122223333:t", mode: live, recipients: [a@example.org]}
  - {topicArn: "not-an-arn", mode: live, recipients: [a@example.com]}
  - {topicArn: "arn:aws:sns:us-west-2:111122223333:u", mode: loud, recipients: [a@example.com], groups: [none]}
  - {topicArn: "arn:aws:sns:us-west-2:111122223333:v", mode: live, recipients: [], defaultSubject: "x\\ny"}
""")
        text = "\n".join(errors)
        for expected in (
            "not in allowedDomains",
            "invalid topic ARN",
            "mode must be",
            "unknown group",
            "no recipients",
            "line break",
        ):
            self.assertIn(expected, text)

    def test_versions_are_append_only_and_concurrent_publishes_conflict(self):
        ddb = FakeDynamo()
        key = "TOPIC#" + TOPIC
        v1 = {"mode": "live", "recipients": ["a@example.com"], "groups": [], "format": "text"}
        self.assertEqual(mailcfg.write(ddb, "cfg", key, v1, 0, "c1"), 1)
        self.assertEqual(mailcfg.write(ddb, "cfg", key, {**v1, "recipients": ["b@example.com"]}, 1, "c2"), 2)
        with self.assertRaises(ClientError):  # planned against version 1, but it is at 2 now
            mailcfg.write(ddb, "cfg", key, {**v1, "recipients": ["c@example.com"]}, 1, "c3")
        rows = ddb.rows("cfg")
        self.assertEqual(sorted(sk for pk, sk in rows if pk == key), ["CURRENT", "V#000001", "V#000002"])
        self.assertEqual(mailcfg.from_ddb(rows[(key, "CURRENT")])["recipients"], ["b@example.com"])
        self.assertEqual(mailcfg.from_ddb(rows[(key, "V#000001")])["recipients"], ["a@example.com"])


if __name__ == "__main__":
    unittest.main()
