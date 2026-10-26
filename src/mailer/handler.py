"""SQS (SNS envelope) -> DynamoDB routing -> authenticated STARTTLS SMTP.

One function serves every queue shard. Delivery is at-least-once (SQS); each (SNS MessageId, recipient)
is claimed in the delivery table with a conditional write and a lease before it is sent, so a redelivered
or replayed message does not mail a recipient with a terminal record (sent, rejected, shadow) again. Gaps
remain where the outcome is uncertain: the SMTP server accepted the message but the final reply was lost, or
the function stopped (or failed to write the record or to release the claim) before the outcome was recorded;
that recipient is mailed again, after the lease has expired where the claim was not released. Delivery is
at-least-once. Message bodies, addresses and SMTP replies are never logged.
"""

import hashlib
import json
import logging
import math
import os
import re
import smtplib
import ssl
import time
import uuid
from email.message import EmailMessage
from email.utils import formatdate
from typing import Any, Callable

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)
AWS = Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 2, "mode": "standard"})
CACHE_SECONDS = int(os.environ.get("CONFIG_CACHE_SECONDS", "60"))
LEASE_SECONDS = int(os.environ.get("LEASE_SECONDS", "300"))
TTL_DAYS = int(os.environ.get("DELIVERY_TTL_DAYS", "30"))


def send_rate(value: str) -> float:
    """MAX_SENDS_PER_SECOND as a positive, finite number; anything else stops the function at start-up."""
    rate = float(value)
    if not math.isfinite(rate) or rate <= 0:
        raise ValueError("MAX_SENDS_PER_SECOND must be a positive, finite number")
    return rate


MAX_PER_SECOND = send_rate(os.environ.get("MAX_SENDS_PER_SECOND", "5"))
SECRET_SECONDS = 3600
SMTP_TIMEOUT = 10
# Time kept back before starting a recipient: one SMTP exchange (connect, TLS, AUTH, send) plus the DynamoDB
# writes. The template requires a function timeout of at least 30 s, so every invocation has room for work.
RESERVE_MS = (SMTP_TIMEOUT + 2) * 1000
TERMINAL = ("sent", "rejected", "shadow")
SUBJECT_MAX = 100  # SNS caps Subject at 100 characters

_ddb = None
_cache: dict[str, tuple[float, Any]] = {}  # key -> (expires_at, value); config items and the SMTP secret
_last_send = 0.0  # monotonic time of the last send in this execution environment (shared by consecutive invocations)


class Retry(Exception):
    """This record failed for now; SQS delivers it again (and to the DLQ after maxReceiveCount)."""


def ddb() -> Any:
    """The DynamoDB client, created once per execution environment."""
    global _ddb
    if _ddb is None:
        _ddb = boto3.client("dynamodb", config=AWS)
    return _ddb


def cached(key: str, load: Callable[[], Any], seconds: float) -> Any:
    """Return the cached value of `key`, calling `load` when it is missing or older than `seconds`."""
    hit = _cache.get(key)
    now = time.monotonic()
    if hit and hit[0] > now:
        return hit[1]
    value = load()
    _cache[key] = (now + seconds, value)
    return value


def _str_list(attr: dict | None) -> list[str]:
    return [v["S"] for v in attr.get("L", [])] if attr else []


def load_item(pk: str) -> dict | None:
    """CURRENT item of a topic or group, as plain values (eventually consistent, cached)."""

    def get() -> dict | None:
        item = (
            ddb()
            .get_item(TableName=os.environ["CONFIG_TABLE"], Key={"pk": {"S": pk}, "sk": {"S": "CURRENT"}})
            .get("Item")
        )
        if not item:
            return None
        return {
            "mode": item.get("mode", {}).get("S", "off"),
            "recipients": _str_list(item.get("recipients")),
            "groups": _str_list(item.get("groups")),
            "members": _str_list(item.get("members")),
            "format": item.get("format", {}).get("S", "text"),
            "defaultSubject": item.get("defaultSubject", {}).get("S", ""),
            "version": int(item.get("version", {}).get("N", "0")),
        }

    return cached("cfg:" + pk, get, CACHE_SECONDS)


def route(topic_arn: str) -> tuple[dict, list[str]] | None:
    """(config, recipients) for a topic, or None when it is not configured or switched off."""
    cfg = load_item("TOPIC#" + topic_arn)
    if not cfg or cfg["mode"] not in ("shadow", "live"):
        return None
    recipients = list(cfg["recipients"])
    for group in cfg["groups"]:
        g = load_item("GROUP#" + group)
        if g is None:
            raise Retry("missing group")  # a configuration error: alarm via DLQ, never a silent partial send
        recipients += g["members"]
    seen, unique = set(), []
    for r in recipients:
        if r.lower() not in seen:
            seen.add(r.lower())
            unique.append(r)
    return cfg, unique


def smtp_settings() -> tuple[dict, str | None]:
    """The SMTP credentials and the optional CA bundle (PEM) from Secrets Manager, cached for an hour."""

    def get() -> tuple[dict, str | None]:
        sm = boto3.client("secretsmanager", config=AWS)
        secret = json.loads(sm.get_secret_value(SecretId=os.environ["SMTP_SECRET_ARN"])["SecretString"])
        ca = None
        if os.environ.get("SMTP_CA_SECRET_ARN"):
            ca = sm.get_secret_value(SecretId=os.environ["SMTP_CA_SECRET_ARN"])["SecretString"].strip() or None
        return secret, ca

    return cached("smtp", get, SECRET_SECONDS)


class Smtp:
    """One authenticated STARTTLS session per invocation, opened on first use. Sends are rate limited per execution
    environment: the time of the last send outlives the session, so consecutive invocations share one limit."""

    def __init__(self) -> None:
        self.conn = None

    def send(self, message: EmailMessage) -> None:
        """Send one message, opening the session first if needed. Raises smtplib errors on refusal."""
        if self.conn is None:
            secret, ca = smtp_settings()
            context = (
                ssl.create_default_context(cadata=ca) if ca else ssl.create_default_context()
            )  # hostname and chain verified
            conn = smtplib.SMTP(os.environ["SMTP_HOST"], int(os.environ.get("SMTP_PORT", "587")), timeout=SMTP_TIMEOUT)
            conn.ehlo()
            conn.starttls(context=context)
            conn.ehlo()
            conn.login(secret["username"], secret["password"])
            self.conn = conn
        global _last_send
        wait = _last_send + 1 / MAX_PER_SECOND - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_send = time.monotonic()
        self.conn.send_message(message)  # raises on refusal; one recipient per message

    def close(self) -> None:
        """Quit the session if one is open; errors while quitting are ignored."""
        if self.conn is not None:
            try:
                self.conn.quit()
            except (smtplib.SMTPException, OSError):
                pass  # the session is discarded either way
            self.conn = None


def recipient_key(address: str) -> str:
    """Delivery-record sort key for an address: a hash, so the table holds no addresses."""
    return "R#" + hashlib.sha256(address.strip().lower().encode()).hexdigest()[:32]


def claim(message_id: str, address: str, topic: str, version: int, mode: str) -> tuple[str, str | None]:
    """'claimed' (we own it), 'done' (a terminal record: sent / rejected / shadow) or 'busy' (anything else: another
    attempt holds the lease, or released it between our write and read; retried, never acknowledged)."""
    now, token = int(time.time()), uuid.uuid4().hex
    try:
        ddb().put_item(
            TableName=os.environ["DELIVERY_TABLE"],
            Item={
                "pk": {"S": "MSG#" + message_id},
                "sk": {"S": recipient_key(address)},
                "status": {"S": "claimed"},
                "lease": {"S": token},
                "leaseUntil": {"N": str(now + LEASE_SECONDS)},
                "topicArn": {"S": topic},
                "configVersion": {"N": str(version)},
                "mode": {"S": mode},
                "expiresAt": {"N": str(now + TTL_DAYS * 86400)},
            },
            ConditionExpression="attribute_not_exists(pk) OR (#s = :claimed AND leaseUntil < :now)",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":claimed": {"S": "claimed"}, ":now": {"N": str(now)}},
        )
        return "claimed", token
    except ClientError as error:
        if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
    item = (
        ddb()
        .get_item(
            TableName=os.environ["DELIVERY_TABLE"],
            ConsistentRead=True,
            Key={"pk": {"S": "MSG#" + message_id}, "sk": {"S": recipient_key(address)}},
        )
        .get("Item", {})
    )
    return ("done" if item.get("status", {}).get("S") in TERMINAL else "busy"), None


def finish(message_id: str, address: str, token: str, status: str, code: int | None = None) -> None:
    """Record the outcome of our own claim (only while we still hold its lease)."""
    values = {":s": {"S": status}, ":t": {"S": token}, ":at": {"N": str(int(time.time()))}}
    expr = "SET #s = :s, finishedAt = :at REMOVE lease, leaseUntil"
    if code is not None:
        values[":c"] = {"N": str(code)}
        expr = "SET #s = :s, finishedAt = :at, smtpCode = :c REMOVE lease, leaseUntil"
    ddb().update_item(
        TableName=os.environ["DELIVERY_TABLE"],
        Key={"pk": {"S": "MSG#" + message_id}, "sk": {"S": recipient_key(address)}},
        UpdateExpression=expr,
        ConditionExpression="lease = :t",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues=values,
    )


def release(message_id: str, address: str, token: str) -> None:
    """Give a claim back after a temporary failure, so the retry can send at once."""
    try:
        ddb().delete_item(
            TableName=os.environ["DELIVERY_TABLE"],
            Key={"pk": {"S": "MSG#" + message_id}, "sk": {"S": recipient_key(address)}},
            ConditionExpression="lease = :t",
            ExpressionAttributeValues={":t": {"S": token}},
        )
    except ClientError:
        pass


def build(envelope: dict, cfg: dict, address: str) -> EmailMessage:
    """The mail for one recipient: plain text, or the whole SNS envelope as JSON for the json format."""
    subject = (
        envelope.get("Subject") or cfg["defaultSubject"] or os.environ.get("DEFAULT_SUBJECT", "Amazon SNS Notification")
    )
    body = json.dumps(envelope, ensure_ascii=False, indent=2) if cfg["format"] == "json" else envelope["Message"]
    sender = os.environ["MAIL_FROM"]
    message = EmailMessage()
    message["From"] = sender
    message["To"] = address
    message["Subject"] = subject[:SUBJECT_MAX]
    message["Date"] = formatdate(localtime=False)
    message["Message-ID"] = f"<{envelope['MessageId']}.{recipient_key(address)[2:14]}@{sender.rsplit('@', 1)[-1]}>"
    message["X-SNS-Topic-Arn"] = envelope["TopicArn"]
    message.set_content(body)
    return message


def parse(record: dict) -> dict:
    """The SNS envelope of an SQS record. Raises ValueError for anything that is not a well-formed notification."""
    envelope = json.loads(record["body"])
    if envelope.get("Type") != "Notification":
        raise ValueError("not an SNS notification (raw message delivery must be off)")
    for key in ("TopicArn", "MessageId", "Message"):
        if not isinstance(envelope.get(key), str):
            raise ValueError("missing " + key)
    subject = envelope.get("Subject")
    if subject is not None and (not isinstance(subject, str) or re.search(r"[\r\n]", subject)):
        raise ValueError("invalid subject")  # header injection: refused before anything is claimed
    return envelope


def log(event: str, **fields: Any) -> None:
    """Write one structured log line; failures and alarms at ERROR level."""
    (LOG.error if event.endswith("failed") or event in ("config_missing", "mail_rejected") else LOG.info)(
        json.dumps({"event": event, **fields})
    )


def process(record: dict, smtp: Smtp, context: Any = None) -> None:
    """Route and mail one SQS record. Raises Retry when the record must be delivered again."""
    try:
        envelope = parse(record)
    except (ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
        log("message_invalid", sqsMessageId=record.get("messageId"), errorType=type(error).__name__)
        raise Retry("invalid") from error  # kept for inspection in the DLQ
    topic, message_id = envelope["TopicArn"], envelope["MessageId"]
    routed = route(topic)
    if routed is None:
        log("config_missing", topicArn=topic, snsMessageId=message_id)
        raise Retry("config missing")
    cfg, recipients = routed
    if not recipients:
        log("config_missing", topicArn=topic, snsMessageId=message_id, reason="no recipients")
        raise Retry("no recipients")
    sent = skipped = 0
    pending = False
    for address in recipients:
        if context is not None and context.get_remaining_time_in_millis() < RESERVE_MS:
            raise Retry("out of time")  # recipients done so far are recorded; the rest are retried
        state, token = claim(message_id, address, topic, cfg["version"], cfg["mode"])
        if state == "done":
            skipped += 1
            continue
        if state == "busy":
            pending = True
            continue
        if cfg["mode"] == "shadow":  # parallel run: resolve and record, send nothing
            finish(message_id, address, token, "shadow")
            continue
        try:
            smtp.send(build(envelope, cfg, address))
        except smtplib.SMTPRecipientsRefused as error:
            code = next(iter(error.recipients.values()))[0]
            if 500 <= code < 600:  # permanent: not retried, alarmed
                finish(message_id, address, token, "rejected", code)
                # logged now, not after the loop: a later temporary failure ends this invocation early, and the retry
                # skips the recorded rejection, so this would be the only chance to raise the alarm
                log("mail_rejected", topicArn=topic, snsMessageId=message_id)
                continue
            release(message_id, address, token)
            smtp.close()
            raise Retry("temporary refusal") from error
        except (smtplib.SMTPException, OSError) as error:
            release(message_id, address, token)
            smtp.close()
            log(
                "mail_failed",
                topicArn=topic,
                snsMessageId=message_id,
                errorType=type(error).__name__,
                receiveCount=record.get("attributes", {}).get("ApproximateReceiveCount"),
            )
            raise Retry("smtp") from error
        finish(message_id, address, token, "sent", 250)
        sent += 1
    log(
        "mail_shadow" if cfg["mode"] == "shadow" else "mail_sent",
        topicArn=topic,
        snsMessageId=message_id,
        configVersion=cfg["version"],
        recipients=len(recipients),
        sent=sent,
        duplicateSkipped=skipped,
        receiveCount=record.get("attributes", {}).get("ApproximateReceiveCount"),
    )
    if pending:
        raise Retry("another attempt holds a lease")


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda entry point for an SQS batch; reports failed records as batch item failures."""
    failures, smtp = [], Smtp()
    try:
        for record in event.get("Records", []):
            if context is not None and context.get_remaining_time_in_millis() < RESERVE_MS:
                failures.append({"itemIdentifier": record["messageId"]})  # leave time; SQS redelivers
                continue
            try:
                process(record, smtp, context)
            except Retry:
                failures.append({"itemIdentifier": record["messageId"]})
            except Exception as error:  # unexpected: this record only, never the whole batch
                log("record_failed", sqsMessageId=record.get("messageId"), errorType=type(error).__name__)
                failures.append({"itemIdentifier": record["messageId"]})
    finally:
        smtp.close()
    return {"batchItemFailures": failures}
