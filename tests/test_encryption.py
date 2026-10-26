"""Checks for shared-key encryption and the service-scoped access contract."""

import re
import unittest
from pathlib import Path
from unittest import mock

import yaml

ROOT = Path(__file__).resolve().parents[1]
KEY_ARN = {"Fn::GetAtt": ["DataKey", "Arn"]}


class CloudFormationLoader(yaml.SafeLoader):
    pass


def intrinsic(loader, tag, node):
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node)
    else:
        value = loader.construct_mapping(node)
    if tag == "GetAtt" and isinstance(value, str):
        value = value.split(".", 1)
    return {tag if tag == "Ref" else "Fn::" + tag: value}


CloudFormationLoader.add_multi_constructor("!", intrinsic)


def template(name):
    loader = CloudFormationLoader((ROOT / "templates" / name).read_text())
    try:
        return loader.get_single_data()
    finally:
        loader.dispose()


def statement_actions(statement):
    value = statement.get("Action", [])
    value = [value] if isinstance(value, str) else value
    if not isinstance(value, list) or not all(isinstance(action, str) for action in value):
        raise AssertionError("review nonliteral actions explicitly instead of skipping them")
    return value


def kms_statements(role):
    def expand(statement):
        if "Fn::If" in statement:
            yield from expand(statement["Fn::If"][1])
            yield from expand(statement["Fn::If"][2])
        else:
            yield statement

    statements = (
        item
        for policy in role["Properties"].get("Policies", [])
        for statement in policy["PolicyDocument"]["Statement"]
        for item in expand(statement)
    )
    return [s for s in statements if any(a == "*" or a.startswith("kms:") for a in statement_actions(s))]


class TemplateLoaderTest(unittest.TestCase):
    def test_intrinsics_and_scalar_types_are_preserved(self):
        text = "ref: !Ref DataKey\narn: !GetAtt DataKey.Arn\nchoice: !If [Enabled, true, false]\n"
        with mock.patch.object(Path, "read_text", return_value=text):
            self.assertEqual(
                template("fixture.yaml"),
                {
                    "ref": {"Ref": "DataKey"},
                    "arn": KEY_ARN,
                    "choice": {"Fn::If": ["Enabled", True, False]},
                },
            )

    def test_python_tags_are_rejected(self):
        with mock.patch.object(Path, "read_text", return_value="!!python/name:builtins.eval ''"):
            with self.assertRaises(yaml.constructor.ConstructorError):
                template("fixture.yaml")


class MigrationPolicyTest(unittest.TestCase):
    def test_unsubscribe_requires_an_explicit_topic_allowlist(self):
        member = template("member.yaml")
        self.assertIn("MigrationTopicArns", member["Parameters"])
        parameter = member["Parameters"]["MigrationTopicArns"]
        self.assertEqual(parameter["Type"], "CommaDelimitedList")
        self.assertEqual(parameter["Default"], "")
        self.assertEqual(
            member["Conditions"]["MigrationEnabled"],
            {"Fn::Not": [{"Fn::Equals": [{"Fn::Select": [0, {"Ref": "MigrationTopicArns"}]}, ""]}]},
        )
        policies = member["Resources"]["MigrationRole"]["Properties"]["Policies"]
        condition, enabled, disabled = policies["Fn::If"]
        self.assertEqual(condition, "MigrationEnabled")
        self.assertEqual(disabled, {"Ref": "AWS::NoValue"})
        statements = [statement for policy in enabled for statement in policy["PolicyDocument"]["Statement"]]
        self.assertEqual(
            statements,
            [{"Effect": "Allow", "Action": ["sns:Unsubscribe"], "Resource": {"Ref": "MigrationTopicArns"}}],
        )

    def test_migration_allowlist_accepts_topics_but_not_wildcards_or_subscriptions(self):
        member = template("member.yaml")
        self.assertIn("MigrationTopicArns", member["Parameters"])
        pattern = member["Parameters"]["MigrationTopicArns"]["AllowedPattern"]
        self.assertIsNotNone(re.fullmatch(pattern, ""))
        for partition, region in (("aws", "us-west-2"), ("aws-cn", "cn-north-1"), ("aws-cn", "cn-northwest-1")):
            arn = f"arn:{partition}:sns:{region}:123456789012:orders"
            with self.subTest(partition=partition, region=region):
                self.assertIsNotNone(re.fullmatch(pattern, arn))
                for value in ("*", arn.replace("orders", "*"), arn.replace("123456789012", "*"), arn + ":sub-id"):
                    self.assertIsNone(re.fullmatch(pattern, value))


class EncryptionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.central = template("central.yaml")
        cls.smtp = template("test-smtp.yaml")
        cls.resources = cls.central["Resources"]

    def key(self):
        keys = [r for r in self.resources.values() if r["Type"] == "AWS::KMS::Key"]
        self.assertEqual(len(keys), 1, "all sample data must use one customer-managed key")
        return keys[0]

    def test_key_rotates_and_survives_stack_deletion_or_replacement(self):
        key = self.key()
        self.assertIs(key["Properties"]["EnableKeyRotation"], True)
        self.assertEqual(key.get("DeletionPolicy"), "Retain")
        self.assertEqual(key.get("UpdateReplacePolicy"), "Retain")
        aliases = [r for r in self.resources.values() if r["Type"] == "AWS::KMS::Alias"]
        self.assertEqual(len(aliases), 1)
        self.assertEqual(aliases[0]["Properties"]["TargetKeyId"], {"Ref": "DataKey"})

    def test_every_table_secret_queue_and_dlq_uses_the_shared_key(self):
        counts = {"AWS::DynamoDB::Table": 0, "AWS::SecretsManager::Secret": 0, "AWS::SQS::Queue": 0}
        for name, resource in self.resources.items():
            kind = resource["Type"]
            if kind not in counts:
                continue
            counts[kind] += 1
            props = resource["Properties"]
            with self.subTest(resource=name):
                if kind == "AWS::DynamoDB::Table":
                    self.assertEqual(
                        props.get("SSESpecification"),
                        {"SSEEnabled": True, "SSEType": "KMS", "KMSMasterKeyId": KEY_ARN},
                    )
                elif kind == "AWS::SecretsManager::Secret":
                    self.assertEqual(props.get("KmsKeyId"), KEY_ARN)
                else:
                    self.assertEqual(props.get("KmsMasterKeyId"), KEY_ARN)
                    self.assertNotIn("SqsManagedSseEnabled", props)
        self.assertEqual(counts, {"AWS::DynamoDB::Table": 3, "AWS::SecretsManager::Secret": 2, "AWS::SQS::Queue": 6})

    def test_delivery_table_has_point_in_time_recovery(self):
        self.assertEqual(
            self.resources["DeliveryTable"]["Properties"].get("PointInTimeRecoverySpecification"),
            {"PointInTimeRecoveryEnabled": True},
        )

    def test_resolved_encryption_and_backup_checks_are_not_suppressed(self):
        for name, resource in self.resources.items():
            skips = resource.get("Metadata", {}).get("checkov", {}).get("skip", [])
            with self.subTest(resource=name):
                self.assertFalse({r["id"] for r in skips} & {"CKV_AWS_119", "CKV_AWS_149", "CKV_AWS_27", "CKV_AWS_28"})

    def test_sns_key_permissions_use_each_active_shards_exact_topic_allowlist(self):
        statements = self.key()["Properties"]["KeyPolicy"]["Statement"]
        sns = {}
        for entry in statements:
            condition = None
            if "Fn::If" in entry:
                condition, entry, otherwise = entry["Fn::If"]
                self.assertEqual(otherwise, {"Ref": "AWS::NoValue"})
            if entry.get("Principal") == {"Service": "sns.amazonaws.com"}:
                sns[entry["Sid"]] = (condition, entry)
        self.assertEqual(len(sns), 3)
        for shard, condition in ((1, None), (2, "Shard2"), (3, "Shard3")):
            policy = self.resources[f"Shard{shard}Policy"]["Properties"]["PolicyDocument"]["Statement"][0]
            actual_condition, entry = sns[f"AllowShard{shard}Topics"]
            self.assertEqual(actual_condition, condition)
            self.assertEqual(entry["Condition"], policy["Condition"])
            self.assertEqual(entry["Condition"], {"ArnEquals": {"aws:SourceArn": {"Ref": f"Shard{shard}TopicArns"}}})
            self.assertEqual(entry["Effect"], "Allow")
            self.assertEqual(set(entry["Action"]), {"kms:Decrypt", "kms:GenerateDataKey*"})
            self.assertEqual(entry["Resource"], "*")  # In a key policy this means this key, not all keys.

    def test_topic_parameters_reject_wildcards_but_preserve_the_empty_default(self):
        account = "1" * 12
        for shard in (1, 2, 3):
            pattern = self.central["Parameters"][f"Shard{shard}TopicArns"].get("AllowedPattern")
            self.assertIsNotNone(pattern)
            self.assertIsNotNone(re.fullmatch(pattern, ""))
            for partition, region in (("aws", "us-west-2"), ("aws-cn", "cn-north-1"), ("aws-cn", "cn-northwest-1")):
                arn = f"arn:{partition}:sns:{region}:{account}:orders"
                self.assertIsNotNone(re.fullmatch(pattern, arn))
                self.assertIsNone(re.fullmatch(pattern, arn.replace("orders", "*")))
                self.assertIsNone(re.fullmatch(pattern, arn.replace(account, "*")))

    def test_key_administration_is_partition_aware_and_delegates_to_account_iam(self):
        statements = self.key()["Properties"]["KeyPolicy"]["Statement"]
        admin = [s for s in statements if s.get("Sid") == "EnableAccountPermissions"]
        self.assertEqual(len(admin), 1)
        self.assertEqual(
            admin[0]["Principal"], {"AWS": {"Fn::Sub": "arn:${AWS::Partition}:iam::${AWS::AccountId}:root"}}
        )
        self.assertEqual(admin[0]["Action"], "kms:*")
        self.assertEqual(admin[0]["Resource"], "*")
        self.assertEqual(admin[0]["Effect"], "Allow")

    def test_mailer_decrypt_is_scoped_to_the_key_and_integrated_services(self):
        kms = kms_statements(self.resources["MailerRole"])
        self.assertEqual(len(kms), 3)
        by_sid = {s["Sid"]: s for s in kms}
        for sid, service in (
            ("DecryptQueueMessages", "sqs"),
            ("DecryptTableKeys", "dynamodb"),
            ("DecryptSmtpSecrets", "secretsmanager"),
        ):
            statement = by_sid[sid]
            self.assertEqual(statement_actions(statement), ["kms:Decrypt"])
            self.assertEqual(statement["Effect"], "Allow")
            self.assertEqual(statement["Resource"], KEY_ARN)
            conditions = statement["Condition"]["StringEquals"]
            self.assertEqual(conditions["kms:CallerAccount"], {"Ref": "AWS::AccountId"})
            self.assertEqual(conditions["kms:ViaService"], {"Fn::Sub": f"{service}.${{AWS::Region}}.amazonaws.com"})
        table_context = by_sid["DecryptTableKeys"]["Condition"]["StringEquals"]
        self.assertEqual(
            table_context["kms:EncryptionContext:aws:dynamodb:tableName"],
            [{"Ref": "ConfigTable"}, {"Ref": "DeliveryTable"}],
        )
        self.assertEqual(table_context["kms:EncryptionContext:aws:dynamodb:subscriberId"], {"Ref": "AWS::AccountId"})
        self.assertEqual(
            by_sid["DecryptSmtpSecrets"]["Condition"]["StringEquals"]["kms:EncryptionContext:SecretARN"],
            [{"Ref": "SmtpSecret"}, {"Ref": "SmtpCaSecret"}],
        )

    def test_smtp_secret_reads_and_writes_have_scoped_kms_permissions(self):
        self.assertIn("DataKeyArn", self.smtp["Parameters"])
        kms = kms_statements(self.smtp["Resources"]["HostRole"])
        self.assertEqual(len(kms), 2)
        by_sid = {s["Sid"]: s for s in kms}
        for sid, actions, secret in (
            ("ReadSmtpSecretKey", {"kms:Decrypt"}, "SmtpSecretArn"),
            ("WriteSmtpCaKey", {"kms:Decrypt", "kms:GenerateDataKey"}, "SmtpCaSecretArn"),
        ):
            statement = by_sid[sid]
            self.assertEqual(set(statement_actions(statement)), actions)
            self.assertEqual(statement["Effect"], "Allow")
            self.assertEqual(statement["Resource"], {"Ref": "DataKeyArn"})
            self.assertEqual(
                statement["Condition"]["StringEquals"],
                {
                    "kms:ViaService": {"Fn::Sub": "secretsmanager.${AWS::Region}.amazonaws.com"},
                    "kms:CallerAccount": {"Ref": "AWS::AccountId"},
                    "kms:EncryptionContext:SecretARN": {"Ref": secret},
                },
            )
        self.assertEqual(self.central["Outputs"].get("DataKeyArn"), {"Value": KEY_ARN})

    def test_permission_guard_includes_scalar_wildcards_and_secondary_policies(self):
        from copy import deepcopy

        role = deepcopy(self.resources["MailerRole"])
        before = kms_statements(role)
        for action in ("kms:*", "*"):
            with self.subTest(action=action):
                grant = {"Effect": "Allow", "Action": action, "Resource": "*"}
                role["Properties"]["Policies"].append({"PolicyDocument": {"Statement": [grant]}})
                self.assertEqual(kms_statements(role), before + [grant])
                role["Properties"]["Policies"].pop()


if __name__ == "__main__":
    unittest.main()
