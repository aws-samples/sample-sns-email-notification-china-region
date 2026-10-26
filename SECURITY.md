# Security

## Reporting a vulnerability

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our
[vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create
a public GitHub issue.

## Security design of this sample

- **SMTP.** The function sends only over STARTTLS with SMTP AUTH and verifies the server certificate and host
  name. Credentials are in AWS Secrets Manager, reached through a VPC interface endpoint; the function's
  environment holds no secrets, only names and ARNs and non-secret settings (SMTP host and port, sender address,
  rate, lease and cache times).
- **Network.** The function runs in private subnets without a NAT gateway or Internet route. Its security group
  allows egress to the SMTP network (`SmtpCidr`), HTTPS to the security group of the Secrets Manager interface
  endpoint, and HTTPS to the AWS managed prefix list of the DynamoDB gateway endpoint. There is no `0.0.0.0/0` rule.
- **Who may send.** Each queue accepts messages from SNS only for the exact topic ARNs listed for its shard
  (`aws:SourceArn`), and the function mails only for topics configured in `mail-config` with mode `shadow` or
  `live`. A message from a topic outside the queue policy is refused before it is queued (watch the topic's SNS
  delivery failures). A queued message whose topic is unknown, off or without recipients is retried, ends in the
  dead-letter queue and raises an alarm.
- **Least privilege across accounts.** Member accounts grant the central account `sns:Subscribe` for the `sqs`
  protocol, a read-only inventory role and an opt-in unsubscribe-only migration role scoped to exact `MigrationTopicArns` (empty by default); SNS does not support `sns:Protocol` for `Unsubscribe`, so the role does not enforce an email-only protocol boundary.
- **Logs.** The function never logs message bodies, addresses or SMTP replies. The optional test SMTP host
  keeps addresses in a log file and message bodies in its spool, readable by their owner only.
- **Encryption at rest.** All three tables, both SMTP secrets, and all queues/DLQs use one customer managed
  KMS key (`DataKeyArn`) with automatic rotation. All three tables have point-in-time recovery. CloudWatch
  Logs still uses service-default encryption. Sharing the data key keeps this sample small, but gives it a
  wider blast radius than separate service keys.

## Customer managed key and access paths

The key policy's account-root statement enables IAM delegation within the central account; it does not
automatically give every IAM principal permission to use the key. Existing administrator policies still
apply. In a **key policy**, `Resource: "*"` means this key. Runtime IAM statements instead reference the
exact key ARN. The key policy does not reference resources that depend on the key, avoiding a deployment
cycle.

SNS receives `kms:GenerateDataKey*` and `kms:Decrypt` only when `aws:SourceArn` matches one of the active
shard's exact topic ARNs. The key and queue policies use the same lists. They may contain topics in member
accounts: do not add the **central** account as `aws:SourceAccount`. A full source ARN already contains
the source account. Topic publishers, the member inventory role and the member migration role do not
receive access to the central key just because SNS delivers their topics.
The topic parameters reject wildcard account/topic values; an empty list retains the initial no-sender
configuration. This sample uses standard topic names, matching the recipient-config validator.

The SNS principal is `sns.amazonaws.com` in both `aws` and `aws-cn`. KMS ViaService values use the
`.amazonaws.com` suffix in **all partitions**, not `${AWS::URLSuffix}`. This sample scopes ViaService to
the stack's Region; its tables are regional, not global tables.
The intended mail path is cross-account within the same Region and partition. Cross-Region delivery,
including regionalized SNS service principals, needs its own review before extending this policy.

| Caller / operation | Required KMS access on DataKeyArn | Conditions and responsibility |
|---|---|---|
| Mailer Lambda: consume SQS | `Decrypt` | `ViaService=sqs.REGION.amazonaws.com`, central CallerAccount; existing SQS actions restrict source queues. Granted by the template. |
| Mailer Lambda: read config and update delivery records | `Decrypt` | DynamoDB ViaService, central CallerAccount/subscriberId, exact ConfigTable/DeliveryTable names. Granted by the template; not table-creation permission. |
| Mailer Lambda: read the two SMTP secrets | `Decrypt` | Secrets Manager ViaService, central CallerAccount, exact `SecretARN` encryption contexts. Granted by the template. |
| Test SMTP instance: read SMTP AUTH secret | `Decrypt` | Secrets Manager ViaService and the SMTP secret's exact `SecretARN`. Granted by the test template using its required `DataKeyArn` parameter. |
| Test SMTP instance: write CA secret | `GenerateDataKey`, `Decrypt` | Secrets Manager ViaService and the CA secret's exact `SecretARN`; Decrypt also supports idempotent version comparison. Granted by the test template. |
| Local `mailcfg.py` operator | `Decrypt` | DynamoDB ViaService/context for ConfigTable; grant to the effective CENTRAL profile/CI role, not to the Lambda role. Offline `validate` needs no AWS permissions. |
| Local `migrate.py` operator | `Decrypt` | DynamoDB ViaService/context for MigrationTable, ConfigTable and DeliveryTable as used by each command. The central identity accesses tables; assumed member roles access SNS only. |
| Operator putting/reading SMTP values with AWS CLI | `GenerateDataKey` + `Decrypt` for writes; `Decrypt` for reads | Secrets Manager ViaService and exact target SecretARN; add these to the operator identity alongside the Secrets Manager API permissions. |
| Operator injecting SQS messages or redriving a DLQ | `GenerateDataKey`, `Decrypt` for the destination; `Decrypt` for all source message keys | SQS ViaService, plus the relevant SQS data/redrive APIs. Keep permission to historical source CMKs until their messages are drained. Not granted to the Lambda. |
| Member InventoryRole / MigrationRole and SNS publishers | No central-key permissions | They list/subscribe/unsubscribe/publish SNS; SNS's service-principal key statement authorizes delivery. Member-topic encryption, if separately enabled, has its own permissions. |
| `scripts/deploy.py` / CloudFormation provisioning identity | Key/alias administration and service setup permissions described below | The script does not pass RoleARN; review any service role already associated with an existing stack. Credentials/profiles do not themselves grant permissions. |
| `scripts/cleanup.py` operator | Existing CloudFormation, DynamoDB and S3 administration | The script does not read item plaintext or delete the retained CMK. Separate key-administration approval is required for eventual key deletion. |
| `scripts/ssm.sh` operator | No data-key access merely to send SSM commands | Commands on the instance use its instance role; additional commands must be reviewed for their own data API/KMS needs. |

Local tools **do not inherit the Lambda execution role**. No operator identity is created or granted
permissions by these templates. The administrator must update the approved central operator/CI policies.
Keep their existing table/secret/queue API permissions restricted to the intended resources.

For a CRUD-only operator of existing tables, the following IAM statement is a starting point. Replace
all placeholders, narrow the table list to the command's needs, and do not add grant-management permissions:

```json
{
  "Effect": "Allow",
  "Action": ["kms:Decrypt"],
  "Resource": "<DATA_KEY_ARN>",
  "Condition": {
    "StringEquals": {
      "kms:CallerAccount": "<CENTRAL_ACCOUNT_ID>",
      "kms:ViaService": "dynamodb.<REGION>.amazonaws.com",
      "kms:EncryptionContext:aws:dynamodb:subscriberId": "<CENTRAL_ACCOUNT_ID>",
      "kms:EncryptionContext:aws:dynamodb:tableName": ["<CONFIG_TABLE>", "<MIGRATION_TABLE>", "<DELIVERY_TABLE>"]
    }
  }
}
```

### Provisioning and key changes are not ordinary CRUD

The effective CloudFormation provisioning identity needs the KMS control-plane permissions required by
the Key/Alias resource handlers: creation, policy management, description, rotation, tagging and alias
create/read/update/delete. `CreateKey` cannot target a key ARN that does not yet exist; restrict creation
with approved request tags and restrict subsequent administration to the managed key/alias. Do not give
these control-plane permissions to the runtime roles or bypass the KMS policy-lockout check.

For selecting the DynamoDB CMK at table creation or when changing encryption, AWS documents `Encrypt`,
`Decrypt`, `ReEncrypt*`, `GenerateDataKey*`, `DescribeKey` and `CreateGrant`. Give the provisioning identity
these permissions on the exact key, separate from its key-administration statement. Scope service use to
DynamoDB; isolate `CreateGrant` in a statement with `kms:GrantIsForAWSResource=true`. Do not attach data
encryption-context conditions to DescribeKey or CreateGrant: grant constraints are not the same request
field as a cryptographic operation's EncryptionContext. DynamoDB manages its own service grants.

Creating secrets with the key needs Secrets Manager `GenerateDataKey` and `Decrypt`. Re-encrypting existing
secret versions when changing the key additionally needs `Encrypt` on the new key and `Decrypt` on the
old key. Keep this separate from routine PutSecretValue permissions. Restrict service calls with
ViaService; no KMS VPC endpoint is needed for these integrated-service calls.

DynamoDB's documented table-key access path uses Decrypt and caches the plaintext table key per caller;
a cold access after
five minutes of inactivity must be tested, as well as configuration updates, delivery writes and all
migration commands. Restores, exports, global tables and key changes need a separate permissions review.
SCPs, permission boundaries, endpoint policies and other IAM statements can still affect access.

### Upgrade, retention and cleanup

Plan an approved maintenance window for an existing stack: pause publishers and disable the event source
mappings before the encryption transition, authorize the provisioning identity, update central resources,
then grant the approved operator identities the new `DataKeyArn` permissions and update the test SMTP stack
with that ARN. Resource encryption and role-policy changes are not atomic; do not assume uninterrupted
polling during the update. Re-enable ingestion only after allowed/denied cross-account topics, cold Lambda
reads/writes, local tools, SMTP secret access and DLQ redrive have been verified in the intended accounts.

`DataKey` has Retain policies for stack deletion and replacement so retained tables/backups remain
decryptable. The alias can be removed with the stack without deleting the key. Record DataKeyArn before
cleanup. `cleanup.py` deliberately leaves the key behind: confirm retained/old tables, backups, secret
versions and queue messages no longer need it before a separately approved key-deletion operation.
Key rotation is not key replacement; neither operation retroactively re-encrypts all older SQS messages.

Official references:
- [China SQS key management and SNS principal](https://docs.amazonaws.cn/en_us/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-key-management.html)
- [KMS ViaService and grant conditions in all partitions](https://docs.amazonaws.cn/en_us/kms/latest/developerguide/conditions-kms.html)
- [DynamoDB CMK permissions, grants and caching](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/encryption.usagenotes.html)
- [Secrets Manager KMS operations](https://docs.aws.amazon.com/secretsmanager/latest/userguide/security-encryption.html)
- [SQS redrive permissions](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-dead-letter-queue-redrive.html)
- [Default KMS key policy and IAM delegation](https://docs.aws.amazon.com/kms/latest/developerguide/key-policy-default.html)

## Your responsibilities before production use

- Review shared-key blast radius, CloudWatch Logs encryption and caller permissions; rotate the SMTP
  credentials separately from the KMS key.
- Restrict `SmtpCidr` to your SMTP servers and review the IAM policies in `templates/`.
- Run `mailcfg.py apply` from a pipeline whose role is the only writer of the `mail-config` table.
- Do not use `templates/test-smtp.yaml` in production: it is a test host with a self-signed CA.
