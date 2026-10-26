# 通过企业 SMTP 接收 Amazon SNS 邮件通知

本示例以 Amazon SQS 与 Lambda 将 SNS 通知经企业 SMTP 发出，并通过 DynamoDB 管理收件人配置，无需修改发布方。

> **Disclaimer**: This repository is provided as an example only and is NOT
> intended for production use. You are responsible for testing, securing, and
> optimizing the code as appropriate for production grade use based on your
> specific quality control practices and standards.

[English](README.md)

## 架构

![架构：成员账号的 SNS 主题投递到中心 SQS 队列分片；VPC 内的 Lambda 按 DynamoDB 路由，经 STARTTLS 与 SMTP AUTH 发往企业 SMTP](docs/architecture.png)

（图源：[`docs/architecture.mmd`](docs/architecture.mmd)。）

成员账号的 SNS 主题投递到中心加密 SQS 队列；私有 VPC 子网中的 Lambda 从 DynamoDB 读取收件人配置，
通过 STARTTLS、SMTP AUTH 和证书校验向企业 SMTP 提交邮件。方案支持 `aws`、`aws-cn` 分区中的多账号；
可选 Postfix 测试主机将邮件保存在本地，无需向外部邮箱投递。

## 前置条件

- Python 3.10+（工具使用 `requirements.txt` 中的 boto3 与 PyYAML）和 AWS CLI v2。
- 中心账号与各成员账号的 AWS CLI profile。ARN 区分分区：中国区使用 `arn:aws-cn:`。
- 用 `tools/` 访问 DynamoDB 的操作员需要数据 key 的 `kms:Decrypt`；[IAM 示例见 SECURITY.md](SECURITY.md#customer-managed-key-and-access-paths)。
- **发信子网到 SMTP 服务器的网络路径。** `central.yaml` 新建一个私有 VPC；`SmtpCidr` 只放行安全组，不建立连通性。请按
  网络团队的方式接入，如 Transit Gateway、VPN、Direct Connect 或 VPC 对等连接，并在 `PrivateRouteTableId` 中加路由；
  让 VPC 内能解析 `SmtpHost`，例如为企业 DNS 域配置 Route 53 Resolver 出站规则。模板不接受现有 VPC 或子网：如需使用，
  请替换其网络部分并保留两个终端节点。
- 一个允许以 `MailFrom` 身份提交邮件的 SMTP 账号；若服务器证书不是公共信任的，还需其 CA 证书。

## 使用的 AWS 服务

- Amazon SNS（现有主题）与 Amazon SQS（队列分片与死信队列）
- AWS Lambda（发信函数，位于 Amazon VPC 内，使用 DynamoDB 网关终端节点与 Secrets Manager 接口终端节点）
- Amazon DynamoDB（路由配置、迁移状态、投递记录）
- AWS Secrets Manager（SMTP 凭证与 CA 证书）
- AWS KMS（客户管理的数据加密密钥）
- Amazon CloudWatch（日志、指标过滤器、告警）、AWS CloudFormation、AWS IAM、Amazon S3（代码包）
- 仅可选的测试 SMTP 主机使用：Amazon EC2、AWS Systems Manager、Amazon SES（`DeliveryMode=ses`）

## 部署

如修改 `NamePrefix`（默认 `sample-sns-email`），`package` 与 `cleanup.py` 请使用相同的 `--prefix`。未知参数名会被拒绝。

```sh
git clone https://github.com/aws-samples/sample-sns-email-notification-china-region.git
cd sample-sns-email-notification-china-region
python3 -m pip install -r requirements.txt
python3 -m unittest discover -s tests
# 1. member account(s): CreateDemoTopics=true only for a trial
python3 scripts/deploy.py --profile MEMBER --region REGION stack sample-sns-email-member templates/member.yaml \
    CentralAccountId=<central-account-id> CreateDemoTopics=true
# 2. central account
python3 scripts/deploy.py --profile CENTRAL --region REGION package        # prints CodeBucket / CodeKey
python3 scripts/deploy.py --profile CENTRAL --region REGION stack sample-sns-email-central templates/central.yaml \
    CodeBucket=<bucket> CodeKey=<key> "Shard1TopicArns=<topic-arn>,<topic-arn>" \
    SmtpHost=<smtp-host-in-certificate> SmtpCidr=<smtp-network> MailFrom=<allowed-sender>
# FunctionTimeoutSeconds >= 30 (the mailer keeps 12 s back before each recipient);
# QueueVisibilityTimeoutSeconds >= 6 x FunctionTimeoutSeconds
# 3. SMTP credentials: put {"username": ..., "password": ...} into the <prefix>/smtp secret (the stack generates
#    a password for the test server), and the CA's PEM into <prefix>/smtp-ca if it is not publicly trusted.
# Optional test SMTP in the central VPC (then update the central stack with SmtpHost = its output):
python3 scripts/deploy.py --profile CENTRAL --region REGION stack sample-sns-email-test-smtp templates/test-smtp.yaml \
    VpcId=<out> LambdaSecurityGroupId=<out> EndpointSecurityGroupId=<out> SmtpSecretArn=<out> SmtpCaSecretArn=<out> \
    DataKeyArn=<central-DataKeyArn-output> DeliveryMode=store
```

测试 SMTP stack 必填 `DataKeyArn`：创建或更新时取中心 stack 的同名输出。

发信 Lambda 的安全组只放行：到 `SmtpCidr` 的 SMTP（`SmtpPort`）、到 Secrets Manager 接口终端节点安全组的 HTTPS，以及到
DynamoDB 网关终端节点 AWS 托管前缀列表的 HTTPS（`DynamoDbPrefixListId`），没有 `0.0.0.0/0` 规则。`scripts/deploy.py` 会自动查找该前缀列表
（`com.amazonaws.<region>.dynamodb`，`aws` 与 `aws-cn` 的命名规则相同）；自行调用 CloudFormation 时需要传入：
`aws ec2 describe-managed-prefix-lists --filters Name=prefix-list-name,Values=com.amazonaws.<region>.dynamodb --query 'PrefixLists[0].PrefixListId'`。
以后为其他服务添加终端节点时，需要为每个终端节点补一条对应的出站规则。

不发真实邮件、只测 SMTP 连通性时用 `DeliveryMode=store`。`DeliveryMode=ses` 经 Amazon SES 转发，仅在 `aws` 分区可用，中国区域请用 `store`（模板会拒绝在中国区域使用 `ses`）；它用主机的实例角色调用 SES，因此不需要 SES SMTP 凭证；测试主机仍会读取中心 SMTP 密钥，并把 Lambda 的入站
AUTH 凭证保存在 `/etc/sasl2/sasldb2`（密钥仍在 Secrets Manager 中）；在 SES 沙盒中需先验证发件域（含 DKIM）和每个收件人。

## 使用

部署后，以配置管理收件人，并按批次迁移主题。

### 投递行为

- **发布方无需改动。** 原始消息投递（raw message delivery）保持关闭，SQS 中每条消息都是 SNS 信封：`TopicArn`、
  `MessageId`、`Subject`、`Message`。`TopicArn` 作为路由键，取代了每个订阅各自的 email 地址。
- **由队列所有者订阅。** 中心账号创建 SQS 订阅，无需确认步骤；每个主题只需允许中心账号 `sns:Subscribe`。一个主题只有一份
  策略文档：请把语句加到已经管理该主题的模板中，另建一个 stack 会覆盖整份策略。
- **两层准入。** 其一，队列策略只接受来自该分片所列精确主题 ARN 的 `sqs:SendMessage`；其二，主题必须在 `mail-config` 中
  且 `mode` 为 `shadow` 或 `live`。
  - 已入队、但主题在 `mail-config` 中不存在、已关闭或没有收件人的消息不会被静默丢弃：退回 SQS，进入 DLQ，并触发 `config-missing` 告警。
  - 主题不在队列策略中时，消息在入队前就被拒绝，不会进入本队列或其 DLQ；请改为观察该主题的 SNS 投递失败指标。
  - SQS 队列策略上限 8 KiB，每个分片约 60 个主题 ARN，规模增长时增加分片。
- **路由数据。** `mail-config` 有两类条目：
  - `TOPIC#<arn>`：`mode`（`off` / `shadow` / `live`）、`recipients`、`groups`、`format`（`text`，或模拟 email-json
    的 `json`）、`defaultSubject`、`ownerTeam`、`version`；
  - `GROUP#<name>`：收件人组的成员。

  每个条目有一行 `CURRENT` 与不可变的 `V#n` 历史行。函数以最终一致读读取并缓存 60 秒，因此修改约一分钟内生效，无需重新部署。
- **幂等。** 对每个（SNS `MessageId`，收件人）组合，函数先在 `mail-delivery` 中条件写入带租约的 `claimed` 记录，发信后
  标记为 `sent`；记录 30 天后由 TTL 过期。
  - 重投或重放的消息会跳过已有终态记录（`sent`、`rejected` 或 `shadow`）的收件人。键只是（SNS `MessageId`，收件人），
    不含 mode 或配置版本：因此 `shadow` 记录也会让同一条消息在主题转为 `live` 后被重放时跳过该收件人。切换为 live、
    回滚和普通 redrive 都不会重置这些记录，也不要手工删除它们。
  - 临时失败后尽力释放占用；重试会跳过终态记录，重新尝试其他收件人，包括发送结果未确认的收件人。
  - 结果不确定时，同一收件人可能收到两次：SMTP 服务器已接收邮件但最终回复丢失（超时或断连）；函数在写入 `sent`
    记录前中止；或写入记录、释放占用失败，此时重试要等租约到期。投递语义为至少一次。
- **失败处理。**
  - 连接错误、超时与 SMTP 4xx 作为批处理项失败上报（`ReportBatchItemFailures`），SQS 最多重试 `maxReceiveCount`
    次后移入 DLQ。
  - 某收件人的 SMTP 5xx 记为 `rejected`，不重试，并触发告警：写入记录后立即记录 `mail_rejected` 事件（不含地址和 SMTP 回复），即使同一消息的后续收件人失败而被重试也一样。
  - Lambda 函数从不记录邮件正文、地址或 SMTP 回复。测试 SMTP 主机的日志中有地址、spool 中有正文，仅属主可读。
- **限流。**
  - `MaxConcurrencyPerShard` 设置事件源映射的最大并发。
  - `ReservedConcurrency` 限制 SMTP 会话数。
  - `MaxSendsPerSecond` 限制每个执行环境的发送速率；同一环境的连续调用共享该限制，并发的各个环境各自限速，总速率为该值乘以环境数（新环境的第一封不延迟）。

### 收件人配置即代码

格式见 `config/examples/mail-config.example.yaml`。流程：

```sh
python3 tools/mailcfg.py validate mail-config.yaml
python3 tools/mailcfg.py --profile CENTRAL --region REGION plan  mail-config.yaml --table sample-sns-email-mail-config
python3 tools/mailcfg.py --profile CENTRAL --region REGION apply mail-config.yaml --table sample-sns-email-mail-config --from-git --execute
python3 tools/mailcfg.py --profile CENTRAL --region REGION history  --table ... --key "TOPIC#<arn>"
python3 tools/mailcfg.py --profile CENTRAL --region REGION rollback --table ... --key "TOPIC#<arn>" --to-version 2 --execute
```

- **校验**：主题 ARN 与地址（整串匹配，含换行即拒绝）、`allowedDomains`、每个主题最多 50 个收件人（更多请用组）、组（`groups` 必须是组名列表）与 mode 是否存在、主题行中的
  换行。文件中缺少的条目不会被删除：关闭主题用 `mode: "off"`（要加引号：不带引号的 `off` 在 YAML 中是 false）。
- **apply**：在一个事务中写入 `V#n+1` 与 `CURRENT`，条件是版本仍为 plan 时的版本，因此并发发布会失败而不是相互覆盖。
  - `--from-git` 只接受 Git 跟踪且无未提交修改的文件。它读取 `HEAD` 中已提交的 blob，并对这些字节原样解析、计划和发布，因此检查与发布之间文件
    变化也不会影响发布内容；该文件最后一次提交记为 `changeId`。
- **rollback**：把旧内容写成新版本，历史保持线性。

请在评审后由 CI 流水线执行 `apply`，并让该角色成为这张表唯一的写入者。

### 迁移手册

按主题分批执行，使用中心账号凭证。除非加 `--execute`，所有对 AWS 的写操作都是 dry run；每条命令先不加 `--execute` 运行并阅读输出。`plan` 没有 `--execute`：它总是创建或覆盖本地 `--out` 文件，请指定一个不含手工修改内容的路径。

```sh
A="--profile CENTRAL --region REGION"; P=sample-sns-email
MIG=$P-mail-migration; CFG=$P-mail-config; DEL=$P-mail-delivery
INV=arn:aws:iam::<member-account-id>:role/$P-inventory; MR=arn:aws:iam::<member-account-id>:role/$P-migration
Q=arn:aws:sqs:REGION:<central-account-id>:$P-s1; T=arn:aws:sns:REGION:<member-account-id>:<topic>

python3 tools/migrate.py $A inventory --table $MIG --inventory-role $INV --execute                      # 1
python3 tools/migrate.py $A plan --table $MIG --out mail-config.yaml                                     # 2
#   review, commit, then: python3 tools/mailcfg.py $A apply mail-config.yaml --table $CFG --from-git --execute
python3 tools/migrate.py $A subscribe --table $MIG --inventory-role $INV --queue-arn $Q --topic $T --execute   # 3
SINCE=$(date -u +%FT%TZ)    # shadow window starts; let the topic's normal traffic flow
python3 tools/migrate.py $A verify --table $MIG --delivery-table $DEL --config-table $CFG --topic $T --since $SINCE --mark shadow-verified --execute   # 4
#   publish mode: live with mailcfg.py, then after the first messages:
LIVE=<time the live config was published>
python3 tools/migrate.py $A verify --table $MIG --delivery-table $DEL --config-table $CFG --topic $T --since $LIVE --mark live --execute   # 5
python3 tools/migrate.py $A remove-email --table $MIG --config-table $CFG --delivery-table $DEL --migration-role $MR --topic $T --execute
python3 tools/migrate.py $A status --table $MIG
```

1. **Inventory（盘点）**：把每个 email / email-json 订阅写入 `mail-migration`，包括待确认的订阅、过滤策略及其作用范围
   和确认状态。
2. **Plan（生成配置）**：把已确认的订阅生成 `mode: shadow` 的配置；待确认的订阅留给其所有者决定。新链路无法精确复现的
   主题会阻止生成：同一主题同时有 email 和 email-json，或过滤策略/作用范围不同。请先拆分或排除这些订阅。
3. **Subscribe（订阅）**：以队列所有者身份创建 SQS 订阅，关闭原始投递，并沿用 email 订阅的过滤策略和作用范围。若已存在
   属性不同的 SQS 订阅则拒绝。请先把主题 ARN 登记到该分片的队列策略中（`Shard1TopicArns` 参数）。
4. **影子运行**：主题处于 shadow 模式时，函数解析并记录收件人但不发信，旧 email 订阅照常投递。`verify --mark
   shadow-verified` 检查每个旧收件人自 `--since` 起都已被新链路解析。
5. **切换**：`verify --mark live` 要求**每个旧收件人**自 `--since` 起在当前 live 配置版本下都有 `sent` 记录；影子记录和
   发给其他收件人的邮件不算，且每个旧收件人必须仍在主题的收件人或其引用的组中。它会记录主题及每个所引用组的版本。
   `remove-email` 会重新解析，任一版本变化或有旧收件人不再被路由时拒绝。从切换到移除期间，收件人可能在几分钟内收到
   重复邮件，这比出现空窗更可取。
   执行 `remove-email` 前，把成员 stack 的 `MigrationTopicArns` 设为精确的目标主题 ARN 列表（默认留空）。
6. **回退**：移除前，把 `mode` 改回 `shadow`，旧订阅继续投递。移除后只能在新链路内回退：
   - 用 `mailcfg.py rollback` 恢复早先的配置版本；
   - 把函数指回早先的版本；
   - 用 `aws sqs start-message-move-task` 重放 DLQ，投递表会防止重复。

   **在已不接受 email 订阅的区域（目前为 AWS 中国区），移除的 email 订阅无法重建。**

### 限制

- 至少一次投递，存在上文所述的重复窗口。`Message-ID` 头为 `<sns-message-id>.<recipient-hash>@<sender-domain>`，便于
  邮箱与 SMTP 服务器识别重复。
- 支持纯文本与 email-json 正文；不复现 SNS 邮件页脚与退订链接。主题行截断为 100 个字符。消息最大 256 KB（SNS 限制）。
- `verify` 与 `remove-email` 的检查会扫描投递表，对示例足够；规模较大时请改用 GSI 或日志指标。
- 测试 SMTP 主机不是生产 MTA：其自签 CA 有效期 30 天，位于公有子网，仅用于安装软件包和调用 SES API。

## 费用

费用取决于实际用量；请参阅 AWS 中国区 [SNS](https://www.amazonaws.cn/sns/pricing/)、[SQS](https://www.amazonaws.cn/sqs/pricing/)、[Lambda](https://www.amazonaws.cn/lambda/pricing/)、[DynamoDB](https://www.amazonaws.cn/dynamodb/pricing/)、[KMS](https://www.amazonaws.cn/kms/pricing/) 和 [Secrets Manager](https://www.amazonaws.cn/secrets-manager/pricing/) 定价页。

## 清理

```sh
python3 scripts/cleanup.py --region REGION --central-profile CENTRAL --member-profile MEMBER             # dry run: prints the plan
python3 scripts/cleanup.py --region REGION --central-profile CENTRAL --member-profile MEMBER --execute
```

如果 stack 名称不以 `sample-sns-email` 开头，请加 `--prefix`。脚本按名称查找 stack，从中心 stack 本身（表资源与
`CodeBucket` 参数）取得表和代码包存储桶，并删除：
- 各 stack；
- 中心 stack 保留的两张表（`mail-config`、`mail-migration`）；
- 代码包存储桶。

清理前记录中心栈的 `DataKeyArn`。删栈时 KMS key 会保留；确认保留数据不再需要它后，另行计划删除（见 [Security](SECURITY.md#upgrade-retention-and-cleanup)）。

每项都必须带有 `project=sample-sns-email` 标签，并在每次变更（关闭删除保护、删除、从存储桶移除每一批对象、删除存储桶本身）前
再次检查；否则脚本停止，不再做任何改动。除“stack 不存在”以外的任何查询错误（例如拒绝访问）同样会使脚本停止；没有可做的动作时
输出 `skipped` 而不是 `done`。检查与变更是两次独立调用，其间被移除的标签不会被发现。

如果使用了 `DeliveryMode=ses`，还需删除为 SES 创建的内容：用 `aws sesv2 delete-email-identity` 删除发件域与已验证的
收件人，`aws sesv2 delete-configuration-set`，以及 DNS 中的 DKIM CNAME 记录。

## 安全

安全问题报告方式见 [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications)；安全边界、权限及运维责任见
[SECURITY.md](SECURITY.md)。

## 免责声明

The sample code; software libraries; command line tools; proofs of concept; templates; or other related technology
(including any of the foregoing that are provided by our personnel) is provided to you as AWS Content under the AWS
Customer Agreement, or the relevant written agreement between you and AWS (whichever applies). You should not use
this AWS Content in your production accounts, or on production or other critical data. You are responsible for
testing, securing, and optimizing the AWS Content, such as sample code, as appropriate for production grade use
based on your specific quality control practices and standards. Deploying AWS Content may incur AWS charges for
creating or using AWS chargeable resources, such as running Amazon EC2 instances or using Amazon S3 storage.

（以上为 AWS 官方免责声明原文，以英文为准。）

## 许可证

本项目采用 MIT-0 许可证，详见 [LICENSE](LICENSE) 文件。
