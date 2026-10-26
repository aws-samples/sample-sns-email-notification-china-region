#!/bin/sh
# usage: ssm.sh "profile/region args" instance 'cmd1' ['cmd2' ...]
# $A is left unquoted on purpose: it holds several AWS CLI arguments.
A=$1; I=$2; shift 2
CMDS=$(python3 -c 'import json,sys;print(json.dumps({"commands":sys.argv[1:]}))' "$@")
CID=$(aws ssm send-command $A --instance-ids "$I" --document-name AWS-RunShellScript --parameters "$CMDS" \
    --query Command.CommandId --output text)
for i in $(seq 1 30); do
    sleep 3
    S=$(aws ssm get-command-invocation $A --command-id "$CID" --instance-id "$I" --query Status --output text 2>/dev/null)
    case $S in
        Success|Failed|TimedOut|Cancelled)
            break
            ;;
    esac
done
aws ssm get-command-invocation $A --command-id "$CID" --instance-id "$I" \
    --query '[Status,StandardOutputContent,StandardErrorContent]' --output text
