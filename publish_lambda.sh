#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

set -e

echo "    AWS CLI (https://aws.amazon.com/cli/), SAM (https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html), Docker and Python3.9 with pip3 required"
echo "    Please, before launching the tool execute \"$ pip3 install ruamel.yaml\""

if [[ $# -ne 5 ]]
then
    echo "Usage: $0 config-path lambda-name forwarder-tag bucket-name region"
    echo "    Arguments:"
    echo "    config-path: full path to the publish configuration"
    echo "    lambda-name: name of the lambda to be published in the account"
    echo "    forwarder-tag: tag of the elastic serverless forwarder to publish"
    echo "    bucket-name: bucket name where to store the zip artifact for the lambda"
    echo "                 (it will be created if it doesn't exists, otherwise "
    echo "                  you need already to have proper access to it)"
    echo "    region: region where to publish in"
    exit 1
fi

PUBLISH_CONFIG="$1"
LABDA_NAME="$2"
TAG_NAME="$3"
BUCKET="$4"
REGION="$5"

TMPDIR=$(mktemp -d /tmp/publish.XXXXXXXXXX)
CLONED_FOLDER="${TMPDIR}/sources"
PACKAGE_FOLDER="${CLONED_FOLDER}/package"
GIT_REPO="https://github.com/elastic/elastic-serverless-forwarder.git"

trap 'rm -rf ${TMPDIR}' EXIT

aws s3api get-bucket-location --bucket "${BUCKET}" || aws s3api create-bucket --acl private --bucket "${BUCKET}" --region "${REGION}" --create-bucket-configuration LocationConstraint="${REGION}"

mkdir -v -p "${CLONED_FOLDER}"
git clone --depth 1 --branch "${TAG_NAME}" "${GIT_REPO}" "${CLONED_FOLDER}"
mkdir -v -p "${PACKAGE_FOLDER}"

pushd "${CLONED_FOLDER}"
cp -v requirements.txt "${PACKAGE_FOLDER}/"
cp -v main_aws.py "${PACKAGE_FOLDER}/"
find {handlers,share,shippers,storage} -not -name "*__pycache__*" -type d -print0|xargs -t -0 -Idirname mkdir -v -p "${PACKAGE_FOLDER}/dirname"
find {handlers,share,shippers,storage} -not -name "*__pycache__*" -name "*.py" -exec cp -v '{}' "${PACKAGE_FOLDER}/{}" \;
cp -v LICENSE.txt "${PACKAGE_FOLDER}/LICENSE.txt"
cp -v docs/README-AWS.md "${PACKAGE_FOLDER}/README.md"

popd

cat <<EOF > "${TMPDIR}/publish-before-sed.yaml"
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Description: >
  Elastic Serverless Forwarder

  SAM Template for publishing

Resources:
  ElasticServerlessForwarderContinuingDLQ:
    Type: AWS::SQS::Queue
    Properties:
      DelaySeconds: 0
      QueueName: !Join [ "-", ["elastic-serverless-forwarder-continuing-dlq", !Select [4, !Split ['-', !Select [2, !Split ['/', !Ref AWS::StackId]]]]]]
      VisibilityTimeout: 910
      SqsManagedSseEnabled: true
  ElasticServerlessForwarderContinuingQueue:
    Type: AWS::SQS::Queue
    Properties:
      DelaySeconds: 0
      QueueName: !Join [ "-", ["elastic-serverless-forwarder-continuing-queue", !Select [4, !Split ['-', !Select [2, !Split ['/', !Ref AWS::StackId]]]]]]
      RedrivePolicy: { "deadLetterTargetArn" : !GetAtt ElasticServerlessForwarderContinuingDLQ.Arn, "maxReceiveCount" : 1 }
      VisibilityTimeout: 910
      SqsManagedSseEnabled: true
  ElasticServerlessForwarderReplayDLQ:
    Type: AWS::SQS::Queue
    Properties:
      DelaySeconds: 0
      QueueName: !Join [ "-", ["elastic-serverless-forwarder-replay-dlq", !Select [4, !Split ['-', !Select [2, !Split ['/', !Ref AWS::StackId]]]]]]
      VisibilityTimeout: 910
      SqsManagedSseEnabled: true
  ElasticServerlessForwarderReplayQueue:
    Type: AWS::SQS::Queue
    Properties:
      DelaySeconds: 0
      QueueName: !Join [ "-", ["elastic-serverless-forwarder-replay-queue", !Select [4, !Split ['-', !Select [2, !Split ['/', !Ref AWS::StackId]]]]]]
      RedrivePolicy: { "deadLetterTargetArn" : !GetAtt ElasticServerlessForwarderReplayDLQ.Arn, "maxReceiveCount" : 3 }
      VisibilityTimeout: 910
      SqsManagedSseEnabled: true
  ApplicationElasticServerlessForwarder:
    Type: AWS::Serverless::Function
    Properties:
      Timeout: 900
      MemorySize: 512
      CodeUri: %codeUri%
      Runtime: python3.9
      Architectures:
        - x86_64
      Handler: main_aws.handler
      Environment:
          Variables:
              SQS_CONTINUE_URL: !Ref ElasticServerlessForwarderContinuingQueue
              SQS_REPLAY_URL: !Ref ElasticServerlessForwarderReplayQueue
              TELEMETRY_DEPLOYMENT_ID: !Select [4, !Split ['-', !Select [2, !Split ['/', !Ref AWS::StackId]]]]
              TELEMETRY_ENABLED: true
              TELEMETRY_ENDPOINT: "https://telemetry.elastic.co/v3/send/esf"
      Events:
        SQSContinuingEvent:
          Type: SQS
          Properties:
            Queue: !GetAtt ElasticServerlessForwarderContinuingQueue.Arn
            Enabled: true
EOF

cat <<EOF > "${TMPDIR}/publish.py"
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import hashlib
import sys
from typing import Any

from ruamel.yaml import YAML


def hex_suffix(src):
    return hashlib.sha256(src.encode("utf-8")).hexdigest()[:10]


def create_events(publish_config: dict[str, Any]):
    events_fragment = {}

    if "kinesis-data-stream" in publish_config:
        assert isinstance(publish_config["kinesis-data-stream"], list)
        for kinesis_data_stream_event in publish_config["kinesis-data-stream"]:
            assert isinstance(kinesis_data_stream_event, dict)

            kinesis_batch_size = 10
            if "batch_size" in kinesis_data_stream_event:
                kinesis_batch_size = int(kinesis_data_stream_event["batch_size"])

            if kinesis_batch_size < 1:
                kinesis_batch_size = 1

            kinesis_starting_position = "TRIM_HORIZON"
            if "starting_position" in kinesis_data_stream_event:
                kinesis_starting_position = kinesis_data_stream_event["starting_position"]

            kinesis_starting_position_timestamp = 0
            if (
                "starting_position_timestamp" in kinesis_data_stream_event
                and kinesis_data_stream_event == "AT_TIMESTAMP"
            ):
                kinesis_starting_position_timestamp = int(
                    kinesis_data_stream_event["ElasticServerlessForwarderKinesisStartingPositionTimestamp"]
                )

            kinesis_batch_window = 0
            if "batching_window_in_second" in kinesis_data_stream_event:
                kinesis_batch_window = int(kinesis_data_stream_event["batching_window_in_second"])

            if kinesis_batch_window < 0:
                kinesis_batch_window = 0

            kinesis_parallelization_factor = 1
            if "parallelization_factor" in kinesis_data_stream_event:
                kinesis_parallelization_factor = int(kinesis_data_stream_event["parallelization_factor"])

            if kinesis_parallelization_factor < 1:
                kinesis_parallelization_factor = 1

            if "arn" in kinesis_data_stream_event:
                kinesis_event_arn = kinesis_data_stream_event["arn"].strip()
                if len(kinesis_event_arn) == 0:
                    continue

                kinesis_event_name = f"KinesisEvent{hex_suffix(kinesis_event_arn)}"
                events_fragment[kinesis_event_name] = {
                    "Type": "Kinesis",
                    "Properties": {
                        "Stream": kinesis_event_arn,
                        "StartingPosition": kinesis_starting_position,
                        "BatchSize": kinesis_batch_size,
                        "MaximumBatchingWindowInSeconds": kinesis_batch_window,
                        "ParallelizationFactor": kinesis_parallelization_factor,
                        "Enabled": True,
                    },
                }

                if kinesis_starting_position_timestamp > 0:
                    events_fragment[kinesis_event_name]["Properties"][
                        "StartingPositionTimestamp"
                    ] = kinesis_starting_position_timestamp

    if "sqs" in publish_config:
        assert isinstance(publish_config["sqs"], list)
        for sqs_event in publish_config["sqs"]:
            assert isinstance(sqs_event, dict)

            sqs_batch_size = 10
            if "batch_size" in sqs_event:
                sqs_batch_size = int(sqs_event["batch_size"])

            if sqs_batch_size < 1:
                sqs_batch_size = 1

            sqs_batch_window = 0
            if "batching_window_in_second" in sqs_event:
                sqs_batch_window = int(sqs_event["batching_window_in_second"])

            if sqs_batch_window < 0:
                sqs_batch_window = 0

            if "arn" in sqs_event:
                sqs_event_arn = sqs_event["arn"].strip()
                if len(sqs_event_arn) == 0:
                    continue

                sqs_event_name = f"SQSEvent{hex_suffix(sqs_event_arn)}"
                events_fragment[sqs_event_name] = {
                    "Type": "SQS",
                    "Properties": {
                        "Queue": sqs_event_arn,
                        "BatchSize": sqs_batch_size,
                        "MaximumBatchingWindowInSeconds": sqs_batch_window,
                        "Enabled": True,
                    },
                }

    if "s3-sqs" in publish_config:
        assert isinstance(publish_config["s3-sqs"], list)
        for s3_sqs_event in publish_config["s3-sqs"]:
            assert isinstance(s3_sqs_event, dict)

            sqs_s3_batch_size = 10
            if s3_sqs_event["batch_size"]:
                sqs_s3_batch_size = int(s3_sqs_event["batch_size"])

            if sqs_s3_batch_size < 1:
                sqs_s3_batch_size = 1

            sqs_s3_batch_window = 0
            if "batching_window_in_second" in s3_sqs_event:
                sqs_s3_batch_window = int(s3_sqs_event["batching_window_in_second"])

            if sqs_s3_batch_window < 0:
                sqs_s3_batch_window = 0

            if "arn" in s3_sqs_event:
                s3_sqs_event_arn = s3_sqs_event["arn"].strip()
                if len(s3_sqs_event_arn) == 0:
                    continue

                s3_sqs_event_name = f"S3SQSEvent{hex_suffix(s3_sqs_event_arn)}"
                events_fragment[s3_sqs_event_name] = {
                    "Type": "SQS",
                    "Properties": {
                        "Queue": s3_sqs_event_arn,
                        "BatchSize": sqs_s3_batch_size,
                        "MaximumBatchingWindowInSeconds": sqs_s3_batch_window,
                        "Enabled": True,
                    },
                }

    if "cloudwatch-logs" in publish_config:
        assert isinstance(publish_config["cloudwatch-logs"], list)
        for cloudwatch_logs_event in publish_config["cloudwatch-logs"]:
            assert isinstance(cloudwatch_logs_event, dict)

            if "arn" in cloudwatch_logs_event:
                cloudwatch_logs_event_arn = cloudwatch_logs_event["arn"].strip()
                if len(cloudwatch_logs_event_arn) == 0:
                    continue

                arn_components = cloudwatch_logs_event_arn.split(":")
                cloudwatch_logs_group_name = arn_components[6]

                cloudwatch_logs_event_name = f"CloudWatchLogsEvent{hex_suffix(cloudwatch_logs_group_name)}"
                events_fragment[cloudwatch_logs_event_name] = {
                    "Type": "CloudWatchLogs",
                    "Properties": {
                        "FilterPattern": "",
                        "LogGroupName": cloudwatch_logs_group_name,
                    },
                }

    return events_fragment


def create_policy(publish_config: dict[str, Any]):
    policy_fragment = {
        "Type": "AWS::IAM::Policy",
        "Properties": {
            "PolicyName": {
                "Fn::Join": [
                    "-",
                    [
                        "elastic-serverless-forwarder-policy",
                        {
                            "Fn::Select": [
                                4,
                                {
                                    "Fn::Split": [
                                        "-",
                                        {"Fn::Select": [2, {"Fn::Split": ["/", {"Ref": "AWS::StackId"}]}]},
                                    ]
                                },
                            ]
                        },
                    ],
                ]
            },
            "PolicyDocument": {"Version": "2012-10-17", "Statement": []},
            "Roles": [{"Ref": "ApplicationElasticServerlessForwarderRole"}],
        },
    }

    if "s3-config-file" in publish_config:
        assert isinstance(publish_config["s3-config-file"], str)

        bucket_name_and_object_key = publish_config["s3-config-file"].replace("s3://", "")
        resource = f"arn:aws:s3:::{bucket_name_and_object_key}"
        if len(resource) > 0:
            policy_fragment["Properties"]["PolicyDocument"]["Statement"].append(
                {"Effect": "Allow", "Action": "s3:GetObject", "Resource": resource}
            )

    if "ssm-secrets" in publish_config:
        assert isinstance(publish_config["ssm-secrets"], list)

        ssm_secrets_arn = [x for x in publish_config["ssm-secrets"] if len(x.strip()) > 0]

        if len(ssm_secrets_arn) > 0:
            policy_fragment["Properties"]["PolicyDocument"]["Statement"].append(
                {"Effect": "Allow", "Action": "secretsmanager:GetSecretValue", "Resource": ssm_secrets_arn}
            )

    if "kms-keys" in publish_config:
        assert isinstance(publish_config["kms-keys"], list)

        kms_keys_arn = [x for x in publish_config["kms-keys"] if len(x.strip()) > 0]
        if len(kms_keys_arn) > 0:
            policy_fragment["Properties"]["PolicyDocument"]["Statement"].append(
                {"Effect": "Allow", "Action": "kms:Decrypt", "Resource": kms_keys_arn}
            )

    if "s3-buckets" in publish_config:
        assert isinstance(publish_config["s3-buckets"], list)

        s3_buckets_arn = [x for x in publish_config["s3-buckets"] if len(x.strip()) > 0]
        if len(s3_buckets_arn) > 0:
            policy_fragment["Properties"]["PolicyDocument"]["Statement"].append(
                {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": s3_buckets_arn}
            )

        resources = []
        for s3_bucket_with_notification in s3_buckets_arn:
            resources.append(f"{s3_bucket_with_notification}/*")

        if len(resources) > 0:
            policy_fragment["Properties"]["PolicyDocument"]["Statement"].append(
                {"Effect": "Allow", "Action": "s3:GetObject", "Resource": resources}
            )

    policy_fragment["Properties"]["PolicyDocument"]["Statement"].append(
        {
            "Effect": "Allow",
            "Action": "sqs:SendMessage",
            "Resource": [
                {"Fn::GetAtt": ["ElasticServerlessForwarderReplayQueue", "Arn"]},
                {"Fn::GetAtt": ["ElasticServerlessForwarderContinuingQueue", "Arn"]},
            ],
        }
    )

    policy_fragment["Properties"]["PolicyDocument"]["Statement"].append(
        {
            "Effect": "Allow",
            "Action": "ec2:DescribeRegions",
            "Resource": "*",
        }
    )

    return policy_fragment


def create_vpc_config(publish_config: dict[str, Any]):
    vpc_config_fragment = {}

    security_groups = []
    if "security-groups" in publish_config:
        assert isinstance(publish_config["security-groups"], list)
        security_groups = [x for x in publish_config["security-groups"] if len(x.strip()) > 0]

    subnets = []
    if "subnets" in publish_config:
        assert isinstance(publish_config["subnets"], list)
        subnets = [x for x in publish_config["subnets"] if len(x.strip()) > 0]

    if len(security_groups) > 0:
        vpc_config_fragment["SecurityGroupIds"] = security_groups

    if len(subnets) > 0:
        vpc_config_fragment["SubnetIds"] = subnets

    if "SubnetIds" in vpc_config_fragment and "SecurityGroupIds" not in vpc_config_fragment:
        vpc_config_fragment["SecurityGroupIds"] = []

    if "SecurityGroupIds" in vpc_config_fragment and "SubnetIds" not in vpc_config_fragment:
        vpc_config_fragment["SubnetIds"] = []

    return vpc_config_fragment

def create_environment_variables(publish_config: dict[str, Any]):
    environment_vars = {}

    if "telemetry" in publish_config:
        if "enabled" in publish_config["telemetry"]:
            environment_vars["TELEMETRY_ENABLED"] = publish_config["telemetry"]["enabled"]

        if "endpoint" in publish_config["telemetry"]:
            environment_vars["TELEMETRY_ENDPOINT"] = publish_config["telemetry"]["endpoint"]

    if "environment" in publish_config:
        for var in publish_config["environment"]:
            environment_vars[var] = publish_config["environment"][var]

    return environment_vars


if __name__ == "__main__":
    publish_config_path = sys.argv[1]
    print(publish_config_path)

    cloudformation_template_path = sys.argv[2]
    print(cloudformation_template_path)

    yaml = YAML()  # default, if not specfied, is 'rt' (round-trip)

    publish_config_yaml = {}
    with open(publish_config_path, "r") as f:
        publish_config_yaml = yaml.load(f)

    assert isinstance(publish_config_yaml, dict)

    cloudformation_yaml = {}
    with open(cloudformation_template_path, "r") as f:
        cloudformation_yaml = yaml.load(f)

    assert isinstance(cloudformation_yaml, dict)

    vpc_config = create_vpc_config(publish_config_yaml)
    if vpc_config:
        cloudformation_yaml["Resources"]["ApplicationElasticServerlessForwarder"]["Properties"][
            "VpcConfig"
        ] = vpc_config

    environment_variables = create_environment_variables(publish_config_yaml)
    for var in environment_variables:
        cloudformation_yaml["Resources"]["ApplicationElasticServerlessForwarder"]["Properties"]["Environment"][
            "Variables"
        ][var] = environment_variables[var]

    created_events = create_events(publish_config_yaml)
    for created_event in created_events:
        cloudformation_yaml["Resources"]["ApplicationElasticServerlessForwarder"]["Properties"]["Events"][
            created_event
        ] = created_events[created_event]

    created_policy = create_policy(publish_config_yaml)
    cloudformation_yaml["Resources"]["ElasticServerlessForwarderPolicy"] = created_policy
    cloudformation_yaml["Resources"]["ApplicationElasticServerlessForwarder"][
        "DependsOn"
    ] = "ElasticServerlessForwarderPolicy"

    if "s3-config-file" in publish_config_yaml:
        assert isinstance(publish_config_yaml["s3-config-file"], str)
        cloudformation_yaml["Resources"]["ApplicationElasticServerlessForwarder"]["Properties"]["Environment"][
            "Variables"
        ]["S3_CONFIG_FILE"] = publish_config_yaml["s3-config-file"]

    if "continuing-queue" in publish_config_yaml:
        assert isinstance(publish_config_yaml["continuing-queue"], dict)
        if "batch_size" in publish_config_yaml["continuing-queue"]:
            cloudformation_yaml["Resources"]["ApplicationElasticServerlessForwarder"]["Properties"]["Events"][
                "SQSContinuingEvent"
            ]["Properties"]["BatchSize"] = publish_config_yaml["continuing-queue"]["batch_size"]
        else:
            cloudformation_yaml["Resources"]["ApplicationElasticServerlessForwarder"]["Properties"]["Events"][
                "SQSContinuingEvent"
            ]["Properties"]["BatchSize"] = 10

        if "batching_window_in_second" in publish_config_yaml["continuing-queue"]:
            cloudformation_yaml["Resources"]["ApplicationElasticServerlessForwarder"]["Properties"]["Events"][
                "SQSContinuingEvent"
            ]["Properties"]["MaximumBatchingWindowInSeconds"] = publish_config_yaml["continuing-queue"][
                "batching_window_in_second"
            ]

    with open(cloudformation_template_path, "w") as f:
        yaml.dump(cloudformation_yaml, f)
EOF

sed -e "s|%codeUri%|${PACKAGE_FOLDER}|g" "${TMPDIR}/publish-before-sed.yaml" > "${TMPDIR}/publish.yaml"
python "${TMPDIR}/publish.py" "${PUBLISH_CONFIG}" "${TMPDIR}/publish.yaml"

sam build --debug --use-container --build-dir "${TMPDIR}/.aws-sam/build/publish" --template-file "${TMPDIR}/publish.yaml" --region "${REGION}"
sam package --template-file "${TMPDIR}/.aws-sam/build/publish/template.yaml" --output-template-file "${TMPDIR}/.aws-sam/build/publish/packaged.yaml" --s3-bucket "${BUCKET}" --region "${REGION}"
sam deploy --stack-name "${LABDA_NAME}" --capabilities CAPABILITY_NAMED_IAM --template "${TMPDIR}/.aws-sam/build/publish/packaged.yaml" --s3-bucket "${BUCKET}" --region "${REGION}"
