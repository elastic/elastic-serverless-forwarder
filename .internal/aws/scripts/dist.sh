#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

set -e

echo "    AWS CLI (https://aws.amazon.com/cli/) and Python3.9 with pip3 required"
echo "    Please, execute from root folder of the repo"

if [[ $# -ne 5 && $# -ne 6 ]]
then
    echo "Usage: $0 sar-app-name semantic-version bucket-name account-id region"
    echo "    Arguments:"
    echo "    sar-app-name: name of the app to be deployed in SAR"
    echo "    semantic-version: semantic version of the app to deploy in SAR"
    echo "    bucket-name: bucket name where to store the zip artifact for SAR code"
    echo "                 (it will be created if it doesn't exists, otherwise "
    echo "                  you need already to have proper access to it)"
    echo "    account-id: AWS account id to use for deploying"
    echo "    region: region where to deploy in SAR for"
    echo "    sar-author-name: name of the author of the app to be deployed in SAR"
    echo "                  (default to Elastic))"
    exit 1
fi

SAR_APP_NAME="$1"
SEMANTIC_VERSION="$2"
BUCKET="$3"
ACCOUNT_ID="$4"
REGION="$5"
SAR_AUTHOR_NAME="${6:-Elastic}"
CODE_URI=$(pwd)
TMPDIR=$(mktemp -d /tmp/dist.XXXXXXXXXX)

trap "rm -rf ${TMPDIR}" EXIT

cat <<EOF > "${TMPDIR}/policy.json"
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service":  "serverlessrepo.amazonaws.com"
            },
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::${BUCKET}/*",
            "Condition" : {
                "StringEquals": {
                    "aws:SourceAccount": "${ACCOUNT_ID}"
                }
            }
        }
    ]
}
EOF

aws s3api put-bucket-policy --bucket "${BUCKET}" --policy "file://${TMPDIR}/policy.json"

sed -e "s|%codeUri%|${CODE_URI}|g" -e "s/%sarAppName%/${SAR_APP_NAME}/g" -e "s/%sarAuthorName%/${SAR_AUTHOR_NAME}/g" -e "s/%semanticVersion%/${SEMANTIC_VERSION}/g" -e "s/%codeURIBucket%/${BUCKET}/g" -e "s/%accountID%/${ACCOUNT_ID}/g" -e "s/%awsRegion%/${REGION}/g" .internal/aws/cloudformation/template.yaml > "${TMPDIR}/template.yaml"

sam build --use-container --build-dir "${TMPDIR}/.aws-sam/build" --template-file "${TMPDIR}/template.yaml" --region "${REGION}"
sam package --template-file "${TMPDIR}/.aws-sam/build/template.yaml" --output-template-file "${TMPDIR}/.aws-sam/build/packaged.yaml" --s3-bucket "${BUCKET}" --region "${REGION}"
sam publish --template "${TMPDIR}/.aws-sam/build/packaged.yaml" --region "${REGION}"
