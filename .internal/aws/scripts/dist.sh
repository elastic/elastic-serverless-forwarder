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
TMPDIR=$(mktemp -d /tmp/dist.XXXXXXXXXX)

trap "rm -rf ${TMPDIR}" EXIT

mkdir "${TMPDIR}/packages"
pip3 install --upgrade --target "${TMPDIR}/packages" -r requirements.txt

pushd "${TMPDIR}/packages"
zip -r "${TMPDIR}/lambda.zip" .

popd

cp LICENSE.txt "${TMPDIR}/LICENSE.txt"
cp docs/README-AWS.md "${TMPDIR}/README.md"

zip -g "${TMPDIR}/lambda.zip" main_aws.py
zip -r -g "${TMPDIR}/lambda.zip" handlers/aws -i "*.py"
zip -r -g "${TMPDIR}/lambda.zip" share -i "*.py"
zip -r -g "${TMPDIR}/lambda.zip" shippers -i "*.py"
zip -r -g "${TMPDIR}/lambda.zip" storage -i "*.py"

aws s3api get-bucket-location --bucket "${BUCKET}" || aws s3api create-bucket --acl private --bucket "${BUCKET}" --region "${REGION}" --create-bucket-configuration LocationConstraint="${REGION}"
aws s3 cp "${TMPDIR}/lambda.zip" "s3://${BUCKET}/lambda.zip"

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

sed -e "s/%sarAppName%/${SAR_APP_NAME}/g" -e "s/%sarAuthorName%/${SAR_AUTHOR_NAME}/g" -e "s/%semanticVersion%/${SEMANTIC_VERSION}/g" -e "s/%codeURIBucket%/${BUCKET}/g" -e "s/%accountID%/${ACCOUNT_ID}/g" -e "s/%awsRegion%/${REGION}/g" .internal/aws/cloudformation/template.yaml > "${TMPDIR}/template.yaml"

sam package --template-file "${TMPDIR}/template.yaml" --output-template-file "${TMPDIR}/packaged.yaml" --s3-bucket "${BUCKET}" --region "${REGION}"
sam publish --template "${TMPDIR}/packaged.yaml" --region "${REGION}"
