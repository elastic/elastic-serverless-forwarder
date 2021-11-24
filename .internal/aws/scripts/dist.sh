#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

set -e

if [[ $# -ne 4 ]]
then
    echo "Usage: $0 sar-app-name bucket-name account-id region"
    exit 1
fi

SAR_APP_NAME="$1"
BUCKET="$2"
ACCOUNT_ID="$3"
REGION="$4"

TMPDIR=$(mktemp -d /tmp/dist.XXXXXXXXXX)

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

sed -e "s/%sarAppName%/${SAR_APP_NAME}/g" -e "s/%codeURIBucket%/${BUCKET}/g" -e "s/%accountID%/${ACCOUNT_ID}/g" -e "s/%awsRegion%/${REGION}/g" .internal/aws/cloudformation/template.yaml > "${TMPDIR}/template.yaml"

sam package --template-file "${TMPDIR}/template.yaml" --output-template-file "${TMPDIR}/packaged.yaml" --s3-bucket "${BUCKET}"
sam publish --template "${TMPDIR}/packaged.yaml" --region "${REGION}"

rm -rf "${TMPDIR}"
