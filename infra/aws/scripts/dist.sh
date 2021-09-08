#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]
then
    echo "Usage: $0 bucket-name account-id region"
    exit 1
fi

BUCKET="$1"
ACCOUNT_ID="$2"
REGION="$3"

pip3 install --upgrade --target ./packages -r tests/requirements/reqs-boto3-newest.txt
pip3 install --upgrade --target ./packages -r tests/requirements/reqs-elasticsearch-7.txt

rm dist/lambda.zip || true

cd packages
zip -r ../dist/lambda.zip .

cd ../
zip -r -g dist/lambda.zip share
zip -r -g dist/lambda.zip shippers
zip -r -g dist/lambda.zip storage

cd handlers/aws
zip -g ../../dist/lambda.zip *.py

aws s3api get-bucket-location --bucket "${BUCKET}" || aws s3api create-bucket --acl private --bucket "${BUCKET}" --region "${REGION}" --create-bucket-configuration LocationConstraint="${REGION}"
aws s3 cp ../../dist/lambda.zip "s3://${BUCKET}/lambda.zip"

cd ../..
cat <<EOF > infra/aws/cloudformation/policy.json
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

aws s3api put-bucket-policy --bucket "${BUCKET}" --policy file://infra/aws/cloudformation/policy.json

sed -e "s/%codeUriBucket%/${BUCKET}/g" infra/aws/cloudformation/template.yaml > "infra/aws/cloudformation/template-${BUCKET}.yaml"

sam package --template-file "infra/aws/cloudformation/template-${BUCKET}.yaml" --output-template-file infra/aws/cloudformation/packaged.yaml --s3-bucket "${BUCKET}"
sam publish --template infra/aws/cloudformation/packaged.yaml --region "${REGION}"

rm infra/aws/cloudformation/policy.json "infra/aws/cloudformation/template-${BUCKET}.yaml"
