#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

set -e

echo "    AWS CLI (https://aws.amazon.com/cli/) and Python3.9 with pip3 required"
echo "    Please, execute from root folder of the repo"

if [[ $# -ne 5 && $# -ne 6 ]]
then
    echo "Usage: $0 config-path lambda-name tag-name bucket-name account-id region"
    echo "    Arguments:"
    echo "    config-path: full path to the publish configuration"
    echo "    lambda-name: name of the lambda to be published in the account"
    echo "    tag-name: tag of the lambda to publis"
    echo "    bucket-name: bucket name where to store the zip artifact for the lambda"
    echo "                 (it will be created if it doesn't exists, otherwise "
    echo "                  you need already to have proper access to it)"
    echo "    account-id: AWS account id to use for publishing"
    echo "    region: region where to publish in"
    exit 1
fi

PUBLISH_CONFIG="$1"
LABDA_NAME="$2"
TAG_NAME="$3"
BUCKET="$4"
ACCOUNT_ID="$5"
REGION="$6"

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

sed -e "s|%codeUri%|${PACKAGE_FOLDER}|g" .internal/aws/cloudformation/publish.yaml > "${TMPDIR}/publish.yaml"
python .internal/aws/scripts/publish.py "${PUBLISH_CONFIG}" "${TMPDIR}/publish.yaml"

sam build --debug --use-container --build-dir "${TMPDIR}/.aws-sam/build/publish" --template-file "${TMPDIR}/publish.yaml" --region "${REGION}"
sam package --template-file "${TMPDIR}/.aws-sam/build/publish/template.yaml" --output-template-file "${TMPDIR}/.aws-sam/build/publish/packaged.yaml" --s3-bucket "${BUCKET}" --region "${REGION}"
sam deploy --stack-name "${LABDA_NAME}" --capabilities CAPABILITY_NAMED_IAM --template "${TMPDIR}/.aws-sam/build/publish/packaged.yaml" --region "${REGION}"
