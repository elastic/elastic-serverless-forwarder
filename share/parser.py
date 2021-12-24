# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.


import json
from typing import Any, Dict


def parse_dataset(output_config: Dict[str, Any], lambda_event: Dict[str, Any]) -> None:
    body: str = lambda_event["Records"][0]["body"]
    json_body: Dict[str, Any] = json.loads(body)

    if "Records" in json_body and len(json_body["Records"]) > 0:
        if "s3" in json_body["Records"][0]:
            s3_key: str = json_body["Records"][0]["s3"]["object"]["key"]

    if "/CloudTrail/" in s3_key or "/CloudTrail-Digest/" in s3_key or "/CloudTrail-Insight/" in s3_key:
        output_config["dataset"] = "aws.cloudtrail"
    elif "vpcflowlogs" in s3_key:
        output_config["dataset"] = "aws.vpc"
    elif "exportedlogs" in s3_key or "cloudwatch" in s3_key or "awslogs" in s3_key:
        output_config["dataset"] = "aws.cloudwatch"
    elif "elasticloadbalancing" in s3_key:
        output_config["dataset"] = "aws.elb"

    return None
