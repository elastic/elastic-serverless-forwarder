# Introduction

The Elastic Serverless Forwarder is an Amazon Web Services (AWS) Lambda function that ships logs from an AWS environment to Elastic.

Please refer to the official [Elastic documentation for Elastic Serverless Forwarder](https://www.elastic.co/guide/en/observability/master/aws-elastic-serverless-forwarder.html) for detailed instructions on how to deploy and configure the forwarder.

## Overview

- Amazon S3 (via SQS event notifications)
- Amazon Kinesis Data Streams
- Amazon CloudWatch Logs subscription filters
- Amazon SQS message payload

![Lambda flow](https://github.com/elastic/elastic-serverless-forwarder/raw/lambda-v0.25.0/docs/lambda-flow.png)

## Important - v1.6.0

#### Version 1.6.0 introduces a new event ID format which is backwards incompatible with previously indexed events. Be aware that previously indexed events would be duplicated if they trigger the forwarder again after upgrading to this version. More information is available at [our troubleshooting documentation](https://www.elastic.co/guide/en/observability/master/aws-serverless-troubleshooting.html#aws-serverless-troubleshooting-event-id-format).

## Resources and links

* [Elastic documentation for Elastic Serverless Forwarder](https://www.elastic.co/guide/en/observability/master/aws-elastic-serverless-forwarder.html)
* [Elastic documentation for integrations](https://docs.elastic.co/en/integrations)
* [Blog: Elastic and AWS Serverless Application Repository (SAR): Speed time to actionable insights with frictionless log ingestion from Amazon S3](https://www.elastic.co/blog/elastic-and-aws-serverless-application-repository-speed-time-to-actionable-insights-with-frictionless-log-ingestion-from-amazon-s3)
