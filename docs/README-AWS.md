# Introduction

The Elastic Serverless Forwarder is an Amazon Web Services (AWS) Lambda function that ships logs from an AWS environment to Elastic.

Please refer to the official [Elastic documentation for Elastic Serverless Forwarder](https://www.elastic.co/guide/en/observability/master/aws-elastic-serverless-forwarder.html) for detailed instructions on how to deploy and configure the forwarder.

## Overview

- Amazon S3 (via SQS event notifications)
- Amazon Kinesis Data Streams
- Amazon CloudWatch Logs subscription filters
- Amazon SQS message payload

![Lambda flow](https://github.com/elastic/elastic-serverless-forwarder/raw/lambda-v0.25.0/docs/lambda-flow.png)

## Resources and links

* [Elastic documentation for Elastic Serverless Forwarder](https://www.elastic.co/guide/en/observability/master/aws-elastic-serverless-forwarder.html)
* [Elastic documentation for integrations](https://docs.elastic.co/en/integrations)
* [Blog: Elastic and AWS Serverless Application Repository (SAR): Speed time to actionable insights with frictionless log ingestion from Amazon S3](https://www.elastic.co/blog/elastic-and-aws-serverless-application-repository-speed-time-to-actionable-insights-with-frictionless-log-ingestion-from-amazon-s3)
