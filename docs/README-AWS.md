# Introduction

This readme file provides initial documentation for the Elastic Serverless Forwarder, an Amazon Web Services (AWS) Lambda function that ships logs from an AWS environment to Elastic.

## Table of contents

- [Overview](#overview)
- [Inputs](#inputs)
- [Deploying Elastic Serverless Forwarder](#deploying-elastic-serverless-forwarder)
- [Permissions and policies](#permissions-and-policies)
- [Sample S3 config file](#sample-s3-config-file)
- [Using Secrets Manager](#using-secrets-manager)
- [Using tags](#using-tags)
- [Defining include/exclude filters](#defining-includeexclude-filters)
- [JSON content discovery](#json-content-discovery)
- [Expanding events from list in json object](#expanding-events-from-list-in-json-object)
- [Manage multiline messages](#manage-multiline-messages)
- [Routing AWS Service Logs](#routing-aws-service-logs)
- [Setting up S3 event notifications for SQS](#setting-up-s3-event-notifications-for-sqs)
- [Troubleshooting and error handling](#troubleshooting-and-error-handling)
- [Resources and links](#resources-and-links)

## Overview
The Elastic Serverless Forwarder is an AWS Lambda function that ships logs from your AWS environment to Elastic. The function can forward data to self-managed or cloud Elastic environments. It supports the following inputs:

- S3 SQS (Simple Queue Service) event notifications
- Kinesis Data Streams
- CloudWatch Logs subscription filters
- Direct SQS message payload

![Lambda flow](https://github.com/elastic/elastic-serverless-forwarder/raw/lambda-v0.25.0/docs/lambda-flow.png)

<!-- more detail on paragraph steps below needed e.g. "A continuing SQS queue is set up by the Lambda deployment automatically. "-->

The Lambda deployment sets up an SQS queue automatically. This is used to trigger a new function invocation that Lambda uses to continue from exactly where the last function run was terminated. By default a Lambda function runs for a maximum of 15 minutes. When processing event data there’s a possibility that the function may be exited by AWS in the middle of processing. The code handles this scenario gracefully by keeping track of the last offset processed.

Any exception or fail scenarios related to ingestion are handled by Lambda gracefully using a replay queue. Data in the replay queue is stored as individual events. Lambda keeps track of any failed events and writes them to a replay queue that the user can consume later on by adding an additional SQS trigger via Lambda.

You can use the [config.yaml file](#sample-s3-config-file) to configure the service for each input type, including information such as SQS queue ARN and Elasticsearch connection details. You can create multiple input sections within the configuration file to map different inputs to specific log types.

The Lambda function also supports writing directly to an index, alias, or custom data stream. This enables existing Elasticsearch users to re-use index templates, ingest pipelines, or dashboards that are already created and connected to business processes.

The best way to get started is to install the appropriate [integrations](https://docs.elastic.co/en/integrations) via Kibana. These integrations include pre-built dashboards, ingest node configurations, and other assets that help you get the most out of the data you ingest. The integrations use [data streams](https://www.elastic.co/guide/en/elasticsearch/reference/current/data-streams.html) with specific [naming conventions](https://www.elastic.co/blog/an-introduction-to-the-elastic-data-stream-naming-scheme) that provide users with more granular controls and flexibility on managing data ingestion.

## Inputs

### Direct SQS message payload

The Lambda function can ingest logs contained within the payload of a SQS body record and send them to Elastic. The SQS queue serves as a trigger for the Lambda function. When a new record gets written to an SQS queue, the Lambda function triggers.

You can set up a separate SQS queue for each type of log. The config parameter for Elasticsearch output `es_datastream_name` is mandatory. If this value is set to an Elasticsearch data stream, the type of log must be correctly defined with configuration parameters. A single configuration file can have many input sections, pointing to different SQS queues that match specific log types.

### S3 SQS Event Notifications

The Lambda function can ingest logs contained in the Simple Storage Service (S3) bucket through an SQS notification (`s3:ObjectCreated`) and send them to Elastic. The SQS queue serves as a trigger for the Lambda function. When a new log file is written to an S3 bucket and meets the criteria (as configured including prefix/suffix), a notification to SQS is generated that triggers the Lambda function.

You can set up separate SQS queues for each type of log (`aws.vpcflow`, `aws.cloudtrail`, `aws.waf`, for example). A single configuration file can have many input sections, pointing to different SQS queues that match specific log types. The `es_datastream_name` parameter in the config file is optional. Lambda supports automatic routing of various AWS service logs to the corresponding data streams for further processing and storage in the Elasticsearch cluster. It supports automatic routing of `aws.cloudtrail`, `aws.cloudwatch_logs`, `aws.elb_logs`, `aws.firewall_logs`, `aws.vpcflow`, and `aws.waf` logs.

For other log types, you can optionally set the `es_datastream_name` value in the configuration file according to the naming convention of the Elasticsearch data stream and integration.  If the `es_datastream_name` is not specified, and the log cannot be matched with any of the above AWS services, then the dataset will be set to `generic` and the namespace set to `default`, pointing to the data stream name `logs-generic-default`.

For more information on creating SQS event notifications for S3 buckets, read the [AWS documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ways-to-add-notification-config-to-bucket.html).

**Note:**
You must set a visibility timeout of `910` seconds for any SQS queues you want to use as a trigger. This is 10 seconds greater than the Elastic Serverless Forwarder Lambda timeout.

### Kinesis Data Streams

The Lambda function can ingest logs contained in the payload of a Kinesis Data Stream record and send them to Elastic. The Kinesis Data Stream serves as a trigger for the Lambda function. When a new record gets written to a Kinesis Data Stream, the Lambda function triggers.

You can set up separate Kinesis Data Streams for each type of log. The `es_datastream_name` parameter in the config file is mandatory. If this value is set to an Elasticsearch data stream, the type of log must be correctly defined with configuration parameters. A single configuration file can have many input sections, pointing to different data streams that match specific log types.

### CloudWatch Logs subscription filters

The Lambda function can ingest logs contained in the message payload of CloudWatch Logs events and send them to Elastic. The CloudWatch Logs service serves as a trigger for the Lambda function. When a new record gets written to a Kinesis data stream, the Lambda function triggers.

You can set up separate CloudWatch Logs groups for each type of log. The `es_datastream_name` parameter in the config file is mandatory. If this value is set to an Elasticsearch data stream, the type of log must be correctly defined with configuration parameters. A single configuration file can have many input sections, pointing to different CloudWatch Logs groups that match specific log types.

## Deploying Elastic Serverless Forwarder

Deploying Elastic Serverless Forwarder consists of the following two high level steps:

1. Add AWS integration in Kibana
2. Deploy the elastic-serverless-forwarder from AWS SAR (Serverless Application Repository)

### Add AWS integration in Kibana

1. Go to **Integrations** in Kibana and search for AWS (or select the **AWS** category to filter the list)
1. Click the AWS integration, select **Settings** and click **Install AWS assets** to install all the AWS integration assets

Adding integrations from Kibana provides appropriate pre-built dashboards, ingest node configurations, and other assets that help you get the most out of the data you ingest. For more information, see [Integrations documentation](https://docs.elastic.co/en/integrations).

There are several deployment methods available via AWS SAR:

### Deploy using AWS Console
1. Login to AWS console and navigate to the Lambda service
1. Click *Create function*
1. Select *Browse serverless app repository*
1. Select *Public applications*
1. Search for *elastic-serverless-forwarder* and select it from results
1. Complete the *Application settings* as follows:
    * `ElasticServerlessForwarderS3ConfigFile` with the value of the S3 url in the format "s3://bucket-name/config-file-name" pointing to the configuration file for your deployment of Elastic Serverless Forwarder (see below), this will populate the `S3_CONFIG_FILE` environment variable of the deployed Lambda.
    * `ElasticServerlessForwarderSSMSecrets` with a comma delimited list of AWS SSM Secrets ARNs referenced in the config yaml file (if any).
    * `ElasticServerlessForwarderKMSKeys` with a comma delimited list of AWS KMS Keys ARNs to be used for decrypting AWS SSM Secrets referenced in the config yaml file (if any).
    * `ElasticServerlessForwarderSQSEvents` with a comma delimited list of Direct SQS queues ARNs to set as event triggers for the Lambda (if any).
    * `ElasticServerlessForwarderS3SQSEvents` with a comma delimited list of S3 SQS Event Notifications ARNs to set as event triggers for the Lambda (if any).
    * `ElasticServerlessForwarderKinesisEvents` with a comma delimited list of Kinesis Data Stream ARNs to set as event triggers for the Lambda (if any).
    * `ElasticServerlessForwarderCloudWatchLogsEvents` with a comma delimited list of Cloudwatch Logs Log Groups ARNs to set subscription filters on the Lambda for (if any).
    * `ElasticServerlessForwarderS3Buckets` with a comma delimited list of S3 buckets ARNs that are the sources of the S3 SQS Event Notifications (if any).
1. Click *Deploy* once your settings have been added
1. On the Applications page for *serverlessrepo-elastic-serverless-forwarder*, click *Deployments*
1. Refresh the *Deployment history* until status updates to `Create complete`
1. (Optional) To enable Elastic APM instrumentation for your new deployment:
    * Go to *Lambda > Functions* within AWS console, find and select the function with *serverlessrepo-elastic-se-ElasticServerlessForward-*
    * Go to *Configuration* tab and select *Environment Variables*
    * Add the following environment variables:

      | Key                       | Value  |
      |---------------------------|--------|
      |`ELASTIC_APM_ACTIVE`       | `true` |
      |`ELASTIC_APM_SECRET_TOKEN` | token  |
      |`ELASTIC_APM_SERVER_URL`	  | url    |

### Deploy using Cloudformation

1. Use the following code to get the semantic version of the latest application:
    ```
    aws serverlessrepo list-application-versions --application-id arn:aws:serverlessrepo:eu-central-1:267093732750:applications/elastic-serverless-forwarder
    ```
1. Save the following yaml content as `sar-application.yaml`:
    ```yaml
    Transform: AWS::Serverless-2016-10-31
    Resources:
      SarCloudformationDeployment:
        Type: AWS::Serverless::Application
        Properties:
          Location:
            ApplicationId: 'arn:aws:serverlessrepo:eu-central-1:267093732750:applications/elastic-serverless-forwarder'
            SemanticVersion: '%SEMANTICVERSION%'  ## CHANGE REFERENCING THE SEMANTIC VERSION (IT MUST BE GREATER THAN 0.30.0)
          Parameters:
            ElasticServerlessForwarderS3ConfigFile: ""          ## FILL WITH THE VALUE OF THE S3 URL IN THE FORMAT "s3://bucket-name/config-file-name" POINTING TO THE CONFIGURATION FILE FOR YOUR DEPLOYMENT OF THE ELASTIC SERVERLESS FORWARDER
            ElasticServerlessForwarderSSMSecrets: ""            ## FILL WITH A COMMA DELIMITED LIST OF AWS SSM SECRETS ARNS REFERENCED IN THE CONFIG YAML FILE (IF ANY).
            ElasticServerlessForwarderKMSKeys: ""               ## FILL WITH A COMMA DELIMITED LIST OF AWS KMS KEYS ARNS TO BE USED FOR DECRYPTING AWS SSM SECRETS REFERENCED IN THE CONFIG YAML FILE (IF ANY).
            ElasticServerlessForwarderSQSEvents: ""             ## FILL WITH A COMMA DELIMITED LIST OF DIRECT SQS QUEUES ARNS TO SET AS EVENT TRIGGERS FOR THE LAMBDA (IF ANY).
            ElasticServerlessForwarderS3SQSEvents: ""           ## FILL WITH A COMMA DELIMITED LIST OF S3 SQS EVENT NOTIFICATIONS ARNS TO SET AS EVENT TRIGGERS FOR THE LAMBDA (IF ANY).
            ElasticServerlessForwarderKinesisEvents: ""         ## FILL WITH A COMMA DELIMITED LIST OF KINESIS DATA STREAM ARNS TO SET AS EVENT TRIGGERS FOR THE LAMBDA (IF ANY).
            ElasticServerlessForwarderCloudWatchLogsEvents: ""  ## FILL WITH A COMMA DELIMITED LIST OF CLOUDWATCH LOGS LOG GROUPS ARNS TO SET SUBSCRIPTION FILTERS ON THE LAMBDA FOR (IF ANY).
            ElasticServerlessForwarderS3Buckets: ""             ## FILL WITH A COMMA DELIMITED LIST OF S3 BUCKETS ARNS THAT ARE THE SOURCES OF THE S3 SQS EVENT NOTIFICATIONS (IF ANY).
    ```
1. Deploy the Lambda function from SAR by running the following command:
    ```commandline
    aws cloudformation deploy --template-file sar-application.yaml --stack-name esf-cloudformation-deployment --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND
    ```

**Note:**
Due to a [bug](https://github.com/aws/serverless-application-model/issues/1320) in AWS CloudFormation, if you want to update the Events settings for the deployment, you will have to manually delete existing settings before applying the new settings.

### Deploy using Terraform

1. Save the following yaml content as `sar-application.tf`
    ```
    provider "aws" {
      region = ""  ## FILL WITH THE AWS REGION WHERE YOU WANT TO DEPLOY THE ELASTIC SERVERLESS FORWARDER
    }

    data "aws_serverlessapplicationrepository_application" "esf_sar" {
      application_id = "arn:aws:serverlessrepo:eu-central-1:267093732750:applications/elastic-serverless-forwarder"
    }

    resource "aws_serverlessapplicationrepository_cloudformation_stack" "esf_cf_stak" {
      name             = "terraform-elastic-serverless-forwarder"
      application_id   = data.aws_serverlessapplicationrepository_application.esf_sar.application_id
      semantic_version = data.aws_serverlessapplicationrepository_application.esf_sar.semantic_version
      capabilities     = data.aws_serverlessapplicationrepository_application.esf_sar.required_capabilities

    parameters = {
        ElasticServerlessForwarderS3ConfigFile         = ""  ## FILL WITH THE VALUE OF THE S3 URL IN THE FORMAT "s3://bucket-name/config-file-name" POINTING TO THE CONFIGURATION FILE FOR YOUR DEPLOYMENT OF THE ELASTIC SERVERLESS FORWARDER

        ElasticServerlessForwarderSSMSecrets           = ""  ## FILL WITH A COMMA DELIMITED LIST OF AWS SSM SECRETS ARNS REFERENCED IN THE CONFIG YAML FILE (IF ANY).

        ElasticServerlessForwarderKMSKeys              = ""  ## FILL WITH A COMMA DELIMITED LIST OF AWS KMS KEYS ARNS TO BE USED FOR DECRYPTING AWS SSM SECRETS REFERENCED IN THE CONFIG YAML FILE (IF ANY).

        ElasticServerlessForwarderSQSEvents            = ""  ## FILL WITH A COMMA DELIMITED LIST OF DIRECT SQS QUEUES ARNS TO SET AS EVENT TRIGGERS FOR THE LAMBDA (IF ANY).

        ElasticServerlessForwarderS3SQSEvents          = ""  ## FILL WITH A COMMA DELIMITED LIST OF S3 SQS EVENT NOTIFICATIONS ARNS TO SET AS EVENT TRIGGERS FOR THE LAMBDA (IF ANY).

        ElasticServerlessForwarderKinesisEvents        = ""  ## FILL WITH A COMMA DELIMITED LIST OF KINESIS DATA STREAM ARNS TO SET AS EVENT TRIGGERS FOR THE LAMBDA (IF ANY).

        ElasticServerlessForwarderCloudWatchLogsEvents = ""  ## FILL WITH A COMMA DELIMITED LIST OF CLOUDWATCH LOGS LOG GROUPS ARNS TO SET SUBSCRIPTION FILTERS ON THE LAMBDA FOR (IF ANY).

        ElasticServerlessForwarderS3Buckets            = ""  ## FILL WITH A COMMA DELIMITED LIST OF S3 BUCKETS ARNS THAT ARE THE SOURCES OF THE S3 SQS EVENT NOTIFICATIONS (IF ANY).
      }
    }
    ```
1. Deploy the Lambda from SAR by running the following commands:
    ```commandline
    terrafrom init
    terrafrom apply
    ```

**Notes:**
* Due to a [bug](https://github.com/aws/serverless-application-model/issues/1320) in AWS CloudFormation, if you want to update the Events settings for the deployment, you will have to manually delete existing settings before applying the new settings.
* Due to a [bug](https://github.com/hashicorp/terraform-provider-aws/issues/24771) in Terraform related to `aws_serverlessapplicationrepository_application`, if you want to delete existing Event parameters you have to set the related `aws_serverlessapplicationrepository_cloudformation_stack.parameters` to a blank space value (`" "`) instead of an empty string (`""`).


## Permissions and policies
A Lambda function's execution role is an AWS Identity and Access Management (IAM) role that grants the function permission to access AWS services and resources. This role is automatically created when the function is deployed and Lambda assumes this role when the function is invoked.

You can view the execution role associated with your function from the *Configuration -> Permissions* section. By default this role starts with the name *serverlessrepo-elastic-se-ElasticServerlessForward-*. A custom policy is added <!-- where --> to grant minimum permissions to Lambda to be able to use the configured SQS queue, S3 buckets, Kinesis data stream, CloudWatch Logs log groups, Secrets manager (if using), and SQS replay queue.

The Lambda funcion is given the following `ManagedPolicyArns` permissions. By default these are automatically added if relevant to the Events configuration:
```
arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole`
arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole`
arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole`
```

In addition to these basic permissions, the following permissions are added <!-- how? by whom?-->:

* For the SQS queue resources that are reported in the `SQS_CONTINUE_URL` and `SQS_REPLAY_URL` environment variables, the following action is allowed:

  `sqs:SendMessage`

* For the S3 bucket resources that are reported in the `S3_CONFIG_FILE` environment variable, the following action is allowed on the S3 buckets' config file object key:

  `s3:GetObject`

* For every S3 bucket resource that SQS queues are receiving notifications from, the following action is allowed on the S3 buckets:

  `s3:ListBucket`

* For every S3 bucket resource that SQS queues are receiving notifications from, the following action is allowed on the S3 buckets' keys:

  `s3:GetObject`

* For every Secret Manager secret that you want to refer in the yaml configuration file, the following action is allowed <!-- more detail? cf. on what above-->:

  `secretsmanager:GetSecretValue`

* For every decrypt key that's not the default one used to encrypt your Secret Manager secrets with, the following action is allowed:

  `kms:Decrypt`

* If any CloudWatch Logs log groups are set as Lambda input, the following actions are allowed for the resource

  `arn:aws:logs:%AWS_REGION%:%AWS_ACCOUNT_ID%:log-group:*:*`:

  `logs:DescribeLogGroups`

#### Lambda resource-based policy for CloudWatch Logs subscription filter input
For CloudWatch Logs subscription filter log group resources that you want to use as triggers of the Lambda function, the following is allowed as a resource-based policy in separate Policy statements:
```
  * Principal: `logs.%AWS_REGION%.amazonaws.com`
  * Action: `lambda:InvokeFunction`
  * Source ARN: `arn:aws:logs:%AWS_REGION%:%AWS_ACCOUNT_ID%:log-group:%LOG_GROUP_NAME%:*`
```

## Sample S3 config file
Elastic Serverless Forwarder relies on a `config.yaml` file to be uploaded to an S3 bucket and referenced by the `S3_CONFIG_FILE` environment variable.

This is the required format, with comments identifying parameters to be edited:
```yaml
inputs:
  - type: "s3-sqs"
    id: "arn:aws:sqs:%REGION%:%ACCOUNT%:%QUEUENAME%"
    outputs:
      - type: "elasticsearch"
        args:
          # either elasticsearch_url or cloud_id, elasticsearch_url takes precedence
          elasticsearch_url: "http(s)://domain.tld:port"
          cloud_id: "cloud_id:bG9jYWxob3N0OjkyMDAkMA=="
          # either api_key or username/password, api_key takes precedence
          api_key: "YXBpX2tleV9pZDphcGlfa2V5X3NlY3JldAo="
          username: "username"
          password: "password"
          es_datastream_name: "logs-generic-default"
          batch_max_actions: 500
          batch_max_bytes: 10485760
  - type: "sqs"
    id: "arn:aws:sqs:%REGION%:%ACCOUNT%:%QUEUENAME%"
    outputs:
      - type: "elasticsearch"
        args:
          # either elasticsearch_url or cloud_id, elasticsearch_url takes precedence
          elasticsearch_url: "http(s)://domain.tld:port"
          cloud_id: "cloud_id:bG9jYWxob3N0OjkyMDAkMA=="
          # either api_key or username/password, api_key takes precedence
          api_key: "YXBpX2tleV9pZDphcGlfa2V5X3NlY3JldAo="
          username: "username"
          password: "password"
          es_datastream_name: "logs-generic-default"
          batch_max_actions: 500
          batch_max_bytes: 10485760
  - type: "kinesis-data-stream"
    id: "arn:aws:kinesis:%REGION%:%ACCOUNT%:stream/%STREAMNAME%"
    outputs:
      - type: "elasticsearch"
        args:
          # either elasticsearch_url or cloud_id, elasticsearch_url takes precedence
          elasticsearch_url: "http(s)://domain.tld:port"
          cloud_id: "cloud_id:bG9jYWxob3N0OjkyMDAkMA=="
          # either api_key or username/password, api_key takes precedence
          api_key: "YXBpX2tleV9pZDphcGlfa2V5X3NlY3JldAo="
          username: "username"
          password: "password"
          es_datastream_name: "logs-generic-default"
          batch_max_actions: 500
          batch_max_bytes: 10485760
  - type: "cloudwatch-logs"
    id: "arn:aws:logs:%AWS_REGION%:%AWS_ACCOUNT_ID%:log-group:%LOG_GROUP_NAME%:*"
    outputs:
      - type: "elasticsearch"
        args:
          # either elasticsearch_url or cloud_id, elasticsearch_url takes precedence
          elasticsearch_url: "http(s)://domain.tld:port"
          cloud_id: "cloud_id:bG9jYWxob3N0OjkyMDAkMA=="
          # either api_key or username/password, api_key takes precedence
          api_key: "YXBpX2tleV9pZDphcGlfa2V5X3NlY3JldAo="
          username: "username"
          password: "password"
          es_datastream_name: "logs-generic-default"
          batch_max_actions: 500
          batch_max_bytes: 10485760
```

### Fields
`inputs.[]`:

A list of inputs (ie: triggers) for the Elastic Serverless Forwarder Lambda.

`inputs.[].type`:

The type of the trigger input (currently `cloudwatch-logs`, `kinesis-data-stream`, `sqs` and`s3-sqs` supported).

`inputs.[].id`:

The ARN of the trigger input according to the type. Multiple input entries can have different unique ids with the same type.

`inputs.[].outputs`:

A list of outputs (i.e. forwarding targets) for the Elastic Serverless Forwarder Lambda function. Only one output can be defined per type.

`inputs.[].outputs.[].type`:

The type of the forwarding target output (currently only `elasticsearch` supported).

`inputs.[].outputs.[].args`:
Custom init arguments for the given forwarding target output.

For `elasticsearch` the following arguments are supported:

  * `args.elasticsearch_url`: Url of elasticsearch endpoint in the format "http(s)://domain.tld:port". Mandatory in case `args.cloud_id` is not provided. Will take precedence over `args.cloud_id` if both are defined.
  * `args.cloud_id`: Cloud ID of elasticsearch endpoint. Mandatory in case `args.elasticsearch_url` is not provided. Will be ignored if `args.elasticsearch_url` is defined as well.
  * `args.username`: Username of the elasticsearch instance to connect to. Mandatory in case `args.api_key` is not provided. Will be ignored if `args.api_key` is defined as well.
  * `args.password` Password of the elasticsearch instance to connect to. Mandatory in case `args.api_key` is not provided. Will be ignored if `args.api_key` is defined as well.
  * `args.api_key`:  Api key of elasticsearch endpoint in the format **base64encode(api_key_id:api_key_secret)**. Mandatory in case `args.username`  and `args.password ` are not provided. Will take precedence over `args.username`/`args.password` if both are defined.
  * `args.es_datastream_name`: Name of data stream or the index where to forward the logs to. Lambda supports automatic routing of various AWS service logs to the corresponding data streams for further processing and storage in the Elasticsearch cluster. It supports automatic routing of `aws.cloudtrail`, `aws.cloudwatch_logs`, `aws.elb_logs`, `aws.firewall_logs`, `aws.vpcflow`, and `aws.waf` logs. For other log types, if using data streams, you can optionally set its value in the configuration file according to the naming convention for data streams and available integrations. If the `es_datastream_name` is not specified and it cannot be matched with any of the above AWS services, then the value will be set to "logs-generic-default". In version **v0.29.1** and earlier, this configuration parameter was named `es_index_or_datastream_name`. Rename the configuration parameter to `es_datastream_name` in your config.yaml file on the S3 bucket to continue using it in the future version. The older name `es_index_or_datastream_name` is deprecated as of version **v0.30.0**. The related backward compatibility code is removed from version **v1.0.0**.
  * `args.batch_max_actions`: Maximum number of actions to send in a single bulk request. Default value: 500
  * `args.batch_max_bytes`: Maximum size in bytes to send in a single bulk request. Default value: 10485760 (10MB)

## Using Secrets Manager

Amazon's Secrets Manager enables you to replace hardcoded credentials in your code, including passwords, with an API call to Secrets Manager to retrieve the secret programmatically. For more info, see the [Secrets Manager documentation from Amazon](https://docs.aws.amazon.com/secretsmanager/index.html).

There are 2 types of secrets that can be used:
- SecretString (plain text or key/value pairs)
- SecretBinary

```yaml
inputs:
  - type: "s3-sqs"
    id: "arn:aws:sqs:%REGION%:%ACCOUNT%:%QUEUENAME%"
    outputs:
      - type: "elasticsearch"
        args:
          elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_url"
          username: "arn:aws:secretsmanager:eu-west-1:123456789:secret:es_secrets:username"
          password: "arn:aws:secretsmanager:eu-west-1:123456789:secret:es_secrets:password"
          es_datastream_name: "logs-generic-default"
```

Please note that the ARN of a secret is `arn:aws:secretsmanager:AWS_REGION:AWS_ACCOUNT_ID:secret:SECRET_NAME`. This is assumed to be the name of a **plain text** or **binary** secret.

In order to use a **key/value** pair secret, you need to provide the key at the end of the arn, as per:  `arn:aws:secretsmanager:AWS_REGION:AWS_ACCOUNT_ID:secret:SECRET_NAME:SECRET_KEY`

Secrets from different regions are supported but, the only version currently retrieved for a secret is `AWSCURRENT`.

**Notes:**
- You cannot use the same secret for both plain text and key/value pairs
- Secrets are case-sensitive
- Any typo or misconfiguration in the config file will be ignored (or exceptions raised); in which case secrets will not be retrieved
- Keys must exist in the Secrets Manager
- Empty values for a given key are not allowed

## Using tags

Adding custom tags is a common way to filter and categorize items in events.

```yaml
inputs:
  - type: "s3-sqs"
    id: "arn:aws:sqs:%REGION%:%ACCOUNT%:%QUEUENAME%"
    tags:
      - "tag1"
      - "tag2"
      - "tag3"
    outputs:
      - type: "elasticsearch"
        args:
          elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_url"
          username: "arn:aws:secretsmanager:eu-west-1:123456789:secret:es_secrets:username"
          password: "arn:aws:secretsmanager:eu-west-1:123456789:secret:es_secrets:password"
          es_datastream_name: "logs-generic-default"
```

Using the above example configuration, the tags will be set in the following way:

`["forwarded", "generic", "tag1", "tag2", "tag3"]`

**Notes:**
- Tags must be defined at input level in the config file
- Tags must be added in the form list only
- Each tag must be a string

## Defining include/exclude filters

You can define multiple filters for inputs to include or exclude events from data ingestion.

```yaml
inputs:
  - type: "s3-sqs"
    id: "arn:aws:sqs:%REGION%:%ACCOUNT%:%QUEUENAME%"
    include:
      - "[a-zA-Z]"
    exclude:
      - "skip this"
      - "skip also this"
    outputs:
      - type: "elasticsearch"
        args:
          elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_url"
          username: "arn:aws:secretsmanager:eu-west-1:123456789:secret:es_secrets:username"
          password: "arn:aws:secretsmanager:eu-west-1:123456789:secret:es_secrets:password"
          es_datastream_name: "logs-generic-default"
```

**Notes:**

`inputs.[].include` can be defined as a list of regular expressions.
If this list is populated, only messages **matching any** of the defined regular expressions will be forwarded to the outputs.

`inputs.[].exclude` can be defined as a list of regular expressions.
If this list is populated, only messages **not matching any** of the defined regular expressions will be forwarded to the outputs i.e. every message will be forwarded to the outputs unless it matches any of the defined regular expressions.

Both config paramaters are optional, and can be set independently of each other. In terms of rule precedence, the exclude filter is applied first and then the include filter.

<!-- check "so exclude takes precedence if both are supplied"? -->

All regular expressions are case-sensitive and should follow [Python's 3.9 regular expression syntax](https://docs.python.org/3.9/library/re.html#regular-expression-syntax).

Messages are scanned for terms, looking for any location where they match. Anchoring at the beginning of the string must be done explicitly with the `^` (caret) special character.

No flags are used when the regular expression is compiled. Please refer to [inline flag documentation](https://docs.python.org/3.9/library/re.html#re.compile) for alternative options for multiline, case-insensitive, and other matching behaviors.

## JSON content discovery

The Elastic Serverless Forwarder is able to discovery automatically JSON content in the payload of an input and collect the JSON objects contained in the payload.
The JSON objects can be either be on a single line or spanning multiple lines. In the second case the Elastic Serverless Forwarder expects each different JSON objects spanning multiple lines to be separated by a newline delimiter.

In case of JSON objects spanning multiple lines a limit of 1000 lines is applied: every JSON object spanning across more 1000 lines will not be collected. Every line composing the whole JSON object will be forwarded individually instead.

Sometimes relaying on the Elastic Serverless Forwarder JSON content auto-discovery feature might have a huge impact on performance, or you have a known payload content of a single JSON object spanning more than 1000 lines. In this case you can provide in the input configuration and hint on the nature of the JSON content: this will change the parsing logic applied and improve performance or overcome the 1000 lines limit.

This setting allows also to disable at all any attempt of JSON content automatic discovery, in case of known plain text content, in order to improve performance.

**Notes:**

`inputs.[].json_content_type` can be defined as a string with on the of the following values:
- *single*: indicates that the content of a single entry in the input payload is a single JSON object. The content can either be on a single line or spanning multiple lines. In this case the whole content of the payload is decoded as JSON object, with no limit on the number of lines the JSON object is spanning on.
- *ndjson*: indicates that the content of a single entry in the input payload is a valid NDJSON format. In NDJSON format multiple single JSON objects formatted on a single line each are separated by a newline delimiter. In this case each line will be decoded as JSON object, improving the parsing performance.
- *disabled*: instructs the Elastic Serverless Forwarder to not attempt any JSON content automatic discovery and treat the content as plain text, improving the performance.

You should note that there is no need to configure the json content type hint when relaying on [Expanding events from list in json object](#expanding-events-from-list-in-json-object), unless you are in the case of a single JSON object spanning more than 1000 lines.


## Expanding events from list in json object

You can extract a list of events to be ingested from a field in the JSON file.

```yaml
inputs:
  - type: "s3-sqs"
    id: "arn:aws:sqs:%REGION%:%ACCOUNT%:%QUEUENAME%"
    expand_event_list_from_field: "Records"
    outputs:
      - type: "elasticsearch"
        args:
          elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_url"
          username: "arn:aws:secretsmanager:eu-west-1:123456789:secret:es_secrets:username"
          password: "arn:aws:secretsmanager:eu-west-1:123456789:secret:es_secrets:password"
          es_datastream_name: "logs-generic-default"
```

**Notes:**

`inputs.[].expand_event_list_from_field` can be defined as a string with the value of a key in the JSON that contains a list of elements that must be sent as events instead of the encompassing JSON.

You should note that when relying on the [Routing AWS Service Logs](#routing-aws-service-logs), any value set for the `expand_event_list_from_field` configuration parameter will be ignored, because this will be automatically handled by the Elastic Serverless Forwarder.

### Example

With the following input:

```json lines
{"Records":[{"key": "value #1"},{"key": "value #2"}]}
{"Records":[{"key": "value #3"},{"key": "value #4"}]}
```

Without setting `expand_event_list_from_field`, two events will be forwarded:

```json lines
{"@timestamp": "2022-06-16T04:06:03.064Z", "message": "{\"Records\":[{\"key\": \"value #1\"},{\"key\": \"value #2\"}]}"}
{"@timestamp": "2022-06-16T04:06:13.888Z", "message": "{\"Records\":[{\"key\": \"value #3\"},{\"key\": \"value #4\"}]}"}
```

If `expand_event_list_from_field` is set to `Records`, four events will be forwarded:

```json lines
{"@timestamp": "2022-06-16T04:06:21.105Z", "message": "{\"key\": \"value #1\"}"}
{"@timestamp": "2022-06-16T04:06:27.204Z", "message": "{\"key\": \"value #2\"}"}
{"@timestamp": "2022-06-16T04:06:31.154Z", "message": "{\"key\": \"value #3\"}"}
{"@timestamp": "2022-06-16T04:06:36.189Z", "message": "{\"key\": \"value #4\"}"}
```

## Manage multiline messages

The forwarded content may contain messages that span multiple lines of text. For example, multiline messages are common in files that contain Java stack traces. In order to correctly handle these multiline events, you need to configure `multiline` settings for a specific input to specify which lines are part of a single event.

**Configuration options**

You can specify the following options for a specific input in the config file to control how the Elastic Serverless Forwarder deals with messages that span multiple lines.

```yaml
inputs:
  - type: "s3-sqs"
    id: "arn:aws:sqs:%REGION%:%ACCOUNT%:%QUEUENAME%"
    multiline:
      type: pattern
      pattern: '^\\['
      negate: true
      match: after
    outputs:
      - type: "elasticsearch"
        args:
          elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123456789:secret:es_url"
          username: "arn:aws:secretsmanager:eu-west-1:123456789:secret:es_secrets:username"
          password: "arn:aws:secretsmanager:eu-west-1:123456789:secret:es_secrets:password"
          es_datastream_name: "logs-generic-default"

```

The Elastic Serverless Forwarder takes all the lines that do not start with `[` and combines them with the previous line that does. For example, you could use this configuration to join the following lines of a multiline message into a single event:
```shell
[beat-logstash-some-name-832-2015.11.28] IndexNotFoundException[no such index]
    at org.elasticsearch.cluster.metadata.IndexNameExpressionResolver$WildcardExpressionResolver.resolve(IndexNameExpressionResolver.java:566)
    at org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.concreteIndices(IndexNameExpressionResolver.java:133)
    at org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.concreteIndices(IndexNameExpressionResolver.java:77)
    at org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction.checkBlock(TransportDeleteIndexAction.java:75)
```

*Notes*: beware that we should escape the opening square bracket (`[`) in the regular expression, since it has a special meaning as metacharacters used for specifying a character class, which is a set of characters that you wish to match. Then we have to escape the backslash (`\`) used for escaping the opening square bracket since we are not using raw string. The final result `^\\[` will produce the regular expression to be compiled properly.

`inputs.[].multiline.type` defines which aggregation method to use. The default is `pattern`. The other options are `count` which lets you aggregate constant number of lines and `while_pattern` which aggregate lines by pattern without match option.

`inputs.[].multiline.pattern` differs somewhat from the patterns supported by Logstash. See [Python's 3.9 regular expression syntax](https://docs.python.org/3.9/library/re.html#regular-expression-syntax) for a list of supported regexp patterns. Depending on how you configure other multiline options, lines that match the specified regular expression are considered either continuations of a previous line or the start of a new multiline event. You can set the `negate` option to negate the pattern. Works only with `pattern` and `while_pattern` types.

`inputs.[].multiline.negate` defines whether the pattern is negated. The default is `false`. Works only with `pattern` and `while_pattern` types.

`inputs.[].multiline.match` changes the grouping of multiple lines according to the schema below:

| Setting for `negate`       | Setting for `match` | Result                                                                                            | Example `pattern: ^b`                                                                                                                             |
|----------------------------|---------------------|---------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
 | `false`                    | `after`             | Consecutive lines that match the pattern are appended to the previous line that doesn’t match.    | ![Lines a b b c b b become "abb" and "cbb"](https://github.com/elastic/elastic-serverless-forwarder/raw/lambda-v1.2.0/docs/false-after-multi.png) |
 | `false`                    | `before`             | Consecutive lines that match the pattern are prepended to the next line that doesn’t match.       | ![Lines b b a b b c become "bba" and "bbc"](https://github.com/elastic/elastic-serverless-forwarder/raw/lambda-v1.2.0/docs/false-before-multi.png) |
 | `true`                     | `after`             | Consecutive lines that don’t match the pattern are appended to the previous line that does match. | ![Lines b a c b d e become "bac" and "bde"](https://github.com/elastic/elastic-serverless-forwarder/raw/lambda-v1.2.0/docs/true-after-multi.png)  |
 | `false`                    | `before`             | Consecutive lines that don’t match the pattern are prepended to the next line that does match.    | ![Lines a c b d e b become "acb" and "deb"](https://github.com/elastic/elastic-serverless-forwarder/raw/lambda-v1.2.0/docs/true-before-multi.png) |

Works only with `pattern` type.

*Notes*: the `after` setting is equivalent to `previous` in [Logstash](https://www.elastic.co/guide/en/logstash/current/plugins-codecs-multiline.html), and `before` is equivalent to `next`.

`inputs.[].multiline.flush_pattern` specifies a regular expression, in which the current multiline will be flushed from memory, ending the multiline-message. Works only with `pattern` type.

`inputs.[].multiline.max_lines` defines the maximum number of lines that can be combined into one event. If the multiline message contains more than `max_lines`, any additional lines are truncated from the event. The default is `500`.

`inputs.[].multiline.max_bytes` defines the maximum number of bytes that can be combined into one event. If the multiline message contains more than `max_bytes`, any additional content is truncated from the event. The default is `10485760`.


`inputs.[].multiline.count_lines` defines the number of lines to aggregate into a single event. Works only with `count` type.

`inputs.[].multiline.skip_newline` defined whether multiline events must be concatenated stripping the line separator. If set to `true`, the line separator will be stripped. The default is `false`.

### Examples of multiline configuration

The examples in this section cover the following use cases:

-   Combining a Java stack trace into a single event

-   Combining C-style line continuations into a single event

-   Combining multiple lines from time-stamped events

**Java stack traces**

Java stack traces consist of multiple lines, with each line after the initial line beginning with whitespace, as in this example:
```java
Exception in thread "main" java.lang.NullPointerException
        at com.example.myproject.Book.getTitle(Book.java:16)
        at com.example.myproject.Author.getBookTitles(Author.java:25)
        at com.example.myproject.Bootstrap.main(Bootstrap.java:14)
```

This configuration merges any line that begins with whitespace up to the previous line.
```yaml
multiline:
  type: pattern
  pattern: '^\s'
  negate: false
  match: after
```

Here’s a Java stack trace that presents a slightly more complex example:
```java
Exception in thread "main" java.lang.IllegalStateException: A book has a null property
       at com.example.myproject.Author.getBookIds(Author.java:38)
       at com.example.myproject.Bootstrap.main(Bootstrap.java:14)
Caused by: java.lang.NullPointerException
       at com.example.myproject.Book.getId(Book.java:22)
       at com.example.myproject.Author.getBookIds(Author.java:35)
       ... 1 more
```

To consolidate these lines into a single event, use the following multiline configuration:
```yaml
multiline:
  type: pattern
  pattern: '^\s+(at|.{3})\s+\\b|^Caused by:'
  negate: false
  match: after
```

In this example, the pattern matches the following lines:
-   a line that begins with spaces followed by the word `at` or `...`
-   a line that begins with the words `Caused by:`

*Notes*: In Python’s string literals, `\b` is the backspace character, ASCII value 8. Since we do not use raw strings, Python would convert the `\b` to a backspace. In order for our regular expression top mach as expected a word boundary we must escape the backslash `\` in `\b` to `\\b`, this will produce the regular expression to be compiled properly.


**Line continuations**

Several programming languages use the backslash (`\`) character at the end of a line to denote that the line continues, as in this example:

```c
printf ("%10.10ld  \t %10.10ld \t %s\
  %f", w, x, y, z );
```

To consolidate these lines into a single event, use the following multiline configuration:
```yaml
multiline:
  type: pattern
  pattern: '\\\\$'
  negate: false
  match: after
```

This configuration merges any line that ends with the `\` character with the line that follows.

*Notes*: beware that we should escape the backslash we want to match a string literals (`\`) twice, since we are not using raw string. The final result `\\\\$` will produce the regular expression to be compiled properly.


**Timestamps**
Activity logs from services such as Elasticsearch typically begin with a timestamp, followed by information on the specific activity, as in this example:

```shell
[2015-08-24 11:49:14,389][INFO ][env                      ] [Letha] using [1] data paths, mounts [[/
(/dev/disk1)]], net usable_space [34.5gb], net total_space [118.9gb], types [hfs]
```

To consolidate these lines into a single event, use the following multiline configuration:
```yaml
multiline:
  type: pattern
  pattern: '^\\[[0-9]{4}-[0-9]{2}-[0-9]{2}'
  negate: true
  match: after
```

This configuration uses the `negate: true` and `match: after` settings to specify that any line that does not match the specified pattern belongs to the previous line.

*Notes*: beware that we should escape the first opening square bracket (`[`) in the regular expression, since it has a special meaning as metacharacters used for specifying a character class, which is a set of characters that you wish to match (as used further in the same regular expression). Then we have to escape the backslash (`\`) used for escaping the opening square bracket since we are not using raw string. The final result `^\\[` will produce the regular expression to be compiled properly.

**Application events**

Sometimes your application logs contain events, that begin and end with custom markers, such as the following example:
```shell
[2015-08-24 11:49:14,389] Start new event
[2015-08-24 11:49:14,395] Content of processing something
[2015-08-24 11:49:14,399] End event
```

To consolidate these lines into a single event, use the following multiline configuration:
```yaml
multiline:
  type: pattern
  pattern: 'Start new event'
  negate: true
  match: after
  flush_pattern: 'End event'
```

The `flush_pattern` option, specifies a regex at which the current multiline will be flushed. If you think of the `pattern` option specifying the beginning of an event, the `flush_pattern` option will specify the end or last line of the event.

*Notes*: this example will not work correctly if start/end log blocks are mixed with non-multiline logs, or if different start/end log blocks overlap with each other. For instance, `Some other log` log lines in the following example will be merged into a *single* multiline document because they neither match `inputs.[].multiline.pattern` nor `inputs.[].multiline.flush_pattern`, and `inputs.[].multiline.negate` is set to `true`.

```shell
[2015-08-24 11:49:14,389] Start new event
[2015-08-24 11:49:14,395] Content of processing something
[2015-08-24 11:49:14,399] End event
[2015-08-24 11:50:14,389] Some other log
[2015-08-24 11:50:14,395] Some other log
[2015-08-24 11:50:14,399] Some other log
[2015-08-24 11:51:14,389] Start new event
[2015-08-24 11:51:14,395] Content of processing something
[2015-08-24 11:51:14,399] End event
```

### Test your regexp pattern for multiline

To make it easier for you to test the regexp patterns in your multiline config, we’ve created a [Multineline Regexp Test](https://replit.com/@AndreaSpacca/Multine-Regexp-Test#main.py). You can simply plug in the regexp pattern at line `3`, along with the `multiline.negate` setting that you plan to use at line `4`, and paste a sample message between the three double quotes delimiters (`""" """`) at line `5`. Then click `Run`, and you’ll see which lines in the message match your specified configuration. For example:

![](https://github.com/elastic/elastic-serverless-forwarder/raw/lambda-v1.2.0/docs/multiline-regexp-test-repl-main.png)
![](https://github.com/elastic/elastic-serverless-forwarder/raw/lambda-v1.2.0/docs/multiline-regexp-test-repl-run.png)



## Routing AWS Service Logs

For `S3 SQS Event Notifications input` the Lambda function supports automatic routing of several AWS service logs to the corresponding [integration data streams](https://docs.elastic.co/en/integrations) for further processing and storage in the Elasticsearch cluster.

Elastic Serverless Forwarder supports automatic routing of the following logs to the corresponding default integration data stream:

* AWS CloudTrail (`aws.cloudtrail`)
* Amazon CloudWatch (`aws.cloudwatch_logs`)
* Elastic Load Balancing (`aws.elb_logs`)
* AWS Network Firewall (`aws.firewall_logs`)
* Amazon VPC Flow (`aws.vpcflow`)
* AWS Web Application Firewall (`aws.waf`)

**Notes:**

For these use cases, setting the `es_datastream_name` field in the configuration file is optional.

For most of the other use cases, you will need to set the `es_datastream_name` field in the configuration file to route the data to a specific data stream or index. This value should be set in the following use cases:

- You want to write the data to a specific index, alias, or custom data stream, and not to the default integration data streams. This can help some users to use existing Elasticsearch assets like index templates, ingest pipelines, or dashboards, that are already set up and connected to business processes.
- When using `Kinesis Data Stream`, `CloudWatch Logs subscription filter` or `Direct SQS message payload` inputs. Only the `S3 SQS Event Notifications` input method supports automatic routing to default integration data streams for several AWS service logs.
- When using `S3 SQS Event Notifications` but where the log type is something *other than* AWS CloudTrail (`aws.cloudtrail`), Amazon CloudWatch Logs (`aws.cloudwatch_logs`), Elastic Load Balancing (`aws.elb_logs`), AWS Network Firewall (`aws.firewall_logs`), Amazon VPC Flow (`aws.vpcflow`), and AWS Web Application Firewall (`aws.waf`).

If the `es_datastream_name` is not specified, and the log cannot be matched with any of the above AWS services, then the dataset will be set to `generic` and the namespace set to `default`, pointing to the data stream name `logs-generic-default`.

## Setting up S3 event notifications for SQS
To set up an S3 event notification for SQS, see the [Amazon S3 docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html).

You need to set the notifications up for the `s3:ObjectCreated:*` event type.

## Troubleshooting and error handling

### Error handling

There are two kind of errors that can occur during execution of the Lambda function:

1. Errors before the ingestion phase begins
2. Errors during the ingestion phase

#### Errors before ingestion
For errors that occur before ingestion begins (1), the function will return a failure. These errors are mostly due to misconfiguration, incorrect permissions for AWS resources, etc. Most importantly, when an error occurs at this stage we don’t have any status for events that are ingested, so there’s no requirement to keep data and the function can fail safely. In the case of SQS messages and Kinesis data stream records, both will go back into the queue and trigger the function again with the same payload.

#### Errors during ingestion
For errors that occur during ingestion (2), the situation is different. We do have a status for *N* failed events out of *X* total events; if we fail the whole function then all *X* events will be processed again. While the *N* failed ones could potentially succeed, the remaining *X-N* will definitely fail, because the data streams are append-only (the function would attempt to recreate already ingested documents using the same document ID).

So the Lambda function won't return a failure for errors during ingestion; instead, the payload of the event that failed to be ingested will be sent to a replay SQS queue. The replay SQS queue is automatically set up by the Lambda deployment automatically. The replay SQS queue is not set as an Event Source Mapping for the Lambda function by default, which means you can investigate and consume (or not) the message as preferred.

You can temporarily set the replay SQS queue as an Event Source Mapping for the Lambda function, which means messages in the queue will be consumed by the Lambda and ingestion retried for transient failures. If the failure persists, the affected log entry will be moved to a DLQ after three retries.

Every other error that occurs during the execution of the Lambda function is silently ignored, and reported to the APM server if instrumentation is enabled.

### Execution timeout
There is a grace period of 2 minutes before the timeout of the Lambda function where no more ingestion will occur. Instead, during this grace period the Lambda will collect and handle any unprocessed payloads in the batch of the input used as trigger.

For CloudWatch Logs event input, Kinesis data stream input, S3 SQS Event Notifications input and direct SQS message payload input, the unprocessed batch will be sent to the SQS continuing queue.

## Resources and links

* [Main Elastic documentation](https://www.elastic.co/guide/index.html)
* [Elastic documentation for integrations](https://docs.elastic.co/en/integrations)
* [Blog: Elastic and AWS Serverless Application Repository (SAR): Speed time to actionable insights with frictionless log ingestion from Amazon S3](https://www.elastic.co/blog/elastic-and-aws-serverless-application-repository-speed-time-to-actionable-insights-with-frictionless-log-ingestion-from-amazon-s3)
