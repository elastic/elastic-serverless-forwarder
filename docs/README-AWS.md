## Introduction:
The Elastic Serverless Forwarder is an AWS Lambda function that ships logs from your AWS environment to Elastic. The function can forward data to Elastic self-managed or Elastic cloud environments. It supports the following inputs:

- S3 SQS Event Notifications input
- Kinesis Data Stream input
- CloudWatch Logs subscription filter input
- Direct SQS message payload input

![Lambda flow](https://github.com/elastic/elastic-serverless-forwarder/raw/lambda-v0.25.0/docs/lambda-flow.png)

A continuing SQS queue is set up by the Lambda deployment automatically. It is used to trigger a new function invocation so that Lambda can continue from exactly where the last function run was terminated. By default a Lambda function runs for a max of 15 minutes. When processing events data there’s a possibility that the function may be exited by AWS in the middle of processing. The code handles this scenario gracefully by keeping track of the last offset it processed.

Any exception or failure scenarios are handled by the Lambda gracefully using a replay queue. The data in the replay queue is stored as individual events. Lambda keeps track of any failed events and writes it to a replay queue that the user can consume from later on by adding additional SQS trigger on the Lambda.

The config yaml file (details described below) acts as an input where the user, based on input type, configures things like SQS queue ARN and Elasticsearch connection information. Multiple input sections can be created in the configuration file pointing to different inputs that match specific log types.

As a first step users should install appropriate [integration](https://docs.elastic.co/en/integrations) assets using the Kibana UI. This sets up appropriate pre-built dashboards, ingest node configurations, and other assets that help you get the most out of the data you ingest. The integrations use [data streams](https://www.elastic.co/guide/en/elasticsearch/reference/current/data-streams.html) with specific [naming conventions](https://www.elastic.co/blog/an-introduction-to-the-elastic-data-stream-naming-scheme) providing users with more granular controls and flexibility on managing the ingested data.

Lambda function also supports writing directly to an index, alias or a custom data stream as well. This can help some users to use the existing Elasticsearch setup like index templates, ingest pipelines or dashboards that you may have already set up and may have developed a business process around it.

**Direct SQS message payload input:**

The Lambda function supports ingesting logs contained in the payload of a SQS body record and sends them to Elastic. The SQS queue serves as a trigger for the Lambda function. When a new record gets written to an SQS queue the Lambda function gets triggered. Users will set up separate SQS queues for each type of logs. The config param for Elasticsearch output `es_datastream_name` is mandatory. If the value is set to an Elasticsearch datastream, the type of logs must be defined with proper value configuration param. A single configuration file can have many input sections, pointing to different SQS queues that match specific log types.

**S3 SQS Event Notifications input:**

The Lambda function supports ingesting logs contained in the S3 bucket through an SQS notification (s3:ObjectCreated) and sends them to Elastic. The SQS queue serves as a trigger for the Lambda function. When a new log file gets written to an S3 bucket and meets the criteria (as configured including prefix/suffix), a notification to SQS is generated that triggers the Lambda function. Users will set up separate SQS queues for each type of logs (i.e. aws.vpcflow, aws.cloudtrail, aws.waf and so on). A single configuration file can have many input sections, pointing to different SQS queues that match specific log types.
The `es_datastream_name` parameter in the config file is optional. Lambda supports automatic routing of various AWS service logs to the corresponding data streams for further processing and storage in the Elasticsearch cluster. It supports automatic routing of `aws.cloudtrail`, `aws.cloudwatch_logs`, `aws.elb_logs`, `aws.firewall_logs`, `aws.vpcflow`, and `aws.waf` logs. For other log types the users can optionally set the `es_datastream_name` value in the configuration file according to the naming convention of Elasticsearch datastream and existing integrations.  If the `es_datastream_name` is not specified, and it cannot be matched with any of the above AWS services then the dataset will be set to "generic" and the namespace to "default" pointing to the data stream name "logs-generic-default".

For more information, read the AWS [documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ways-to-add-notification-config-to-bucket.html) about creating an SQS event notifications for S3 buckets.

#### Notes on SQS queues
The SQS queues you want to use as trigger must have a visibility timeout of 910 seconds, 10 seconds more than the Elastic Serverless Forwarder Lambda timeout.

**Kinesis Data Stream input:**

The Lambda function supports ingesting logs contained in the payload of a Kinesis data stream record and sends them to Elastic. The Kinesis data stream serves as a trigger for the Lambda function. When a new record gets written to a Kinesis data stream the Lambda function gets triggered. Users will set up separate Kinesis data streams for each type of logs, The config param for Elasticsearch output `es_datastream_name` is mandatory. If the value is set to an Elasticsearch datastream, the type of logs must be defined with proper value configuration param. A single configuration file can have many input sections, pointing to different Kinesis data streams that match specific log types.

**CloudWatch Logs subscription filter input:**
The Lambda function supports ingesting logs contained in the message payload of CloudWatch Logs events. The CloudWatch Logs serves as a trigger for the Lambda function. Users will set up separate Cloudwatch log groups for each type of logs, The config param for Elasticsearch output `es_datastream_name` is mandatory. If the value is set to an Elasticsearch datastream, the type of logs must be defined with proper value configuration param. A single configuration file can have many input sections, pointing to different CloudWatch Logs log groups that match specific log types.


### Deployment:
At a high level the deployment consists of the following steps:

**Step1:** Install appropriate integration(s) from the Kibana UI. This sets up appropriate pre-built dashboards, ingest node configurations, and other assets that help you get the most out of the data you ingest. To see the full list of available integrations and install appropriate integrations, go to **Management** > **Integrations** in Kibana UI. For example browse or search for AWS, select the AWS integration, select Settings and click Install AWS assets to install all the AWS integrations.

**Step2:** Browse for elastic-serverless-forwarder in SAR repository in AWS Console. Review and deploy the application and provide appropriate configuration information.

## How to deploy Elastic Serverless Forwarder Lambda application from the AWS Serverless Application Repository.

### AWS Console
* Login to the AWS console
* Navigate to the Lambda service
  * Click on "Create function"
  * Select "Browse serverless app repository"
  * Select "Public applications" tab
  * In the search box type "elastic-serverless-forwarder" and submit
  * Look for "elastic-serverless-forwarder" in the results and click on it
  * On the "Application settings" fill the following parameters
    * `ElasticServerlessForwarderS3ConfigFile` with the value of the S3 url in the format "s3://bucket-name/config-file-name" pointing to the configuration file for your deployment of Elastic Serverless Forwarder (see below), this will populate the `S3_CONFIG_FILE` environment variable of the deployed Lambda.
    * `ElasticServerlessForwarderSSMSecrets` with a comma delimited list of AWS SSM Secrets ARNs referenced in the config yaml file (if any).
    * `ElasticServerlessForwarderKMSKeys` with a comma delimited list of AWS KMS Keys ARNs to be used for decrypting AWS SSM Secrets referenced in the config yaml file (if any).
    * `ElasticServerlessForwarderSQSEvents` with a comma delimited list of Direct SQS queues ARNs to set as event triggers for the Lambda (if any).
    * `ElasticServerlessForwarderS3SQSEvents` with a comma delimited list of S3 SQS Event Notifications ARNs to set as event triggers for the Lambda (if any).
    * `ElasticServerlessForwarderKinesisEvents` with a comma delimited list of Kinesis Data Stream ARNs to set as event triggers for the Lambda (if any).
    * `ElasticServerlessForwarderCloudWatchLogsEvents` with a comma delimited list of Cloudwatch Logs Log Groups ARNs to set subscription filters on the Lambda for (if any).
    * `ElasticServerlessForwarderS3Buckets` with a comma delimited list of S3 buckets ARNs that are the sources of the S3 SQS Event Notifications (if any).
  * Click on the "Deploy" button in the bottom right corner
* Once the Applications page for "serverlessrepo-elastic-serverless-forwarder" is loaded
  * Click on "Deployments" tab
    * Monitor the "Deployment history" refreshing its status until the Status shows as "Create complete
* Go to "Lambda > Functions" page in the AWS console and look for the Function Name with prefix "serverlessrepo-elastic-se-ElasticServerlessForward-" and click on it
  * Go to "Configuration" tab and select "Environment Variables"
  * You can additionally add the following environment variables to enable Elastic APM instrumentation for your deployment of Elastic Serverless Forwarder
    * | Key                       | Value  |
      |---------------------------|--------|
      |`ELASTIC_APM_ACTIVE`       | `true` |
      |`ELASTIC_APM_SECRET_TOKEN` | token  |
      |`ELASTIC_APM_SERVER_URL`	  | url    |

### Cloudformation

* Get the latest application semantic version
```
aws serverlessrepo list-application-versions --application-id arn:aws:serverlessrepo:eu-central-1:267093732750:applications/elastic-serverless-forwarder
```

* Save the following yaml content as `sar-application.yaml`
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
* Deploy the Lambda from SAR running the following command:
  * ```commandline
    aws cloudformation deploy --template-file sar-application.yaml --stack-name esf-cloudformation-deployment --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND
    ```

#### Notes
Due to a [bug](https://github.com/aws/serverless-application-model/issues/1320) in AWS CloudFormation, if you want to update the Events settings for the deployed Lambda, you will have to execute a deployment deleting the existing ones before actually apply the new updated ones.

### Terraform
* Save the following yaml content as `sar-application.tf`
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
* Deploy the Lambda from SAR running the following command:
  * ```commandline
    terrafrom init
    ```
  * ```commandline
    terrafrom apply
    ```
#### Notes
* Due to a [bug](https://github.com/aws/serverless-application-model/issues/1320) in AWS CloudFormation, if you want to update the Events settings for the deployed Lambda, you will have to execute a deployment deleting the existing ones before actually apply the new updated ones.
* Due to a [bug](https://github.com/hashicorp/terraform-provider-aws/issues/24771) in Terraform related to `aws_serverlessapplicationrepository_application` resource, in order to delete existing Events you have to set the related `aws_serverlessapplicationrepository_cloudformation_stack.parameters` to a blank space value (`" "`) instead that to an empty string (`""`), otherwise the parameter won't be deleted.


#### Lambda IAM permissions and policies
A Lambda function has a policy, called an execution role, that grants it permission to access AWS services and resources. Lambda assumes the role when the function is invoked. The role is automatically created when the Function is deployed. The Execution role associated with your function can be seen in the Configuration->Permissions section and by default starts with the name “serverlessrepo-elastic-se-ElasticServerlessForward-”. An custom policy is added to grant minimum permissions to the Lambda to be able to use configured continuing SQS queue, S3 buckets, Kinesis data stream, CloudWatch Logs Log Groups, Secrets manager (if using) and replay SQS queue.

The Lambda is given the following `ManagedPolicyArns`. By default, these are automatically added if relevant to the Events set up:
`ManagedPolicyArns`:
* `arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole`
* `arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole`
* `arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole`

On top of this basic permission the following ones are added:
* For the SQS queues resources that are reported in the `SQS_CONTINUE_URL` and `SQS_REPLAY_URL` environment variable the following action is allowed:
  * `sqs:SendMessage`

* For the S3 bucket resource that's reported in the `S3_CONFIG_FILE` environment variable the following action is allowed on the S3 buckets' config file object key:
  * `s3:GetObject`

* For every S3 bucket resource that SQS queues are receiving notification from used by triggers of the Lambda the following action is allowed on the S3 buckets:
  * `s3:ListBucket`

* For every S3 bucket resource that SQS queues are receiving notification from used by triggers of the Lambda the following action is allowed on the S3 buckets' keys:
  * `s3:GetObject`

* For every Secret Manager secret that you want to refer in the yaml configuration file (see below) the following action is allowed:
  * `secretsmanager:GetSecretValue`

* For every decrypt key that's not the default one that you used to encrypt your Secret Manager secrets with, the following action is allowed:
  * `kms:Decrypt`

* If any CloudWatch Logs log groups is set as input of the Lamda, the following action is allowed for the resource `arn:aws:logs:%AWS_REGION%:%AWS_ACCOUNT_ID%:log-group:*:*`:
  * `logs:DescribeLogGroups`

#### Lambda Resource-based policy for CloudWatch Logs subscription filter input
* For CloudWatch Logs subscription filter log group resources that you want to use as triggers of the Lambda the following is allowed as Resource-based policy in separated Policy statements:
  * Principal: `logs.%AWS_REGION%.amazonaws.com`
  * Action: `lambda:InvokeFunction`
  * Source ARN: `arn:aws:logs:%AWS_REGION%:%AWS_ACCOUNT_ID%:log-group:%LOG_GROUP_NAME%:*`


## S3_CONFIG_FILE
The Elastic Serverless Forwarder Lambda rely on a config yaml file to be uploaded to an S3 bucket and referenced by the `S3_CONFIG_FILE` environment variable.

This is the format of the config yaml file
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
    id: "arn:aws:logs:%AWS_REGION%:%AWS_ACCOUNT_ID%:log-group:%LOG_GROUP_NAME%"
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

#### Fields
`inputs.[]`:
A list of inputs (ie: triggers) for the Elastic Serverless Forwarder Lambda

`inputs.[].type`:
The type of the trigger input (currently `cloudwatch-logs`, `kinesis-data-stream`, `sqs` and`s3-sqs` supported)

`inputs.[].id`:
The arn of the trigger input according to the type. Multiple input entries can have different unique ids with the same type.

`inputs.[].outputs`:
A list of outputs (ie: forwarding targets) for the Elastic Serverless Forwarder Lambda. Only one output per type can be defined

`inputs.[].outputs.[].type`:
The type of the forwarding target output (currently only `elasticsearch` supported)

`inputs.[].outputs.[].args`:
Custom init arguments for the given forwarding target output
* for `elasticsearch` type the following arguments are supported:
  * `args.elasticsearch_url`: Url of elasticsearch endpoint in the format "http(s)://domain.tld:port". Mandatory in case `args.cloud_id` is not provided. Will take precedence over `args.cloud_id` if both are defined.
  * `args.cloud_id`: Cloud ID of elasticsearch endpoint. Mandatory in case `args.elasticsearch_url` is not provided. Will be ignored if `args.elasticsearch_url` is defined as well.
  * `args.username`: Username of the elasticsearch instance to connect to. Mandatory in case `args.api_key` is not provided. Will be ignored if `args.api_key` is defined as well.
  * `args.password` Password of the elasticsearch instance to connect to. Mandatory in case `args.api_key` is not provided. Will be ignored if `args.api_key` is defined as well.
  * `args.api_key`:  Api key of elasticsearch endpoint in the format **base64encode(api_key_id:api_key_secret)**. Mandatory in case `args.username`  and `args.password ` are not provided. Will take precedence over `args.username`/`args.password` if both are defined.
  * `args.es_datastream_name`: Name of data stream or the index where to forward the logs to. Lambda supports automatic routing of various AWS service logs to the corresponding data streams for further processing and storage in the Elasticsearch cluster. It supports automatic routing of `aws.cloudtrail`, `aws.cloudwatch_logs`, `aws.elb_logs`, `aws.firewall_logs`, `aws.vpcflow`, and `aws.waf` logs. For other log types, if using data stream, the users can optionally set its value in the configuration file according to the naming convention for data streams and available integrations. If the `es_datastream_name` is not specified and it cannot be matched with any of the above AWS services then the value will be set to "logs-generic-default". In version **v0.29.1** and before this configuration parameter was named `es_index_or_datastream_name`. Rename the configuration parameter to `es_datastream_name` in your configuration yaml file on the S3 bucket, to continue using it in the future version. The older name `es_index_or_datastream_name` is deprecated as of version **v0.30.0**. The related backward compatibility code is removed from version **v1.0.0**.
  * `args.batch_max_actions`: Maximum number of actions to send in a single bulk request. Default value: 500
  * `args.batch_max_bytes`: Maximum size in bytes to send in a single bulk request. Default value: 10485760 (10MB)

## Secrets Manager Support
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
There are 2 types of secrets that can be used:
- SecretString (plain text or key/value pairs)
- SecretBinary

#### Usage
Beware that the arn of a secret is `arn:aws:secretsmanager:AWS_REGION:AWS_ACCOUNT_ID:secret:SECRET_NAME`. This is assumed to be the name of a **plain text** or a **binary** secret.

In order to use a **key/value** pair secret you need to provide the key at the end of the arn like `arn:aws:secretsmanager:AWS_REGION:AWS_ACCOUNT_ID:secret:SECRET_NAME:SECRET_KEY`.

Currently, the only version retrieved for a secret is `AWSCURRENT`.

It supports secrets from different regions.

#### Notes
- Cannot use the same secret for both plain text and key/value pair
- It's case-sensitive
- Any typo or misconfiguration in the config file will be ignored (or exceptions risen), therefore the secrets will not be retrieved
- Keys must exist in the Secrets Manager
- Empty value for a given key is not allowed

## Tags support
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
Using the above configuration, the tags will be set in the following way`["forwarded", "generic", "tag1", "tag2", "tag3"]`

#### Notes
- Tags must be placed at input level in the config file
- Tags must be added in the form list only
- Each tag must be a string

## Include/exclude filter support
It is possible to handle at input level multiple filters for including or excluding events to be ingested.
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

#### Notes

`inputs.[].include` can be defined as a list of regular expressions.
If the list is defined only the messages **matching any** of the defined regular expression will be forwarder to the outputs.

`inputs.[].exclude` can be defined as a list of regular expressions.
If the list is defined only the messages **not matching all** of the defined regular expressions will be forwarder to the outputs. Ie: every message that will be forwarder to the outputs unless matching any of the defined regular expressions.

Both config params are optional, and can be set independently of each other: exclude is applied first and then include is applied, so exclude takes precedence if both are supplied.

Regular expressions supported follow [Python 3.9 regular expression systanx](https://docs.python.org/3.9/library/re.html#regular-expression-syntax).
They are case-sensitive and are scanned through the original ingested message, looking for any location where they match: anchoring at the beginning of the string must be done with explicit `^` (caret.) special character.
When the regular expression is compiled no flags are used, please refer to [inline flag documentation](https://docs.python.org/3.9/library/re.html#re.compile) for alternative to multiline, case-insensitive and other matching behaviour.


## Expand events from a list in a json object
In case of JSON content it is possible to extract a list of events to be ingested from a field in the JSON.
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

#### Notes
`inputs.[].expand_event_list_from_field` can be defined as string with the value of a key in the JSON that contains a list of elements that must be sent as events instead of the encompassing JSON.

Beware that when relying on the [Routing support for AWS Service Logs](#routing-support-for-aws-services-logs), you must not to set the `expand_event_list_from_field` configuration param, since it will be taken care automatically by the Elastic Serverless Forwarder.

#### Example:
Having the following content from the input:
```json lines
{"Records":[{"key": "value #1"},{"key": "value #2"}]}
{"Records":[{"key": "value #3"},{"key": "value #4"}]}
```
- without setting `expand_event_list_from_field` two events will be forwarded:
    ```json lines
    {"@timestamp": "2022-06-16T04:06:03.064Z", "message": "{\"Records\":[{\"key\": \"value #1\"},{\"key\": \"value #2\"}]}"}
    {"@timestamp": "2022-06-16T04:06:13.888Z", "message": "{\"Records\":[{\"key\": \"value #3\"},{\"key\": \"value #4\"}]}"}
    ```

- if `expand_event_list_from_field` value is set to `Records`, four events will be forwaderd:
    ```json lines
    {"@timestamp": "2022-06-16T04:06:21.105Z", "message": "{\"key\": \"value #1\"}"}
    {"@timestamp": "2022-06-16T04:06:27.204Z", "message": "{\"key\": \"value #2\"}"}
    {"@timestamp": "2022-06-16T04:06:31.154Z", "message": "{\"key\": \"value #3\"}"}
    {"@timestamp": "2022-06-16T04:06:36.189Z", "message": "{\"key\": \"value #4\"}"}
    ```


## Routing support for AWS Services Logs
When using Elastic integrations, as a first step users should install appropriate [integration](https://docs.elastic.co/en/integrations) assets using the Kibana UI. This sets up appropriate pre-built dashboards, ingest node configurations, and other assets that help you get the most out of the data you ingest. The integrations use [data streams](https://www.elastic.co/guide/en/elasticsearch/reference/current/data-streams.html) with specific [naming conventions](https://www.elastic.co/blog/an-introduction-to-the-elastic-data-stream-naming-scheme) providing users with more granular controls and flexibility on managing the ingested data.

For `S3 SQS Event Notifications input` the Lambda function supports automatic routing of several AWS service logs to the corresponding [integration](https://docs.elastic.co/en/integrations) [data streams](https://docs.elastic.co/en/integrations) for further processing and storage in the Elasticsearch cluster. It supports automatic routing of AWS CloudTrail (`aws.cloudtrail`), Amazon CloudWatch Logs (`aws.cloudwatch_logs`), Elastic Load Balancing(`aws.elb_logs`), AWS Network Firewall (`aws.firewall_logs`), Amazon VPC Flow (`aws.vpcflow`) & AWS Web Application Firewall (`aws.waf`) logs to corresponding default integrations data streams. Setting the `es_datastream_name` field in the configuration file is optional for this use case.

For most of the other use cases, the user will need to set the `es_datastream_name` field in the configuration file to route the data to a specific data stream or an index. This value should be set in the following use cases:
- Users want to write the data to a specific index, alias or a custom data stream and not to the default integration data streams. This can help some users to use the existing Elasticsearch setup like index templates, ingest pipelines or dashboards that you may have already set up and may have developed a business process around it and don’t want to change it.
- When using `Kinesis Data Stream`, `CloudWatch Logs subscription filter` or `Direct SQS message payload` input. Only `S3 SQS Event Notifications input` method supports automatic routing to default integrations data streams for several AWS services logs.
- When using `S3 SQS Event Notifications input` but the log types is something other than AWS CloudTrail (`aws.cloudtrail`), Amazon CloudWatch Logs (`aws.cloudwatch_logs`), Elastic Load Balancing (`aws.elb_logs`), AWS Network Firewall (`aws.firewall_logs`), Amazon VPC Flow (`aws.vpcflow`) & AWS Web Application Firewall (`aws.waf`).

If the `es_datastream_name` is not specified and it cannot be matched with any of the above AWS services then the dataset will be set to "generic" and the namespace to "default" pointing to the data stream name "logs-generic-default".

## Setting up S3 event notification to SQS
In order to set up an S3 event notification to SQS please look at the official documentation: https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html

The event type to set up in the notification should be `s3:ObjectCreated:*`

The Elastic Serverless Forwarder Lambda needs to be provided extra IAM policies in order to access S3 and SQS resources in your account: please refer to [Lambda IAM permissions and policies](#lambda-iam-permissions-and-policies).

## Error handling
There are two kind of errors that can happen during the execution of the Lambda:
1. Errors before the ingestion phase started
2. Errors during the ingestion phase

For errors at (1), the Lambda will return a failure: these errors are mostly due to misconfiguration, improper permission on the AWS resources, etc. Most importantly when an error occurs at this stage we don’t have any state yet about the events that are ingested, so there’s no consistency to keep and we can safely let the Lambda return a failure.
In the case of SQS message and Kinesis data stream record, both of them will go back to the queue and will trigger the Lambda again with the same payload.

For errors at (2) the situation is different: we have now a state for N failed events out of total X events, if we fail the whole Lambda all the X events will be processed again. While the N failed ones could now succeed, the remaining X-N will now fail, since the datastreams are append-only and we would try to recreate already ingested documents (the ID of the document is deterministic).

In case any of these errors will happen the Lambda won't return a failure. However, the payload of the event that failed to be ingested will be sent to a replay SQS queue.
The replay SQS queue is set up by the Lambda deployment automatically.
The replay SQS queue is not set as Event Source Mapping for the Lambda by default: the user can consume the message as preferred in order to investigate the failure.
It is possible anyway to temporarily set the replay SQS queue as Event Source Mapping for the Lambda: in this case the messages in the queue will be consumed by the Lambda and tried to be ingested again if the failure was transient.
In case the failure will persist the affected log entry will be moved to a DLQ after three retries.

Every other error occurring during the execution of the Lambda is silently ignored and reported to the APM server is instrumentation is enabled.

## Execution timeout
There is a grace period of 2 minutes before the timeout of the Lambda where no more ingestion will happen. Instead, during this grace period the Lambda will collect and handle any unprocessed payload in the batch of the input used as trigger.
In case of an CloudWatch Logs event input, S3 SQS Event Notifications input and direct SQS message payload input, the unprocessed batch will be sent to the SQS continuing queue.
In case of a Kinesis Data Stream input the Lambda will return the sequence numbers of the unprocessed batch in the `batchItemFailures` response: allowing the affected records to be included in following batches that will trigger the Lambda. It is therefore important to set enough number of retry attempts and/or lower the size of the batches in order for the whole batch to be able to be processed at most during a single execution of the Lambda and/or giving some extra retry attemps for the whole content to be processed by multiple executions of the Lambda.
