## Note:
This functionality is in beta and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Introduction:
The Elastic Serverless Forwarder is an AWS Lambda function that ships logs from your AWS environment to Elastic. The function can forward data to Elastic self-managed or Elastic cloud environments. It supports the following inputs:

- S3 SQS Event Notifications input
- Kinesis Data Stream input

The config yaml file (details described below) acts as an input where the user, based on input type, configures things like SQS queue ARN and Elasticsearch connection information. Multiple input sections can be created in the configuration file pointing to different queues that match specific log types.

A continuing SQS queue is set up by the Lambda deployment automatically. It is used to trigger a new function invocation so that Lambda can continue from exactly where the last function run was terminated. By default a Lambda function runs for a max of 15 minutes. When processing large log files there’s a possibility that the function may be exited by AWS in the middle of processing a log file. The code handles this scenario gracefully by keeping track of the file and offset its processing.

Application supports automatic routing of various AWS service logs to the corresponding data streams for further processing and storage in the Elasticsearch cluster.

Any exception or failure scenarios are handled by the Lambda gracefully using a replay queue. Lambda keeps track of any failed events and writes it to a replay queue that the user can consume from later on.


As a first step users should install appropriate integrations in the Kibana UI. This sets up appropriate pre-built dashboards, ingest node configurations, and other assets that help you get the most out of the data you ingest.

**S3 SQS Event Notifications input:**
The Lambda function supports ingesting logs contained in the S3 bucket through an SQS notification (s3:ObjectCreated) and sends them to Elastic. The SQS queue serves as a trigger for the Lambda function. When a new log file gets written to an S3 bucket and meets the criteria (as configured including prefix/suffix), a notification to SQS is generated that triggers the Lambda function. Users will set up separate SQS queues for each type of logs (i.e. aws.vpcflow, aws.cloudtrail, aws.waf and so on). A single configuration file can have many input sections, pointing to different SQS queues that match specific log types.
The `es_index_or_datastream_name` parameter in the config file is optional. Lambda supports automatic routing of various AWS service logs to the corresponding data streams for further processing and storage in the Elasticsearch cluster. It supports automatic routing of `aws.cloudtrail`, `aws.cloudwatch_logs`, `aws.elb_logs`, `aws.firewall_logs`, `aws.vpcflow`, and `aws.waf` logs. For other log types the users can optionally set the `es_index_or_datastream_name` value in the configuration file according to the naming convention of Elasticsearch datastream and existing integrations.  If the `es_index_or_datastream_name` is not specified and it cannot be matched with any of the above AWS services then the dataset will be set to "generic" and the namespace to "default" pointing to the data stream name "logs-generic-default".

**Kinesis Data Stream input:**
The Lambda function supports ingesting logs contained in the payload of a Kinesis data stream record and sends them to Elastic. The Kinesis data stream serves as a trigger for the Lambda function. When a new record gets written to a Kinesis data stream the Lambda function gets triggered. Users will set up separate Kinesis data streams for each type of logs, The config param for Elasticsearch output `es_index_or_datastream_name` is mandatory. If the value is set to an Elasticsearch datastream, the type of logs must be defined with proper value configuration param. A single configuration file can have many input sections, pointing to different Kinesis data streams that match specific log types.


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
  * Click on the "Deploy" button in the bottom right corner
* Once the Applications page for "serverlessrepo-elastic-serverless-forwarder" is loaded
  * Click on "Deployments" tab
    * Monitor the "Deployment history" refreshing its status until the Status shows as "Create complete
* Go to "Lambda > Functions" page in the AWS console and look for the Function Name with prefix "serverlessrepo-elastic-se-ElasticServerlessForward-" and click on it
  * Go to "Configuration" tab and select "Environment Variables"
  * Click on "Edit" on the "Environment Variables" tab and add the environment variable `S3_CONFIG_FILE` with the value of the S3 url in the format "s3://bucket-name/config-file-name" point to the configuration file for your Elastic Forwarder for Serverless (see below)
  * You can additionally add the following environment variables to enable Elastic APM instrumentation to your deployment of Elastic Forwarder for Serverless
    * | Key                       | Value  |
      |---------------------------|--------|
      |`ELASTIC_APM_ACTIVE`       | `true` |
      |`ELASTIC_APM_SECRET_TOKEN` | token  |
      |`ELASTIC_APM_SERVER_URL`	  | url    |
  * Still in the "Configuration" tab select "Permissions"
    * Click on the link of the IAM role for the Lambda under *Execution role* -> *Role name*
    * In the new window add a new policy to the role, as described at [Lambda IAM permissions and policies](#lambda-iam-permissions-and-policies)
  * Back to the "Configuration" tab in the Lambda window select "Triggers"
    * You can see an already defined SQS trigger for a queue with the prefix `elastic-serverless-forwarder-continuing-queue-`. This is an internal queue and should not be modified, disabled or removed.
    * Click on "Add trigger"
    * From "Trigger configuration" dropdown select "SQS"
    * In the "SQS queue" field chose the queue or insert the ARN of the queue you want to use as trigger for your Elastic Serverless Forwarder
      * The SQS queue you want to use as trigger must have a visibility timeout of 910 seconds, 10 seconds more than the Elastic Forwarder for Serverless Lambda timeout.
    * Click on "Add"

### Cloudformation
* Save the following yaml content as `sar-application.yaml`
```yaml
Transform: AWS::Serverless-2016-10-31
Resources:
  SarCloudformationDeployment:
    Type: AWS::Serverless::Application
    Properties:
      Location:
        ApplicationId: 'arn:aws:serverlessrepo:eu-central-1:267093732750:applications/elastic-serverless-forwarder'
        SemanticVersion: %VERSION%

```

* Deploy the Lambda from SAR running the following command:
  * ```commandline
    aws cloudformation deploy --template-file sar-application.yaml --stack-name esf-cloudformation-deployment --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND
    ```

* Import json template from deployed stack running the following commands:
  * ```commandline
    PARENT_STACK_ARN=$(aws cloudformation describe-stacks --stack-name esf-cloudformation-deployment --query Stacks[0].StackId --output text)
    LAMBDA_STACK_ARN=$(aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE --query "StackSummaries[?ParentId==\`${PARENT_STACK_ARN}\`].StackId" --output text)
    aws cloudformation get-template --stack-name "${LAMBDA_STACK_ARN}" --query TemplateBody > sar-lambda.json
    ```

* Edit sar-lambda.json to add required IAM permissions for the Lambda to run:
  * Add `Policies` to Resources.`ElasticServerlessForwarderFunctionRole.Properties`
  ```json
   "Policies": [
    {
      "PolicyName": "ElasticServerlessForwarderFunctionRolePolicySQSContinuingQueue", ## ADD AS IT IS FOR THE CONTINUING QUEUE
      "PolicyDocument": {
        "Version": "2012-10-17",
        "Statement": [
          {
          "Action": [
            "sqs:SendMessage"
          ],
          "Resource": {
            "Fn::GetAtt": [
              "ElasticServerlessForwarderContinuingQueue",
              "Arn"
            ]
          },
          "Effect": "Allow"
          }
        ]
      }
    },
    {
      "PolicyName": "ElasticServerlessForwarderFunctionRolePolicySQSReplayQueue", ## ADD AS IT IS FOR THE REPLAY QUEUE
      "PolicyDocument": {
        "Version": "2012-10-17",
        "Statement": [
          {
          "Action": [
            "sqs:SendMessage"
          ],
          "Resource": {
            "Fn::GetAtt": [
              "ElasticServerlessForwarderReplayQueue",
              "Arn"
            ]
          },
          "Effect": "Allow"
          }
        ]
      }
    },
    {
      "PolicyName": "ElasticServerlessForwarderFunctionRolePolicyS3Configfile", ## ADAPT TO THE CONFIG FILE IN THE S3 BUCKET
      "PolicyDocument": {
        "Version": "2012-10-17",
        "Statement": [
          {
          "Action": [
            "s3:GetObject"
          ],
          "Resource": "arn:aws:s3:::%CONFIG_FILE_BUCKET_NAME%/%CONFIG_FILE_OBJECT_KEY%",
          "Effect": "Allow"
          }
        ]
      }
    },
    {
      "PolicyName": "ElasticServerlessForwarderFunctionRolePolicySQS", ## ADD FOR YOUR SQS QUEUE
      "PolicyDocument": {
        "Version": "2012-10-17",
        "Statement": [
          {
          "Action": [
            "sqs:GetQueueUrl"
          ],
          "Resource": [
            "arn:aws:sqs:%AWS_REGION%:%AWS_ACCOUNT_ID%:%QUEUE_NAME%",
            ...
          ],
          "Effect": "Allow"
          }
        ]
      }
    },
      {
      "PolicyName": "ElasticServerlessForwarderFunctionRolePolicyKinesis", ## ADD FOR YOUR KINESIS STREAM
      "PolicyDocument": {
        "Version": "2012-10-17",
        "Statement": [
          {
          "Action": [
            "kinesis:GetRecords",
            "kinesis:GetShardIterator",
            "kinesis:DescribeStream",
            "kinesis:ListShards",
            "kinesis:ListStreams"
          ],
          "Resource": [
          "arn:aws:kinesis:%AWS_REGION%:%AWS_ACCOUNT_ID%:stream/%STREAM_NAME%",
            ...
          ],
          "Effect": "Allow"
          }
        ]
      }
    },
    {
      "PolicyName": "ElasticServerlessForwarderFunctionRolePolicyS3", ## ADD FOR YOUR S3 BUCKET
      "PolicyDocument": {
        "Version": "2012-10-17",
        "Statement": [
          {
          "Action": [
            "s3:GetObject"
          ],
          "Resource": [
            "arn:aws:s3:::%BUCKET_NAME%/*",
            ...
          ],
          "Effect": "Allow"
          }
        ]
      }
    },
    {
      "PolicyName": "ElasticServerlessForwarderFunctionRolePolicySM", ## ADD FOR YOUR SECRET MANAGER SECRET
      "PolicyDocument": {
        "Version": "2012-10-17",
        "Statement": [
          {
          "Action": [
            "secretsmanager:GetSecretValue"
          ],
          "Resource": [
            "arn:aws:secretsmanager:%AWS_REGION%:%AWS_ACCOUNT_ID%:secret:%SECRET_NAME%",
            ...
          ],
          "Effect": "Allow"
          }
        ]
      }
    },
    {
      "PolicyName": "ElasticServerlessForwarderFunctionRolePolicyKMS", ## ADD FOR YOUR KMS DECRYPT KEY
      "PolicyDocument": {
        "Version": "2012-10-17",
        "Statement": [
          {
          "Action": [
            "kms:Decrypt"
          ],
          "Resource": [
            "arn:aws:kms:%AWS_REGION%:%AWS_ACCOUNT_ID%:key/%KEY_ID%",
            ...
          ],
          "Effect": "Allow"
          }
        ]
      }
    }
  ]
  ```

* Edit sar-lambda.json to further customise your deployment of Elastic Forwarder for Serverless
  * Examples:
    * Adding environment variables: add entries in `Resources.ElasticServerlessForwarderFunction.Environment.Variables`
      ```json
      "Environment": {
        "Variables": {
          "SQS_CONTINUE_URL": { # Do not remove this
            "Ref": "ElasticServerlessForwarderContinuingQueue"
          },
          "SQS_REPLAY_URL": { # Do not remove this
            "Ref": "ElasticServerlessForwarderReplayQueue"
          },
          "ELASTIC_APM_ACTIVE": "true",
          "ELASTIC_APM_SECRET_TOKEN": "%ELASTIC_APM_SECRET_TOKEN%",
          "ELASTIC_APM_SERVER_URL": "%ELASTIC_APM_SERVER_URL%",
          "S3_CONFIG_FILE": "s3://bucket-name/config-file-name"
        }
      },
      ```
    * Adding an Event Source Mapping: add a new resource in the template
      ```json
      "CustomSQSEvent": {
        "Type": "AWS::Lambda::EventSourceMapping",
        "Properties": {
          "BatchSize": 1,
          "Enabled": true,
          "FunctionName": { # You must reference to `ElasticServerlessForwarderFunction` resource
            "Ref": "ElasticServerlessForwarderFunction"
          },
          "EventSourceArn": "%SQS_ARN%"
        }
      }
      ```

* Update the stack running the following command:
  * ```commandline
    aws cloudformation update-stack --stack-name "${LAMBDA_STACK_ARN}" --template-body file://./sar-lambda.json --capabilities CAPABILITY_IAM
    ```

#### Lambda IAM permissions and policies
A Lambda function has a policy, called an execution role, that grants it permission to access AWS services and resources. Lambda assumes the role when the function is invoked. The role is automatically created when the Function is deployed. The Execution role associated with your function can be seen in the Configuration->Permissions section and by default starts with the name “serverlessrepo-elastic-se-ElasticServerlessForward-”. You can add additional policies to grant minimum permission to the Lambda to be able to use configured continuing SQS queue, S3 buckets, Secrets manager (if using) and replay SQS queue.

Verify the Lambda is given AssumeRole permission to the following `ManagedPolicyArns`. By default this is automatically created:
`ManagedPolicyArns`:
* `arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole`
* `arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole`

On top of this basic permission the following policies must be provided:
* For the SQS queues resources that are reported in the `SQS_CONTINUE_URL` and `SQS_REPLAY_URL` environment variable the following action must be allowed:
  * `sqs:SendMessage`

* For SQS queue resources that you want to use as triggers of the Lambda the proper permissions are already included by `arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole`.
 Only the following extra action must be allowed:
    * `sqs:GetQueueUrl`

* For Kinesis data stream resources that you want to use as triggers of the Lambda the following action must be allowed on the Kinesis data streams:
  * `kinesis:GetRecords`
  * `kinesis:GetShardIterator`
  * `kinesis:DescribeStream`
  * `kinesis:ListShards`
  * `kinesis:ListStreams`

* For every S3 bucket resource that's reported in the `S3_CONFIG_FILE` environment variable the following action must be allowed on the S3 buckets' config file object key:
  * `s3:GetObject`

* For every S3 bucket resource that SQS queues are receiving notification from used by triggers of the Lambda the following action must be allowed on the S3 buckets' keys:
  * `s3:GetObject`

* For every Secret Manager secret that you want to refer in the yaml configuration file (see below) the following action must be allowed:
  * `secretsmanager:GetSecretValue`

* For every decrypt key that's not the default one that you used to encrypt your Secret Manager secrets with, the following action must be allowed:
  * `kms:Decrypt`

#### Sample policy:
  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "VisualEditor0",
        "Effect": "Allow",
        "Action": "s3:GetObject",
        ## ADAPT TO THE CONFIG FILE IN THE S3 BUCKET
        "Resource": "arn:aws:s3:::%CONFIG_FILE_BUCKET_NAME%/%CONFIG_FILE_OBJECT_KEY%"
      },
      {
        "Sid": "VisualEditor1",
        "Effect": "Allow",
        "Action": "sqs:SendMessage",
        "Resource": [
          ## ADAPT TO THE VALUE OF ENV VARIABLES `SQS_CONTINUE_URL` AND `SQS_REPLAY_URL`
          "arn:aws:sqs:%AWS_REGION%:%AWS_ACCOUNT_ID%:%SQS_CONTINUE_URL_NAME%",
          "arn:aws:sqs:%AWS_REGION%:%AWS_ACCOUNT_ID%:%SQS_REPLAY_URL_NAME%"
        ]
      },
      {
        "Sid": "VisualEditor0",
        "Effect": "Allow",
        "Action": "s3:GetObject",
        "Resource": [
          ## ADD FOR YOUR S3 BUCKET,
          "arn:aws:s3:::%BUCKET_NAME%/*",
          ...
        ]
      },
      {
        "Sid": "VisualEditor2",
        "Effect": "Allow",
        "Action": "sqs:GetQueueUrl",
        "Resource": [
          ## ADD FOR YOUR SQS QUEUES
          "arn:aws:sqs:%AWS_REGION%:%AWS_ACCOUNT_ID%:%QUEUE_NAME%",
          ...
        ]
      },
      {
        "Sid": "VisualEditor2",
        "Effect": "Allow",
        "Action": [
            "kinesis:GetRecords",
            "kinesis:GetShardIterator",
            "kinesis:DescribeStream",
            "kinesis:ListShards",
            "kinesis:ListStreams"
        ],
        "Resource": [
          ## ADD FOR YOUR KINESIS DATA STREAMS
          "arn:aws:kinesis:%AWS_REGION%:%AWS_ACCOUNT_ID%:stream/%STREAM_NAME%",
          ...
        ]
      },
      {
        "Sid": "VisualEditor1",
        "Effect": "Allow",
        "Action": "secretsmanager:GetSecretValue",
        "Resource": [
          ## ADD FOR YOUR SECRET MANAGER SECRETS
          "arn:aws:secretsmanager:%AWS_REGION%:%AWS_ACCOUNT_ID%:secret:%SECRET_NAME%",
          ...
        ]
      },
      {
        "Sid": "VisualEditor1",
        "Effect": "Allow",
        "Action": "kms:Decrypt",
        "Resource": [
          ## ADD FOR YOUR KMS DECRYPT KEYS
          "arn:aws:kms:%AWS_REGION%:%AWS_ACCOUNT_ID%:key/%KEY_ID%",
          ...
        ]
      }
    ]
  }
  ```

## S3_CONFIG_FILE
The Elastic Forwarder for Serverless Lambda rely on a config yaml file to be uploaded to an S3 bucket and referenced by the `S3_CONFIG_FILE` environment variable.

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
          es_index_or_datastream_name: "logs-generic-default"
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
          es_index_or_datastream_name: "logs-generic-default"
          batch_max_actions: 500
          batch_max_bytes: 10485760
```

#### Fields
`inputs.[]`:
A list of inputs (ie: triggers) for the Elastic Forwarder for Serverless Lambda

`inputs.[].type`:
The type of the trigger input (currently `kinesis-data-stream` and `s3-sqs` supported)

`inputs.[].id`:
The arn of the trigger input according to the type. Multiple input entries can have different unique ids with the same type.

`inputs.[].outputs`:
A list of outputs (ie: forwarding targets) for the Elastic Forwarder for Serverless Lambda. Only one output per type can be defined

`inputs.[].outputs.[].type`:
The type of the forwarding target output (currently only `elasticsearch` supported)

`inputs.[].outputs.[].args`:
Custom init arguments for the given forwarding target output
* for `elasticsearch` type the following arguments are supported:
  * `args.elasticsearch_url`: Url of elasticsearch endpoint in the format "http(s)://domain.tld:port". Mandatory in case `args.cloud_id` is not provided. Will take precedence over `args.cloud_id` if both are defined.
  * `args.cloud_id`: Cloud ID of elasticsearch endpoint. Mandatory in case `args.elasticsearch_url` is not provided. Will be ignored if `args.elasticsearch_url` is defined as well.
  * `args.username`: Username of the elasticsearch instance to connect to. Mandatory in case `args.api_key` is not provided. Will be ignored if `args.api_key` is defined as well.
  * `args.password` Password of the elasticsearch instance to connect to. Mandatory in case `args.api_key` is not provided. Will be ignored if `args.api_key` is defined as well.
  * `args.api_key`:  Api key of elasticsearch endpoint in the format username(api_key_id:api_key_secret). Mandatory in case `args.username`  and `args.password ` are not provided. Will take precedence over `args.username`/`args.password` if both are defined.
  * `args.es_index_or_datastream_name`: Name of the index or data stream where to forward the logs to. Lambda supports automatic routing of various AWS service logs to the corresponding data streams for further processing and storage in the Elasticsearch cluster. It supports automatic routing of `aws.cloudtrail`, `aws.cloudwatch_logs`, `aws.elb_logs`, `aws.firewall_logs`, `aws.vpcflow`, and `aws.waf` logs. For other log types, if using data stream, the users can optionally set its value in the configuration file according to the naming convention for data streams and available integrations. If the `es_index_or_datastream_name` is not specified and it cannot be matched with any of the above AWS services then the value will be set to "logs-generic-default".
  * `args.batch_max_actions`: Maximum number of actions to send in a single bulk request. Default value: 500
  * `args.batch_max_bytes`: Maximum size in bytes to send in a sigle bulk request. Default value: 10485760 (10MB)

## Secrets Manager Support
```yaml
inputs:
  - type: "s3-sqs"
    id: "arn:aws:sqs:%REGION%:%ACCOUNT%:%QUEUENAME%"
    outputs:
      - type: "elasticsearch"
        args:
          elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_url"
          username: "arn:aws:secretsmanager:eu-west-1:123-456-789:secret:es_secrets:username"
          password: "arn:aws:secretsmanager:eu-west-1:123-456-789:secret:es_secrets:password"
          es_index_or_datastream_name: "logs-generic-default"
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
          elasticsearch_url: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:es_url"
          username: "arn:aws:secretsmanager:eu-west-1:123-456-789:secret:es_secrets:username"
          password: "arn:aws:secretsmanager:eu-west-1:123-456-789:secret:es_secrets:password"
          es_index_or_datastream_name: "logs-generic-default"
```
Using the above configuration, the tags will be set in the following way`["preserve_original_event", "forwarded", "generic", "tag1", "tag2", "tag3"]`

#### Notes
- Tags must be placed at input level in the config file
- Tags must be added in the form list only
- Each tag must be a string

## Routing support for AWS Services Logs
If the `es_index_or_datastream_name` field is empty or not set in the config file, the ESF Lambda will try to guess where the logs came from

If the origin is unknown, or it cannot be matched with any of the following `aws.cloudtrail`, `aws.cloudwatch_logs`, `aws.elb_logs`, `aws.firewall_logs`, `aws.vpcflow`, `aws.waf`, then the `es_index_or_datastream_name` will be set to **"logs-generic-default"**

## S3 event notification to SQS
In order to set up an S3 event notification to SQS please look at the official documentation: https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html

The event type to set up in the notification should be `s3:ObjectCreated:*`

The Elastic Forwarder for Serverless Lambda doesn't need to be provided extra IAM policies in order to access S3 and SQS resources in your account: the policies to grant only the minimum required permissions for the Lambda to run are already defined in the SAM template when creating the Lambda from the Serverless Application Repository.

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
In case of an S3 SQS Event Notifications input, the unprocessed batch will be sent to the SQS continuing queue.
In case of a Kinesis Data Stream input the Lambda will return the sequence numbers of the unprocessed batch in the `batchItemFailures` response: allowing the affected records to be included in following batches that will trigger the Lambda. It is therefore important to set enough number of retry attempts and/or lower the size of the batches in order for the whole batch to be able to be processed at most during a single execution of the Lambda and/or giving some extra retry attemps for the whole content to be processed by multiple executions of the Lambda.
