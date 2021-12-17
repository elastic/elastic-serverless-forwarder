## Note:
This functionality is in beta and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Introduction:
The Elastic Serverless Forwarder is an AWS Lambda function that ships logs from your AWS environment to Elastic. The function can forward logs to Elastic self-managed or Elastic cloud environments. It supports the following inputs:

- SQS S3 Notifications

The config yaml file (details described below) acts as an input where the user, based on input type, configures things like SQS queue ARN, Elasticsearch connection information and  dataset/namespace. The dataset and namespace helps map logs data to specific data streams for processing and storage. Multiple input sections can be created in the configuration file pointing to different queues that match specific log types, identified in the configuration by dataset and namespace.

A continuing SQS queue is set up and permissions set up automatically by the Lambda deployment. It is used to make sure the next invocation of the function can continue from exactly where the last function left. By default a Lambda function runs for a max of 15 minutes. When processing large log files there’s a possibility that the function may be exited by AWS in the middle of processing a log file. The code handles this scenario gracefully by keeping track of the file and offset its processing.

As a first step users should install appropriate integrations in the Kibana UI. This sets up appropriate pre-built dashboards, ingest node configurations, and other assets that help you get the most out of the data you ingest.

**SQS S3 Notifications input:**
The Lambda function supports ingesting logs contained in the S3 bucket through an SQS notification and sending them to Elastic. The SQS queue serves as a trigger for the Lambda function. When a new log file gets written to an S3 bucket and meets the criteria (as configured including prefix/suffix), a notification to SQS is generated that triggers the Lambda function. Users will set up separate SQS queues for each type of logs (i.e. redis.log, ngnix.log and so on). A single configuration file can have many input sections, pointing to different queues that match specific log types identified in the configuration by dataset and namespace. The dataset and namespace helps the function send the logs to the corresponding data streams for further processing and storage in the Elasticsearch cluster.

### Deployment:
At a high level the deployment consists of the following steps:
**Step1:** Install appropriate integration(s) from the Kibana UI. This sets up appropriate pre-built dashboards, ingest node configurations, and other assets that help you get the most out of the data you ingest. To see the full list of available integrations and install appropriate integrations, go to **Management** > **Integrations** in Kibana.
**Step2:** Browse for elastic-serverless-forwarder in SAR repository in AWS Console. Review and deploy the application and provide appropriate configuration information. Below are the detailed steps.

## How to deploy Elastic Forwarder for Serverless Lambda application from the AWS Serverless Application Repository.


### AWS Console
* Login to the AWS console
* Navigate to the Lambda service
  * Click on "Create a function"
  * Click on "Browse serverless app repository"
  * Select "Public applications" tab
  * In the search box type "elastic-serverless-forwarder" and submit
  * Look for "elastic-serverless-forwarder" in the results and click on it
  * Click on the "Deploy" button in the bottom right corner
* Once on the Application page for "serverlessrepo-elastic-serverless-forwarder" loaded afterward
  * Click on "Deployments" tab
    * Monitor the "Deployment history" refreshing its status until the "Lambda application" "Resource type" has "Create complete" status
* Go to "Lambda > Functions" page in the AWS console a look for the Function Name with prefix "serverlessrepo-elastic-se-ElasticServerlessForward-" and click on it
  * Go to "Configuration" tab and select "Environment Variables"
  * Click on "Edit" on the "Environment Variables" tab and add the environment variable `S3_CONFIG_FILE` with the value of the S3 url in the format "s3://bucket-name/config-file-name" point to the configuration file for your Elastic Forwarder for Serverless (see below)
  * You can additionally add the following environment variables to enable Elastic APM instrumentation to your deployment of Elastic Forwarder for Serverless
    * | Key                       | Value  |
      |---------------------------|--------|
      |`ELASTIC_APM_ACTIVE`       | `true` |
      |`ELASTIC_APM_SECRET_TOKEN` | token  |
      |`ELASTIC_APM_SERVER_URL`	  | url    |
  * Still in the "Configuration" tab select "Triggers"
    * You can see an already defined SQS trigger for a queue with the prefix `elastic-serverless-forwarder-continuing-queue-`. This is an internal queue and should not be modified, disabled or removed.
    * Click on "Add trigger"
    * From "Trigger configuration" dropdown select "SQS"
    * In the "SQS queue" field chose the queue or insert the ARN of the queue you want to use as trigger for your Elastic Forwarder for Serverless
      * The SQS queue you want to use as trigger must have a visibility timeout of 910 seconds, 10 seconds more than the Elastic Forwarder for Serverless Lambda timeout.
    * Click on "Add"
    *
#### Lambda IAM permissions and policies
Assure the Lambda is given AssumeRole permission to the following `ManagedPolicyArns`:
* `arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole`
* `arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole`

On top of this basic permission the following policies must be provided:
* For the SQS queue resource that's reported in the `SQS_CONTINUE_URL` environment variable the following action must be allowed:
  * `sqs:SendMessage`

* For every S3 bucket resource that's reported in the `SQS_CONTINUE_URL` environment variable the following action must be allowed on the S3 buckets' config file object key:
  * `s3:GetObject`


* For every S3 bucket resource that SQS queues are receiving notification from used by triggers of the Lambda the following action must be allowed on the S3 buckets' keys:
  * `s3:GetObject`

* For every Secret Manager secret that you want to refer in the yaml configuration file (see below) the following action must be allowed:
  * `secretsmanager:GetSecretValue`

* For SQS queue resource that you want to use as triggers of the Lambda the proper permission are arleady included by `arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole`:


### Cloudformation
* Save the following yaml content as `sar-application.yaml`
```yaml
Transform: AWS::Serverless-2016-10-31
Resources:
  SarCloudformationDeployment:
    Type: AWS::Serverless::Application
    Properties:
      Location:
        ApplicationId: 'arn:aws:serverlessrepo:%REGION%:267093732750:applications/elastic-serverless-forwarder'
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
      "PolicyName": "ElasticServerlessForwarderFunctionRolePolicySM", ## ADD FOR YOUR SM SECRET START
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


## S3_CONFIG_FILE
The Elastic Forwarder for Serverless Lambda rely on a config yaml file to be uploaded to an S3 bucket and referenced by the `S3_CONFIG_FILE` environment variable.

This is the format of the config yaml file
```yaml
inputs:
  - type: "sqs"
    id: "arn:aws:sqs:%REGION%:%ACCOUNT%:%QUEUENAME%"
    outputs:
      - type: "elasticsearch"
        args:
          # either elasticsearch_url or cloud_id, elasticsearch_url takes precedence
          elasticsearch_url: "http(s)://domain.tld:port"
          cloud_id: "cloud_id:bG9jYWxob3N0OjkyMDAkMA=="
          # either api_key or username/password, apy_key takes precedence
          api_key: "YXBpX2tleV9pZDphcGlfa2V5X3NlY3JldAo="
          username: "username"
          password: "password"
          dataset: "generic"
          namespace: "default"
```

#### Fields
`inputs.[]`:
A list of inputs (ie: triggers) for the Elastic Forwarder for Serverless Lambda

`inputs.[].type`:
The type of the trigger input (currently only `sqs` supported)

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
  * `args.dataset`: Dataset for the data stream where to forward the logs to. Default value: "generic"
  * `args.namespace`: Namespace for the data stream where to forward the logs to. Default value: "default"

## Secrets Manager Support
```yaml
inputs:
  - type: "sqs"
    id: "arn:aws:secretsmanager:eu-central-1:123-456-789:secret:plain_text_secret"
    outputs:
      - type: "elasticsearch"
        args:
          elasticsearch_url: "arn:aws:secretsmanager:eu-west-1:123-456-789:secret:es_secrets:elasticsearch_url"
          username: "arn:aws:secretsmanager:eu-west-1:123-456-789:secret:es_secrets:username"
          password: "arn:aws:secretsmanager:eu-west-1:123-456-789:secret:es_secrets:password"
          dataset: "generic"
          namespace: "default"
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

## S3 event notification to SQS
In order to set up an S3 event notification to SQS please look at the official documentation: https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html

The event type to set up in the notification should be `s3:ObjectCreated:*`

The Elastic Forwarder for Serverless Lambda doesn't need to be provided extra IAM policies in order to access S3 and SQS resources in your account: the policies to grant only the minimum required permissions for the Lambda to run are already defined in the SAM template when creating the Lamda from the Serverless Application Repository.

