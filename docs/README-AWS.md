## Introduction:
The Elastic Serverless Forwarder is an AWS Lambda function that ships logs from your AWS environment to Elastic. The function can forward logs to Elastic self-managed or Elastic cloud environments. It supports the following inputs:

- SQS S3 Notifications

The config yaml file (details described below) acts as an input where the user, based on input type, configures things like SQS queue ARN, Elasticsearch connection information and  dataset/namespace. The dataset and namespace helps map logs data to specific data streams for processing and storage. Multiple input sections can be created in the configuration file pointing to different queues that match specific log types, identified in the configuration by dataset and namespace.

A continuing SQS queue is setup and permissions set up automatically by the Lambda deployment. It is used to make sure the next invocation of the function can continue from exactly where the last function left. By default a Lambda function runs for a max of 15 minutes. When processing large log files there’s a possibility that the function may be exited by AWS in the middle of processing a log file. The code handles this scenario gracefully by keeping track of the file and offset its processing.

As a first step users should install appropriate integrations in the Kibana UI. This sets up appropriate pre-built dashboards, ingest node configurations, and other assets that help you get the most out of the data you ingest.

**SQS S3 Notifications input:**
The Lambda function supports ingesting logs contained in the S3 bucket through an SQS notification and send them to Elastic. The SQS queue serves as a trigger for the Lambda function. When a new log file gets written to an S3 bucket and meets the criteria (as configured including prefix/suffix), a notification to SQS is generated that triggers the Lambda function. Users will set up separate SQS queues for each type of logs (i.e. redis.log, ngnix.log and so on). A single configuration file can have many input sections, pointing to different queues that match specific log types identified in the configuration by dataset and namespace. The dataset and namespace helps the function send the logs to the corresponding data streams for further processing and storage in the Elasticsearch cluster.

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
  * Click on "Show apps that create custom IAM roles or resource policies" checkbox
  * In the search box type "elastic-serverless-forwarder" and submit
  * Look for "elastic-serverless-forwarder" in the results and click on it
* In the page on "Application settings" section check the "I acknowledge that this app creates custom IAM roles." checkbox
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
      * The SQS queue you want to use as trigger must have a visibility timeout of 900 seconds, equal to the Elastic Forwarder for Serverless Lambda timeout.
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
        ApplicationId: 'arn:aws:serverlessrepo:%REGION%:%ELASTIC_ACCOUNT_ID%:applications/elastic-serverless-forwarder'
        SemanticVersion: %VERSION%

```

* Deploy the Lambda from SAR running the following command:
  * ```commandline
    aws cloudformation deploy --template-file sar-application.yaml --stack-name sar-cloudformation-deployment --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND
    ```

* Import json template from deployed stack running the following commands:
  * ```commandline
    PARENT_STACK_ARN=$(aws cloudformation describe-stacks --stack-name sar-cloudformation-deployment --query Stacks[0].StackId --output text)
    LAMBDA_STACK_ARN=$(aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE --query "StackSummaries[?ParentId==\`${PARENT_STACK_ARN}\`].StackId" --output text)
    aws cloudformation get-template --stack-name "${LAMBDA_STACK_ARN}" --query TemplateBody > sar-lambda.json
    ```

  * Edit sar-lambda.json to customise your deployment of Elastic Forwarder for Serverless
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
            "FunctionName": { # You muste reference to `ElasticServerlessForwarderFunction` resource
              "Ref": "ElasticServerlessForwarderFunction"
            },
            "EventSourceArn": "%SQS_ARN%"
          }
        }
        ```

* Update the stack running the following command:
  * ```commandline
    aws cloudformation update-stack --stack-name "${LAMBDA_STACK_ARN}" --template-body file://./sar-lambda.json
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
  * `args.elasticsearch_url`: Url of elasticsearch endpoint in the format "http(s)://domain.tld:port". Mandatory in case `args.cloud_id` is not provided. Will take precedence over `args.cloud_id` if both defined.
  * `args.cloud_id`: Cloud ID of elasticsearch endpoint. Mandatory in case `args.elasticsearch_url` is not provided. Will be ignored if `args.elasticsearch_url` is defined as well.
  * `args.username`: Username of the elasticsearch instance to connect to. Mandatory in case `args.api_key` is not provided. Will be ignored if `args.api_key` is defined as well.
  * `args.password` Password of the elasticsearch instance to connect to. Mandatory in case `args.api_key` is not provided. Will be ignored if `args.api_key` is defined as well.
  * `args.api_key`:  Api key of elasticsearch endpoint in the format username(api_key_id:api_key_secret). Mandatory in case `args.userame`  and `args.password ` are not provided. Will take precedence over `args.username`/`args.password` if both defined.
  * `args.dataset`: Dataset for the data stream where to forward the logs to. Default value: "generic"
  * `args.namespace`: Namespace for the data stream where to forward the logs to. Default value: "default"


## S3 event notification to SQS
In order to set up an S3 event notification to SQS please look at the official documentation: https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html

The event type to setup in the notification should be `s3:ObjectCreated:*`

The Elastic Forwarder for Serverless Lambda doesn't need to be provided extra IAM policies in order to access S3 and SQS resources in your account: the policies to grant only the minimum required permissions for the Lambda to run are already defined in the SAM template when creating the Lamda from the Serverless Application Repository.

