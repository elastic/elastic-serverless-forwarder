### How to deploy Elastic Forwarder for Serverless Lambda application from the AWS Serverless Application Repository.

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


### S3_CONFIG_FILE
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
          dataset: "aws.vpcflow"
          namespace: "default"
```

#### Fields
`inputs.[]`:
A list of inputs (ie: triggers) for the Elastic Forwarder for Serverless Lambda

`inputs.[].type`:
The type of the trigger input (currently only `sqs` supported)

`inputs.[].id`:
The arn of the trigger input according to the type. Multiple unique id per type can be defined

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
  * `args.api_key`:  Api key of elasticsearch endpoint in the format username(api_key_id:api_key_secret). Mandatory in case `args.cloud_id` is not provided. Will take precedence over `args.username` if both defined.
  * `args.dataset`: Dataset for the data stream where to forward the logs to. Default value: "generic"
  * `args.namespace`: Namespace for the data stream where to forward the logs to. Default value: "default"


### S3 event notification to SQS
In order to setup an S3 event notification to SQS please look at the official documentation: https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html

The event type to setup in the notification should be `s3:ObjectCreated:*`

The Elastic Forwarder for Serverless Lambda doesn't need to be provided extra IAM policies in order to access S3 and SQS resources in your account: the policies to grant only the minimum required permission for the Lambda to run are already defined in the SAM template when creating the Lamda from the Serverless Application Repository.
