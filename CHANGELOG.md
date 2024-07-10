### v1.17.0 - 2024/07/10
##### Features
* Add dead letter index for ES outputs [733](https://github.com/elastic/elastic-serverless-forwarder/pull/733).

### v1.16.0 - 2024/07/09
##### Features
* Prevent duplicate _id events from reaching the replay queue [729](https://github.com/elastic/elastic-serverless-forwarder/pull/729).

### v1.15.0 - 2024/05/29
##### Features
* Enable multiple outputs for each input [725](https://github.com/elastic/elastic-serverless-forwarder/pull/725).

### v1.14.0 - 2024/05/07
##### Bug fixes
* Report misconfigured input ids as an error instead of warning, and place those messages in the replaying queue [#711](https://github.com/elastic/elastic-serverless-forwarder/pull/711).

### v1.13.1 - 2024/03/07
##### Features
* Add documentation and optimise performance for `root_fields_to_add_to_expanded_event` [#642](https://github.com/elastic/elastic-serverless-forwarder/pull/642)

### v1.13.0 - 2024/02/23
##### Features
* Go beyond 4096b limit on CF Parameter for event triggers on SAR deployment [#627](https://github.com/elastic/elastic-serverless-forwarder/pull/627)

### v1.12.0 - 2024/02/13
##### Features
* Add outputs for Lambda function ARN and IAM Role ARN [#552](https://github.com/elastic/elastic-serverless-forwarder/pull/552)

### v1.11.0 - 2023/12/08
##### Features
* Add user agent with information about ESF version and host environment: [#537](https://github.com/elastic/elastic-serverless-forwarder/pull/537)
* Remove calls to `sqs.DeleteMessage` and refactor storage decorators: [#544](https://github.com/elastic/elastic-serverless-forwarder/pull/544)
##### Bug fixes
* Fix regression when both `json_content_type: single` and `expand_event_list_from_field` are set: [#553](https://github.com/elastic/elastic-serverless-forwarder/pull/553)

### v1.10.0 - 2023/10/27
##### Features
* Move `_id` field to `@metadata._id` in logstash output: [#507](https://github.com/elastic/elastic-serverless-forwarder/pull/507)

### v1.9.0 - 2023/08/24
##### Features
* Allow the possibility to set a prefix for role and policy when deploying with the `publish_lambda.sh` script: [#399](https://github.com/elastic/elastic-serverless-forwarder/pull/399)

### v1.8.1 - 2023/05/04
##### Bug fixes
* Explicitly set `SqsManagedSseEnabled` in CF template for replay and continuing queues for stack created before September/October 2022: [#353](https://github.com/elastic/elastic-serverless-forwarder/pull/353)

### v1.8.0 - 2023/03/21
##### Features
* Add `root_fields_to_add_to_expanded_event` input setting to merge fields at root level when expanding a list of events from field: [#290](https://github.com/elastic/elastic-serverless-forwarder/pull/290)

##### Bug fixes
* Reduced logging verbosity: [#287](https://github.com/elastic/elastic-serverless-forwarder/pull/287)
* Fix potential `PolicyLengthExceededException` when using `cloudwatch-logs` input: [#283](https://github.com/elastic/elastic-serverless-forwarder/pull/283)

### v1.7.2 - 2023/03/08
##### Bug fixes
* Fix events mutation across different outputs of the same input, proper handling of multiple outputs in the replay queue handler, proper handling of non json content where no json object start was ever met in the storage json collector: [#260](https://github.com/elastic/elastic-serverless-forwarder/pull/260)
* Fix throttling exception in `cloudwatch-logs` input related to DescribeLogStreams: [#276](https://github.com/elastic/elastic-serverless-forwarder/pull/276)

### v1.7.1 - 2023/02/16
##### Bug fixes
* Fix wrong resolved `expand_event_list_from_field` with AWS CloudTrail due to race condition: [#244](https://github.com/elastic/elastic-serverless-forwarder/pull/244)
* Fix shipper cache to include output type in the key: [#245](https://github.com/elastic/elastic-serverless-forwarder/pull/245)

### v1.7.0 - 2023/02/01
##### Features
* Added support for Logstash as output (Technical Preview): [#210](https://github.com/elastic/elastic-serverless-forwarder/pull/210)

### v1.6.1 - 2023/02/03
##### Bug fixes
* Changed event ID format to use a SHA3 384bit hash of AWS-provided ids: [#227](https://github.com/elastic/elastic-serverless-forwarder/pull/227)
* Fix `kinesis-data-stream` data payload type decoding and empty fields in message attributes in `sqs` continuation: [#228](https://github.com/elastic/elastic-serverless-forwarder/pull/228)
* Handle missing matching `_id` from failed actions in elasticsearch output: [#230](https://github.com/elastic/elastic-serverless-forwarder/pull/230)

### v1.6.0 - 2023/01/26
##### Features
* Allow for extra customisation on event triggers and vpc at deployment: [#201](https://github.com/elastic/elastic-serverless-forwarder/pull/201)
* Improve instrumented performance bumping elastic-apm to 6.14.0: [#220](https://github.com/elastic/elastic-serverless-forwarder/pull/220)
##### Bug fixes
* Changed ID generation logic to use AWS-provided ids: [#214](https://github.com/elastic/elastic-serverless-forwarder/pull/214)

### v1.5.0 - 2022/10/20
##### Features
* Allow to connect to ES with self-signed certificate through certificate fingerprint assertion: [#173](https://github.com/elastic/elastic-serverless-forwarder/pull/173)

### v1.4.0 - 2022/10/04
##### Features
* Allow to use a CloudWatch Logs Log Stream ARN as ID for `cloudwatch-logs` input type: [#160](https://github.com/elastic/elastic-serverless-forwarder/pull/160)
* Respect batch settings of original input in replay queue: [#157](https://github.com/elastic/elastic-serverless-forwarder/pull/157)

### v1.3.0 - 2022/08/11
##### Features
* Add `json_content_type` input setting as optional hint for json content auto discovery: [#145](https://github.com/elastic/elastic-serverless-forwarder/pull/145)
* Switch timeout handling for `kinesis-data-stream` input to continuing queue like any other input: [#146](https://github.com/elastic/elastic-serverless-forwarder/pull/146)
* Handle continuation from offset in the list when dealing with `expand_event_list_from_field`: [#147](https://github.com/elastic/elastic-serverless-forwarder/pull/147)
* Improve json parser and dumper performance: [#148](https://github.com/elastic/elastic-serverless-forwarder/pull/148)

### v1.2.1 - 2022/07/15
##### Bug fixes
* Fix multiline documentation to properly render: [#137](https://github.com/elastic/elastic-serverless-forwarder/pull/137)

### v1.2.0 - 2022/07/11
##### Features
* Add support for collecting multiline messages in a single event: [#135](https://github.com/elastic/elastic-serverless-forwarder/pull/135)

### v1.1.2 - 2022/06/24
##### Bug fixes
* Handle proper ARN format for CloudWatch Logs Log Group in the macro, as received from `ElasticServerlessForwarderCloudWatchLogsEvents` CloudFormation Parameter : [#130](https://github.com/elastic/elastic-serverless-forwarder/pull/130)

### v1.1.1 - 2022/06/20
##### Bug fixes
* Handle properly gzip content regardless of the content type in S3 storage: [#128](https://github.com/elastic/elastic-serverless-forwarder/pull/128)

### v1.1.0 - 2022/06/17
##### Features
* Add support for expanding an events list from a json field: [#124](https://github.com/elastic/elastic-serverless-forwarder/pull/124)

### v1.0.0 - 2022/06/17
##### Features
* Promote to GA: [#126](https://github.com/elastic/elastic-serverless-forwarder/pull/126)
##### Breaking changes
* Removed deprecated `es_index_or_datastream_name` config param: [#126](https://github.com/elastic/elastic-serverless-forwarder/pull/126)

### v0.30.0 - 2022/05/16
##### Features
* Add support for AWS IaC deployment with CloudFormation and terraform: [#115](https://github.com/elastic/elastic-serverless-forwarder/pull/115)
##### Deprecation
* Deprecate `es_index_or_datastream_name` config param in favour of `es_datastream_name` one: [#115](https://github.com/elastic/elastic-serverless-forwarder/pull/115)

### v0.29.1 - 2022/05/04
##### Bug fixes
* Handle properly `cloudwatch-logs` content payload: [#113](https://github.com/elastic/elastic-serverless-forwarder/pull/113)

### v0.29.0 - 2022/04/25
##### Features
* Add support for AWS CloudTrail logs: [#110](https://github.com/elastic/elastic-serverless-forwarder/pull/110)

### v0.28.4 - 2022/04/19
##### Bug fixes
* Handle properly flushing in `kinesis-data-stream` input type, handle properly empty messages in include exclude filters, handle properly empty lines in `JsonCollector` decorator, identify properly CloudWatch Logs payload: [#107](https://github.com/elastic/elastic-serverless-forwarder/pull/107)

### v0.28.3 - 2022/04/15
##### Features
* Make the integration scope discovery available at Input component: [#106](https://github.com/elastic/elastic-serverless-forwarder/pull/106)

### v0.28.2 - 2022/04/14
##### Bug fixes
* Handle failure of replayed messages: [#105](https://github.com/elastic/elastic-serverless-forwarder/pull/105)

### v0.28.1 - 2022/04/12
##### Bug fixes
* Handle properly messages in the continuing queue originated from the continuing queue itself: [#104](https://github.com/elastic/elastic-serverless-forwarder/pull/104)

### v0.28.0 - 2022/04/11
##### Features
* Add support for providing `S3_CONFIG_FILE` env variable as cloudformation param: [#103](https://github.com/elastic/elastic-serverless-forwarder/pull/103)

### v0.27.0 - 2022/04/04
##### Features
* Add support for collecting json content spanning multiple lines: [#99](https://github.com/elastic/elastic-serverless-forwarder/pull/99)

### v0.26.0 - 2022/03/22
##### Features
* Add support for include/exclude filter: [#97](https://github.com/elastic/elastic-serverless-forwarder/pull/97)

### v0.25.1 - 2022/03/21
##### Bug fixes
* Extract `fields` subfields at event root and make metadata for CloudWatch Logs in event in sync with Elastic Agent: [#98](https://github.com/elastic/elastic-serverless-forwarder/pull/98)

### v0.25.0 - 2022/03/15
##### Features
* Support handling of continuing queue with batch size greater than 1: [#95](https://github.com/elastic/elastic-serverless-forwarder/pull/95)

### v0.24.0 - 2022/03/15
##### Features
* Add support for CloudWatch Logs subscription filter input: [#94](https://github.com/elastic/elastic-serverless-forwarder/pull/94)

### v0.23.0 - 2022/03/09
##### Features
* Add support for direct `sqs` input: [#91](https://github.com/elastic/elastic-serverless-forwarder/pull/91)

### v0.22.0 - 2022/03/03
##### Features
* Set default `S3_CONFIG_FILE` env variable to "s3://": [#90](https://github.com/elastic/elastic-serverless-forwarder/pull/90)

### v0.21.1 - 2022/02/17
##### Bug fixes
* Remove `aws.lambda`, `aws.sns` and `aws.s3_storage_lens` metrics datasets auto-discovery [#82](https://github.com/elastic/elastic-serverless-forwarder/pull/82)


### v0.21.0 - 2022/02/15
##### Breaking changes
* Add support for sending data to an index or alias on top of datastream for the Elasticsearch output (`dataset` and `namespace` config params replaced by `es_index_or_datastream_name`): [#73](https://github.com/elastic/elastic-serverless-forwarder/pull/73)

### v0.20.1 - 2022/02/08
##### Bug fixes
* Set HTTP compression always on and max retries to not exceed 15 mins in the ES client [#69](https://github.com/elastic/elastic-serverless-forwarder/pull/69)

### v0.20.0 - 2022/02/07
##### Features
* Add support for `kinesis-data-stream` input: [#66](https://github.com/elastic/elastic-serverless-forwarder/pull/66)

### v0.19.0 - 2022/02/02
##### Features
* Expose `batch_max_actions` and `batch_max_bytes` config params for ES shipper: [#65](https://github.com/elastic/elastic-serverless-forwarder/pull/65)

### v0.18.0 - 2022/01/13
##### Features
* Handle batches of SQS records: [#63](https://github.com/elastic/elastic-serverless-forwarder/pull/63)

### v0.17.0 - 2021/12/30
##### Features
* Replay queue for ES ingestion phase failure: [#60](https://github.com/elastic/elastic-serverless-forwarder/pull/60)

### v0.16.0 - 2021/12/17
##### Features
* Routing support for AWS Services logs: [#58](https://github.com/elastic/elastic-serverless-forwarder/pull/58)

### v0.15.0 - 2021/12/24
##### Features
* Let the Lambda fail and end up in built-in retry mechanism in case of errors before ingestion phase: [#57](https://github.com/elastic/elastic-serverless-forwarder/pull/57)

### v0.14.0 - 2021/12/17
##### Features
* Support for tags: [#45](https://github.com/elastic/elastic-serverless-forwarder/pull/45)

### v0.13.0 - 2021/12/14
##### Features
* General performance refactoring after stress test outcome: [#48](https://github.com/elastic/elastic-serverless-forwarder/pull/48)

### v0.12.0 - 2021/12/06
##### Features
* Support for Secrets Manager: [#42](https://github.com/elastic/elastic-serverless-forwarder/pull/42)

### v0.11.0 - 2021/11/04
##### Bug fixes
* Integration tests for AWS Lambda handler: [#24](https://github.com/elastic/elastic-serverless-forwarder/pull/24)
* Proper handling of empty lines in `by_lines` decorator: [#26](https://github.com/elastic/elastic-serverless-forwarder/pull/26)

### v0.10.0 - 2021/11/03
##### Features
* Support for cloud_id and api_key in elasticsearch client: [#14](https://github.com/elastic/elastic-serverless-forwarder/pull/14)
##### Bug fixes
* Tests for remaining packages: [#12](https://github.com/elastic/elastic-serverless-forwarder/pull/12)

### v0.9.0 - 2021/10/20
##### Features
* Optimise storage memory usage: [#11](https://github.com/elastic/elastic-serverless-forwarder/pull/11)
##### Bug fixes
* Tests for share packages: [#10](https://github.com/elastic/elastic-serverless-forwarder/pull/10)

### v0.8.0 - 2021/10/19
##### Bug fixes
* Refactoring in offset marker: [#9](https://github.com/elastic/elastic-serverless-forwarder/pull/9)

### v0.5.4 - 2021/10/12
##### Features
* Support for type checking and related refactoring: [#8](https://github.com/elastic/elastic-serverless-forwarder/pull/8)


### v0.3.14 - 2021/10/07
##### Features
* Config support on AWS Lambda handler: [#7](https://github.com/elastic/elastic-serverless-forwarder/pull/7)

### v0.0.15 - 2021/09/10
##### Features
* First draft of the AWS Lambda handler with no config: [#2](https://github.com/elastic/elastic-serverless-forwarder/pull/2)

### v0.0.1-dev0 - 2021/09/07

#### Bootstrapping the project for Elastic Serverless Forwarder
