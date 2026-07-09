This directory is meant to include resources to help the maintenance of ESF. This file will have notes that are considered important for understanding the code. If you want a way to test your changes locally, you should redirect to [this file](./how-to-test-locally/README.md) and follow the steps there on how to do that.


Contents of this file:

- [Errors on replay queue](#errors-on-replay-queue)
- [Understanding triggers](#understanding-triggers)
  * [Replay queue triggers](#replay-queue-triggers)
    + [Example 1: Ingestion error](#example-1--ingestion-error)
      - [Patch 1: replay queue is the only input](#patch-1--replay-queue-is-the-only-input)
      - [Patch 2: replay queue and cloudwatch logs (wrong password) are inputs](#patch-2--replay-queue-and-cloudwatch-logs--wrong-password--are-inputs)
      - [Patch 3: replay queue and cloudwatch logs (correct password) are inputs](#patch-3--replay-queue-and-cloudwatch-logs--correct-password--are-inputs)
    + [Example 2: Configuration error](#example-2--configuration-error)
      - [Patch 1: replay queue is the only input](#patch-1--replay-queue-is-the-only-input-1)
      - [Patch 2: replay queue and cloudwatch logs are inputs](#patch-2--replay-queue-and-cloudwatch-logs-are-inputs)
    + [Conclusion on errors and replay queue](#conclusion-on-errors-and-replay-queue)




# Errors on replay queue

When ESF cannot finish processing an event, it causes an error. Currently, we have two types of errors. They can be categorized in:

- Ingestion errors: In case the output is unavailable, or the API key was set wrongly, among others.
- Configuration errors: In case the event source responsible for triggering ESF does not have a mapping input `id` in the `config.yaml` file. You can refer to the [PR](https://github.com/elastic/elastic-serverless-forwarder/pull/711) that introduced this change for more details.

The way these errors are sent to the replay queue is different. For:

- Ingestion errors: the data is transformed, and the `body` of the message is updated. For clearance, ingestion errors are taken care of by the shippers. Elasticsearch shipper [enriches events](https://github.com/elastic/elastic-serverless-forwarder/blob/acbe70242afad1d5061d64fd4d12b7e647de3768/shippers/es.py#L141-L146). This transformation is important to understand what kind of triggers ESF attributes to each event.
- Configuration errors: these errors never reach a shipper. A shipper can only be reached if we have knowledge about an input's outputs. In this error, we cannot get that information from our `config.yaml` file. This way, our data here is not transformed nor enriched by shippers.


# Understanding triggers

We have triggers that can be set by the user and will not cause an error if written in the `config.yaml` file. At the moment of writing this document, the triggers are ["cloudwatch-logs", "s3-sqs", "sqs" and "kinesis-data-stream"](https://github.com/elastic/elastic-serverless-forwarder/blob/acbe70242afad1d5061d64fd4d12b7e647de3768/share/config.py#L14). However, when parsing the configuration, ESF can determine a new type of trigger. This trigger comes from the code and cannot be set by the user. Currently, the only trigger available of this kind has the value `replay-sqs`.

## Replay queue triggers

There are two triggers associated with the replay queue: `replay-sqs` and `sqs`. The trigger depends on what the `body` field of the message looks like.

The trigger `replay-sqs` is only returned if the event that ESF lambda is processing matches a certain condition. Currently, we can find that condition in [this part of the code](https://github.com/elastic/elastic-serverless-forwarder/blob/acbe70242afad1d5061d64fd4d12b7e647de3768/handlers/aws/utils.py#L301-L306), which analyzes the `body` of a message. Like explained in the previous section, ingestion errors transform the `body` of a message before placing it in the replay queue. This transformation will match the condition, and the trigger returned will be `replay-sqs`. Otherwise, the trigger will be `sqs` (this should be set by the user in the `config.yaml` file).

Below you can find an example for each type of error, and how ESF processes it. If you prefer to skip the examples, you can move to [Conclusion on errors and replay queue](#conclusion-on-errors-and-replay-queue) for the most important points of this process.

### Example 1: Ingestion error

In this example:
- We will force a message to be sent by the replay queue caused by an ingestion error.
- ESF has access to the outputs meant for this input, so the `body` field transformation by the shipper will happen, and the `replay-sqs` trigger will be used.

How it unfolds:
1. My `config.yaml` looks like this:
```yaml
"inputs":
- "id": "arn:aws:logs:eu-west-2:627286350134:log-group:constanca-test-local-esf:*"
  "outputs":
  - "args":
      "api_key": "<Wrong API Key>"
      "elasticsearch_url": "<URL>"
      "es_datastream_name": "logs-esf.cloudwatch-default"
    "type": "elasticsearch"
  "type": "cloudwatch-logs"
```
The API key is set incorrectly, so we can have an ingestion error.

2. I send a log event in the cloudwatch logs group configured above. This will trigger ESF. Since it causes an error, the message will go to the replay queue.

During this event processing, ESF will:
- Be triggered by an event. The type of this event is `cloudwatch-logs`. We then see an exception from Elasticsearch shipper (our ingestion error), and the message is placed in the replay queue.
- Be triggered by another event. The type of this event is `sqs`. This comes from the continuing queue. Since there is nothing else to read, we do not see any more events, and the processing ends here.

3. My replay queue now has one message, in which the `body` is:
```json
{"output_type":"elasticsearch","output_args":{"es_datastream_name":"logs-esf.cloudwatch-default"},"event_payload":{"@timestamp":"2024-05-28T09:36:00.399548Z","tags":["forwarded","esf-cloudwatch"],"data_stream":{"type":"logs","dataset":"esf.cloudwatch","namespace":"default"},"event":{"dataset":"esf.cloudwatch"},"_op_type":"create","_index":"logs-esf.cloudwatch-default","_id":"1716888957320-771ee3f1b39c0fee772d9663dfde1789ca7f439aef0fde6ff427b65f71feb3472848da359e3732f0d6efa8b8b579d6f5-000000000000","message":"new log event","log":{"offset":0,"file":{"path":"constanca-test-local-esf/log-stream-test"}},"aws":{"cloudwatch":{"log_group":"constanca-test-local-esf","log_stream":"log-stream-test","event_id":"38287903171364237963636713518763811415048325874206244864"}},"cloud":{"provider":"aws","region":"eu-west-2","account":{"id":"627286350134"}}},"event_input_id":"arn:aws:logs:eu-west-2:627286350134:log-group:constanca-test-local-esf:*"}
```

Notice that this `body` was transformed by the shipper, and enriched with new fields. This is important, as this is what causes ESF to associate this message with the trigger `replay-sqs`.

Now from this step, I will study some patches:


#### Patch 1: replay queue is the only input

I update my `config.yaml` to get the events from the replay queue:
```yaml
inputs:
  - id: "arn:aws:sqs:eu-west-2:627286350134:constanca-test-esf-replay-queue"
    outputs:
      - args:
          api_key: "<Correct API key>"
          elasticsearch_url: "<URL>"
          es_datastream_name: "logs-esf.replay_sqs-default"
        type: "elasticsearch"
    type: "sqs"
```


Once ESF starts, the messages inside the replay queue will be pulled.

<details>

<summary>This is an example of the events ESF will receive. </summary>

```json
{
   "Records":[
      {
         "messageId":"0d930851-037c-46ca-b311-aec3a0d14916",
         "receiptHandle":"AQEB6ahP+ZGyQrAMfWBdCDew/IJEXH0vlY9Z/pm2ISu3/fXp2o9TLMoy9XNtw+q0SFjxlaJuYPIgCRfJDIuN2j+WGfwe9QOnJOqS4B7BuJXc+MZMpfvc1XdpFQeNb4WpXgeg+OP/GvSMJnTzyMnrwsDHxK1eBbo0WuG/kctBwXtfjpanxUaF758FX7nJODILDIt90rdlDqkkGEn9b6SMGTzfU3AlqCRf/4Tkn2p8LeFqoRjvyJrRAQyk54atyqZfefHA/5BnfJHRD7hVNN2LDquv9lsWcU2hl54Uzd+XprPIwiLxab+GneKRYXFjfu+deg9Y3fNLPHxmO3G7U47nx0aHOAX9Bthm2/bEj4cctTLxnYHh8xmawlvm0uZDhiEXL4IphzmXtpPpY1dspmIjEJy/MaFAcPQa2000kKKqd51zkAY=",
         "body":{
           "output_type":"elasticsearch",
           "output_args":{
              "es_datastream_name":"logs-esf.cloudwatch-default"
           },
           "event_payload":{
              "@timestamp":"2024-05-28T12:26:33.477616Z",
              "tags":[
                 "forwarded",
                 "esf-cloudwatch"
              ],
              "data_stream":{
                 "type":"logs",
                 "dataset":"esf.cloudwatch",
                 "namespace":"default"
              },
              "event":{
                 "dataset":"esf.cloudwatch"
              },
              "_op_type":"create",
              "_index":"logs-esf.cloudwatch-default",
              "_id":"1716899125321-df6ff5c04cff293e2019db9ff9785eccc6d57cc1fd0e4370f9c12e28501bbde2e2db76c5ea42b42edb14e323afdf8d01-000000000000",
              "message":"b",
              "log":{
                 "offset":0,
                 "file":{
                    "path":"constanca-test-local-esf/log-stream-test"
                 }
              },
              "aws":{
                 "cloudwatch":{
                    "log_group":"constanca-test-local-esf",
                    "log_stream":"log-stream-test",
                    "event_id":"38288129925363717368211359570016329495993881213158293504"
                 }
              },
              "cloud":{
                 "provider":"aws",
                 "region":"eu-west-2",
                 "account":{
                    "id":"627286350134"
                 }
              }
           },
           "event_input_id":"arn:aws:logs:eu-west-2:627286350134:log-group:constanca-test-local-esf:*"
        },
         "attributes":{
            "ApproximateReceiveCount":"1",
            "AWSTraceHeader":"Root=1-6655cd35-0ee65d947b3018a667ab7dbb;Parent=1bc3afb43cb7d8d4;Sampled=0;Lineage=6337b594:0",
            "SentTimestamp":"1716899193965",
            "SenderId":"AROAZEDJODE3BNRA4F4I6:constanca-test-esf",
            "ApproximateFirstReceiveTimestamp":"1716899280688"
         },
         "messageAttributes":{

         },
         "md5OfBody":"8271c018a1f584474fbe6eceb302e5df",
         "eventSource":"aws:sqs",
         "eventSourceARN":"arn:aws:sqs:eu-west-2:627286350134:constanca-test-esf-replay-queue",
         "awsRegion":"eu-west-2"
      }
   ]
}
```

</details>


Since the `body` of the message inside contains the necessary fields for `replay-sqs` trigger, then this will be the trigger used for ESF.

Since the new `config.yaml` file does not have an input for the cloudwatch logs group, this message will fail to be processed once again with `InputConfigException`. The message will be placed in the replay queue. After the maximum amount of tries, the message will be moved to the DLQ.


#### Patch 2: replay queue and cloudwatch logs (wrong password) are inputs

My `config.yaml` looks like this:

```yaml
"inputs":
- "id": "arn:aws:sqs:eu-west-2:627286350134:constanca-test-esf-replay-queue"
  "outputs":
  - "args":
      "api_key": "<Correct API Key>"
      "elasticsearch_url": "<URL>"
      "es_datastream_name": "logs-esf.replay_sqs-default"
    "type": "elasticsearch"
  "type": "sqs"
- "id": "arn:aws:logs:eu-west-2:627286350134:log-group:constanca-test-local-esf:*"
  "outputs":
  - "args":
      "api_key": "<Wrong API key>"
      "elasticsearch_url": "<URL>"
      "es_datastream_name": "logs-esf.cloudwatch-default"
    "type": "elasticsearch"
  "type": "cloudwatch-logs"
```

This `config.yaml` will fail with yet another ingestion error, this time `ReplayHandlerException`. This happens because ESF will use the outputs of the input `arn:aws:logs:eu-west-2:627286350134:log-group:constanca-test-local-esf:*` instead of the outputs of `arn:aws:sqs:eu-west-2:627286350134:constanca-test-esf-replay-queue`. The message is put in the replay queue again. It will be processed again, until the maximum amount of tries, and then placed in the DLQ.

This means that when ESF uses `replay-sqs` trigger, nothing inside the replay input matters.

#### Patch 3: replay queue and cloudwatch logs (correct password) are inputs

My `config.yaml` looks like this:

```yaml
"inputs":
- "id": "arn:aws:sqs:eu-west-2:627286350134:constanca-test-esf-replay-queue"
  "outputs":
  - "args":
      "api_key": "..."
      "elasticsearch_url": "..."
      "es_datastream_name": "logs-esf.replay_sqs-default"
    "type": "elasticsearch"
  "type": "sqs"
- "id": "arn:aws:logs:eu-west-2:627286350134:log-group:constanca-test-local-esf:*"
  "outputs":
  - "args":
      "api_key": "<Correct API key>"
      "elasticsearch_url": "<URL>"
      "es_datastream_name": "logs-esf.cloudwatch-default"
    "type": "elasticsearch"
  "type": "cloudwatch-logs"
```

This case succeeds.


### Example 2: Configuration error

In this example:
- We will force a message to be sent by the replay queue caused by a configuration error.
- ESF does not have access to the outputs meant for this input since it is not part of the `config.yaml`. The `body` field will not be transformed by any shipper.

How it unfolds:
1. My `config.yaml` looks like this:
```yaml
"inputs":
# Notice that this ARN is misconfigured, and does not correspond to the event source
- "id": "arn:aws:logs:eu-west-2:627286350134:log-group:WRONG-ARN:*"
  "outputs":
  - "args":
      "api_key": "<API Key>"
      "elasticsearch_url": "<URL>"
      "es_datastream_name": "logs-esf.cloudwatch-default"
    "type": "elasticsearch"
  "type": "cloudwatch-logs"
```

And I have a cloudwatch logs group that is set as an event source for ESF.

2. I send a log event. This log event will correctly trigger ESF. Since it causes an error, the message will go to the replay queue.

During this event processing, ESF will:
- Identify the trigger `cloudwatch-logs` for the event received. We then see an error for `No input defined` and the message is placed in the replay queue.
- Lambda shuts down.

3. My replay queue now has one message, in which the `body` is:
```
misconfigured id
```

This `body` is the exact message that my log stream in the cloudwatch logs group sent. Since the message did not go through any shippers, there was no transformation.


Now from this step, I will study some patches:

#### Patch 1: replay queue is the only input

My `config.yaml` looks like this:

```yaml
inputs:
  - id: "arn:aws:sqs:eu-west-2:627286350134:constanca-test-esf-replay-queue"
    outputs:
      - args:
          api_key: "<API key>"
          elasticsearch_url: "<URL>"
          es_datastream_name: "logs-esf.replay_sqs-default"
        type: "elasticsearch"
    type: "sqs"
```

Since the `body` was not transformed, this trigger will be the exact same one that the user defined. That is, `sqs`.

The messages from the replay queue will be processed, and they will still fail since the input for the cloudwatch logs group is not defined. After the maximum amount of attempts, the message will be placed in the DLQ.


#### Patch 2: replay queue and cloudwatch logs are inputs

My `config.yaml` looks like this:

```yaml
"inputs":
- "id": "arn:aws:sqs:eu-west-2:627286350134:constanca-test-esf-replay-queue"
  "outputs":
  - "args":
      "api_key": "<Correct API Key>"
      "elasticsearch_url": "<URL>"
      "es_datastream_name": "logs-esf.replay_sqs-default"
    "type": "elasticsearch"
  "type": "sqs"
- "id": "arn:aws:logs:eu-west-2:627286350134:log-group:constanca-test-local-esf:*"
  "outputs":
  - "args":
      "api_key": "<Correct API key>"
      "elasticsearch_url": "<URL>"
      "es_datastream_name": "logs-esf.cloudwatch-default"
    "type": "elasticsearch"
  "type": "cloudwatch-logs"
```

This will fail. ESF will get the `config.yaml` file that is part of the message attributes field in the message placed in the replay queue. This is the `config.yaml` file that has the misconfigured id.


### Conclusion on errors and replay queue

- Both ingestion and configuration errors are put on the replay queue
- Ingestion errors are transformed by the shippers, while configuration errors are not
- Ingestion errors cause the trigger `replay-sqs` to be activated
- Configuration errors use the trigger `sqs`, set by the user
- The `config.yaml` to consume messages affected by ingestion errors needs to have inputs for the replay queue and for the resource that produced those messages
- The input for the replay queue to consume ingestion errors is completely discarded. The output used will not be the one from the replay queue, but the one from the input that produced the message
- There is no way to consume messages in the replay queue put there for configuration errors
