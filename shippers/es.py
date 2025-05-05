# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
import http
import uuid
from typing import Any, Dict, Optional, Union

import elasticapm  # noqa: F401
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import SerializationError
from elasticsearch.helpers import bulk as es_bulk
from elasticsearch.serializer import Serializer

import share.utils
from share import json_dumper, json_parser, normalise_event, shared_logger
from share.environment import get_environment
from share.version import version

from .shipper import EventIdGeneratorCallable, ReplayHandlerCallable

_EVENT_BUFFERED = "_EVENT_BUFFERED"
_EVENT_SENT = "_EVENT_SENT"
# List of HTTP status codes that are considered retryable
_retryable_http_status_codes = [
    http.HTTPStatus.TOO_MANY_REQUESTS,
]


class JSONSerializer(Serializer):
    mimetype = "application/json"

    def loads(self, s: str) -> Any:
        try:
            return json_parser(s)
        except Exception as e:
            raise SerializationError(s, e)

    def dumps(self, data: Any) -> str:
        if isinstance(data, str):
            return data

        if isinstance(data, bytes):
            return data.decode("utf-8")

        try:
            return json_dumper(data)
        except Exception as e:
            raise SerializationError(data, e)


class ElasticsearchShipper:
    """
    Elasticsearch Shipper.
    This class implements concrete Elasticsearch Shipper
    """

    def __init__(
        self,
        elasticsearch_url: str = "",
        username: str = "",
        password: str = "",
        cloud_id: str = "",
        api_key: str = "",
        es_datastream_name: str = "",
        es_dead_letter_index: str = "",
        tags: list[str] = [],
        batch_max_actions: int = 500,
        batch_max_bytes: int = 10 * 1024 * 1024,
        ssl_assert_fingerprint: str = "",
    ):
        self._bulk_actions: list[dict[str, Any]] = []

        self._bulk_batch_size = batch_max_actions

        self._bulk_kwargs: dict[str, Any] = {
            "max_retries": 4,
            "stats_only": False,
            "raise_on_error": False,
            "raise_on_exception": False,
            "max_chunk_bytes": batch_max_bytes,
        }

        if batch_max_actions > 0:
            self._bulk_kwargs["chunk_size"] = batch_max_actions

        es_client_kwargs: dict[str, Any] = {}
        if elasticsearch_url:
            es_client_kwargs["hosts"] = [elasticsearch_url]
            self._output_destination = elasticsearch_url
        elif cloud_id:
            es_client_kwargs["cloud_id"] = cloud_id
            self._output_destination = cloud_id
        else:
            raise ValueError("You must provide one between elasticsearch_url or cloud_id")

        if username:
            es_client_kwargs["http_auth"] = (username, password)
        elif api_key:
            es_client_kwargs["api_key"] = api_key
        else:
            raise ValueError("You must provide one between username and password or api_key")

        if ssl_assert_fingerprint:
            es_client_kwargs["verify_certs"] = False
            es_client_kwargs["ssl_assert_fingerprint"] = ssl_assert_fingerprint

        es_client_kwargs["serializer"] = JSONSerializer()

        self._replay_args: dict[str, Any] = {}

        self._es_client = self._elasticsearch_client(**es_client_kwargs)
        self._replay_handler: Optional[ReplayHandlerCallable] = None
        self._event_id_generator: Optional[EventIdGeneratorCallable] = None

        self._es_datastream_name = es_datastream_name
        self._es_dead_letter_index = es_dead_letter_index
        self._tags = tags

        self._es_index = ""
        self._dataset = ""
        self._namespace = ""

    @staticmethod
    def _elasticsearch_client(**es_client_kwargs: Any) -> Elasticsearch:
        """
        Getter for elasticsearch client
        Extracted for mocking
        """

        es_client_kwargs["timeout"] = 30
        es_client_kwargs["max_retries"] = 4
        es_client_kwargs["http_compress"] = True
        es_client_kwargs["retry_on_timeout"] = True
        es_client_kwargs["headers"] = {
            "User-Agent": share.utils.create_user_agent(esf_version=version, environment=get_environment())
        }

        return Elasticsearch(**es_client_kwargs)

    def _enrich_event(self, event_payload: dict[str, Any]) -> None:
        """
        This method enrich with default metadata the ES event payload.
        Currently, hardcoded for logs type
        """
        if "fields" not in event_payload:
            return

        event_payload["tags"] = ["forwarded"]

        if self._dataset != "":
            event_payload["data_stream"] = {
                "type": "logs",
                "dataset": self._dataset,
                "namespace": self._namespace,
            }

            event_payload["event"] = {"dataset": self._dataset}
            event_payload["tags"] += [self._dataset.replace(".", "-")]

        event_payload["tags"] += self._tags

    def _handle_outcome(self, actions: list[dict[str, Any]], errors: tuple[int, Union[int, list[Any]]]) -> list[Any]:
        assert isinstance(errors[1], list)

        success = errors[0]
        failed: list[Any] = []
        for error in errors[1]:
            action_failed = [action for action in actions if action["_id"] == error["create"]["_id"]]
            # an ingestion pipeline might override the _id, we can only skip in this case
            if len(action_failed) != 1:
                continue

            shared_logger.warning(
                "elasticsearch shipper", extra={"error": error["create"]["error"], "_id": error["create"]["_id"]}
            )

            if "status" in error["create"] and error["create"]["status"] == http.HTTPStatus.CONFLICT:
                # Skip duplicate events on dead letter index and replay queue
                continue

            failed_error = {"action": action_failed[0]} | self._parse_error(error["create"])

            failed.append(failed_error)

        if len(failed) > 0:
            shared_logger.warning("elasticsearch shipper", extra={"success": success, "failed": len(failed)})
        else:
            shared_logger.info("elasticsearch shipper", extra={"success": success, "failed": len(failed)})

        return failed

    def _parse_error(self, error: dict[str, Any]) -> dict[str, Any]:
        """
        Parses the error response from Elasticsearch and returns a
        standardised error field.

        The error field is a dictionary with the following keys:

        - `message`: The error message
        - `type`: The error type

        If the error is not recognised, the `message` key is set
        to "Unknown error".

        It also sets the status code in the http field if it is present
        as a number in the response.
        """
        field: dict[str, Any] = {"error": {"message": "Unknown error", "type": "unknown"}}

        if "status" in error and isinstance(error["status"], int):
            # Collecting the HTTP response status code in the
            # error field, if present, and the type is an integer.
            #
            # Sometimes the status code is a string, for example,
            # when the connection to the server fails.
            field["http"] = {"response": {"status_code": error["status"]}}

        if "error" not in error:
            return field

        if isinstance(error["error"], str):
            # Can happen with connection errors.
            field["error"]["message"] = error["error"]
            if "exception" in error:
                # The exception field is usually an Exception object,
                # so we convert it to a string.
                field["error"]["type"] = str(type(error["exception"]))
        elif isinstance(error["error"], dict):
            # Can happen with status 5xx errors.
            # In this case, we look for the "reason" and "type" fields.
            if "reason" in error["error"]:
                field["error"]["message"] = error["error"]["reason"]
            if "type" in error["error"]:
                field["error"]["type"] = error["error"]["type"]

        return field

    def set_event_id_generator(self, event_id_generator: EventIdGeneratorCallable) -> None:
        self._event_id_generator = event_id_generator

    def set_replay_handler(self, replay_handler: ReplayHandlerCallable) -> None:
        self._replay_handler = replay_handler

    def send(self, event: dict[str, Any]) -> str:
        self._replay_args["es_datastream_name"] = self._es_datastream_name

        if not hasattr(self, "_es_index") or self._es_index == "":
            self._discover_dataset(event_payload=event)

        self._enrich_event(event_payload=event)

        event["_op_type"] = "create"

        if "_index" not in event:
            event["_index"] = self._es_index

        if "_id" not in event and self._event_id_generator is not None:
            event["_id"] = self._event_id_generator(event)

        event = normalise_event(event_payload=event)

        self._bulk_actions.append(event)

        if len(self._bulk_actions) < self._bulk_batch_size:
            return _EVENT_BUFFERED

        self.flush()

        return _EVENT_SENT

    def flush(self) -> None:
        if len(self._bulk_actions) == 0:
            return

        errors = es_bulk(self._es_client, self._bulk_actions, **self._bulk_kwargs)
        failed = self._handle_outcome(actions=self._bulk_actions, errors=errors)

        # Send failed requests to dead letter index, if enabled
        if len(failed) > 0 and self._es_dead_letter_index:
            failed = self._send_dead_letter_index(failed)

        # Send remaining failed requests to replay queue, if enabled
        if isinstance(failed, list) and len(failed) > 0 and self._replay_handler is not None:
            for outcome in failed:
                if "action" not in outcome:
                    shared_logger.error("action could not be extracted to be replayed", extra={"outcome": outcome})
                    continue

                self._replay_handler(self._output_destination, self._replay_args, outcome["action"])

        self._bulk_actions = []

        return

    def _send_dead_letter_index(self, actions: list[Any]) -> list[Any]:
        """
        Index the failed actions in the dead letter index (DLI).

        This function attempts to index failed actions to the DLI, but may not do so
        for one of the following reasons:

        1. The failed action could not be encoded for indexing in the DLI.
        2. ES returned an error on the attempt to index the failed action in the DLI.
        3. The failed action error is retryable (connection error or status code 429).

        Retryable errors are not indexed in the DLI, as they are expected to be
        sent again to the data stream at `es_datastream_name` by the replay handler.

        Args:
            actions (list[Any]): A list of actions to index in the DLI.

        Returns:
            list[Any]: A list of actions that were not indexed in the DLI due to one of
            the reasons mentioned above.
        """
        non_indexed_actions: list[Any] = []
        encoded_actions = []

        for action in actions:
            if (
                "http" not in action  # no http status: connection error
                or action["http"]["response"]["status_code"] in _retryable_http_status_codes
            ):
                # We don't want to forward this action to
                # the dead letter index.
                #
                # Add the action to the list of non-indexed
                # actions and continue with the next one.
                non_indexed_actions.append(action)
                continue

            # Reshape event to dead letter index
            encoded = self._encode_dead_letter(action)
            if not encoded:
                shared_logger.error("cannot encode dead letter index event from payload", extra={"action": action})
                non_indexed_actions.append(action)

            encoded_actions.append(encoded)

        # If no action can be encoded, return original action list as failed
        if len(encoded_actions) == 0:
            return non_indexed_actions

        errors = es_bulk(self._es_client, encoded_actions, **self._bulk_kwargs)
        failed = self._handle_outcome(actions=encoded_actions, errors=errors)

        if not isinstance(failed, list) or len(failed) == 0:
            return non_indexed_actions

        for action in failed:
            event_payload = self._decode_dead_letter(action)

            if not event_payload:
                shared_logger.error("cannot decode dead letter index event from payload", extra={"action": action})
                continue

            non_indexed_actions.append(event_payload)

        return non_indexed_actions

    def _encode_dead_letter(self, outcome: dict[str, Any]) -> dict[str, Any]:
        if "action" not in outcome or "error" not in outcome:
            return {}

        # Assign random id in case bulk() results in error, it can be matched to the original
        # action
        encoded = {
            "@timestamp": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "_id": str(uuid.uuid4()),
            "_index": self._es_dead_letter_index,
            "_op_type": "create",
            "message": json_dumper(outcome["action"]),
            "error": outcome["error"],
        }

        if "http" in outcome:
            # the `http.response.status_code` is not
            # always present in the error field.
            encoded["http"] = outcome["http"]

        return encoded

    def _decode_dead_letter(self, dead_letter_outcome: dict[str, Any]) -> dict[str, Any]:
        if "action" not in dead_letter_outcome or "message" not in dead_letter_outcome["action"]:
            return {}

        return {"action": json_parser(dead_letter_outcome["action"]["message"])}

    def _discover_dataset(self, event_payload: Dict[str, Any]) -> None:
        if self._es_datastream_name != "":
            if self._es_datastream_name.startswith("logs-"):
                datastream_components = self._es_datastream_name.split("-")
                if len(datastream_components) == 3:
                    self._dataset = datastream_components[1]
                    self._namespace = datastream_components[2]
                else:
                    shared_logger.debug(
                        "es_datastream_name not matching logs datastream pattern, no dataset and namespace set"
                    )
            else:
                shared_logger.debug(
                    "es_datastream_name not matching logs datastream pattern, no dataset and namespace set"
                )

            self._es_index = self._es_datastream_name
            return
        else:
            self._namespace = "default"
            if "meta" not in event_payload or "integration_scope" not in event_payload["meta"]:
                self._dataset = "generic"
            else:
                self._dataset = event_payload["meta"]["integration_scope"]
                if self._dataset == "aws.cloudtrail-digest":
                    self._dataset = "aws.cloudtrail"

        if self._dataset == "generic":
            shared_logger.debug("dataset set to generic")

        shared_logger.debug("dataset", extra={"dataset": self._dataset})

        self._es_index = f"logs-{self._dataset}-{self._namespace}"
        self._es_datastream_name = self._es_index
