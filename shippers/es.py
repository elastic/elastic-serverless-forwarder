# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
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
_VERSION_CONFLICT = 409


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

            if "status" in error["create"] and error["create"]["status"] == _VERSION_CONFLICT:
                # Skip duplicate events on dead letter index and replay queue
                continue

            failed.append({"error": error["create"]["error"], "action": action_failed[0]})

        if len(failed) > 0:
            shared_logger.warning("elasticsearch shipper", extra={"success": success, "failed": len(failed)})
        else:
            shared_logger.info("elasticsearch shipper", extra={"success": success, "failed": len(failed)})

        return failed

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
        encoded_actions = []
        dead_letter_errors: list[Any] = []
        for action in actions:
            # Reshape event to dead letter index
            encoded = self._encode_dead_letter(action)
            if not encoded:
                shared_logger.error("cannot encode dead letter index event from payload", extra={"action": action})
                dead_letter_errors.append(action)

            encoded_actions.append(encoded)

        # If no action can be encoded, return original action list as failed
        if len(encoded_actions) == 0:
            return dead_letter_errors

        errors = es_bulk(self._es_client, encoded_actions, **self._bulk_kwargs)
        failed = self._handle_outcome(actions=encoded_actions, errors=errors)

        if not isinstance(failed, list) or len(failed) == 0:
            return dead_letter_errors

        for action in failed:
            event_payload = self._decode_dead_letter(action)

            if not event_payload:
                shared_logger.error("cannot decode dead letter index event from payload", extra={"action": action})
                continue

            dead_letter_errors.append(event_payload)

        return dead_letter_errors

    def _encode_dead_letter(self, outcome: dict[str, Any]) -> dict[str, Any]:
        if "action" not in outcome or "error" not in outcome:
            return {}

        # Assign random id in case bulk() results in error, it can be matched to the original
        # action
        return {
            "@timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "_id": f"{uuid.uuid4()}",
            "_index": self._es_dead_letter_index,
            "_op_type": "create",
            "message": json_dumper(outcome["action"]),
            "error": outcome["error"],
        }

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
