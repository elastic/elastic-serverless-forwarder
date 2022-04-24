# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Dict, Optional, Union

import elasticapm  # noqa: F401
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk as es_bulk

from share import shared_logger

from .shipper import CommonShipper, EventIdGeneratorCallable, ReplayHandlerCallable

_EVENT_BUFFERED = "_EVENT_BUFFERED"
_EVENT_SENT = "_EVENT_SENT"


class ElasticsearchShipper(CommonShipper):
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
        es_index_or_datastream_name: str = "",
        tags: list[str] = [],
        batch_max_actions: int = 500,
        batch_max_bytes: int = 10 * 1024 * 1024,
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
        elif cloud_id:
            es_client_kwargs["cloud_id"] = cloud_id
        else:
            raise ValueError("You must provide one between elasticsearch_url or cloud_id")

        if username:
            es_client_kwargs["http_auth"] = (username, password)
        elif api_key:
            es_client_kwargs["api_key"] = api_key
        else:
            raise ValueError("You must provide one between username and password or api_key")

        self._replay_args: dict[str, Any] = {}

        self._es_client = self._elasticsearch_client(**es_client_kwargs)
        self._replay_handler: Optional[ReplayHandlerCallable] = None
        self._event_id_generator: Optional[EventIdGeneratorCallable] = None

        self._es_index_or_datastream_name = es_index_or_datastream_name
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

        return Elasticsearch(**es_client_kwargs)

    @staticmethod
    def _normalise_event(event_payload: dict[str, Any]) -> None:
        """
        This method move fields payload in the event at root level and then removes it with meta payload
        It has to be called as last step after any operation on the event payload just before sending to the cluster
        """
        if "fields" in event_payload:
            fields: dict[str, Any] = event_payload["fields"]
            for field_key in fields.keys():
                event_payload[field_key] = fields[field_key]

            del event_payload["fields"]

        if "meta" in event_payload:
            del event_payload["meta"]

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

    def _handle_outcome(self, errors: tuple[int, Union[int, list[Any]]]) -> None:
        assert isinstance(errors[1], list)

        success = errors[0]
        failed = len(errors[1])
        for error in errors[1]:
            action_failed = [action for action in self._bulk_actions if action["_id"] == error["create"]["_id"]]
            assert len(action_failed) == 1
            if self._replay_handler is not None:
                self._replay_handler("elasticsearch", self._replay_args, action_failed[0])

        if failed > 0:
            shared_logger.error("elasticsearch shipper", extra={"success": success, "failed": failed})
            return

        shared_logger.info("elasticsearch shipper", extra={"success": success, "failed": failed})

        return

    def set_event_id_generator(self, event_id_generator: EventIdGeneratorCallable) -> None:
        self._event_id_generator = event_id_generator

    def set_replay_handler(self, replay_handler: ReplayHandlerCallable) -> None:
        self._replay_handler = replay_handler

    def send(self, event: dict[str, Any]) -> str:
        self._replay_args["es_index_or_datastream_name"] = self._es_index_or_datastream_name

        if not hasattr(self, "_es_index") or self._es_index == "":
            self._discover_dataset(event_payload=event)

        self._enrich_event(event_payload=event)

        event["_op_type"] = "create"

        if "_index" not in event:
            event["_index"] = self._es_index

        if "_id" not in event and self._event_id_generator is not None:
            event["_id"] = self._event_id_generator(event)

        self._normalise_event(event_payload=event)

        self._bulk_actions.append(event)

        if len(self._bulk_actions) < self._bulk_batch_size:
            return _EVENT_BUFFERED

        errors = es_bulk(self._es_client, self._bulk_actions, **self._bulk_kwargs)
        self._handle_outcome(errors=errors)
        self._bulk_actions = []

        return _EVENT_SENT

    def flush(self) -> None:
        if len(self._bulk_actions) > 0:
            errors = es_bulk(self._es_client, self._bulk_actions, **self._bulk_kwargs)
            self._handle_outcome(errors=errors)

        self._bulk_actions = []

        return

    def _discover_dataset(self, event_payload: Dict[str, Any]) -> None:
        if self._es_index_or_datastream_name != "":
            if self._es_index_or_datastream_name.startswith("logs-"):
                datastream_components = self._es_index_or_datastream_name.split("-")
                if len(datastream_components) == 3:
                    self._dataset = datastream_components[1]
                    self._namespace = datastream_components[2]
                else:
                    shared_logger.debug(
                        "es_index_or_datastream_name not matching logs datastream pattern, no dataset "
                        "and namespace set"
                    )
            else:
                shared_logger.debug(
                    "es_index_or_datastream_name not matching logs datastream pattern, no dataset and namespace set"
                )

            self._es_index = self._es_index_or_datastream_name
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
            shared_logger.info("dataset set to generic")

        shared_logger.debug("dataset", extra={"dataset": self._dataset})

        self._es_index = f"logs-{self._dataset}-{self._namespace}"
        self._es_index_or_datastream_name = self._es_index
