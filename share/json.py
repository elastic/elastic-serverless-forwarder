# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, AnyStr

import orjson


def json_dumper(json_object: Any) -> str:
    if isinstance(json_object, bytes):
        json_object = json_object.decode("utf-8")

    return orjson.dumps(json_object).decode("utf-8")


def json_parser(payload: AnyStr) -> Any:
    return orjson.loads(payload)
