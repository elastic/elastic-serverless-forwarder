# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, AnyStr

import ujson


def json_dumper(json_object: Any) -> str:
    return ujson.dumps(json_object, ensure_ascii=False, reject_bytes=False)


def json_parser(payload: AnyStr) -> Any:
    return ujson.loads(payload)
