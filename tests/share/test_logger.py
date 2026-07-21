# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import io
import json
import logging
from typing import Any, cast
from unittest import TestCase

import pytest

from share.logger import AWSCompatibleFormatter


def _formatted(level: int, msg: str = "test") -> dict[str, Any]:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(AWSCompatibleFormatter())
    log = logging.getLogger(f"test.{level}")
    log.setLevel(logging.DEBUG)
    log.propagate = False
    log.handlers = [handler]
    log.log(level, msg)
    return cast(dict[str, Any], json.loads(stream.getvalue().strip()))


@pytest.mark.unit
class TestAWSCompatibleFormatter(TestCase):
    def test_level_field_is_root_level(self) -> None:
        record = _formatted(logging.INFO)
        self.assertIn("level", record)

    def test_ecs_log_level_preserved(self) -> None:
        record = _formatted(logging.INFO)
        self.assertEqual(record["log.level"], "info")

    def test_debug_mapping(self) -> None:
        self.assertEqual(_formatted(logging.DEBUG)["level"], "DEBUG")

    def test_info_mapping(self) -> None:
        self.assertEqual(_formatted(logging.INFO)["level"], "INFO")

    def test_warning_maps_to_warn(self) -> None:
        self.assertEqual(_formatted(logging.WARNING)["level"], "WARN")

    def test_error_mapping(self) -> None:
        self.assertEqual(_formatted(logging.ERROR)["level"], "ERROR")

    def test_critical_maps_to_fatal(self) -> None:
        self.assertEqual(_formatted(logging.CRITICAL)["level"], "FATAL")

    def test_level_and_log_level_are_independent(self) -> None:
        record = _formatted(logging.WARNING)
        self.assertEqual(record["level"], "WARN")
        self.assertEqual(record["log.level"], "warning")
