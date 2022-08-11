# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from unittest import TestCase

import pytest

from share import json_dumper, json_parser


@pytest.mark.unit
class TestJsonParser(TestCase):
    def test_json_parser(self) -> None:
        with self.subTest("loads raises"):
            with self.assertRaises(Exception):
                json_parser("[")

            with self.subTest("loads array"):
                loaded = json_parser("[1, 2, 3]")
                assert [1, 2, 3] == loaded

            with self.subTest("loads dict"):
                loaded = json_parser('{"key":"value"}')
                assert {"key": "value"} == loaded

            with self.subTest("loads scalar"):
                loaded = json_parser('"a string"')
                assert "a string" == loaded


@pytest.mark.unit
class TestJsonDumper(TestCase):
    def test_json_dumper(self) -> None:
        with self.subTest("dumps raises"):
            with self.assertRaises(Exception):
                json_dumper(set())

        with self.subTest("dumps bytes"):
            dumped = json_dumper(b"bytes")
            assert '"bytes"' == dumped

        with self.subTest("dumps str"):
            dumped = json_dumper("string")
            assert '"string"' == dumped

        with self.subTest("dumps dict"):
            dumped = json_dumper({"key": "value"})
            assert '{"key":"value"}' == dumped
