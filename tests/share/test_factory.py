# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from unittest import TestCase

import pytest

from share import CountMultiline, MultilineFactory, PatternMultiline, WhileMultiline


@pytest.mark.unit
class TestMultilineFactory(TestCase):
    def test_create(self) -> None:
        with self.subTest("create count multiline success"):
            multiline = MultilineFactory.create(multiline_type="count", count_lines=1)

            assert isinstance(multiline, CountMultiline)

        with self.subTest("create count multiline error"):
            with self.assertRaises(TypeError):
                MultilineFactory.create(multiline_type="count")

        with self.subTest("create pattern multiline success"):
            multiline = MultilineFactory.create(multiline_type="pattern", pattern=".+", match="after")

            assert isinstance(multiline, PatternMultiline)

        with self.subTest("create pattern multiline error"):
            with self.assertRaises(TypeError):
                MultilineFactory.create(multiline_type="pattern")

        with self.subTest("create while_pattern multiline success"):
            multiline = MultilineFactory.create(multiline_type="while_pattern", pattern=".+")

            assert isinstance(multiline, WhileMultiline)

        with self.subTest("create while_pattern multiline error"):
            with self.assertRaises(TypeError):
                MultilineFactory.create(multiline_type="while_pattern")

        with self.subTest("create invalid type"):
            with self.assertRaisesRegex(
                ValueError,
                "^You must provide one of the following multiline types: "
                "count, pattern, while_pattern. invalid type given$",
            ):
                MultilineFactory.create(multiline_type="invalid type")
