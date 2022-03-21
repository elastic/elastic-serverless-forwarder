# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from __future__ import annotations

from unittest import TestCase

import pytest

from share import IncludeExcludeFilter, IncludeExcludeRule

_event = {
    "nested_level": {
        "str_key": "a message",
        "int_key": 1,
    },
    "single_level": "another message",
    "numeric_level": 1,
}


@pytest.mark.unit
class TestIncludeExclude(TestCase):
    def test_include_exclude(self) -> None:
        with self.subTest("no rules"):
            include_exclude_filter = IncludeExcludeFilter()
            assert include_exclude_filter.filter(_event) is True

        with self.subTest("exclude rule single level path key match"):
            include_exclude_filter = IncludeExcludeFilter(
                exclude_patterns=[IncludeExcludeRule(path_key="single_level", pattern="message")]
            )
            assert include_exclude_filter.filter(_event) is False

        with self.subTest("exclude rule single level path key not match"):
            include_exclude_filter = IncludeExcludeFilter(
                exclude_patterns=[IncludeExcludeRule(path_key="single_level", pattern="not matching")]
            )
            assert include_exclude_filter.filter(_event) is True

        with self.subTest("exclude rule nested level path key match"):
            include_exclude_filter = IncludeExcludeFilter(
                exclude_patterns=[IncludeExcludeRule(path_key="nested_level.str_key", pattern="message")]
            )
            assert include_exclude_filter.filter(_event) is False

        with self.subTest("exclude rule nested level path key not match"):
            include_exclude_filter = IncludeExcludeFilter(
                exclude_patterns=[IncludeExcludeRule(path_key="nested_level.str_key", pattern="not matching")]
            )
            assert include_exclude_filter.filter(_event) is True

        with self.subTest("exclude rule single level path key no str"):
            include_exclude_filter = IncludeExcludeFilter(
                exclude_patterns=[IncludeExcludeRule(path_key="numeric_level", pattern="message")]
            )
            assert include_exclude_filter.filter(_event) is True

        with self.subTest("exclude rule nested level path key no str"):
            include_exclude_filter = IncludeExcludeFilter(
                exclude_patterns=[IncludeExcludeRule(path_key="nested_level.int_key", pattern="message")]
            )
            assert include_exclude_filter.filter(_event) is True

        with self.subTest("include rule single level path key match"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(path_key="single_level", pattern="message")]
            )
            assert include_exclude_filter.filter(_event) is True

        with self.subTest("include rule single level path key not match"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(path_key="single_level", pattern="not matching")]
            )
            assert include_exclude_filter.filter(_event) is False

        with self.subTest("include rule nested level path key match"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(path_key="nested_level.str_key", pattern="message")]
            )
            assert include_exclude_filter.filter(_event) is True

        with self.subTest("include rule nested level path key not match"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(path_key="nested_level.str_key", pattern="not matching")]
            )
            assert include_exclude_filter.filter(_event) is False

        with self.subTest("include rule single level path key no str"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(path_key="numeric_level", pattern="message")]
            )
            assert include_exclude_filter.filter(_event) is False

        with self.subTest("include rule nested level path key no str"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(path_key="nested_level.int_key", pattern="message")]
            )
            assert include_exclude_filter.filter(_event) is False

        with self.subTest("both rules single level path key exclude priority"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(path_key="single_level", pattern="message")],
                exclude_patterns=[IncludeExcludeRule(path_key="single_level", pattern="message")],
            )
            assert include_exclude_filter.filter(_event) is False

        with self.subTest("both rules nested level path key exclude priority"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(path_key="nested_level.str_key", pattern="message")],
                exclude_patterns=[IncludeExcludeRule(path_key="nested_level.str_key", pattern="message")],
            )
            assert include_exclude_filter.filter(_event) is False

        with self.subTest("both rules single level path key include match"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(path_key="single_level", pattern="message")],
                exclude_patterns=[IncludeExcludeRule(path_key="string_level", pattern="not matching")],
            )
            assert include_exclude_filter.filter(_event) is True

        with self.subTest("both rules nested level path key include match"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(path_key="nested_level.str_key", pattern="message")],
                exclude_patterns=[IncludeExcludeRule(path_key="string_level", pattern="not matching")],
            )
            assert include_exclude_filter.filter(_event) is True

        with self.subTest("both rules single level path key no str in exclude"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(path_key="single_level", pattern="message")],
                exclude_patterns=[IncludeExcludeRule(path_key="numeric_level", pattern="message")],
            )
            assert include_exclude_filter.filter(_event) is True

        with self.subTest("both rules nested level path key no str in exclude"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(path_key="nested_level.str_key", pattern="message")],
                exclude_patterns=[IncludeExcludeRule(path_key="nested_level.numeric_key", pattern="message")],
            )
            assert include_exclude_filter.filter(_event) is True

        with self.subTest("both rules single level path key no str in include"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(path_key="numeric_level", pattern="message")],
                exclude_patterns=[IncludeExcludeRule(path_key="string_level", pattern="not matching")],
            )
            assert include_exclude_filter.filter(_event) is False

        with self.subTest("both rules nested level path key no str in include"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(path_key="nested_level.numeric_key", pattern="message")],
                exclude_patterns=[IncludeExcludeRule(path_key="nested_level.str_key", pattern="not matching")],
            )
            assert include_exclude_filter.filter(_event) is False
