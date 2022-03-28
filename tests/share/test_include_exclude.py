# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from __future__ import annotations

from unittest import TestCase

import pytest

from share import IncludeExcludeFilter, IncludeExcludeRule

_message = "a message"


@pytest.mark.unit
class TestIncludeExclude(TestCase):
    def test_include_exclude(self) -> None:
        with self.subTest("no rules"):
            include_exclude_filter = IncludeExcludeFilter()
            assert include_exclude_filter.filter(_message) is True

        with self.subTest("exclude rule match"):
            include_exclude_filter = IncludeExcludeFilter(exclude_patterns=[IncludeExcludeRule(pattern="message")])
            assert include_exclude_filter.filter(_message) is False

        with self.subTest("exclude rule not match"):
            include_exclude_filter = IncludeExcludeFilter(exclude_patterns=[IncludeExcludeRule(pattern="not matching")])
            assert include_exclude_filter.filter(_message) is True

        with self.subTest("include rule match"):
            include_exclude_filter = IncludeExcludeFilter(include_patterns=[IncludeExcludeRule(pattern="message")])
            assert include_exclude_filter.filter(_message) is True

        with self.subTest("include rule not match"):
            include_exclude_filter = IncludeExcludeFilter(include_patterns=[IncludeExcludeRule(pattern="not matching")])
            assert include_exclude_filter.filter(_message) is False

        with self.subTest("both rules exclude priority"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(pattern="message")],
                exclude_patterns=[IncludeExcludeRule(pattern="message")],
            )
            assert include_exclude_filter.filter(_message) is False

        with self.subTest("both rules include match"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(pattern="message")],
                exclude_patterns=[IncludeExcludeRule(pattern="not matching")],
            )
            assert include_exclude_filter.filter(_message) is True

        with self.subTest("both rules no match"):
            include_exclude_filter = IncludeExcludeFilter(
                include_patterns=[IncludeExcludeRule(pattern="not matching")],
                exclude_patterns=[IncludeExcludeRule(pattern="not matching")],
            )
            assert include_exclude_filter.filter(_message) is False
