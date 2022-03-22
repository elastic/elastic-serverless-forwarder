# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from __future__ import annotations

import re
from typing import Optional


class IncludeExcludeRule:
    """
    IncludeExcludeRule represents a pattern rule
    """

    def __init__(self, pattern: str):
        self.pattern = re.compile(pattern)

    def __eq__(self, other: object) -> bool:
        assert isinstance(other, IncludeExcludeRule)

        return self.pattern == other.pattern


class IncludeExcludeFilter:
    """
    Base class for IncludeExclude filter
    """

    def __init__(
        self,
        include_patterns: Optional[list[IncludeExcludeRule]] = None,
        exclude_patterns: Optional[list[IncludeExcludeRule]] = None,
    ):
        self._include_rules: Optional[list[IncludeExcludeRule]] = None
        self._exclude_rules: Optional[list[IncludeExcludeRule]] = None

        if include_patterns is not None and len(include_patterns) > 0:
            self.include_rules = include_patterns

        if exclude_patterns is not None and len(exclude_patterns) > 0:
            self.exclude_rules = exclude_patterns

        self._always_yield = self._include_rules is None and self._exclude_rules is None

        self._include_only = self._include_rules is not None and self._exclude_rules is None
        self._exclude_only = self._exclude_rules is not None and self._include_rules is None

    def _is_included(self, message: str) -> bool:
        assert self._include_rules is not None

        for include_rule in self._include_rules:
            if include_rule.pattern.search(message) is not None:
                return True

        return False

    def _is_excluded(self, message: str) -> bool:
        assert self._exclude_rules is not None

        for exclude_rule in self._exclude_rules:
            if exclude_rule.pattern.search(message) is not None:
                return True

        return False

    def filter(self, message: str) -> bool:
        """
        filter returns True if the event is included or not excluded
        """

        if self._always_yield:
            return True

        if self._include_only:
            return self._is_included(message)

        if self._exclude_only:
            return not self._is_excluded(message)

        if self._is_excluded(message):
            return False

        return self._is_included(message)

    def __eq__(self, other: object) -> bool:
        assert isinstance(other, IncludeExcludeFilter)

        return self.include_rules == other.include_rules and self.exclude_rules == other.exclude_rules

    @property
    def include_rules(self) -> Optional[list[IncludeExcludeRule]]:
        return self._include_rules

    @include_rules.setter
    def include_rules(self, value: list[IncludeExcludeRule]) -> None:
        self._include_rules = value

    @property
    def exclude_rules(self) -> Optional[list[IncludeExcludeRule]]:
        return self._exclude_rules

    @exclude_rules.setter
    def exclude_rules(self, value: list[IncludeExcludeRule]) -> None:
        self._exclude_rules = value
