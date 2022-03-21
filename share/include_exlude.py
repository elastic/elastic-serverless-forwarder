# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from __future__ import annotations

import re
from typing import Any, Optional


def _get_by_key_path(key_path: str, data: dict[str, Any]) -> Any:
    path_list = key_path.split(".")
    result: Any = data
    for key in path_list:
        try:
            coerced_key = int(key) if key.isnumeric() else key
            result = result[coerced_key]
        except KeyError:
            result = None
            break

    return result


class IncludeExcludeRule:
    """
    IncludeExcludeRule represents a pattern rule for a path key
    """

    def __init__(self, path_key: str, pattern: str):
        self.path_key = path_key
        self.pattern = re.compile(pattern)

    def __eq__(self, other: object) -> bool:
        assert isinstance(other, IncludeExcludeRule)

        return self.path_key == other.path_key and self.pattern == other.pattern


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

    def _is_included(self, event: dict[str, Any]) -> bool:
        assert self._include_rules is not None

        for include_rule in self._include_rules:
            value_in_event = _get_by_key_path(include_rule.path_key, event)

            if value_in_event is not None and isinstance(value_in_event, str):
                if include_rule.pattern.search(value_in_event) is not None:
                    return True

        return False

    def _is_excluded(self, event: dict[str, Any]) -> bool:
        assert self._exclude_rules is not None

        for exclude_rule in self._exclude_rules:
            value_in_event = _get_by_key_path(exclude_rule.path_key, event)

            if value_in_event is not None and isinstance(value_in_event, str):
                if exclude_rule.pattern.search(value_in_event) is not None:
                    return True

        return False

    def filter(self, event: dict[str, Any]) -> bool:
        """
        filter returns True if the event is included or not excluded
        """

        if self._always_yield:
            return True

        if self._include_only:
            return self._is_included(event=event)

        if self._exclude_only:
            return not self._is_excluded(event=event)

        if self._is_excluded(event=event):
            return False

        return self._is_included(event=event)

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
