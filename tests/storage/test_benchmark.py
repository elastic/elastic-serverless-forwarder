# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import base64
import random
import string
from typing import Any, Optional

import cysimdjson
import mock
import orjson
import pytest
import pytest_benchmark.fixture
import rapidjson
import simdjson
import simplejson
import ujson

from share import CountMultiline, PatternMultiline, ProtocolMultiline, WhileMultiline
from storage import PayloadStorage

_LENGTH_BELOW_THRESHOLD: int = 40
_LENGTH_ABOVE_THRESHOLD: int = 1024 * 10
_LENGTH_1M: int = 1024**2

_IS_PLAIN: str = "_IS_PLAIN"
_IS_JSON: str = "_IS_JSON"
_IS_JSON_LIKE: str = "_IS_JSON_LIKE"
_IS_MULTILINE_COUNT: str = "_IS_MULTILINE_COUNT"
_IS_MULTILINE_PATTERN: str = "_IS_MULTILINE_PATTERN"
_IS_MULTILINE_WHILE: str = "_IS_MULTILINE_WHILE"

cysimdjson_parser = cysimdjson.JSONParser()
simdjson_parser = simdjson.Parser()


def json_parser_cysimdjson(payload: bytes) -> None:
    cysimdjson_parser.parse(payload)


def json_parser_simdjson(payload: bytes) -> None:
    simdjson_parser.parse(payload)


def get_by_lines_parameters() -> list[tuple[int, str, bytes]]:
    parameters: list[Any] = []
    for length_multiplier in [_LENGTH_BELOW_THRESHOLD, _LENGTH_ABOVE_THRESHOLD]:
        for content_type in [
            _IS_PLAIN,
            _IS_JSON,
            _IS_JSON_LIKE,
            _IS_MULTILINE_COUNT,
            _IS_MULTILINE_PATTERN,
            _IS_MULTILINE_WHILE,
        ]:
            for newline in [b"", b"\n", b"\r\n"]:
                parameters.append(
                    pytest.param(
                        length_multiplier,
                        content_type,
                        newline,
                        id=f"newline length {len(newline)} for content type {content_type} "
                        f"with length multiplier {length_multiplier}",
                    )
                )

    return parameters


def multiline_processor(content_type: str) -> Optional[ProtocolMultiline]:
    processor: Optional[ProtocolMultiline] = None
    if content_type == _IS_MULTILINE_COUNT:
        processor = CountMultiline(count_lines=3)
    elif content_type == _IS_MULTILINE_PATTERN:
        processor = PatternMultiline(pattern="MultilineStart", match="after", negate=True, flush_pattern="\\\\$")
    elif content_type == _IS_MULTILINE_WHILE:
        processor = WhileMultiline(pattern="MultilineStart", negate=True)

    return processor


class MockContentBase:
    f_size_gzip: int = 0
    f_size_plain: int = 0
    f_content_gzip: bytes = b""
    f_content_plain: bytes = b""

    mock_content: bytes = b""

    @staticmethod
    def init_content(content_type: str, newline: bytes, length_multiplier: int = _LENGTH_ABOVE_THRESHOLD) -> None:
        if len(newline) == 0:
            if content_type == _IS_JSON:
                mock_content = (
                    b"{"
                    + newline
                    + b'"'
                    + "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(1, 4))).encode(
                        "utf-8"
                    )
                    + b'"'
                    + newline
                    + b":"
                    + newline
                    + b'"'
                    + "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(1, 4))).encode(
                        "utf-8"
                    )
                    + b'"'
                    + newline
                    + b"}"
                )
            elif content_type.startswith("_IS_MULTILINE"):
                mock_content = (
                    b"MultilineStart"
                    + "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(1, 20))).encode(
                        "utf-8"
                    )
                    + b"\\"
                )
            else:
                mock_content = "".join(
                    random.choices(string.ascii_letters + string.digits, k=random.randint(1, 20))
                ).encode("utf-8")
        else:
            if content_type == _IS_JSON:
                # every json entry is from 14 to 39 chars, repeated for half of length_multiplier
                mock_content = newline.join(
                    [
                        b"{"
                        + newline
                        + b'"'
                        + "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(1, 5))).encode(
                            "utf-8"
                        )
                        + b'"'
                        + newline
                        + b":"
                        + newline
                        + b'"'
                        + "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(1, 5))).encode(
                            "utf-8"
                        )
                        + b'"'
                        + newline
                        + b"}"
                        + newline
                        for _ in range(1, int(length_multiplier / 2))
                    ]
                )
            elif content_type.startswith("_IS_MULTILINE"):
                # every line is from 0 to 20 chars, repeated for length_multiplier
                mock_content = newline.join(
                    [
                        b"MultilineStart"
                        + newline
                        + "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 20))).encode(
                            "utf-8"
                        )
                        + newline
                        + b"\\\\"
                        for _ in range(1, length_multiplier)
                    ]
                )
            else:
                # every line is from 0 to 20 chars, repeated for length_multiplier
                mock_content = newline + newline.join(
                    [
                        "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 20))).encode(
                            "utf-8"
                        )
                        for _ in range(1, length_multiplier)
                    ]
                )

        if content_type == _IS_JSON_LIKE:
            mock_content = b"{" + mock_content

        MockContentBase.mock_content = mock_content


class Setup:
    @staticmethod
    def setup() -> None:
        if len(MockContentBase.mock_content) == 0:
            MockContentBase.init_content(content_type=_IS_JSON, newline=b"\n", length_multiplier=_LENGTH_1M)


def wrap(payload: str) -> int:
    payload_storage = PayloadStorage(payload=payload, multiline_processor=None)
    lines = payload_storage.get_by_lines(range_start=0)
    last_length: int = 0
    for line in lines:
        last_length = line[2]

    return last_length


@pytest.mark.benchmark(group="plain")
@mock.patch("storage.decorator.json_parser", new=lambda x: orjson.loads(x))
def test_json_collector_plain_orjson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = MockContentBase.mock_content[1:]
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)

    assert last_length == original_length


@pytest.mark.benchmark(group="json")
@mock.patch("storage.decorator.json_parser", new=lambda x: orjson.loads(x))
def test_json_collector_json_orjson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = MockContentBase.mock_content
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)
    if original.endswith(b"\n" * 2):
        original_length -= 2

    assert last_length == original_length


@pytest.mark.benchmark(group="json like")
@mock.patch("storage.decorator.json_parser", new=lambda x: orjson.loads(x))
def test_json_collector_json_like_orjson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = MockContentBase.mock_content
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)

    assert last_length == original_length


@pytest.mark.benchmark(group="plain")
@mock.patch("storage.decorator.json_parser", new=lambda x: simplejson.loads(x))
def test_json_collector_plain_simplejson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = MockContentBase.mock_content[1:]
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)

    assert last_length == original_length


@pytest.mark.benchmark(group="json")
@mock.patch("storage.decorator.json_parser", new=lambda x: simplejson.loads(x))
def test_json_collector_json_simplejson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = MockContentBase.mock_content
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)
    if original.endswith(b"\n" * 2):
        original_length -= 2

    assert last_length == original_length


@pytest.mark.benchmark(group="json like")
@mock.patch("storage.decorator.json_parser", new=lambda x: simplejson.loads(x))
def test_json_collector_json_like_simplejson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = b"{" + MockContentBase.mock_content
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)

    assert last_length == original_length


@pytest.mark.benchmark(group="plain")
@mock.patch("storage.decorator.json_parser", new=lambda x: ujson.loads(x))
def test_json_collector_plain_ujson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = MockContentBase.mock_content[1:]
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)

    assert last_length == original_length


@pytest.mark.benchmark(group="json")
@mock.patch("storage.decorator.json_parser", new=lambda x: ujson.loads(x))
def test_json_collector_json_ujson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = MockContentBase.mock_content
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)
    if original.endswith(b"\n" * 2):
        original_length -= 2

    assert last_length == original_length


@pytest.mark.benchmark(group="json like")
@mock.patch("storage.decorator.json_parser", new=lambda x: ujson.loads(x))
def test_json_collector_json_like_ujson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = b"{" + MockContentBase.mock_content
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)

    assert last_length == original_length


@pytest.mark.benchmark(group="plain")
@mock.patch("storage.decorator.json_parser", new=json_parser_simdjson)
def test_json_collector_plain_simdjson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = MockContentBase.mock_content[1:]
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)

    assert last_length == original_length


@pytest.mark.benchmark(group="json")
@mock.patch("storage.decorator.json_parser", new=json_parser_simdjson)
def test_json_collector_json_simdjson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = MockContentBase.mock_content
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)
    if original.endswith(b"\n" * 2):
        original_length -= 2

    assert last_length == original_length


@pytest.mark.benchmark(group="json like")
@mock.patch("storage.decorator.json_parser", new=json_parser_simdjson)
def test_json_collector_json_like_simdjson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = b"{" + MockContentBase.mock_content
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)

    assert last_length == original_length


@pytest.mark.benchmark(group="plain")
@mock.patch("storage.decorator.json_parser", new=lambda x: rapidjson.loads(x))
def test_json_collector_plain_rapidjson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = MockContentBase.mock_content[1:]
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)

    assert last_length == original_length


@pytest.mark.benchmark(group="json")
@mock.patch("storage.decorator.json_parser", new=lambda x: rapidjson.loads(x))
def test_json_collector_json_rapidjson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = MockContentBase.mock_content
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)
    if original.endswith(b"\n" * 2):
        original_length -= 2

    assert last_length == original_length


@pytest.mark.benchmark(group="json like")
@mock.patch("storage.decorator.json_parser", new=lambda x: rapidjson.loads(x))
def test_json_collector_json_like_rapidjson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = b"{" + MockContentBase.mock_content
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)

    assert last_length == original_length


@pytest.mark.benchmark(group="plain")
@mock.patch("storage.decorator.json_parser", new=json_parser_cysimdjson)
def test_json_collector_plain_cysimdjson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = MockContentBase.mock_content[1:]
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)

    assert last_length == original_length


@pytest.mark.benchmark(group="json")
@mock.patch("storage.decorator.json_parser", new=json_parser_cysimdjson)
def test_json_collector_json_cyimdjson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = MockContentBase.mock_content
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)
    if original.endswith(b"\n" * 2):
        original_length -= 2

    assert last_length == original_length


@pytest.mark.benchmark(group="json like")
@mock.patch("storage.decorator.json_parser", new=json_parser_cysimdjson)
def test_json_collector_json_like_cysimdjson(benchmark: pytest_benchmark.fixture.BenchmarkFixture) -> None:
    Setup.setup()
    original: bytes = b"{" + MockContentBase.mock_content
    last_length = benchmark.pedantic(wrap, [base64.b64encode(original).decode("utf-8")], iterations=1, rounds=100)
    original_length: int = len(original)

    assert last_length == original_length
