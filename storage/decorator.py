# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import gzip
from io import BytesIO
from typing import Any, Iterator, Optional, Union

from share import ExpandEventListFromField, FeedIterator, ProtocolMultiline, json_parser, shared_logger

from .storage import CHUNK_SIZE, ProtocolStorageType, StorageDecoratorCallable, StorageDecoratorIterator, StorageReader


def by_lines(func: StorageDecoratorCallable[ProtocolStorageType]) -> StorageDecoratorCallable[ProtocolStorageType]:
    """
    ProtocolStorage decorator for returning content split by lines
    """

    def wrapper(
        storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool
    ) -> StorageDecoratorIterator:
        ending_offset: int = range_start
        unfinished_line: bytes = b""

        iterator = func(storage, range_start, body, is_gzipped)

        for data, _, _, _, _ in iterator:
            assert isinstance(data, bytes)

            unfinished_line += data
            lines = unfinished_line.decode("utf-8").splitlines()

            if len(lines) == 0:
                continue

            if unfinished_line.find(b"\r\n") > -1:
                newline = b"\r\n"
            elif unfinished_line.find(b"\n") > -1:
                newline = b"\n"
            else:
                newline = b""

            # replace unfinished_line with the last element removed from lines, trailing with newline
            if unfinished_line.endswith(newline):
                unfinished_line = lines.pop().encode() + newline
            else:
                unfinished_line = lines.pop().encode()

            for line in lines:
                line_encoded = line.encode("utf-8")
                starting_offset = ending_offset
                ending_offset += len(line_encoded) + len(newline)
                shared_logger.debug("by_line lines", extra={"offset": ending_offset})

                yield line_encoded, starting_offset, ending_offset, newline, None

        if len(unfinished_line) > 0:
            if unfinished_line.endswith(b"\r\n"):
                newline = b"\r\n"
            elif unfinished_line.endswith(b"\n"):
                newline = b"\n"
            else:
                newline = b""

            unfinished_line = unfinished_line.rstrip(newline)

            starting_offset = ending_offset
            ending_offset += len(unfinished_line) + len(newline)

            shared_logger.debug("by_line unfinished_line", extra={"offset": ending_offset})

            yield unfinished_line, starting_offset, ending_offset, newline, None

    return wrapper


def multi_line(func: StorageDecoratorCallable[ProtocolStorageType]) -> StorageDecoratorCallable[ProtocolStorageType]:
    """
    ProtocolStorage decorator for returning content collected by multiline
    """

    def wrapper(
        storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool
    ) -> StorageDecoratorIterator:
        multiline_processor: Optional[ProtocolMultiline] = storage.multiline_processor
        if not multiline_processor:
            iterator = func(storage, range_start, body, is_gzipped)
            for data, starting_offset, ending_offset, newline, event_expanded_offset in iterator:
                assert isinstance(data, bytes)

                shared_logger.debug("multi_line skipped", extra={"offset": ending_offset})

                yield data, starting_offset, ending_offset, newline, event_expanded_offset
        else:
            ending_offset = range_start

            def iterator_to_multiline_feed() -> FeedIterator:
                for data, _, _, newline, _ in func(storage, range_start, body, is_gzipped):
                    assert isinstance(data, bytes)
                    yield data, newline

            multiline_processor.feed = iterator_to_multiline_feed()

            for multiline_data, multiline_ending_offset, newline in multiline_processor.collect():
                starting_offset = ending_offset
                ending_offset += multiline_ending_offset
                shared_logger.debug("multi_line lines", extra={"offset": ending_offset})

                yield multiline_data, starting_offset, ending_offset, newline, None

    return wrapper


class JsonCollectorState:
    def __init__(self, storage: ProtocolStorageType):
        self.storage: ProtocolStorageType = storage

        self.starting_offset: int = 0
        self.ending_offset: int = 0

        self.unfinished_line: bytes = b""

        self.has_an_object_start: bool = False

        self.is_a_json_object: bool = False
        self.is_a_json_object_circuit_broken: bool = False
        self.is_a_json_object_circuit_breaker: int = 0


def json_collector(
    func: StorageDecoratorCallable[ProtocolStorageType],
) -> StorageDecoratorCallable[ProtocolStorageType]:
    """
    ProtocolStorage decorator for returning content by collected json object (if any) spanning multiple lines

    If `json_content_type` is `single` and we don't have any `expand_event_list_from_field` set, we just collect all the
    content, and yield. If `json_content_type` is `disabled` or we have a multiline processor set, we yield what we
    receive from the previous decorator. If `json_content_type` is `None` or `ndjson` we try to parse che content as
    json if we find the beginning of a potential json object (ie: the `{` char). This is done appending one line after
    one line and passing the content to the json parser until it will be able to parse a full json object. If the
    content is ndjson every json parsing attempt will be successful, in case it isn't the parsing will succeed only
    after we collect a full json object spanning multiple lines. A circuit breaker is present in order to stop trying to
    parse the json if we reached 1000 lines: in this case we yield the content as it is, line by line.
    Once a json object is parsed, if we have an `expand_event_list_from_field` set we pass the json the "events list
    from field expander" and yield the expanded events list instead.
    """

    def _handle_offset(offset_skew: int, json_collector_state: JsonCollectorState) -> None:
        json_collector_state.starting_offset = json_collector_state.ending_offset
        json_collector_state.ending_offset += offset_skew

    def _collector(
        data: bytes, newline: bytes, json_collector_state: JsonCollectorState
    ) -> Iterator[tuple[bytes, Optional[dict[str, Any]]]]:
        try:
            # let's buffer the content
            # we receive data without newline
            # let's append it as well
            json_collector_state.unfinished_line += data + newline

            # let's try to decode
            json_object = json_parser(json_collector_state.unfinished_line)

            # it didn't raise: we collected a json object
            data_to_yield = json_collector_state.unfinished_line

            # let's reset the buffer
            json_collector_state.unfinished_line = b""

            # let's increase the offset for yielding
            _handle_offset(len(data_to_yield), json_collector_state)

            # let's decrease the circuit breaker by the number of lines in the data to yield
            if newline != b"":
                json_collector_state.is_a_json_object_circuit_breaker -= data_to_yield.count(newline) - 1
            else:
                json_collector_state.is_a_json_object_circuit_breaker -= 1

            # let's trim surrounding newline
            data_to_yield = data_to_yield.strip(b"\r\n").strip(b"\n")

            # let's set the flag for json object
            json_collector_state.is_a_json_object = True

            # finally yield
            yield data_to_yield, json_object
        # it raised ValueError: we didn't collect enough content
        # to reach the end of the json object
        # let's keep iterating
        except ValueError:
            # it's an empty line, let's yield it
            if (
                json_collector_state.is_a_json_object
                and len(json_collector_state.unfinished_line.strip(b"\r\n").strip(b"\n")) == 0
            ):
                # let's reset the buffer
                json_collector_state.unfinished_line = b""

                # let's increase the offset for yielding
                _handle_offset(len(newline), json_collector_state)

                # finally yield
                yield b"", None
            else:
                # buffer was not a complete json object
                # let's increase the circuit breaker
                json_collector_state.is_a_json_object_circuit_breaker += 1

                # if the first 1k lines are not a json object let's give up
                if json_collector_state.is_a_json_object_circuit_breaker > 1000:
                    json_collector_state.is_a_json_object_circuit_broken = True

    def _by_lines_fallback(json_collector_state: JsonCollectorState) -> StorageDecoratorIterator:
        @by_lines
        def wrapper(
            storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool
        ) -> StorageDecoratorIterator:
            data_to_yield: bytes = body.read()
            yield data_to_yield, 0, range_start, b"", None

        for line, _, _, newline, _ in wrapper(
            json_collector_state.storage,
            json_collector_state.ending_offset,
            BytesIO(json_collector_state.unfinished_line),
            False,
        ):
            assert isinstance(line, bytes)

            _handle_offset(len(line) + len(newline), json_collector_state)

            # let's reset the buffer
            json_collector_state.unfinished_line = b""

            # let's set the flag for direct yield from now on
            json_collector_state.has_an_object_start = False

            yield line, _, _, newline, None

    def _collect_single(iterator: StorageDecoratorIterator) -> StorageDecoratorIterator:
        # we get the original iterator, we collect everything in a list that we merge later and extract values from
        single: list[tuple[Union[StorageReader, bytes], int, int, bytes]] = list(
            [
                (data, starting_offset, ending_offset, newline)
                for data, starting_offset, ending_offset, newline, _ in iterator
            ]
        )

        newline = single[0][-1]
        starting_offset = single[0][1]
        ending_offset = single[-1][2]

        data_to_yield: bytes = newline.join([x[0] for x in single])
        yield data_to_yield, starting_offset, ending_offset, newline, None

    def wrapper(
        storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool
    ) -> StorageDecoratorIterator:
        json_collector_state = JsonCollectorState(storage=storage)

        multiline_processor: Optional[ProtocolMultiline] = storage.multiline_processor
        if storage.json_content_type == "disabled" or multiline_processor:
            iterator = func(storage, range_start, body, is_gzipped)
            for data, starting_offset, ending_offset, newline, _ in iterator:
                assert isinstance(data, bytes)
                shared_logger.debug("json_collector skipped", extra={"offset": ending_offset})

                yield data, starting_offset, ending_offset, newline, None
        else:
            event_list_from_field_expander: Optional[ExpandEventListFromField] = storage.event_list_from_field_expander

            json_collector_state.ending_offset = range_start

            iterator = func(storage, range_start, body, is_gzipped)
            # if we know it's a single json we wrap the iterator with _collect_single
            # and mark the object as json and started
            if storage.json_content_type == "single":
                iterator = _collect_single(iterator=iterator)
                json_collector_state.is_a_json_object = True
                json_collector_state.has_an_object_start = True

            for data, starting_offset, ending_offset, newline, _ in iterator:
                assert isinstance(data, bytes)

                # let's wait for the start of a json object
                if not json_collector_state.has_an_object_start:
                    # if range_start is greater than zero, or we have leading space, data can be empty
                    stripped_data = data.decode("utf-8").lstrip()
                    if len(stripped_data) > 0 and stripped_data[0] == "{":
                        # we mark the potentiality of a json object start
                        # CAVEAT: if the log entry starts with `{` but the
                        # content is not json, we buffer the first 10k lines
                        # before the circuit breaker kicks in
                        json_collector_state.has_an_object_start = True

                    # if it has not a json object start we can just forward the content by lines
                    if not json_collector_state.has_an_object_start:
                        _handle_offset(len(data) + len(newline), json_collector_state)
                        yield data, starting_offset, ending_offset, newline, None

                # it has a json object start, let's apply our logic
                if json_collector_state.has_an_object_start:
                    # it is a single json and we have not a field expander, let's yield the content
                    if event_list_from_field_expander is None and storage.json_content_type == "single":
                        yield data, starting_offset, ending_offset, newline, None
                    else:
                        # it is not single, or we have a field expander. let's try to collect the data as json
                        for data_to_yield, json_object in _collector(data, newline, json_collector_state):
                            shared_logger.debug(
                                "json_collector objects", extra={"offset": json_collector_state.ending_offset}
                            )
                            # we have a field expander, let's yield the expansion
                            if event_list_from_field_expander is not None:
                                for (
                                    expanded_log_event,
                                    expanded_starting_offset,
                                    expanded_ending_offset,
                                    expanded_event_n,
                                ) in event_list_from_field_expander.expand(
                                    data_to_yield,
                                    json_object,
                                    json_collector_state.starting_offset,
                                    json_collector_state.ending_offset,
                                ):
                                    yield (
                                        expanded_log_event,
                                        expanded_starting_offset,
                                        expanded_ending_offset,
                                        newline,
                                        expanded_event_n,
                                    )
                            else:
                                # we do not have a field expander, let's yield the expansion
                                yield (
                                    data_to_yield,
                                    json_collector_state.starting_offset,
                                    json_collector_state.ending_offset,
                                    newline,
                                    None,
                                )

                            del json_object

                    # check if we hit the circuit broken
                    if json_collector_state.is_a_json_object_circuit_broken:
                        # let's yield what we have so far
                        for line, _, _, original_newline, _ in _by_lines_fallback(json_collector_state):
                            yield (
                                line,
                                json_collector_state.starting_offset,
                                json_collector_state.ending_offset,
                                original_newline,
                                None,
                            )

            # in this case we could have a trailing new line in what's left in the buffer
            # or the content had a leading `{` but was not a json object before the circuit breaker intercepted it,
            # or we waited for the object start and never reached:
            # let's fallback to by_lines()
            if not json_collector_state.is_a_json_object:
                for line, _, _, newline, _ in _by_lines_fallback(json_collector_state):
                    yield line, json_collector_state.starting_offset, json_collector_state.ending_offset, newline, None

    return wrapper


def inflate(func: StorageDecoratorCallable[ProtocolStorageType]) -> StorageDecoratorCallable[ProtocolStorageType]:
    """
    ProtocolStorage decorator for returning inflated content in case the original is gzipped
    """

    def wrapper(
        storage: ProtocolStorageType, range_start: int, body: BytesIO, is_gzipped: bool
    ) -> StorageDecoratorIterator:
        iterator = func(storage, range_start, body, is_gzipped)
        for data, _, _, _, _ in iterator:
            if is_gzipped:
                gzip_stream = gzip.GzipFile(fileobj=data)  # type:ignore
                gzip_stream.seek(range_start)
                while True:
                    inflated_chunk: bytes = gzip_stream.read(CHUNK_SIZE)
                    if len(inflated_chunk) == 0:
                        break

                    buffer: BytesIO = BytesIO()
                    buffer.write(inflated_chunk)

                    shared_logger.debug("inflate inflate")
                    yield buffer.getvalue(), 0, 0, b"", None
            else:
                shared_logger.debug("inflate plain")
                yield data, 0, 0, b"", None

    return wrapper
