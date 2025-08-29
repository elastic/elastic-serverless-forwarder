# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import struct
from typing import Any, Generator, Optional
from io import BytesIO

from .logger import logger as shared_logger


class Message:
    """Represents an IPFIX message with its header information."""

    def __init__(
        self, version: int, length: int, export_time: int,
        sequence_number: int, observation_domain_id: int, start_offset: int
    ) -> None:
        self.version = version
        self.length = length
        self.export_time = export_time
        self.sequence_number = sequence_number
        self.observation_domain_id = observation_domain_id
        self.start_offset = start_offset

    def __repr__(self) -> str:
        return (
            f"Message(version={self.version}, length={self.length}, "
            f"export_time={self.export_time}, sequence_number={self.sequence_number}, "
            f"observation_domain_id={self.observation_domain_id}, start_offset={self.start_offset})"
        )

    def to_dict(self) -> dict:
        """Convert message header to dictionary format."""
        return {
            "version": self.version,
            "length": self.length,
            "export_time": self.export_time,
            "sequence_number": self.sequence_number,
            "observation_domain_id": self.observation_domain_id,
            "start_offset": self.start_offset
        }


class TemplateSet:
    """Represents an IPFIX template set with field definitions."""

    def __init__(self, template_id: int) -> None:
        self.template_id = template_id
        self.fields: list[tuple[str, int, str]] = []

    def add_field(self, name: str, length: int, data_type: str) -> None:
        """Add a field definition to the template."""
        self.fields.append((name, length, data_type))

    def get_fields(self) -> list[tuple[str, int, str]]:
        """Get all field definitions."""
        return self.fields

    @classmethod
    def parse_from_data(cls, data: bytes) -> 'TemplateSet':
        """Parse template set from binary data."""
        try:
            from processors.ie import RFC_5102_INFO_ELEMENT
        except ImportError:
            # Fallback for standalone usage
            RFC_5102_INFO_ELEMENT = {}
            shared_logger.warning("Could not import RFC_5102_INFO_ELEMENT, using empty mapping")

        offset = 0
        template_id, field_count = struct.unpack_from("!HH", data, offset)
        offset += 4

        template = cls(template_id)

        for _ in range(field_count):
            field_id, field_length = struct.unpack_from("!HH", data, offset)
            offset += 4

            # RFC_5102_INFO_ELEMENT returns (name, dataType) tuple
            field_info = RFC_5102_INFO_ELEMENT.get(field_id, (f"field_{field_id}", "unknown"))
            field_name = field_info[0] if len(field_info) > 0 else f"field_{field_id}"
            field_type = field_info[1] if len(field_info) > 1 else "unknown"

            template.add_field(field_name, field_length, field_type)

        return template


class DataRecord:
    """Represents an IPFIX data record."""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    def set_field(self, name: str, value: Any) -> None:
        """Set a field value in the record."""
        self.data[name] = value

    def get_data(self) -> dict[str, Any]:
        """Get the complete record data."""
        return self.data

    @classmethod
    def parse_from_template(
        cls, template: TemplateSet, data: bytes, msg_header: dict
    ) -> 'DataRecord':
        """Parse a data record using the provided template."""
        try:
            from processors.ie import convert
        except ImportError:
            # Fallback for standalone usage
            def convert(field_type: str, data: bytes) -> Any:
                if field_type in ["unsigned32", "unsigned64"]:
                    return int.from_bytes(data, 'big')
                elif field_type in ["unsigned8", "unsigned16"]:
                    return int.from_bytes(data, 'big')
                else:
                    return data.hex()

        record = cls()
        offset = 0

        # Add message header info
        record.set_field("@timestamp", msg_header.get("export_time"))
        record.set_field("header", msg_header)

        # Parse fields according to template
        for field_name, field_length, field_type in template.get_fields():
            if offset + field_length > len(data):
                break

            field_data = data[offset:offset + field_length]

            try:
                value = convert(field_type, field_data)
                record.set_field(field_name, value)
            except Exception as e:
                shared_logger.debug(f"Error parsing field {field_name}: {e}")
                record.set_field(field_name, field_data.hex())

            offset += field_length

        return record


class IPFIXStreamingParser:
    """Streaming IPFIX parser that yields records one by one."""

    def __init__(self, data_source: BytesIO) -> None:
        self.file = data_source
        self.offset = 0
        self.closed = False
        self.templates: dict[int, TemplateSet] = {}
        self.buffer_size = 8192  # 8KB chunks for reading

    def close(self) -> None:
        """Close the parser and release resources."""
        self.closed = True
        if hasattr(self.file, 'close'):
            self.file.close()

    def read(self, size: int) -> bytes:
        """Read specified number of bytes from the data source."""
        data = self.file.read(size)
        self.offset += len(data)
        if not data or len(data) != size:
            self.closed = True
        return data

    def parse_message_header(self) -> Optional[Message]:
        """Parse the IPFIX message header."""
        raw = self.read(16)
        if len(raw) < 16:
            return None

        version, length = struct.unpack_from("!HH", raw[:4])
        if version != 10:
            shared_logger.warning(f"Unsupported IPFIX version: {version}")
            return None

        export_time, sequence_number, observation_domain_id = \
            struct.unpack_from("!III", raw[4:])

        return Message(
            version=version,
            length=length,
            export_time=export_time,
            sequence_number=sequence_number,
            observation_domain_id=observation_domain_id,
            start_offset=self.offset - 16
        )

    def parse_flowset_header(self) -> Optional[tuple[int, int]]:
        """Parse the IPFIX flowset header."""
        raw = self.read(4)
        if len(raw) < 4:
            return None
        set_id, length = struct.unpack("!HH", raw)
        return (set_id, length)

    def parse_template_set(self, data: bytes) -> None:
        """Parse a template set and store the template definitions."""
        try:
            template = TemplateSet.parse_from_data(data)
            if hasattr(template, 'template_id'):
                self.templates[template.template_id] = template
                shared_logger.debug(f"Parsed template {template.template_id} with {len(template.fields)} fields")
            else:
                shared_logger.warning(f"Template parsing returned invalid object: {type(template)}")
        except Exception as e:
            shared_logger.warning(f"Error parsing template set: {e}")

    def parse_data_set(
        self, template_id: int, data: bytes, msg_header: dict
    ) -> Generator[dict[str, Any], None, None]:
        """Parse a data set using the corresponding template and yield records."""
        template = self.templates.get(template_id)
        if not template:
            shared_logger.warning(f"Missing template for set {template_id}")
            return

        # Calculate record size from template
        record_size = sum(length for _, length, _ in template.get_fields())

        if record_size == 0:
            shared_logger.warning(f"Template {template_id} has zero record size")
            return

        # Parse multiple records from the data set
        offset = 0
        while offset + record_size <= len(data):
            record_data = data[offset:offset + record_size]
            try:
                record = DataRecord.parse_from_template(template, record_data, msg_header)

                # Add processor metadata
                record_dict = record.get_data()
                record_dict["processor"] = {
                    "type": "ipfix",
                    "processed_at": record_dict.get("@timestamp")
                }

                yield record_dict
                offset += record_size
            except Exception as e:
                shared_logger.warning(f"Error parsing data record: {e}")
                break

    def parse_records(self) -> Generator[dict[str, Any], None, None]:
        """
        Parse IPFIX records from the data source, yielding them one by one.

        Args:
            max_records: Maximum number of records to process in one batch

        Yields:
            dict: Individual IPFIX records
        """
        record_count = 0

        try:
            while not self.closed:
                # Parse message header
                msg_header_obj = self.parse_message_header()
                if not msg_header_obj:
                    break

                msg_header = msg_header_obj.to_dict()
                bytes_remaining = msg_header_obj.length - 16

                shared_logger.debug(f"Processing IPFIX message: {bytes_remaining} bytes remaining")

                # Process all sets in this message
                while bytes_remaining > 0 and not self.closed:
                    # Parse flowset header
                    flowset_header = self.parse_flowset_header()
                    if not flowset_header:
                        break

                    set_id, set_length = flowset_header

                    # Read the set data
                    set_data_length = set_length - 4  # Subtract header size
                    if set_data_length <= 0:
                        break

                    set_data = self.read(set_data_length)
                    if len(set_data) < set_data_length:
                        break

                    bytes_remaining -= set_length

                    if set_id == 2:  # Template Set
                        self.parse_template_set(set_data)
                    elif set_id == 3:  # Options Template Set
                        shared_logger.debug("Skipping Options Template Set")
                    elif set_id >= 256:  # Data Set
                        # Yield records from this data set
                        for record in self.parse_data_set(set_id, set_data, msg_header):
                            yield record
                            record_count += 1
                    else:
                        shared_logger.debug(f"Skipping unknown set ID {set_id}")

        except Exception as e:
            shared_logger.error(f"Error in IPFIX parsing: {e}")
        finally:
            if record_count > 0:
                shared_logger.info(
                    "IPFIX parser: Successfully processed records",
                    extra={"record_count": record_count}
                )


def parse_ipfix_stream(data_source: BytesIO) -> Generator[dict[str, Any], None, None]:
    """
    Parse IPFIX data from a stream and yield individual records.

    This is the main entry point for streaming IPFIX parsing. It creates a parser
    instance and yields records one by one, which is memory-efficient for large files.

    Args:
        data_source: Either raw bytes or a BytesIO stream containing IPFIX data
        max_records: Maximum number of records to process in one batch

    Yields:
        dict: Individual IPFIX records with parsed fields

    Example:
        ```python
        with open('data.ipfix', 'rb') as f:
            data = f.read()

        for record in parse_ipfix_stream(data):
            print(f"Record: {record}")
        ```
    """
    parser = IPFIXStreamingParser(data_source)
    try:
        yield from parser.parse_records()
    finally:
        parser.close()
