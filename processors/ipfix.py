import struct
from typing import Dict, Any, Optional

from .processor import BaseProcessor, ProcessorResult
from .registry import register_processor
from .ie import RFC_5102_INFO_ELEMENT, convert


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
        """Parse template set from raw bytes."""
        template_id, field_count = struct.unpack_from("!HH", data)
        template = cls(template_id)

        offset = 4
        for _ in range(field_count):
            field_id, field_len = struct.unpack_from("!HH", data, offset)
            name, data_type = RFC_5102_INFO_ELEMENT.get(
                field_id, (f"unknown_{field_id}", "unknown"))
            template.add_field(name, field_len, data_type)
            offset += 4

        return template


class DataRecord:
    """Represents a single IPFIX data record."""

    def __init__(self, timestamp: int, header: dict) -> None:
        self.data: dict[str, Any] = {
            '@timestamp': timestamp,
            'header': header
        }

    def add_field(self, name: str, value: Any) -> None:
        """Add a field value to the record."""
        self.data[name] = value

    def get_data(self) -> dict[str, Any]:
        """Get the complete record data."""
        return self.data

    @classmethod
    def parse_from_template(cls, template: TemplateSet, data: bytes,
                            msg_header: dict) -> Optional['DataRecord']:
        """Parse data record using a template."""
        fields = template.get_fields()
        if not fields:
            return None

        record = cls(msg_header['export_time'], msg_header)
        offset = 0

        for name, length, data_type in fields:
            value = data[offset:offset + length]
            converted_value = convert(value, data_type)
            record.add_field(name, converted_value)
            offset += length

        return record


class IPFIXReader:
    """Main IPFIX file reader class."""

    def __init__(self, file_path=None, file_bytes=None):
        if file_path:
            self.file = open(file_path, "rb")
        elif file_bytes:
            from io import BytesIO
            self.file = BytesIO(file_bytes)
        else:
            raise ValueError("IPFIXReader requires file_path or file_bytes")
        self.offset = 0
        self.closed = False
        self.templates: dict[int, TemplateSet] = {}
        self.records: list[DataRecord] = []

    def close(self) -> None:
        """Close the file and mark reader as closed."""
        self.closed = True
        self.file.close()

    def read(self, size: int) -> bytes:
        """Read specified number of bytes from file."""
        data = self.file.read(size)
        self.offset += len(data)
        if not data or len(data) != size:
            self.close()
        return data

    def parse_message_header(self) -> Message:
        """Parse the IPFIX message header."""
        raw = self.read(16)
        if len(raw) < 16:
            raise Exception("Failed to read header -- EOF?")
        version, length = struct.unpack_from("!HH", raw[:4])
        if version != 10:
            raise ValueError("Unsupported IPFIX version")
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

    def parse_flowset_header(self) -> tuple:
        """Parse the IPFIX flowset header."""
        raw = self.read(4)
        set_id, length = struct.unpack("!HH", raw)
        return (set_id, length)

    def parse_template_set(self, data: bytes) -> None:
        """Parse a template set and store the template definitions."""
        template = TemplateSet.parse_from_data(data)
        self.templates[template.template_id] = template

    def parse_data_set(
        self, template_id: int, data: bytes, msg_header: dict
    ) -> None:
        """Parse a data set using the corresponding template."""
        template = self.templates.get(template_id)
        if not template:
            print(f"[!] Missing template for set {template_id}")
            return

        # Calculate record size from template
        record_size = sum(length for _, length, _ in template.get_fields())

        # Parse multiple records from the data set
        offset = 0
        while offset + record_size <= len(data):
            record_data = data[offset:offset + record_size]
            record = DataRecord.parse_from_template(template, record_data, msg_header)
            if record:
                self.records.append(record)
            offset += record_size

    def parse_message(self) -> None:
        """Parse an entire IPFIX message."""
        header = self.parse_message_header()
        end = header.start_offset + header.length

        while self.offset < end:
            set_id, length = self.parse_flowset_header()
            body = self.read(length - 4)

            if set_id == 2:
                self.parse_template_set(body)
            elif set_id >= 256:
                self.parse_data_set(
                    set_id, body, msg_header=header.to_dict())
            else:
                print(f"[!] Skipping set ID {set_id}")

    def next(self) -> Optional[DataRecord]:
        while not self.closed:
            if len(self.records) > 0:
                rec = self.records.pop(0)
                return rec
            try:
                self.parse_message()
            except Exception as e:
                print("[ERROR]", e)
                self.close()
                return None


@register_processor("ipfix")
class IPFIXProcessor(BaseProcessor):
    """
    IPFIX processor for processing IPFIX data from binary files
    """

    def process(self, event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> ProcessorResult:
        """
        Process an IPFIX event. This processor expects the event to contain either:
        - A 'body' field with raw IPFIX bytes data
        - A 'path' field pointing to an IPFIX file

        Returns a ProcessorResult containing all decoded IPFIX records
        """
        from share import shared_logger

        try:
            # Accept either file_path or bytes in event
            file_path = event.get("path")
            file_bytes = event.get("body")

            if not file_path and not file_bytes:
                shared_logger.warning("IPFIX processor: No file_path or body data provided in event")
                return ProcessorResult()

            reader = IPFIXReader(file_path=file_path, file_bytes=file_bytes)
            processed_records = []

            record_count = 0
            while True:
                record: DataRecord | None = reader.next()
                if record is None:
                    break

                record_data = record.get_data()

                # Enhance the record with additional metadata
                if context:
                    if "input_id" in context:
                        record_data["input_id"] = context["input_id"]
                    if "input_type" in context:
                        record_data["input_type"] = context["input_type"]

                # Add processor metadata
                record_data["processor"] = {
                    "type": "ipfix",
                    "processed_at": record_data.get("@timestamp")
                }

                processed_records.append(record_data)
                record_count += 1

                # Limit the number of records processed in a single batch to avoid memory issues
                if record_count >= 10000:
                    shared_logger.warning(
                        "IPFIX processor: Reached maximum record limit per batch",
                        extra={"record_count": record_count}
                    )
                    break

            reader.close()

            if processed_records:
                shared_logger.info(
                    "IPFIX processor: Successfully processed records",
                    extra={"record_count": len(processed_records)}
                )
                return ProcessorResult(processed_records)
            else:
                shared_logger.info("IPFIX processor: No records found in IPFIX data")
                return ProcessorResult()

        except Exception as e:
            shared_logger.error(
                "IPFIX processor: Error processing IPFIX data",
                extra={"error": str(e), "event_keys": list(event.keys()) if event else []}
            )
            return ProcessorResult()
