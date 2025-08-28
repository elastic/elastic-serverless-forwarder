# IPFIX Processor Integration Guide

## Overview

The IPFIX processor has been successfully implemented in the Elastic Serverless Forwarder. This processor can read S3 notifications, download and inflate gzip IPFIX files, decode the binary IPFIX content, and send the parsed JSON data to configured outputs.

## Configuration

### Basic Configuration

To use the IPFIX processor, configure your `config.yml` as follows:

```yaml
inputs:
  - type: s3-sqs
    id: arn:aws:sqs:eu-central-1:123456789:ipfix-s3-sqs-queue
    json_content_type: disabled  # Required for binary files
    binary_processor_type: ipfix  # Enable IPFIX binary processing
    processors:
      - type: ipfix
    outputs:
      - type: elasticsearch
        args:
          elasticsearch_url: https://your-cluster.es.amazonaws.com
          api_key: your_api_key
          es_datastream_name: logs-ipfix-default
```

### Configuration Parameters

- **`json_content_type: disabled`**: Required for binary files to prevent text processing
- **`binary_processor_type: ipfix`**: Tells the S3 storage to use IPFIX binary processing
- **`processors.type: ipfix`**: Specifies the IPFIX processor for the processing chain

## Features

### Supported IPFIX Features

1. **IPFIX Message Header Parsing**: Extracts version, length, export time, sequence number, and observation domain ID
2. **Template Set Processing**: Parses template definitions for data field mapping
3. **Data Set Processing**: Converts binary data records to JSON using templates
4. **Information Element Support**: Includes comprehensive RFC 5102 information element definitions
5. **Data Type Conversion**: Converts binary data to appropriate JSON types (integers, IP addresses, MAC addresses, etc.)
6. **Gzip Decompression**: Automatically handles gzipped IPFIX files from S3
7. **Error Handling**: Robust error handling with detailed logging

### Output Format

Each IPFIX flow record is converted to a JSON document with the following structure:

```json
{
  "@timestamp": 1640995200,
  "header": {
    "version": 10,
    "length": 44,
    "export_time": 1640995200,
    "sequence_number": 1,
    "observation_domain_id": 1,
    "start_offset": 0
  },
  "sourceIPv4Address": "192.168.1.1",
  "destinationIPv4Address": "10.0.0.1",
  "processor": {
    "type": "ipfix",
    "processed_at": 1640995200
  },
  "aws": {
    "s3": {
      "bucket": {"name": "bucket-name", "arn": "arn:aws:s3:::bucket-name"},
      "object": {"key": "path/to/file.ipfix.gz"}
    }
  },
  "cloud": {
    "provider": "aws",
    "region": "us-east-1",
    "account": {"id": "123456789"}
  },
  "log": {
    "offset": 0,
    "file": {
      "path": "https://bucket-name.s3.us-east-1.amazonaws.com/path/to/file.ipfix.gz"
    }
  }
}
```

## Data Processing Flow

1. **S3 Notification**: Lambda receives SQS notification of new IPFIX file in S3
2. **File Download**: S3 storage downloads the binary file
3. **Decompression**: If gzipped, the file is automatically decompressed
4. **IPFIX Parsing**: The IPFIX processor:
   - Parses message headers
   - Extracts template definitions
   - Converts data records using templates
   - Maps IPFIX information elements to human-readable names
5. **JSON Conversion**: Each flow record becomes a separate JSON document
6. **Metadata Enhancement**: AWS and file metadata is added to each record
7. **Output**: Records are sent to configured outputs (Elasticsearch, Logstash, etc.)

## Supported Information Elements

The processor includes support for RFC 5102 information elements including:

- **Flow identification**: flowId, templateId, observationDomainId
- **IP addresses**: sourceIPv4Address, destinationIPv4Address, sourceIPv6Address, destinationIPv6Address
- **Ports**: sourceTransportPort, destinationTransportPort, udpSourcePort, tcpSourcePort
- **Timestamps**: flowStartSeconds, flowEndSeconds, flowStartMilliseconds, flowEndMilliseconds
- **Counters**: octetDeltaCount, packetDeltaCount, octetTotalCount, packetTotalCount
- **Protocol info**: protocolIdentifier, ipClassOfService, tcpControlBits
- **Network interfaces**: ingressInterface, egressInterface
- **And many more** (see `processors/ie.py` for complete list)

## Performance Considerations

- **Batch Processing**: Processor limits to 10,000 records per batch to prevent memory issues
- **Compression Support**: Handles gzipped files efficiently
- **Error Recovery**: Continues processing even if individual records fail
- **Lambda Timeouts**: Works with the existing timeout and continuation queue system

## Testing

The implementation includes a test suite that verifies:

- IPFIX file parsing
- Template and data set processing
- JSON conversion
- Error handling

Run tests with:
```bash
python3 test_ipfix_integration.py
```

## Error Handling

The processor includes comprehensive error handling:

- **File Processing Errors**: Logged with file details and continue processing
- **Template Errors**: Missing templates are logged but don't stop processing
- **Data Conversion Errors**: Invalid data is logged and processing continues
- **Memory Limits**: Processing is batched to prevent memory exhaustion

## Logging

All IPFIX processing activities are logged with structured logging including:

- File processing statistics
- Template parsing results
- Data record counts
- Error details with context

## Troubleshooting

### Common Issues

1. **No records processed**: Check that `json_content_type: disabled` is set
2. **Binary processor not used**: Ensure `binary_processor_type: ipfix` is configured
3. **Template errors**: IPFIX files must contain template sets before data sets
4. **Memory issues**: Large files are automatically batched to prevent memory problems

### Debug Logging

Enable debug logging to see detailed processing information:

```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

## Architecture

The IPFIX processor integrates with the existing Elastic Serverless Forwarder architecture:

- **Storage Layer**: S3Storage handles file download and binary processor detection
- **Processor Layer**: IPFIXProcessor handles IPFIX parsing and JSON conversion
- **Handler Layer**: S3 SQS trigger manages the flow and metadata addition
- **Output Layer**: Existing shippers handle delivery to Elasticsearch/Logstash

This ensures IPFIX processing works seamlessly with all existing features like continuation queues, error handling, and multi-output support.
