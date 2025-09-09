from copy import deepcopy
from datetime import datetime, timezone, timedelta
import json
from typing import Dict, Any, List, Optional

from .processor import BaseProcessor, ProcessorResult
from .registry import register_processor
import share.ecs_helper as ecs_helper
from share.logger import logger as shared_logger
import processors.ie as ie
import re

SNAKE_CASE_PATTERN_1 = re.compile('(.)([A-Z][a-z]+)')
SNAKE_CASE_PATTERN_2 = re.compile('([a-z0-9])([A-Z])')


def snakify(string) -> str:
    intermediary = SNAKE_CASE_PATTERN_1.sub(r'\1_\2', string)
    snaked = SNAKE_CASE_PATTERN_2.sub(r'\1_\2', intermediary).lower()
    return snaked


def convert_to_snake_case(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert camelCase keys to snake_case."""
    result = {}
    for key, value in data.items():
        snake_key = RFC_5102_INFO_ELEMENT_SNAKE_CASE.get(key)
        if snake_key is None:
            snake_key = snakify(key)
        result[snake_key] = value
    return result


RFC_5102_INFO_ELEMENT_SNAKE_CASE = {v[0]: snakify(
    v[0]) for k, v in ie.RFC_5102_INFO_ELEMENT.items()}


def export_to_ecs(netflow_packet: Dict[str, Any],
                  exporter_address: Optional[str] = None,
                  internal_networks: List = [],
                  flow_timestamp: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Convert a NetFlow/IPFIX packet to ECS (Elastic Common Schema) format.

    Args:
        netflow_packet: Dictionary containing NetFlow/IPFIX fields
        exporter_address: IP address of the NetFlow exporter
        internal_networks: List of internal network CIDRs for locality
        flow_timestamp: Timestamp of the flow (defaults to current time)

    Returns:
        Dictionary in ECS format
    """
    if internal_networks == []:
        internal_networks = ecs_helper.convert_networks(
            ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"])

    if flow_timestamp is None:
        flow_timestamp = datetime.now(timezone.utc)

    ts = netflow_packet.get("header", {}).get("export_time")
    if ts is not None:
        dt = datetime.fromtimestamp(ts, timezone.utc)
        ts = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    else:
        ts = 'NO TIMESTAMP'

    # Initialize ECS structure
    ecs_event = {
        "event": {
            "created": datetime.now(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "kind": "event",
            "category": ["network"],
            "action": "netflow_flow",
            "type": ["connection"]
        },
        "observer": {},
        "source": {},
        "destination": {},
        "network": {},
        "flow": {},
        "related": {"ip": []},
        "netflow": {},
        "@timestamp": ts
    }

    # Add observer information
    if exporter_address:
        observer_ip = ecs_helper.extract_ip_from_address(exporter_address)
        ecs_event["observer"]["ip"] = [observer_ip]
        ecs_event["related"]["ip"].append(observer_ip)

    # Process timing information
    process_timing_fields(netflow_packet, ecs_event, flow_timestamp)

    # Process source fields
    process_source_fields(netflow_packet, ecs_event, internal_networks)

    # Process destination fields
    process_destination_fields(netflow_packet, ecs_event, internal_networks)

    # Process network fields
    process_network_fields(netflow_packet, ecs_event)

    # Process flow fields
    process_flow_fields(netflow_packet, ecs_event, internal_networks)

    # Handle biflow direction (client/server assignment)
    process_biflow_direction(netflow_packet, ecs_event)

    # Process WLAN fields if present
    process_wlan_fields(netflow_packet, ecs_event, internal_networks)

    # Convert all NetFlow fields to snake_case and add to netflow namespace
    ecs_event["netflow"] = convert_to_snake_case(netflow_packet)
    ecs_event["netflow"]["type"] = "netflow_flow"

    header = ecs_event["netflow"].get("header", {})

    # Add exporter info to netflow namespace
    if exporter_address:
        ecs_event["netflow"]["exporter"] = {
            "address": exporter_address,
            "source_id": header.get("observation_domain_id", -1),
            "timestamp": ts,
            "version": header.get("version", -1),
        }

    # Process additional flags
    process_additional_flags(netflow_packet, ecs_event, internal_networks)

    # arbitrary field rename
    ecs_event["netflow"]["source_ipv4_address"] = ecs_event["netflow"].pop(
        "source_i_pv4_address", None)
    ecs_event["netflow"]["destination_ipv4_address"] = \
        ecs_event["netflow"].pop("destination_i_pv4_address", None)

    # Only delete header if it exists
    if 'header' in ecs_event["netflow"]:
        del ecs_event["netflow"]['header']

    # Clean up empty fields
    ecs_helper.cleanup_empty_fields(ecs_event)

    return ecs_event


def process_additional_flags(
        packet: Dict[str, Any], ecs_event: Dict[str, Any],
        internal_networks: List[str]
) -> None:
    """Process additional NetFlow fields."""
    # TCP Control Bits
    tcp_ctrl_bits = 0
    try:
        if isinstance(tcp_ctrl_bits, str):
            tcp_ctrl_bits = int(tcp_ctrl_bits, 16)
    except (ValueError, TypeError):
        shared_logger.warning(
            f"Invalid tcpControlBits value: {tcp_ctrl_bits}, skipping TCP flags processing")

    if tcp_ctrl_bits:
        tcp_flags = []
        if tcp_ctrl_bits & 0x20:
            tcp_flags.append("URG")
        if tcp_ctrl_bits & 0x10:
            tcp_flags.append("ACK")
        if tcp_ctrl_bits & 0x08:
            tcp_flags.append("PSH")
        if tcp_ctrl_bits & 0x04:
            tcp_flags.append("RST")
        if tcp_ctrl_bits & 0x02:
            tcp_flags.append("SYN")
        if tcp_ctrl_bits & 0x01:
            tcp_flags.append("FIN")

        ecs_event["netflow"]["tcp_flags"] = tcp_flags

    # Process related IPs
    related_ips = ecs_event["related"]["ip"]
    related_ips = list(set(related_ips))
    related_ips.sort()
    ecs_event["related"]["ip"] = related_ips


def process_timing_fields(
        packet: Dict[str, Any], ecs_event: Dict[str, Any],
        flow_timestamp: datetime
) -> None:
    """Process flow timing fields."""
    sys_uptime = packet.get("uptimeMillis") or packet.get(
        "systemInitTimeMilliseconds")
    start_uptime = packet.get("flowStartSysUpTime")
    end_uptime = packet.get("flowEndSysUpTime")

    if sys_uptime and start_uptime and start_uptime <= sys_uptime:
        start_offset_ms = start_uptime - sys_uptime
        ecs_event["event"]["start"] = (
            flow_timestamp +
            timedelta(milliseconds=start_offset_ms)
        ).isoformat()

    if sys_uptime and end_uptime and end_uptime <= sys_uptime:
        end_offset_ms = end_uptime - sys_uptime
        ecs_event["event"]["end"] = (
            flow_timestamp +
            timedelta(milliseconds=end_offset_ms)
        ).isoformat()

    # Calculate duration
    if "start" in ecs_event["event"] and "end" in ecs_event["event"]:
        start_time = datetime.fromisoformat(
            ecs_event["event"]["start"].replace('Z', '+00:00'))
        end_time = datetime.fromisoformat(
            ecs_event["event"]["end"].replace('Z', '+00:00'))
        duration_ns = int(
            (end_time - start_time).total_seconds() * 1_000_000_000)
        ecs_event["event"]["duration"] = duration_ns
    elif packet.get("flowDurationMilliseconds"):
        duration_ns = packet["flowDurationMilliseconds"] * 1_000_000
        ecs_event["event"]["duration"] = duration_ns


def process_source_fields(
        packet: Dict[str, Any], ecs_event: Dict[str, Any],
        internal_networks: List
) -> None:
    """Process source IP, port, MAC, and bytes/packets."""
    source_ip = packet.get("sourceIPv4Address") or packet.get(
        "sourceIPv6Address")
    if source_ip:
        ecs_event["source"]["ip"] = source_ip
        ecs_event["related"]["ip"].append(source_ip)
        ecs_event["source"]["locality"] = ecs_helper.get_ip_locality(
            source_ip, internal_networks)

    if packet.get("sourceTransportPort"):
        ecs_event["source"]["port"] = packet["sourceTransportPort"]

    if packet.get("sourceMacAddress"):
        ecs_event["source"]["mac"] = packet["sourceMacAddress"]

    # Source bytes and packets
    source_bytes = (packet.get("octetDeltaCount") or
                    packet.get("octetTotalCount") or
                    packet.get("initiatorOctets"))
    if source_bytes:
        # Handle hex string format
        if isinstance(source_bytes, str):
            source_bytes = int(source_bytes, 16)
        ecs_event["source"]["bytes"] = source_bytes

    source_packets = (packet.get("packetDeltaCount") or
                      packet.get("packetTotalCount") or
                      packet.get("initiatorPackets"))
    if source_packets:
        if isinstance(source_packets, str):
            source_packets = int(source_packets, 16)
        ecs_event["source"]["packets"] = source_packets


def process_destination_fields(
        packet: Dict[str, Any], ecs_event: Dict[str, Any],
        internal_networks: List
) -> None:
    """Process destination IP, port, MAC, and bytes/packets."""
    dest_ip = packet.get("destinationIPv4Address") or packet.get(
        "destinationIPv6Address")
    if dest_ip:
        ecs_event["destination"]["ip"] = dest_ip
        ecs_event["related"]["ip"].append(dest_ip)
        ecs_event["destination"]["locality"] = ecs_helper.get_ip_locality(
            dest_ip, internal_networks)

    if packet.get("destinationTransportPort"):
        ecs_event["destination"]["port"] = packet["destinationTransportPort"]

    if packet.get("destinationMacAddress"):
        ecs_event["destination"]["mac"] = packet["destinationMacAddress"]

    # Destination bytes and packets (reverse counters)
    dest_bytes = (packet.get("reverseOctetDeltaCount") or
                  packet.get("reverseOctetTotalCount") or
                  packet.get("responderOctets"))
    if dest_bytes:
        if isinstance(dest_bytes, str):
            dest_bytes = int(dest_bytes, 16)
        ecs_event["destination"]["bytes"] = dest_bytes

    dest_packets = (packet.get("reversePacketDeltaCount") or
                    packet.get("reversePacketTotalCount") or
                    packet.get("responderPackets"))
    if dest_packets:
        if isinstance(dest_packets, str):
            dest_packets = int(dest_packets, 16)
        ecs_event["destination"]["packets"] = dest_packets


def process_network_fields(
    packet: Dict[str, Any], ecs_event: Dict[str, Any]
) -> None:
    """Process network-level fields."""
    protocol = packet.get("protocolIdentifier")
    ip_version = packet.get("ipVersion")

    if protocol is not None:
        ecs_event["network"]["iana_number"] = str(protocol)
        ecs_event["network"]["transport"] = ecs_helper.get_protocol_name(protocol)

    # Network direction
    flow_direction = packet.get("flowDirection")
    if flow_direction is not None:
        ecs_event["network"]["direction"] = "inbound" if flow_direction == 0 \
            else "outbound"
    else:
        ecs_event["network"]["direction"] = "unknown"

    # Network totals
    source_bytes = ecs_event.get("source", {}).get("bytes", 0)
    dest_bytes = ecs_event.get("destination", {}).get("bytes", 0)
    if source_bytes or dest_bytes:
        ecs_event["network"]["bytes"] = source_bytes + dest_bytes

    source_packets = ecs_event.get("source", {}).get("packets", 0)
    dest_packets = ecs_event.get("destination", {}).get("packets", 0)
    if source_packets or dest_packets:
        ecs_event["network"]["packets"] = source_packets + dest_packets

    if ip_version and ip_version in [4, 6]:
        ecs_event["network"]["type"] = f'ipv{ip_version}'

    # WLAN SSID
    if packet.get("wlanSSID"):
        ecs_event["network"]["name"] = packet["wlanSSID"]

    # Community ID
    source_ip = ecs_event.get("source", {}).get("ip")
    dest_ip = ecs_event.get("destination", {}).get("ip")
    source_port = ecs_event.get("source", {}).get("port", 0)
    dest_port = ecs_event.get("destination", {}).get("port", 0)

    if source_ip and dest_ip and protocol is not None:
        community_id = ecs_helper.calculate_community_id(
            source_ip, dest_ip, source_port, dest_port, protocol)
        if community_id:
            ecs_event["network"]["community_id"] = community_id


def process_flow_fields(
        packet: Dict[str, Any], ecs_event: Dict[str, Any],
        internal_networks: List
) -> None:
    """Process flow-level fields."""
    source_ip = ecs_event.get("source", {}).get("ip")
    dest_ip = ecs_event.get("destination", {}).get("ip")
    source_port = ecs_event.get("source", {}).get("port", 0)
    dest_port = ecs_event.get("destination", {}).get("port", 0)
    protocol = packet.get("protocolIdentifier", 0)

    if source_ip and dest_ip:
        ecs_event["flow"]["id"] = ecs_helper.calculate_flow_id(
            source_ip, dest_ip, source_port, dest_port, protocol)
        ecs_event["flow"]["locality"] = ecs_helper.get_ip_locality_combined(
            source_ip, dest_ip, internal_networks)


def process_biflow_direction(
    packet: Dict[str, Any], ecs_event: Dict[str, Any]
) -> None:
    """Handle biflow direction and client/server assignment."""
    biflow_direction = packet.get("biflowDirection")
    if biflow_direction is not None and \
            ecs_event.get("source") and ecs_event.get("destination"):
        if biflow_direction == 2:  # reverseInitiator
            # Swap source and destination
            ecs_event["source"] = ecs_event["destination"]
            ecs_event["destination"] = ecs_event["source"]

        ecs_event["event"]["category"] = ["network", "session"]
        ecs_event["client"] = ecs_event["source"].copy()
        ecs_event["server"] = ecs_event["destination"].copy()


def process_wlan_fields(
        packet: Dict[str, Any], ecs_event: Dict[str, Any],
        internal_networks: List
) -> None:
    """Process WLAN-specific fields."""
    flow_direction = packet.get("flowDirection")
    sta_ip = packet.get("staIPv4Address")
    sta_mac = packet.get("staMacAddress")
    wtp_mac = packet.get("wtpMacAddress")

    if flow_direction is not None and sta_mac and wtp_mac:
        if flow_direction == 1:  # outbound
            # Swap for outbound traffic
            if sta_ip:
                ecs_event["destination"]["ip"] = sta_ip
                ecs_event["destination"]["locality"] = ecs_helper.get_ip_locality(
                    sta_ip, internal_networks)
                ecs_event["related"]["ip"].append(sta_ip)
            ecs_event["destination"]["mac"] = sta_mac
            ecs_event["source"]["mac"] = wtp_mac
        else:  # inbound
            if sta_ip:
                ecs_event["source"]["ip"] = sta_ip
                ecs_event["source"]["locality"] = ecs_helper.get_ip_locality(
                    sta_ip, internal_networks)
                ecs_event["related"]["ip"].append(sta_ip)
            ecs_event["source"]["mac"] = sta_mac
            ecs_event["destination"]["mac"] = wtp_mac


@register_processor("ipfix_ecs")
class ECSProcessor(BaseProcessor):
    """
    Processor to convert NetFlow/IPFIX data to ECS (Elastic Common Schema) format.
    """

    def process(self, event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> ProcessorResult:
        """
        Process NetFlow/IPFIX event data and convert to ECS format.

        Args:
            event: The event to process, should contain NetFlow/IPFIX fields
            context: Optional context information (exporter_address, etc.)

        Returns:
            ProcessorResult containing the ECS-formatted event
        """
        if context is None:
            context = {}

        # Extract context information
        # TODO: Make exporter_address, internal_networks configurable
        exporter_address = context.get("exporter_address", "10.0.0.1")
        internal_networks = context.get("internal_networks", [])
        flow_timestamp = event.get("flow_timestamp")

        # The event should contain the netflow/ipfix data
        message = event.get("fields", {}).get("message", {})

        netflow_packet = {}
        try:
            netflow_packet = json.loads(message) if isinstance(message, str) else message
            shared_logger.info(
                "Successfully parsed binary processor output as JSON",
                extra={
                    "raw_message": message,
                }
            )
        except json.JSONDecodeError as e:
            shared_logger.error(
                "Failed to parse binary processor output as JSON",
                extra={
                    "error": str(e),
                    "raw_message": message,
                }
            )
        try:
            # Convert to ECS format
            ecs_event = export_to_ecs(
                netflow_packet=netflow_packet,
                exporter_address=exporter_address,
                internal_networks=internal_networks,
                flow_timestamp=flow_timestamp
            )

            event["fields"]["message"] = json.dumps(ecs_event)
            shared_logger.info(
                "Successfully converted NetFlow/IPFIX data to ECS format",
                extra={
                    "ecs_event": ecs_event,
                    "raw_event": event,
                }
            )

            return ProcessorResult(event)

        except Exception as e:
            # Log error and return empty result
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error converting to ECS format: {str(e)}")
            return ProcessorResult()
