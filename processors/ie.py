import functools
import struct
from typing import Any
import ipaddress

RFC_5102_INFO_ELEMENT = {
    1: ("octetDeltaCount", "unsigned64"),
    2: ("packetDeltaCount", "unsigned64"),
    4: ("protocolIdentifier", "unsigned8"),
    5: ("ipClassOfService", "unsigned8"),
    6: ("tcpControlBits", "unsigned8"),
    7: ("sourceTransportPort", "unsigned16"),
    8: ("sourceIPv4Address", "ipv4Address"),
    9: ("sourceIPv4PrefixLength", "unsigned8"),
    10: ("ingressInterface", "unsigned32"),
    11: ("destinationTransportPort", "unsigned16"),
    12: ("destinationIPv4Address", "ipv4Address"),
    13: ("destinationIPv4PrefixLength", "unsigned8"),
    14: ("egressInterface", "unsigned32"),
    15: ("ipNextHopIPv4Address", "ipv4Address"),
    16: ("bgpSourceAsNumber", "unsigned32"),
    17: ("bgpDestinationAsNumber", "unsigned32"),
    18: ("bgpNextHopIPv4Address", "ipv4Address"),
    19: ("postMCastPacketDeltaCount", "unsigned64"),
    20: ("postMCastOctetDeltaCount", "unsigned64"),
    21: ("flowEndSysUpTime", "unsigned32"),
    22: ("flowStartSysUpTime", "unsigned32"),
    23: ("postOctetDeltaCount", "unsigned64"),
    24: ("postPacketDeltaCount", "unsigned64"),
    25: ("minimumIpTotalLength", "unsigned64"),
    26: ("maximumIpTotalLength", "unsigned64"),
    27: ("sourceIPv6Address", "ipv6Address"),
    28: ("destinationIPv6Address", "ipv6Address"),
    29: ("sourceIPv6PrefixLength", "unsigned8"),
    30: ("destinationIPv6PrefixLength", "unsigned8"),
    31: ("flowLabelIPv6", "unsigned32"),
    32: ("icmpTypeCodeIPv4", "unsigned16"),
    33: ("igmpType", "unsigned8"),
    36: ("flowActiveTimeout", "unsigned16"),
    37: ("flowIdleTimeout", "unsigned16"),
    40: ("exportedOctetTotalCount", "unsigned64"),
    41: ("exportedMessageTotalCount", "unsigned64"),
    42: ("exportedFlowRecordTotalCount", "unsigned64"),
    44: ("sourceIPv4Prefix", "ipv4Address"),
    45: ("destinationIPv4Prefix", "ipv4Address"),
    46: ("mplsTopLabelType", "unsigned8"),
    47: ("mplsTopLabelIPv4Address", "ipv4Address"),
    52: ("minimumTTL", "unsigned8"),
    53: ("maximumTTL", "unsigned8"),
    54: ("fragmentIdentification", "unsigned32"),
    55: ("postIpClassOfService", "unsigned8"),
    56: ("sourceMacAddress", "macAddress"),
    57: ("postDestinationMacAddress", "macAddress"),
    58: ("vlanId", "unsigned16"),
    59: ("postVlanId", "unsigned16"),
    60: ("ipVersion", "unsigned8"),
    61: ("flowDirection", "unsigned8"),
    62: ("ipNextHopIPv6Address", "ipv6Address"),
    63: ("bgpNextHopIPv6Address", "ipv6Address"),
    64: ("ipv6ExtensionHeaders", "unsigned32"),
    70: ("mplsTopLabelStackSection", "octetArray"),
    71: ("mplsLabelStackSection2", "octetArray"),
    72: ("mplsLabelStackSection3", "octetArray"),
    73: ("mplsLabelStackSection4", "octetArray"),
    74: ("mplsLabelStackSection5", "octetArray"),
    75: ("mplsLabelStackSection6", "octetArray"),
    76: ("mplsLabelStackSection7", "octetArray"),
    77: ("mplsLabelStackSection8", "octetArray"),
    78: ("mplsLabelStackSection9", "octetArray"),
    79: ("mplsLabelStackSection10", "octetArray"),
    80: ("destinationMacAddress", "macAddress"),
    81: ("postSourceMacAddress", "macAddress"),
    85: ("octetTotalCount", "unsigned64"),
    86: ("packetTotalCount", "unsigned64"),
    88: ("fragmentOffset", "unsigned16"),
    90: ("mplsVpnRouteDistinguisher", "octetArray"),
    128: ("bgpNextAdjacentAsNumber", "unsigned32"),
    129: ("bgpPrevAdjacentAsNumber", "unsigned32"),
    130: ("exporterIPv4Address", "ipv4Address"),
    131: ("exporterIPv6Address", "ipv6Address"),
    132: ("droppedOctetDeltaCount", "unsigned64"),
    133: ("droppedPacketDeltaCount", "unsigned64"),
    134: ("droppedOctetTotalCount", "unsigned64"),
    135: ("droppedPacketTotalCount", "unsigned64"),
    136: ("flowEndReason", "unsigned8"),
    137: ("commonPropertiesId", "unsigned64"),
    138: ("observationPointId", "unsigned64"),
    139: ("icmpTypeCodeIPv6", "unsigned16"),
    140: ("mplsTopLabelIPv6Address", "ipv6Address"),
    141: ("lineCardId", "unsigned32"),
    142: ("portId", "unsigned32"),
    143: ("meteringProcessId", "unsigned32"),
    144: ("exportingProcessId", "unsigned32"),
    145: ("templateId", "unsigned16"),
    146: ("wlanChannelId", "unsigned8"),
    147: ("wlanSSID", "string"),
    148: ("flowId", "unsigned64"),
    149: ("observationDomainId", "unsigned32"),
    150: ("flowStartSeconds", "dateTimeSeconds"),
    151: ("flowEndSeconds", "dateTimeSeconds"),
    152: ("flowStartMilliseconds", "dateTimeMilliseconds"),
    153: ("flowEndMilliseconds", "dateTimeMilliseconds"),
    154: ("flowStartMicroseconds", "dateTimeMicroseconds"),
    155: ("flowEndMicroseconds", "dateTimeMicroseconds"),
    156: ("flowStartNanoseconds", "dateTimeNanoseconds"),
    157: ("flowEndNanoseconds", "dateTimeNanoseconds"),
    158: ("flowStartDeltaMicroseconds", "unsigned64"),
    159: ("flowEndDeltaMicroseconds", "unsigned64"),
    160: ("systemInitTimeMilliseconds", "dateTimeMilliseconds"),
    161: ("flowDurationMilliseconds", "unsigned32"),
    162: ("flowDurationMicroseconds", "unsigned32"),
    163: ("observedFlowTotalCount", "unsigned64"),
    164: ("ignoredPacketTotalCount", "unsigned64"),
    165: ("ignoredOctetTotalCount", "unsigned64"),
    166: ("notSentFlowTotalCount", "unsigned64"),
    167: ("notSentPacketTotalCount", "unsigned64"),
    168: ("notSentOctetTotalCount", "unsigned64"),
    169: ("destinationIPv6Prefix", "ipv6Address"),
    170: ("sourceIPv6Prefix", "ipv6Address"),
    171: ("postOctetTotalCount", "unsigned64"),
    172: ("postPacketTotalCount", "unsigned64"),
    173: ("flowKeyIndicator", "unsigned64"),
    174: ("postMCastPacketTotalCount", "unsigned64"),
    175: ("postMCastOctetTotalCount", "unsigned64"),
    176: ("icmpTypeIPv4", "unsigned8"),
    177: ("icmpCodeIPv4", "unsigned8"),
    178: ("icmpTypeIPv6", "unsigned8"),
    179: ("icmpCodeIPv6", "unsigned8"),
    180: ("udpSourcePort", "unsigned16"),
    181: ("udpDestinationPort", "unsigned16"),
    182: ("tcpSourcePort", "unsigned16"),
    183: ("tcpDestinationPort", "unsigned16"),
    184: ("tcpSequenceNumber", "unsigned32"),
    185: ("tcpAcknowledgementNumber", "unsigned32"),
    186: ("tcpWindowSize", "unsigned16"),
    187: ("tcpUrgentPointer", "unsigned16"),
    188: ("tcpHeaderLength", "unsigned8"),
    189: ("ipHeaderLength", "unsigned8"),
    190: ("totalLengthIPv4", "unsigned16"),
    191: ("payloadLengthIPv6", "unsigned16"),
    192: ("ipTTL", "unsigned8"),
    193: ("nextHeaderIPv6", "unsigned8"),
    194: ("mplsPayloadLength", "unsigned32"),
    195: ("ipDiffServCodePoint", "unsigned8"),
    196: ("ipPrecedence", "unsigned8"),
    197: ("fragmentFlags", "unsigned8"),
    198: ("octetDeltaSumOfSquares", "unsigned64"),
    199: ("octetTotalSumOfSquares", "unsigned64"),
    200: ("mplsTopLabelTTL", "unsigned8"),
    201: ("mplsLabelStackLength", "unsigned32"),
    202: ("mplsLabelStackDepth", "unsigned32"),
    203: ("mplsTopLabelExp", "unsigned8"),
    204: ("ipPayloadLength", "unsigned32"),
    205: ("udpMessageLength", "unsigned16"),
    206: ("isMulticast", "unsigned8"),
    207: ("ipv4IHL", "unsigned8"),
    208: ("ipv4Options", "octetArray"),
    209: ("tcpOptions", "octetArray"),
    210: ("paddingOctets", "octetArray"),
    211: ("collectorIPv4Address", "ipv4Address"),
    212: ("collectorIPv6Address", "ipv6Address"),
    213: ("exportInterface", "unsigned32"),
    214: ("exportProtocolVersion", "unsigned8"),
    215: ("exportTransportProtocol", "unsigned8"),
    216: ("collectorTransportPort", "unsigned16"),
    217: ("exporterTransportPort", "unsigned16"),
    218: ("tcpSynTotalCount", "unsigned64"),
    219: ("tcpFinTotalCount", "unsigned64"),
    220: ("tcpRstTotalCount", "unsigned64"),
    221: ("tcpPshTotalCount", "unsigned64"),
    222: ("tcpAckTotalCount", "unsigned64"),
    223: ("tcpUrgTotalCount", "unsigned64"),
    224: ("ipTotalLength", "unsigned64"),
    237: ("postMplsTopLabelExp", "unsigned8"),
    238: ("tcpWindowScale", "unsigned8")
}


@functools.lru_cache(maxsize=500)
def convert(raw_bytes: bytes, field_type: str) -> Any:
    """Convert raw IPFIX bytes to a JSON-serializable Python"""
    """type based on field_type."""

    if field_type in ("unsigned8", "unsigned16", "unsigned32", "unsigned64",
                      "signed8", "signed16", "signed32", "signed64",):
        return int.from_bytes(raw_bytes, byteorder='big')
    try:
        match field_type:
            case "float32":
                return struct.unpack("!f", raw_bytes)[0]
            case "float64":
                return struct.unpack("!d", raw_bytes)[0]
            case "boolean":
                return raw_bytes != b'\x00'
            case "macAddress":
                return "-".join(f"{b:02x}" for b in raw_bytes).upper()
            case "ipv4Address":
                return str(ipaddress.IPv4Address(raw_bytes))
            case "ipv6Address":
                return str(ipaddress.IPv6Address(raw_bytes))
            case "string" | "octetArray":
                try:
                    return raw_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    return raw_bytes.hex()
            case "dateTimeSeconds":
                return struct.unpack("!I", raw_bytes)[0]
            case "dateTimeMilliseconds":
                # truncate in app logic if needed
                return struct.unpack("!Q", raw_bytes)[0]
            case "dateTimeMicroseconds":
                return struct.unpack("!Q", raw_bytes)[0]
            case "dateTimeNanoseconds":
                return struct.unpack("!Q", raw_bytes)[0]
            case _:
                # Fallback: hex-encode
                return raw_bytes.hex()
    except struct.error:
        return raw_bytes.hex()
