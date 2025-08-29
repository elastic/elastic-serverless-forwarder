
import base64
import functools
import hashlib
from ipaddress import IPv4Address, IPv6Address, ip_network, ip_address
import socket
import struct
from typing import Any, Dict, List, Union

import xxhash


def convert_networks(networks: List[str]) -> List:
    result = []

    for net in networks:
        result.append(ip_network(net, strict=False))
    return result


def format_mac_address(mac: str) -> str:
    """Format MAC address to ECS standard (XX-XX-XX-XX-XX-XX)."""
    # Remove any existing separators and convert to uppercase
    mac_clean = mac.replace(":", "").replace("-", "").replace(".", "").upper()

    # Add dashes every 2 characters
    if len(mac_clean) == 12:
        return "-".join(mac_clean[i:i+2] for i in range(0, 12, 2))

    return mac


@functools.lru_cache(maxsize=50)
def calculate_flow_id(
        source_ip: str, dest_ip: str, source_port: int,
        dest_port: int, protocol: int
) -> str:
    """Calculate flow ID using xxhash (bidirectional)."""
    try:
        # Normalize to ensure bidirectional flows have same ID
        if source_port >= dest_port:
            ip1, port1, ip2, port2 = source_ip, source_port, dest_ip, dest_port
        else:
            ip1, port1, ip2, port2 = dest_ip, dest_port, source_ip, source_port

        # Create hash input
        h = xxhash.xxh64()
        h.update(socket.inet_pton(
            socket.AF_INET if '.' in ip1 else socket.AF_INET6, ip1))
        h.update(struct.pack('>H', port1))
        h.update(socket.inet_pton(
            socket.AF_INET if '.' in ip2 else socket.AF_INET6, ip2))
        h.update(struct.pack('>H', port2))
        h.update(struct.pack('B', protocol))

        return base64.urlsafe_b64encode(h.digest()).decode().rstrip('=')
    except Exception:
        return ""


@functools.lru_cache(maxsize=50)
def calculate_community_id(
        source_ip: str, dest_ip: str, source_port: int,
        dest_port: int, protocol: int
) -> str:
    """Calculate Community ID v1 hash.

    Mirrors the reference algorithm (see Elastic / community-id spec):
      * 2-byte big-endian seed (0 by default)
      * raw source IP bytes + raw destination IP bytes
      * protocol byte + zero pad byte
      * for TCP/UDP/SCTP/ICMP(v4/v6) append two 16-bit values:
          - TCP/UDP/SCTP: source & dest ports
          - ICMP(v4/v6): (type, equivType) if bidirectional pair known
                         else (type, code)
      * bidirectional flows are canonically ordered by (IP, port)
    """

    IPPROTO_ICMP = 1
    IPPROTO_TCP = 6
    IPPROTO_UDP = 17
    IPPROTO_ICMPv6 = 58
    IPPROTO_SCTP = 132

    icmp_v4_equiv = {
        8: 0, 0: 8,          # echo
        13: 14, 14: 13,      # timestamp
        15: 16, 16: 15,      # information
        10: 9, 9: 10,        # router solicitation/advertisement
        17: 18, 18: 17,      # address mask
    }
    icmp_v6_equiv = {
        128: 129, 129: 128,  # echo
        133: 134, 134: 133,  # router solicitation/advertisement
        135: 136, 136: 135,  # neighbor solicitation/advertisement
        130: 131, 131: 130,  # listener query/report (approx mapping)
    }

    try:
        sp = int(source_port) if source_port is not None else 0
        dp = int(dest_port) if dest_port is not None else 0
        proto = int(protocol) if protocol is not None else 0

        is_one_way = False
        if proto in (IPPROTO_ICMP, IPPROTO_ICMPv6):
            table = icmp_v4_equiv if proto == IPPROTO_ICMP else icmp_v6_equiv
            icmp_type = sp & 0xFF
            icmp_code = dp & 0xFF
            if icmp_type in table:
                sp = icmp_type
                dp = table[icmp_type]
            else:
                sp = icmp_type
                dp = icmp_code
                is_one_way = True

        src_ip_bytes = get_ip_object(source_ip).packed
        dst_ip_bytes = get_ip_object(dest_ip).packed

        if not is_one_way:
            swap = False
            if src_ip_bytes > dst_ip_bytes:
                swap = True
            elif src_ip_bytes == dst_ip_bytes and sp > dp:
                swap = True
            if swap:
                source_ip, dest_ip = dest_ip, source_ip
                src_ip_bytes, dst_ip_bytes = dst_ip_bytes, src_ip_bytes
                sp, dp = dp, sp

        hasher = hashlib.sha1()
        hasher.update(struct.pack('>H', 0))          # seed
        hasher.update(src_ip_bytes)
        hasher.update(dst_ip_bytes)
        hasher.update(bytes([proto & 0xFF, 0]))      # protocol + pad

        if proto in (IPPROTO_TCP, IPPROTO_UDP, IPPROTO_SCTP,
                     IPPROTO_ICMP, IPPROTO_ICMPv6):
            hasher.update(struct.pack('>H', sp & 0xFFFF))
            hasher.update(struct.pack('>H', dp & 0xFFFF))

        return '1:' + base64.b64encode(hasher.digest()).decode('ascii')
    except Exception:
        return ''


@functools.cache
def get_ip_object(ip: str) -> Union[IPv4Address, IPv6Address]:
    return ip_address(ip)


@functools.lru_cache(maxsize=300)
def ip_is_local_internal(ip_obj) -> bool:
    return ip_obj.is_loopback or ip_obj.is_link_local or ip_obj.is_unspecified


def get_ip_locality(ip: str, internal_networks: List) -> str:
    """Determine if an IP is internal or external."""
    try:

        ip_obj = get_ip_object(ip)

        # Check for local/loopback addresses
        if ip_is_local_internal(ip_obj):
            return "internal"

        for i in internal_networks:
            if ip_obj in i:
                return "internal"

    except Exception:
        pass

    return "external"


def get_ip_locality_combined(
        source_ip: str, dest_ip: str, internal_networks: List
) -> str:
    """Determine flow locality based on both source and destination."""
    source_locality = get_ip_locality(source_ip, internal_networks)
    dest_locality = get_ip_locality(dest_ip, internal_networks)

    if source_locality == "external" or dest_locality == "external":
        return "external"
    return "internal"


def get_protocol_name(protocol_num: int) -> str:
    """Convert protocol number to name."""
    match protocol_num:
        case 1:
            return "icmp"
        case 6:
            return "tcp"
        case 17:
            return "udp"
        case 58:
            return "ipv6-icmp"

    return f"unknown ({protocol_num})"


def extract_ip_from_address(address: str) -> str:
    """Extract IP from IP:port format."""
    if ':' in address:
        if address.startswith('['):
            # IPv6 format [::1]:port
            return address[1:address.rfind(']')]
        else:
            # IPv4 format 1.2.3.4:port
            return address[:address.rfind(':')]
    return address


def cleanup_empty_fields(data: Dict[str, Any]) -> None:
    """Remove empty dictionaries and lists."""
    keys_to_remove = []
    for key, value in data.items():
        if isinstance(value, dict):
            cleanup_empty_fields(value)
            if not value:
                keys_to_remove.append(key)
        elif isinstance(value, list) and not value:
            keys_to_remove.append(key)

    for key in keys_to_remove:
        del data[key]
