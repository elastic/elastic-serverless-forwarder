
use serde::{Serialize, Deserialize};
use pyo3::{
    conversion::IntoPyObjectExt,
    exceptions::*,
    prelude::*,
    types::{PyDict, PyAny, PyString, PyInt, PyByteArray},
};

extern crate phf;
use phf::phf_map;

use std::collections::HashMap;

pub fn into_bool(buffer: &[u8]) -> bool {
    buffer[0] != 0u8
}

pub fn into_f32(buffer: &[u8]) -> f32 {
    f32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]])
}

pub fn into_f64(buffer: &[u8]) -> f64 {
    f64::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3],
                        buffer[4], buffer[5], buffer[6], buffer[7]])
}

pub fn into_u8(buffer: &[u8]) -> u8 {
    buffer[0]
}

pub fn into_u16(buffer: &[u8]) -> u16 {
    u16::from_be_bytes([buffer[0], buffer[1]])
}

pub fn into_u32(buffer: &[u8]) -> u32 {
    u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]])
}

pub fn into_u64(buffer: &[u8]) -> u64 {
    u64::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3],
                        buffer[4], buffer[5], buffer[6], buffer[7]])
}

pub fn into_i8(buffer: &[u8]) -> i8 {
    buffer[0] as i8
}

pub fn into_i16(buffer: &[u8]) -> i16 {
    i16::from_be_bytes([buffer[0], buffer[1]])
}

pub fn into_i32(buffer: &[u8]) -> i32 {
    i32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]])
}

pub fn into_i64(buffer: &[u8]) -> i64 {
    i64::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3],
                        buffer[4], buffer[5], buffer[6], buffer[7]])
}

#[derive(Serialize)]
pub enum Value {
    #[serde(untagged)]
    Bool(bool),
    #[serde(untagged)]
    F32(f32),
    #[serde(untagged)]
    F64(f64),
    #[serde(untagged)]
    I8(i8),
    #[serde(untagged)]
    I16(i16),
    #[serde(untagged)]
    I32(i32),
    #[serde(untagged)]
    I64(i64),
    #[serde(untagged)]
    U8(u8),
    #[serde(untagged)]
    U16(u16),
    #[serde(untagged)]
    U32(u32),
    #[serde(untagged)]
    U64(u64),
    #[serde(untagged)]
    Ipv4(String),
    #[serde(untagged)]
    Ipv6(String),
    #[serde(untagged)]
    Mac(String),
    #[serde(untagged)]
    Octets(Vec<u8>),
    #[serde(untagged)]
    StringT(String),
}

pub type ValueMapping = HashMap<String, Value>;

// need to create a From &[u8] to <T> for T in Value

trait ValueDecoder {
    type Output;
    fn ingest(&self, buffer: &[u8]) -> Self::Output;
}

// use phf

impl Value {
    fn get_as_boolean(buffer: &[u8]) -> Self {
        Self::Bool(into_bool(buffer))
    }
    fn get_as_f32(buffer: &[u8]) -> Self {
        Self::F32(into_f32(buffer))
    }
    fn get_as_f64(buffer: &[u8]) -> Self {
        Self::F64(into_f64(buffer))
    }
    fn get_as_i8(buffer: &[u8]) -> Self {
        Self::I8(into_i8(buffer))
    }
    fn get_as_i16(buffer: &[u8]) -> Self {
        Self::I16(into_i16(buffer))
    }
    fn get_as_i32(buffer: &[u8]) -> Self {
        Self::I32(into_i32(buffer))
    }
    fn get_as_i64(buffer: &[u8]) -> Self {
        Self::I64(into_i64(buffer))
    }
    fn get_as_u8(buffer: &[u8]) -> Self {
        Self::U8(buffer[0])
    }
    fn get_as_u16(buffer: &[u8]) -> Self {
        Self::U16(into_u16(buffer))
    }
    fn get_as_u32(buffer: &[u8]) -> Self {
        Self::U32(into_u32(buffer))
    }
    fn get_as_u64(buffer: &[u8]) -> Self {
        Self::U64(into_u64(buffer))
    }
}

#[derive(Clone)]
pub struct Element {
    name: &'static str,
    typeT: &'static str,
}

fn to_mac(buffer: &[u8]) -> String {
    let mut strings = buffer.iter()
        .fold(String::new(), |acc, &n| {
            if acc.len() > 0 {
                format!("{0}:{1:02x}", acc, n)
            } else {
                format!("{:x}", n)
            }
        });

    strings
}

impl Element {
    pub fn Name(&self) -> String {
        self.name.to_string()
    }
    pub fn translate(&self, buffer: &[u8], length: usize) -> Value {

        match self.typeT {
            "boolean" => Value::get_as_boolean(buffer),
            "f32" => Value::get_as_f32(buffer),
            "f64" => Value::get_as_f64(buffer),
            "i8" => Value::get_as_i8(buffer),
            "i16" => Value::get_as_i16(buffer),
            "i32" => Value::get_as_i32(buffer),
            "i64" => Value::get_as_i64(buffer),
            "u8" => Value::get_as_u8(buffer),
            "u16" => Value::get_as_u16(buffer),
            "u32" => Value::get_as_u32(buffer),
            "u64" => Value::get_as_u64(buffer),
            "octetarray" => Value::Octets(buffer[0..length].to_vec()),
            "macAddress" => Value::Mac(to_mac(&buffer[0..length])),
            "ipv4Address" => Value::Ipv4(format!("{:}.{:}.{:}.{:}", buffer[0], buffer[1], buffer[2], buffer[3])),
            "ipv6Address" => Value::Ipv6(format!("{:x}:{:x}:{:x}:{:x}:{:x}:{:x}", buffer[0], buffer[1], buffer[2],
                buffer[3], buffer[4], buffer[5])),
            _ => Value::Octets(buffer[0..length].to_vec()),
        }
    }
}

mod fields {
    use super::Element;
    include!(concat!(env!("OUT_DIR"), "/fields.rs"));
}

pub use fields::*;

/*
impl<'py> IntoPyObject<'py> for Value {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyLookupError;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(
            match self {
                Value::U8(v) => v.into_bound_py_any(py)?,
                _ => PyString::new(py, "unknown"),
            }
        )
    }
}


pub static RFC_5102_INFO_ELEMENT : phf::Map<u32, &Element> = phf_map! {

    1   => &Element{name: "octetDeltaCount", typeT: "u64"},
    2   => &Element{name: "packetDeltaCount", typeT: "u64"},
    3   => &Element{name: "RESERVED_3", typeT: "octetarray"},
    4   => &Element{name: "protocolIdentifier", typeT: "u8"},
    5   => &Element{name: "ipClassOfService", typeT: "u8"},
    6   => &Element{name: "tcpControlBits", typeT: "u8"},
    7   => &Element{name: "sourceTransportPort", typeT: "u16"},
    8   => &Element{name: "sourceIPv4Address", typeT: "ipv4Address"},
    9   => &Element{name: "sourceIPv4PrefixLength", typeT: "u8"},

    10  => &Element{name: "ingressInterface", typeT: "u32"},
    11  => &Element{name: "destinationTransportPort", typeT: "u16"},
    12  => &Element{name: "destinationIPv4Address", typeT: "ipv4Address"},
    13  => &Element{name: "destinationIPv4PrefixLength", typeT: "u8"},
    14  => &Element{name: "egressInterface", typeT: "u32"},
    15  => &Element{name: "ipNextHopIPv4Address", typeT: "ipv4Address"},
    16  => &Element{name: "bgpSourceAsNumber", typeT: "u32"},
    17  => &Element{name: "bgpDestinationAsNumber", typeT: "u32"},
    18  => &Element{name: "bgpNextHopIPv4Address", typeT: "ipv4Address"},
    19  => &Element{name: "postMCastPacketDeltaCount", typeT: "u64"},
    20  => &Element{name: "postMCastOctetDeltaCount", typeT: "u64"},
    21  => &Element{name: "flowEndSysUpTime", typeT: "u32"},
    22  => &Element{name: "flowStartSysUpTime", typeT: "u32"},
    23  => &Element{name: "postOctetDeltaCount", typeT: "u64"},
    24  => &Element{name: "postPacketDeltaCount", typeT: "u64"},
    25  => &Element{name: "minimumIpTotalLength", typeT: "u64"},
    26  => &Element{name: "maximumIpTotalLength", typeT: "u64"},
    27  => &Element{name: "sourceIPv6Address", typeT: "ipv6Address"},
    28  => &Element{name: "destinationIPv6Address", typeT: "ipv6Address"},
    29  => &Element{name: "sourceIPv6PrefixLength", typeT: "u8"},
    30  => &Element{name: "destinationIPv6PrefixLength", typeT: "u8"},
    31  => &Element{name: "flowLabelIPv6", typeT: "u32"},
    32  => &Element{name: "icmpTypeCodeIPv4", typeT: "u16"},
    33  => &Element{name: "igmpType", typeT: "u8"},
    34  => &Element{name: "RESERVED_34", typeT: "octetarray"},
    35  => &Element{name: "RESERVED_35", typeT: "octetarray"},
    36  => &Element{name: "flowActiveTimeout", typeT: "u16"},
    37  => &Element{name: "flowIdleTimeout", typeT: "u16"},
    38  => &Element{name: "RESERVED_38", typeT: "octetarray"},
    39  => &Element{name: "RESERVED_39", typeT: "octetarray"},
    40  => &Element{name: "exportedOctetTotalCount", typeT: "u64"},
    41  => &Element{name: "exportedMessageTotalCount", typeT: "u64"},
    42  => &Element{name: "exportedFlowRecordTotalCount", typeT: "u64"},
    43  => &Element{name: "RESERVED_43", typeT: "octetarray"},
    44  => &Element{name: "sourceIPv4Prefix", typeT: "ipv4Address"},
    45  => &Element{name: "destinationIPv4Prefix", typeT: "ipv4Address"},
    46  => &Element{name: "mplsTopLabelType", typeT: "u8"},
    47  => &Element{name: "mplsTopLabelIPv4Address", typeT: "ipv4Address"},
    48  => &Element{name: "RESERVED_48", typeT: "octetarray"},
    49  => &Element{name: "RESERVED_49", typeT: "octetarray"},
    50  => &Element{name: "RESERVED_50", typeT: "octetarray"},
    51  => &Element{name: "RESERVED_51", typeT: "octetarray"},
    52  => &Element{name: "minimumTTL", typeT: "u8"},
    53  => &Element{name: "maximumTTL", typeT: "u8"},
    54  => &Element{name: "fragmentIdentification", typeT: "u32"},
    55  => &Element{name: "postIpClassOfService", typeT: "u8"},
    56  => &Element{name: "sourceMacAddress", typeT: "macAddress"},
    57  => &Element{name: "postDestinationMacAddress", typeT: "macAddress"},
    58  => &Element{name: "vlanId", typeT: "u16"},
    59  => &Element{name: "postVlanId", typeT: "u16"},
    60  => &Element{name: "ipVersion", typeT: "u8"},
    61  => &Element{name: "flowDirection", typeT: "u8"},
    62  => &Element{name: "ipNextHopIPv6Address", typeT: "ipv6Address"},
    63  => &Element{name: "bgpNextHopIPv6Address", typeT: "ipv6Address"},
    64  => &Element{name: "ipv6ExtensionHeaders", typeT: "u32"},
    65  => &Element{name: "RESERVED_65", typeT: "octetarray"},
    66  => &Element{name: "RESERVED_66", typeT: "octetarray"},
    67  => &Element{name: "RESERVED_67", typeT: "octetarray"},
    68  => &Element{name: "RESERVED_68", typeT: "octetarray"},
    69  => &Element{name: "RESERVED_69", typeT: "octetarray"},
    70  => &Element{name: "mplsTopLabelStackSection", typeT: "octetArray"},
    71  => &Element{name: "mplsLabelStackSection2", typeT: "octetArray"},
    72  => &Element{name: "mplsLabelStackSection3", typeT: "octetArray"},
    73  => &Element{name: "mplsLabelStackSection4", typeT: "octetArray"},
    74  => &Element{name: "mplsLabelStackSection5", typeT: "octetArray"},
    75  => &Element{name: "mplsLabelStackSection6", typeT: "octetArray"},
    76  => &Element{name: "mplsLabelStackSection7", typeT: "octetArray"},
    77  => &Element{name: "mplsLabelStackSection8", typeT: "octetArray"},
    78  => &Element{name: "mplsLabelStackSection9", typeT: "octetArray"},
    79  => &Element{name: "mplsLabelStackSection10", typeT: "octetArray"},
    80  => &Element{name: "destinationMacAddress", typeT: "macAddress"},
    81  => &Element{name: "postSourceMacAddress", typeT: "macAddress"},
    82  => &Element{name: "RESERVED_82", typeT: "octetarray"},
    83  => &Element{name: "RESERVED_83", typeT: "octetarray"},
    84  => &Element{name: "RESERVED_84", typeT: "octetarray"},
    85  => &Element{name: "octetTotalCount", typeT: "u64"},
    86  => &Element{name: "packetTotalCount", typeT: "u64"},
    87  => &Element{name: "RESERVED_87", typeT: "octetarray"},
    88  => &Element{name: "fragmentOffset", typeT: "u16"},
    89  => &Element{name: "RESERVED_89", typeT: "octetarray"},
    90  => &Element{name: "mplsVpnRouteDistinguisher", typeT: "octetArray"},
    91  => &Element{name: "RESERVED_91", typeT: "octetarray"},
    92  => &Element{name: "RESERVED_92", typeT: "octetarray"},
    93  => &Element{name: "RESERVED_93", typeT: "octetarray"},
    94  => &Element{name: "RESERVED_94", typeT: "octetarray"},
    95  => &Element{name: "RESERVED_95", typeT: "octetarray"},
    96  => &Element{name: "RESERVED_96", typeT: "octetarray"},
    97  => &Element{name: "RESERVED_97", typeT: "octetarray"},
    98  => &Element{name: "RESERVED_98", typeT: "octetarray"},
    99  => &Element{name: "RESERVED_99", typeT: "octetarray"},

    100 => &Element{name: "RESERVED_100", typeT: "octetarray"},
    101 => &Element{name: "RESERVED_101", typeT: "octetarray"},
    102 => &Element{name: "RESERVED_102", typeT: "octetarray"},
    103 => &Element{name: "RESERVED_103", typeT: "octetarray"},
    104 => &Element{name: "RESERVED_104", typeT: "octetarray"},
    105 => &Element{name: "RESERVED_105", typeT: "octetarray"},
    106 => &Element{name: "RESERVED_106", typeT: "octetarray"},
    107 => &Element{name: "RESERVED_107", typeT: "octetarray"},
    108 => &Element{name: "RESERVED_108", typeT: "octetarray"},
    109 => &Element{name: "RESERVED_109", typeT: "octetarray"},
    110 => &Element{name: "RESERVED_110", typeT: "octetarray"},
    111 => &Element{name: "RESERVED_111", typeT: "octetarray"},
    112 => &Element{name: "RESERVED_112", typeT: "octetarray"},
    113 => &Element{name: "RESERVED_113", typeT: "octetarray"},
    114 => &Element{name: "RESERVED_114", typeT: "octetarray"},
    115 => &Element{name: "RESERVED_115", typeT: "octetarray"},
    116 => &Element{name: "RESERVED_116", typeT: "octetarray"},
    117 => &Element{name: "RESERVED_117", typeT: "octetarray"},
    118 => &Element{name: "RESERVED_118", typeT: "octetarray"},
    119 => &Element{name: "RESERVED_119", typeT: "octetarray"},
    120 => &Element{name: "RESERVED_120", typeT: "octetarray"},
    121 => &Element{name: "RESERVED_121", typeT: "octetarray"},
    122 => &Element{name: "RESERVED_122", typeT: "octetarray"},
    123 => &Element{name: "RESERVED_123", typeT: "octetarray"},
    124 => &Element{name: "RESERVED_124", typeT: "octetarray"},
    125 => &Element{name: "RESERVED_125", typeT: "octetarray"},
    126 => &Element{name: "RESERVED_126", typeT: "octetarray"},
    127 => &Element{name: "RESERVED_127", typeT: "octetarray"},

    128 => &Element{name: "bgpNextAdjacentAsNumber", typeT: "u32"},
    129 => &Element{name: "bgpPrevAdjacentAsNumber", typeT: "u32"},
    130 => &Element{name: "exporterIPv4Address", typeT: "ipv4Address"},
    131 => &Element{name: "exporterIPv6Address", typeT: "ipv6Address"},
    132 => &Element{name: "droppedOctetDeltaCount", typeT: "u64"},
    133 => &Element{name: "droppedPacketDeltaCount", typeT: "u64"},
    134 => &Element{name: "droppedOctetTotalCount", typeT: "u64"},
    135 => &Element{name: "droppedPacketTotalCount", typeT: "u64"},
    136 => &Element{name: "flowEndReason", typeT: "u8"},
    137 => &Element{name: "commonPropertiesId", typeT: "u64"},
    138 => &Element{name: "observationPointId", typeT: "u64"},
    139 => &Element{name: "icmpTypeCodeIPv6", typeT: "u16"},
    140 => &Element{name: "mplsTopLabelIPv6Address", typeT: "ipv6Address"},
    141 => &Element{name: "lineCardId", typeT: "u32"},
    142 => &Element{name: "portId", typeT: "u32"},
    143 => &Element{name: "meteringProcessId", typeT: "u32"},
    144 => &Element{name: "exportingProcessId", typeT: "u32"},
    145 => &Element{name: "templateId", typeT: "u16"},
    146 => &Element{name: "wlanChannelId", typeT: "u8"},
    147 => &Element{name: "wlanSSID", typeT: "string"},
    148 => &Element{name: "flowId", typeT: "u64"},
    149 => &Element{name: "observationDomainId", typeT: "u32"},
    150 => &Element{name: "flowStartSeconds", typeT: "u32"},
    151 => &Element{name: "flowEndSeconds", typeT: "u32"},
    152 => &Element{name: "flowStartMilliseconds", typeT: "u64"},
    153 => &Element{name: "flowEndMilliseconds", typeT: "u64"},
    154 => &Element{name: "flowStartMicroseconds", typeT: "u64"},
    155 => &Element{name: "flowEndMicroseconds", typeT: "u64"},
    156 => &Element{name: "flowStartNanoseconds", typeT: "u64"},
    157 => &Element{name: "flowEndNanoseconds", typeT: "u64"},
    158 => &Element{name: "flowStartDeltaMicroseconds", typeT: "u64"},
    159 => &Element{name: "flowEndDeltaMicroseconds", typeT: "u64"},
    160 => &Element{name: "systemInitTimeMilliseconds", typeT: "u64"},
    161 => &Element{name: "flowDurationMilliseconds", typeT: "u32"},
    162 => &Element{name: "flowDurationMicroseconds", typeT: "u32"},
    163 => &Element{name: "observedFlowTotalCount", typeT: "u64"},
    164 => &Element{name: "ignoredPacketTotalCount", typeT: "u64"},
    165 => &Element{name: "ignoredOctetTotalCount", typeT: "u64"},
    166 => &Element{name: "notSentFlowTotalCount", typeT: "u64"},
    167 => &Element{name: "notSentPacketTotalCount", typeT: "u64"},
    168 => &Element{name: "notSentOctetTotalCount", typeT: "u64"},
    169 => &Element{name: "destinationIPv6Prefix", typeT: "ipv6Address"},
    170 => &Element{name: "sourceIPv6Prefix", typeT: "ipv6Address"},
    171 => &Element{name: "postOctetTotalCount", typeT: "u64"},
    172 => &Element{name: "postPacketTotalCount", typeT: "u64"},
    173 => &Element{name: "flowKeyIndicator", typeT: "u64"},
    174 => &Element{name: "postMCastPacketTotalCount", typeT: "u64"},
    175 => &Element{name: "postMCastOctetTotalCount", typeT: "u64"},
    176 => &Element{name: "icmpTypeIPv4", typeT: "u8"},
    177 => &Element{name: "icmpCodeIPv4", typeT: "u8"},
    178 => &Element{name: "icmpTypeIPv6", typeT: "u8"},
    179 => &Element{name: "icmpCodeIPv6", typeT: "u8"},
    180 => &Element{name: "udpSourcePort", typeT: "u16"},
    181 => &Element{name: "udpDestinationPort", typeT: "u16"},
    182 => &Element{name: "tcpSourcePort", typeT: "u16"},
    183 => &Element{name: "tcpDestinationPort", typeT: "u16"},
    184 => &Element{name: "tcpSequenceNumber", typeT: "u32"},
    185 => &Element{name: "tcpAcknowledgementNumber", typeT: "u32"},
    186 => &Element{name: "tcpWindowSize", typeT: "u16"},
    187 => &Element{name: "tcpUrgentPointer", typeT: "u16"},
    188 => &Element{name: "tcpHeaderLength", typeT: "u8"},
    189 => &Element{name: "ipHeaderLength", typeT: "u8"},
    190 => &Element{name: "totalLengthIPv4", typeT: "u16"},
    191 => &Element{name: "payloadLengthIPv6", typeT: "u16"},
    192 => &Element{name: "ipTTL", typeT: "u8"},
    193 => &Element{name: "nextHeaderIPv6", typeT: "u8"},
    194 => &Element{name: "mplsPayloadLength", typeT: "u32"},
    195 => &Element{name: "ipDiffServCodePoint", typeT: "u8"},
    196 => &Element{name: "ipPrecedence", typeT: "u8"},
    197 => &Element{name: "fragmentFlags", typeT: "u8"},
    198 => &Element{name: "octetDeltaSumOfSquares", typeT: "u64"},
    199 => &Element{name: "octetTotalSumOfSquares", typeT: "u64"},
    200 => &Element{name: "mplsTopLabelTTL", typeT: "u8"},
    201 => &Element{name: "mplsLabelStackLength", typeT: "u32"},
    202 => &Element{name: "mplsLabelStackDepth", typeT: "u32"},
    203 => &Element{name: "mplsTopLabelExp", typeT: "u8"},
    204 => &Element{name: "ipPayloadLength", typeT: "u32"},
    205 => &Element{name: "udpMessageLength", typeT: "u16"},
    206 => &Element{name: "isMulticast", typeT: "u8"},
    207 => &Element{name: "ipv4IHL", typeT: "u8"},
    208 => &Element{name: "ipv4Options", typeT: "octetArray"},
    209 => &Element{name: "tcpOptions", typeT: "octetArray"},
    210 => &Element{name: "paddingOctets", typeT: "octetArray"},
    211 => &Element{name: "collectorIPv4Address", typeT: "ipv4Address"},
    212 => &Element{name: "collectorIPv6Address", typeT: "ipv6Address"},
    213 => &Element{name: "exportInterface", typeT: "u32"},
    214 => &Element{name: "exportProtocolVersion", typeT: "u8"},
    215 => &Element{name: "exportTransportProtocol", typeT: "u8"},
    216 => &Element{name: "collectorTransportPort", typeT: "u16"},
    217 => &Element{name: "exporterTransportPort", typeT: "u16"},
    218 => &Element{name: "tcpSynTotalCount", typeT: "u64"},
    219 => &Element{name: "tcpFinTotalCount", typeT: "u64"},
    220 => &Element{name: "tcpRstTotalCount", typeT: "u64"},
    221 => &Element{name: "tcpPshTotalCount", typeT: "u64"},
    222 => &Element{name: "tcpAckTotalCount", typeT: "u64"},
    223 => &Element{name: "tcpUrgTotalCount", typeT: "u64"},
    224 => &Element{name: "ipTotalLength", typeT: "u64"},
    237 => &Element{name: "postMplsTopLabelExp", typeT: "u8"},
    238 => &Element{name: "tcpWindowScale", typeT: "u8"}
};
*/

/*
@functools.lru_cache(maxsize=500)
def convert(raw_bytes: bytes, field_type: str) -> Any:
    """Convert raw IPFIX bytes to a JSON-serializable Python"""
    """type based on field_type."""

    if field_type in ("u8", "u16", "u32", "u64",
                      "signed8", "signed16", "signed32", "signed64",):
        return int.from_bytes(raw_bytes, byteorder='big')
    try:
        if field_type == "float32":
            return struct.unpack("!f", raw_bytes)[0]
        elif field_type == "float64":
            return struct.unpack("!d", raw_bytes)[0]
        elif field_type == "boolean":
            return raw_bytes != b'\x00'
        elif field_type == "macAddress":
            return "-".join(f"{b:02x}" for b in raw_bytes).upper()
        elif field_type == "ipv4Address":
            return str(ipaddress.IPv4Address(raw_bytes))
        elif field_type == "ipv6Address":
            return str(ipaddress.IPv6Address(raw_bytes))
        elif field_type == "string" or field_type == "octetArray":
            try:
                return raw_bytes.decode("utf-8")
            except UnicodeDecodeError:
                return raw_bytes.hex()
        elif field_type == "dateTimeSeconds":
            return struct.unpack("!I", raw_bytes)[0]
        elif field_type == "dateTimeMilliseconds":
            # truncate in app logic if needed
            return struct.unpack("!Q", raw_bytes)[0]
        elif field_type == "dateTimeMicroseconds":
            return struct.unpack("!Q", raw_bytes)[0]
        elif field_type == "dateTimeNanoseconds":
            return struct.unpack("!Q", raw_bytes)[0]
        else:
            return raw_bytes.hex()
    except struct.error:
        return raw_bytes.hex()
*/

