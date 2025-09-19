
use serde::{Serialize, Deserialize};
use serde_json::Value as SerdeValue;
use pyo3::{
    exceptions::*,
    prelude::*,
};

use std::collections::HashMap;

use crate::error::{ParsingResult};

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
        Self::U8(into_u8(buffer))
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
    snake_name: &'static str,
    type_t: &'static str,
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
    pub fn name(&self) -> String {
        self.name.to_string()
    }
    pub fn translate(&self, buffer: &[u8], length: usize) -> Value {

        match self.type_t {
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

fn export_to_ecs(mapping: ValueMapping, exporter_address: Option<String>, internal_networks: Vec<String>, op_timestamp: Option<String>) -> ParsingResult<String> {

    // we already understand the mappings, so just work with that
    // ... maybe have it be a Value method

    let timestamp : String;
    let mut internal_networks = internal_networks;

    if internal_networks.len() == 0 {
        for n in ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"].into_iter() {
            internal_networks.push(n.to_string());
        }
    }

    if let Some(t) = op_timestamp {
        timestamp = t;
    } else {
        timestamp = String::from("now");
    }

    Ok(String::from("{}"))
}

#[derive(Debug, Serialize, Deserialize)]
struct EventMapping {
    created: String,
    kind: String,
    category: Vec<String>,
    action: String,
    #[serde(rename="type")]
    event_type: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Event {
    header: EventMapping,
    observer: SerdeValue,
    source: SerdeValue,
    destination: SerdeValue,
    network: SerdeValue,
    flow: SerdeValue,
    related: SerdeValue,
    netflow: SerdeValue,
    #[serde(rename="@timestamp")]
    timestamp: String,
}

/*
def export_to_ecs(netflow_packet: Dict[str, Any],
                  exporter_address: Optional[str] = None,
                  internal_networks: List = [],
                  flow_timestamp: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Convert a NetFlow/IPFIX packet to ECS (Elastic Common Schema) format.

    Args:
        netflow_packet: Dictionary containing NetFlow/IPFIX fields
        exporter_address: IP address of the NetFlow exporter
        internal_nettworks: List of internal network CIDRs for locality
        flow_timestamp: Timestamp of the flow (defaults to current time)

    Returns:
        Dictionary in ECS format
    """

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
        ecs_event["observer"]["ip"] = observer_ip
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

*/
