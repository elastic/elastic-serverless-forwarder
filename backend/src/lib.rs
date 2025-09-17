
mod error;

use pyo3::{
    prelude::*,
    types::{PyDict},
};

use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, BufRead, Read},
};

type MapString = HashMap<String, String>;

use crate::error::ParsingError;
type Result<T> = std::result::Result<T, ParsingError>;

enum State {
    Pre,
    Reading(BufReader<File>),
    Done,
}

// Class that takes a path to a file
// It has an iterator method
#[pyclass]
struct IpfixProcessor {
    file: String,

    state: State,
}

#[pymethods]
impl IpfixProcessor {
    #[new]
    fn new(file: String) -> Self {
        //
        Self {
            file,
            state: State::Pre,
        }
    }

    fn open(&mut self) -> bool {
        self.open_ex()
    }

    fn has_more(&mut self) -> bool {
        self.has_more_ex()
    }

    fn next(&mut self) -> MapString {
        // read the header
        if let Ok(m) = self.read_next() {
            m
        } else {
            HashMap::new()
        }
    }
}

// apply serde
#[derive(Default)]
struct Header {
    version: u16,
    length: u16,
    export_time: u32,
    sequence_number: u32,
    observation_domain_id: u32
}

#[derive(Default)]
struct Template {
    id: u16,
}

#[derive(Default)]
struct DataRecord {

}

enum Record {
    Template(Template),
    Data(DataRecord),
}

// apply serde
#[derive(Default)]
struct FlowSet {
    version: u16,
    length: u16,
    records: Vec<Record>,
}

impl FlowSet {
    fn new() -> Self {
        Self::default()
    }
}

impl Header {
    fn new() -> Self {
        Self::default()
    }

    fn from(binary: &[u8]) -> Result<Self> {
        // read the header or fail

        let version = u16::from_be_bytes([binary[0], binary[1]]);
        let length = u16::from_be_bytes([binary[2], binary[3]]);
        let export_time = u32::from_be_bytes([binary[4], binary[5], binary[6], binary[7]]);
        let sequence_number = u32::from_be_bytes([binary[8], binary[9], binary[10], binary[11]]);
        let observation_domain_id = u32::from_be_bytes([binary[12], binary[13], binary[14], binary[15]]);
        Ok(Header {
            version,
            length,
            export_time,
            sequence_number,
            observation_domain_id,
        })
    }
}

impl IpfixProcessor {
    fn read_next(&mut self) -> Result<MapString> {
        if let State::Reading(r) = &mut self.state {
            let mut buf = [0u8;16];

            let header_count : usize = r.read(&mut buf)?;
            if header_count != 16 {
                return Err(ParsingError::Unknown);
            }

            // parse header
            let header = Header::from(&buf)?;

            // now, read the entire length of the message
            let message_length : usize = header.length.into() - 16;
            let message = vec![0u8; header.length.into()];

            let message_count : usize = r.read(message.as_mut())?;
            if message_count != header.length.into() {
                return Err(ParsingError::Unknown);
            }

            // loop {
            //   parse flowset header
            //   parse template set or data set
            // }
            let mut m = HashMap::new();
            m.insert("header".to_string(), format!("header with length {0}", header.length));
            return Ok(m)
        }
        Ok(HashMap::new())
    }


    fn read_flowset(&mut self) -> Result<FlowSet> {
        Ok(FlowSet::new())
    }

    fn open_ex(&mut self) -> bool {
        match File::open(&self.file) {
            Ok(f) => {
                let rd = BufReader::new(f);
                self.state = State::Reading(rd);
                true
            }
            Err(e) => {
                println!("failed to open! {e}");
                false
            }
        }
    }

    fn has_more_ex(&mut self) -> bool {
        if let State::Reading(r) = &mut self.state {
            if let Ok(b) = r.fill_buf() {
                b.len() > 0
            } else {
                false
            }
        } else {
            false
        }
    }
}

// Open the ipfix file
// Read the header and consume the message
// Return a dict

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn _backend(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_class::<IpfixProcessor>()?;
    Ok(())
}

