
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

type Result<T> = std::result::Result<T, error::ParsingError>;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn _backend(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}

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

}

impl IpfixProcessor {
    fn read_next(&mut self) -> Result<MapString> {
        // parse header
        // loop {
        //   parse flowset header
        //   parse template set or data set
        // }
        Ok(HashMap::new())
    }

    fn read_header(&mut self) -> Result<Header> {
        // read the header or fail

        Ok(Header::new())
    }

    fn read_flowset(&mut self) -> Result<FlowSet> {
        Ok(FlowSet::new())
    }

    fn open(&mut self) -> bool {
        if let Ok(f) = File::open(&self.file) {
            let rd = BufReader::new(f);
            self.state = State::Reading(rd);
            true
        } else {
            false
        }
    }

    fn has_more_ex(&mut self) -> bool {
        if let State::Reading(r) = &mut self.state {
            if let Ok(b) = r.fill_buf() {
                b.len() > 0
            } else {
                false
            }
        } else if let State::Pre = &self.state {
            self.open()
        } else {
            false
        }
    }
}

// Open the ipfix file
// Read the header and consume the message
// Return a dict
