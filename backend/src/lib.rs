
mod error;

use serde::{Serialize, Deserialize};

use pyo3::{
    exceptions::{PyEOFError},
    prelude::*,
    types::{PyDict},
};

use std::{
    collections::{HashMap, VecDeque},
    convert::Into,
    fs::File,
    io::{BufReader, BufRead, Read},
    sync::{Arc, Mutex},
};

type MapString = HashMap<String, String>;

use crate::error::ParsingError;
type Result<T> = std::result::Result<T, ParsingError>;

mod convert;
use convert::*;

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

    templates: HashMap<u16, Template>,
    records: Arc<Mutex<VecDeque<ValueMapping>>>,
}

#[pymethods]
impl IpfixProcessor {
    #[new]
    fn new(file: String) -> PyResult<Self> {
        //
        let mut me = Self {
            file,
            state: State::Pre,
            templates: HashMap::new(),
            records: Arc::new(Mutex::new(VecDeque::new())),
        };

        me.open_ex()?;
        Ok(me)
    }

    fn open(&mut self) -> bool {
        match self.open_ex() {
            Ok(_) => true,
            _ => false,
        }
    }

    fn has_more(&mut self) -> bool {
        let _ = self.read_next();
        if self.records.lock().unwrap().len() > 0 {
            return true;
        }
        false
    }

    fn next(&mut self) -> PyResult<String> {

        let _ = self.read_next();
        // make sure to read the next one
        match self.records.lock().unwrap().pop_front() {
            Some(v) => Ok(serde_json::to_string(&v).unwrap()),
            None => Err(PyEOFError::new_err("No more records, I reckon")),
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

struct U16(u16);

struct Field {
    id: u16,
    field_type: String,
    length: u16,
    element: Element,
}

impl Field {
    fn from_data(buffer: &[u8]) -> Result<Self> {
        let id : u16 = into_u16(buffer);
        let length : u16 = into_u16(&buffer[2..]);
        let element = RFC_5102_INFO_ELEMENT.get(&(id as u32)).map(|e| (**e).clone()).ok_or(ParsingError::NotFound(format!("Could not find element id {id}")))?;

        Ok(Self {
            id,
            field_type: format!("Field {id} with length {length}"),
            length,
            element,
        })

    }
}

#[derive(Default)]
struct Template {
    id: u16,
    length: u16,
    fields: Vec<Field>,
}

#[derive(Default)]
struct OptionsTemplate {
    id: u16,
    length: u16,
    fields: Vec<Field>,
}

impl OptionsTemplate {
    fn from_data(buffer: &[u8]) -> Result<Self> {
        // buffer[0..1] is set-id 2
        // what is our length?
        let length = into_u16(&buffer[2..]);

        // what is our id?
        let id = into_u16(&buffer[4..]);
        let field_count = into_u16(&buffer[6..]);

        // how many scopes?
        todo!("how many scopes do we have here?");

        let mut offset : usize = 8;

        // grab all the fields
        // loop over the full length
        //
        let mut fields = Vec::new();

        let mut record_length = 0;
        for _ in 0..field_count {
            let field = Field::from_data(&buffer[offset..])?;
            record_length += field.length;
            fields.push(field);
            offset += 4;

        }

        Ok(OptionsTemplate {
            id,
            length,
            fields,
        })

    }

    fn get_length(&self) -> u16 {
        self.fields.iter()
            .map(|f| f.length) // map each field to its length
            .sum()

    }

    fn translate(&self, buffer: &[u8]) -> Result<ValueMapping> {
        // create a Map
        let mut result = HashMap::new();

        let mut offset = 0usize;

        for field in self.fields.iter() {
            let element = field.element.clone();
            let name = element.Name();
            let value = element.translate(&buffer[offset..], field.length as usize);

            offset += field.length as usize;

            result.insert(name, value);
        }

        Ok(result)

    }
}
impl Template {
    fn from_data(buffer: &[u8]) -> Result<Self> {
        // buffer[0..1] is set-id 2
        // what is our length?
        let length = into_u16(&buffer[2..]);

        // what is our id?
        let id = into_u16(&buffer[4..]);
        let field_count = into_u16(&buffer[6..]);

        let mut offset : usize = 8;

        // grab all the fields
        // loop over the full length
        //
        let mut fields = Vec::new();

        let mut record_length = 0;
        for _ in 0..field_count {
            let field = Field::from_data(&buffer[offset..])?;
            record_length += field.length;
            fields.push(field);
            offset += 4;

        }

        Ok(Template {
            id,
            length,
            fields,
        })

    }

    fn get_length(&self) -> u16 {
        self.fields.iter()
            .map(|f| f.length) // map each field to its length
            .sum()

    }

    fn translate(&self, buffer: &[u8]) -> Result<ValueMapping> {
        // create a Map
        let mut result = HashMap::new();

        let mut offset = 4usize;

        for field in self.fields.iter() {
            let element = field.element.clone();
            let name = element.Name();
            let value = element.translate(&buffer[offset..], field.length as usize);

            offset += field.length as usize;

            result.insert(name, value);
        }

        Ok(result)

    }
}

impl Header {
    fn new() -> Self {
        Self::default()
    }

    fn from(binary: &[u8]) -> Result<Self> {
        // read the header or fail

        let version = into_u16(binary);

        if version != 10u16 {
            println!("header version is wrong: wanted 10, got {version}");
            return Err(ParsingError::InvalidVersion(version));
        }

        let length = into_u16(&binary[2..]);
        let export_time = into_u32(&binary[4..]);
        let sequence_number = into_u32(&binary[8..]);
        let observation_domain_id = into_u32(&binary[12..]);
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
    fn read_next(&mut self) -> Result<()> {
        if let State::Reading(r) = &mut self.state {
            let mut buf = [0u8;16];

            let _ = r.read_exact(&mut buf)?;

            // parse header
            let header = Header::from(&buf)?;

            // now, read the entire length of the message
            let message_length : usize = (header.length as usize) - 16usize;
            let mut message = vec![0u8; message_length];

            let _ = r.read_exact(message.as_mut())?;

            let mut offset = 0;
            loop {
                if offset >= message.len() - 4 {
                    break;
                }

                offset = self.parse_data(message.as_ref(), offset)?;
            }
        }

        Ok(())
    }

    fn open_ex(&mut self) -> Result<()> {
        let f = File::open(&self.file)?;
        let rd = BufReader::new(f);
        self.state = State::Reading(rd);
        Ok(())
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

    fn store_template(&mut self, t: Template) {
        // save it in a map
        self.templates.insert(t.id, t);

    }

    fn parse_records(&mut self, buffer: &[u8]) -> Result<usize> {
        let mut offset = 0usize;
        let template_id = into_u16(&buffer[offset..]);
        let length = into_u16(&buffer[offset +2..]);

        let template = self.templates.get(&template_id)
            .ok_or(ParsingError::NotFound(format!("template with id {template_id}")))?;

        let record_size : usize = template.get_length().into();

        while offset + record_size < buffer.len() {

            // let the template translate the data
            let record = template.translate(&buffer[offset..])?;

            // lock the thing and push our record
            {
                self.records.lock().unwrap().push_back(record);
            }

            offset += record_size;
        }

        Ok(offset)
    }

    fn parse_data(&mut self, buffer: &[u8], start_offset: usize) -> Result<usize> {
        let mut offset = start_offset;
        let mut count = 0usize;

        while offset + 4 < buffer.len() {

            // read the flowset
            let set_id = into_u16(&buffer[offset..]);
            let length = into_u16(&buffer[offset + 2..]);

            let consumed = match set_id {
                2u16 => {

                    // parse template
                    let tmp = Template::from_data(&buffer[offset..])?;
                    let len = tmp.length as usize;

                    self.store_template(tmp);
                    len
                }
                3u16 => {
                    let tmp = OptionsTemplate::from_data(&buffer[offset..])?;
                    println!("got an options template {set_id} -- {0}", tmp.id);

                    //self.store_template(tmp);
                    tmp.length as usize
                }
                _ => {
                    if set_id < 256 {
                        println!("got a template {set_id} -- bytes remaining is {0}!", buffer.len() - offset);
                        println!("length is {length}!");
                        panic!("can't have a template smaller than 256!");
                    }
                    // parse data records
                    let len = self.parse_records(&buffer[offset..])? + 4;
                    len
                }
            };
            offset += consumed;
        }

        return Ok(offset);
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

