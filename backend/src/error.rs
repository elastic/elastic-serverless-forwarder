
use pyo3::{
    prelude::*,
    exceptions::*,
};

use thiserror::Error;
use std::io;

#[derive(Error, Debug)]
pub enum ParsingError {
    #[error("data store disconnected")]
    Disconnect(#[from] io::Error),
    #[error("the data for key `{0}` is not available")]
    NotFound(String),
    #[error("invalid header (expected {expected:?}, found {found:?})")]
    InvalidHeader {
        expected: String,
        found: String,
    },
    #[error("Reached the end of the file")]
    EOF,
    #[error("Expected 10, but got {0}")]
    InvalidVersion(u16),
    #[error("unknown data store error")]
    Unknown,
}

// convert to python exceptions
//
impl<'py> From<ParsingError> for PyErr {
    fn from(e: ParsingError) -> PyErr {
        match e {
            ParsingError::EOF => PyEOFError::new_err(format!("{e}")),
            ParsingError::NotFound(_) => PyEOFError::new_err(format!("{e}")),
            _ => PyException::new_err("Parsing Error"),
        }
    }

}

pub type ParsingResult<T> = Result<T, ParsingError>;
