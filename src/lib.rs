use log::LevelFilter;
use pyo3::prelude::*;
use simplelog::{CombinedLogger, Config, TerminalMode, TermLogger};

use crate::client::Client;

mod json_config;
mod schema_registry;
pub mod adapters;
mod metadata;
mod client;

#[pymodule]
/// A Python module implemented in Rust.
fn lt_kafka(_py: Python, m: &PyModule) -> PyResult<()> {
    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Info, Config::default(), TerminalMode::Mixed),
        ]
    ).unwrap();

    m.add_class::<Client>()?;

    Ok(())
}