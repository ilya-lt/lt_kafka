use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3::types::PyBytes;
use rdkafka::consumer::BaseConsumer;
use rdkafka::config::RDKafkaLogLevel;
use simplelog::{CombinedLogger, TermLogger, Config, TerminalMode};
use log::LevelFilter;
use rdkafka::consumer::Consumer;
use pyo3::exceptions::PyException;
use rdkafka::Message;
use std::convert::TryInto;
use crate::schema_registry::SchemaRegistry;
use crate::adapters::{MemoryAdapter, RegistryAdapter, Schema};

mod json_config;
mod schema_registry;
pub mod adapters;

#[pyfunction]
/// Formats the sum of two numbers as string.
fn consume(py:Python, filename: &str, callback: &PyAny) -> PyResult<()> {

    let config = json_config::read_config(filename)
        .map_err(|e| PyErr::new::<PyException,String>(format!("error reading config file: {}", e)))?;
    let consumer: BaseConsumer =
        json_config::client_config(&config)
            .map_err(|e| PyErr::new::<PyException,String>(format!("error configuring consumer: {}", e)))?
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .map_err(|e| PyErr::new::<PyException,String>(format!("error creating consumer: {}", e)))?;

    let topic_vec: Vec<&str> = config
        .topics
        .iter()
        .map(|x| x.as_str())
        .collect();

    consumer.subscribe(topic_vec.as_slice())
        .map_err(|e| PyErr::new::<PyException,String>(format!("error subscribing to topics: {}", e)))?;

    let registry = SchemaRegistry::new(config.schema_registry);
    let mut adapter = MemoryAdapter::new(registry);

    for result in consumer.iter() {
        let msg = result
            .map_err(|e| PyErr::new::<PyException,String>(format!("error reading message: {}", e)))?;

        let payload = match msg.payload() {
            None => Err(PyErr::new::<PyException,&str>("No body given on message")),
            Some(data) => Ok(data)
        }?;

        let schema_id = u32::from_be_bytes(payload[1..5].try_into()?);

        if let Some(subject_name) = adapter.get_subject_name(schema_id)
            .map_err(|e| PyErr::new::<PyException, String>(
            format!("error retrieving subject name: {}", e)
            ))?.to_owned() {

            let should_process_args = (schema_id, subject_name.as_str());

            if callback.call_method1("should_process", should_process_args)?.is_true()? {
                if let Schema::New(schema) = adapter.get_schema(schema_id)
                    .map_err(|e| PyErr::new::<PyException, String>(
                        format!("error retrieving schema: {}", e)
                    ))? {
                    let new_schema_args = (schema_id, subject_name.as_str(), schema);

                    callback.call_method1("new_schema", new_schema_args)?;
                }

                let record_args = (schema_id, subject_name.as_str(), PyBytes::new(py, &payload[5..]));
                callback.call_method1("record", record_args)?;
            }
        }
    }

    Ok(())
}

#[pymodule]
/// A Python module implemented in Rust.
fn lt_kafka(_py: Python, m: &PyModule) -> PyResult<()> {
    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Info, Config::default(), TerminalMode::Mixed),
        ]
    ).unwrap();

    m.add_function(wrap_pyfunction!(consume, m)?)?;

    Ok(())
}