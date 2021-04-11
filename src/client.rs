use std::convert::TryInto;

use log::info;
use pyo3::{PyErr, PyResult};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::util::Timeout;

use crate::adapters::{MemoryAdapter, RegistryAdapter, Schema};
use crate::json_config;
use crate::json_config::ConsumerConfig;
use crate::metadata::Metadata;
use crate::schema_registry::SchemaRegistry;

#[pyclass]
pub struct Client {
    config: ConsumerConfig,
    registry: SchemaRegistry,
    producer: BaseProducer,
}

#[pymethods]
impl Client {
    #[new]
    fn new(filename: &str) -> PyResult<Self> {
        let config = json_config::read_config(filename)
            .map_err(|e| PyErr::new::<PyException, String>(format!("error reading config file: {}", e)))?;

        let registry = SchemaRegistry::new(config.schema_registry.to_owned());

        let producer: BaseProducer =
            json_config::client_config(&config)
                .map_err(|e| PyErr::new::<PyException, String>(format!("error configuring consumer: {}", e)))?
                .set_log_level(RDKafkaLogLevel::Debug)
                .create()
                .map_err(|e| PyErr::new::<PyException, String>(format!("error creating consumer: {}", e)))?;

        Ok(Client {
            config,
            registry,
            producer,
        })
    }

    fn register_schema(&self, subject: &str, schema: &str) -> PyResult<u32> {
        self.registry.register_schema(subject, schema.to_string())
            .map_err(|e| PyErr::new::<PyException, String>(
                format!("error retrieving schema: {}", e)
            ))
    }

    fn produce(&self, topic: &str, schema_id: u32, key: &[u8], value: &[u8]) -> PyResult<()> {
        let mut full_message: Vec<u8> = Vec::with_capacity(value.len() + 5);

        full_message.push(0);
        full_message.extend_from_slice(&schema_id.to_be_bytes());
        full_message.extend_from_slice(value);

        self.producer.send(
            BaseRecord::to(topic)
                .payload(full_message.as_slice())
                .key(key)
        ).map_err(|e| PyErr::new::<PyException, String>(
            format!("failed to produce message: {}", e.0)
        ))
    }

    fn consume(&self, py: Python, callback: &PyAny) -> PyResult<()> {
        let consumer: BaseConsumer =
            json_config::client_config(&self.config)
                .map_err(|e| PyErr::new::<PyException, String>(format!("error configuring consumer: {}", e)))?
                .set_log_level(RDKafkaLogLevel::Debug)
                .create()
                .map_err(|e| PyErr::new::<PyException, String>(format!("error creating consumer: {}", e)))?;

        let topic_vec: Vec<&str> = self.config
            .topics
            .iter()
            .map(|x| x.as_str())
            .collect();

        consumer.subscribe(topic_vec.as_slice())
            .map_err(|e| PyErr::new::<PyException, String>(format!("error subscribing to topics: {}", e)))?;

        let mut adapter = MemoryAdapter::new(&self.registry);

        for result in consumer.iter() {
            let msg = result
                .map_err(|e| PyErr::new::<PyException, String>(format!("error reading message: {}", e)))?;

            let payload = match msg.payload() {
                None => Err(PyErr::new::<PyException, &str>("No body given on message")),
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


                    let record_args = (
                        schema_id,
                        subject_name.as_str(),
                        PyBytes::new(py, &payload[5..]),
                        Metadata::new(&msg)
                    );

                    callback.call_method1("record", record_args)?;
                }
            }
        }

        Ok(())
    }

    fn flush(&self) -> PyResult<()> {
        info!("flushing {} in-flight messages", self.producer.in_flight_count());

        self.producer.flush(Timeout::Never);

        Ok(())
    }
}