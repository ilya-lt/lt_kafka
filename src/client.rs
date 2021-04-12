use std::collections::HashMap;
use std::convert::TryInto;
use std::time::Duration;

use log::info;
use pyo3::{PyErr, PyResult};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rdkafka::{Message, Offset, TopicPartitionList};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::error::KafkaError::MessageProduction;
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::util::Timeout;
use retry::delay::Fixed;
use retry::OperationResult;
use retry::retry;

use crate::adapters::{MemoryAdapter, RegistryAdapter, Schema};
use crate::json_config;
use crate::json_config::LtClientConfig;
use crate::metadata::Metadata;
use crate::schema_registry::SchemaRegistry;

#[pyclass]
pub struct Client {
    config: LtClientConfig,
    registry: SchemaRegistry,
    producer: BaseProducer,
}


fn is_queue_full(result: &KafkaError) -> bool {
    if let MessageProduction(code) = result {
        return *code == RDKafkaErrorCode::QueueFull;
    } else {
        false
    }
}


fn message_iteration<T: RegistryAdapter>(
    py: Python,
    result: KafkaResult<BorrowedMessage>,
    callback: &PyAny,
    adapter: &mut T,
    should_process: &mut HashMap<u32, bool>,
) -> PyResult<()> {
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
        if !should_process.contains_key(&schema_id) {
            let should_process_args = (schema_id, subject_name.as_str());

            should_process.insert(
                schema_id,
                callback.call_method1("should_process", should_process_args)?.is_true()?,
            );
        }

        if *should_process.get(&schema_id).unwrap() {
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

    Ok(())
}


#[pymethods]
impl Client {
    #[new]
    fn new(filename: &str) -> PyResult<Self> {
        let config = json_config::read_config(filename)
            .map_err(|e| PyErr::new::<PyException, String>(format!("error reading config file: {}", e)))?;

        let registry = SchemaRegistry::new(config.schema_registry.to_owned());

        let producer: BaseProducer =
            json_config::producer_config(&config)
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

        retry(Fixed::from_millis(100), || {
            let resp = self.producer.send(
                BaseRecord::to(topic)
                    .payload(full_message.as_slice())
                    .key(key)
            );

            if let Err((e, _)) = resp {
                if is_queue_full(&e) {
                    info!("Queue is full, polling");
                    let messages = self.producer.poll(Timeout::After(Duration::from_secs(1)));

                    info!("Sent {} messages", messages);
                    OperationResult::Retry(e)
                } else {
                    OperationResult::Err(e)
                }
            } else {
                OperationResult::Ok(())
            }
        }).map_err(|e| PyErr::new::<PyException, String>(
            format!("failed to produce message: {}", e)
        ))
    }

    unsafe fn consume(&self, py: Python, callback: &PyAny, topics: Vec<&str>, group_id: Option<&str>) -> PyResult<()> {
        let consumer: BaseConsumer =
            json_config::consumer_config(&self.config, group_id)
                .map_err(|e| PyErr::new::<PyException, String>(format!("error configuring consumer: {}", e)))?
                .set_log_level(RDKafkaLogLevel::Debug)
                .create()
                .map_err(|e| PyErr::new::<PyException, String>(format!("error creating consumer: {}", e)))?;

        if group_id == None {
            let mut list = TopicPartitionList::new();

            for topic in topics {
                let result = consumer.fetch_metadata(
                    Some(topic),
                    Timeout::After(Duration::from_secs(60)),
                )
                    .map_err(|e| PyErr::new::<PyException, String>(
                        format!("failed to read metadata: {}", e)
                    ))?;

                for mt in result.topics() {
                    for partition in mt.partitions() {
                        list.add_partition_offset(topic, partition.id(), Offset::End)
                            .map_err(|e| PyErr::new::<PyException, String>(
                                format!("failed to add partition: {}", e)
                            ))?;
                    }
                }
            }

            consumer.assign(&list)
                .map_err(|e| PyErr::new::<PyException, String>(
                    format!("failed to assign topic & partitions: {}", e)
                ))?;
        } else {
            consumer.subscribe(topics.as_slice())
                .map_err(|e| PyErr::new::<PyException, String>(format!("error subscribing to topics: {}", e)))?;
        }

        let mut adapter = MemoryAdapter::new(&self.registry);
        let mut should_process: HashMap<u32, bool> = HashMap::new();

        for result in consumer.iter() {
            let pool = py.new_pool();
            let new_py = pool.python();

            message_iteration(
                new_py,
                result,
                callback,
                &mut adapter,
                &mut should_process,
            )?
        }

        Ok(())
    }

    fn flush(&self) -> PyResult<()> {
        info!("flushing {} in-flight messages", self.producer.in_flight_count());

        self.producer.flush(Timeout::Never);

        Ok(())
    }
}