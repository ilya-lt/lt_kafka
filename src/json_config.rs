use std::error::Error;
use std::fs::File;
use std::path::Path;

use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use serde_json::Value;

use crate::schema_registry::SchemaRegistryConfig;

#[derive(Serialize, Deserialize)]
struct KafkaConfigSections {
    base: Map<String, Value>,
    consumer: Map<String, Value>,
    producer: Map<String, Value>,
}

#[derive(Serialize, Deserialize)]
pub struct LtClientConfig {
    kafka_config: KafkaConfigSections,
    pub topics: Vec<String>,
    pub schema_registry: SchemaRegistryConfig,
}


pub fn read_config<P: AsRef<Path>>(filename: P) -> Result<LtClientConfig, Box<dyn Error>> {
    let f = File::open(filename)?;
    let d: LtClientConfig = serde_json::from_reader(f)?;
    Ok(d)
}

fn write_config(config: &mut ClientConfig, section: &Map<String, Value>) -> Result<(), Box<dyn Error>> {
    for (config_key, config_value) in section {
        let str_value = match config_value {
            Value::Null => Err("got null"),
            Value::Bool(true) => Ok(String::from("true")),
            Value::Bool(false) => Ok(String::from("false")),
            Value::Number(n) => Ok(n.to_string()),
            Value::String(v) => Ok(v.to_owned()),
            Value::Array(_) => Err("got array"),
            Value::Object(_) => Err("got object")
        }.map_err(|e| format!("got error parsing \"{}\": {}", config_key, e))?;

        config.set(config_key, str_value);
    }

    Ok(())
}

pub fn consumer_config(client_config: &LtClientConfig) -> Result<ClientConfig, Box<dyn Error>> {
    let mut config = ClientConfig::new();

    write_config(&mut config, &client_config.kafka_config.base)?;
    write_config(&mut config, &client_config.kafka_config.consumer)?;

    Ok(config)
}

pub fn producer_config(client_config: &LtClientConfig) -> Result<ClientConfig, Box<dyn Error>> {
    let mut config = ClientConfig::new();

    write_config(&mut config, &client_config.kafka_config.base)?;
    write_config(&mut config, &client_config.kafka_config.producer)?;

    Ok(config)
}