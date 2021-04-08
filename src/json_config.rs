use std::error::Error;
use std::fs::File;
use std::path::Path;

use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use serde_json::Value;
use crate::schema_registry::SchemaRegistryConfig;

#[derive(Serialize, Deserialize)]
pub struct ConsumerConfig {
    kafka_config: Map<String, Value>,
    pub topics: Vec<String>,
    pub schema_registry: SchemaRegistryConfig,
}


pub fn read_config<P: AsRef<Path>>(filename: P) -> Result<ConsumerConfig, Box<dyn Error>> {
    let f = File::open(filename)?;
    let d: ConsumerConfig = serde_json::from_reader(f)?;
    Ok(d)
}

pub fn client_config(consumer_config: &ConsumerConfig) -> Result<ClientConfig, Box<dyn Error>> {
    let mut config = ClientConfig::new();

    for (config_key, config_value) in &consumer_config.kafka_config {
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

    Ok(config)
}