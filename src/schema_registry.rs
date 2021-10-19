use std::error::Error;
use std::io::{Error as IOError, ErrorKind};

use log::info;
use rdkafka::message::ToBytes;
use reqwest::blocking::Response;
use rocksdb::{DB, DBWithThreadMode, SingleThreaded};
use serde::{Deserialize, Serialize};
use std::str;

#[derive(Serialize, Deserialize, Clone)]
pub struct SchemaRegistryConfig {
    hostname: String,
    username: String,
    password: String,
}

pub struct SchemaRegistry {
    config: SchemaRegistryConfig,
    db: DBWithThreadMode<SingleThreaded>
}

#[derive(Serialize, Deserialize)]
struct SchemaRequestResponse {
    schema: String
}

#[derive(Serialize, Deserialize)]
struct VersionRequestResponse {
    subject: String,
    version: u32,
}

#[derive(Serialize, Deserialize)]
struct SchemaRegisterRequest {
    schema: String
}

#[derive(Serialize, Deserialize)]
struct SchemaRegisterResponse {
    id: u32
}

impl SchemaRegistry {
    fn registry_get(&self, path: String) -> Result<Response, Box<dyn Error>> {
        let client = reqwest::blocking::Client::new();

        Ok(
            client.get(format!(
                "https://{}/{}",
                self.config.hostname,
                path
            ))
                .header(
                    "Authorization",
                    format!(
                        "Basic {}", base64::encode(format!(
                            "{}:{}",
                            self.config.username,
                            self.config.password
                        ))
                    ),
                )
                .send()?
        )
    }

    pub fn get_subject_name(&self, schema_id: u32) -> Result<Option<String>, Box<dyn Error>> {
        let cache_key = format!("subject_name_{}", schema_id);
        let result = self.db.get(cache_key.as_bytes())?;

        if let Some(result_bytes) = result {
            let result_string = str::from_utf8(result_bytes.as_slice())?.to_owned();
            info!("cache_hit subject name for:{} subject_name:{}", schema_id, result_string);
            Ok(Some(result_string))
        } else {
            let subject_name = {
                let body: Vec<VersionRequestResponse> = self.registry_get(format!(
                    "schemas/ids/{}/versions",
                    schema_id
                ))?.json()?;

                body
                    .iter()
                    .max_by_key(|i| i.version)
                    .map(|r| r.subject.to_owned())
            };

            match &subject_name {
                None => {
                    info!("missing subject name for:{}", schema_id);
                }
                Some(sn) => {
                    info!("query subject name for:{} subject_name:{}", schema_id, sn);
                    self.db.put(cache_key.as_bytes(), sn.to_bytes())?;
                }
            }

            Ok(subject_name)
        }
    }

    pub fn get_schema(&self, schema_id: u32) -> Result<String, Box<dyn Error>> {
        let body: SchemaRequestResponse = self.registry_get(format!(
            "schemas/ids/{}",
            schema_id
        ))?.json()?;

        Ok(body.schema)
    }

    pub fn register_schema(&self, subject: &str, schema: String) -> Result<u32, Box<dyn Error>> {
        let client = reqwest::blocking::Client::new();
        let request = SchemaRegisterRequest { schema };
        let response = client.post(format!(
            "https://{}/{}",
            self.config.hostname,
            format!("subjects/{}/versions", subject)
        )).header(
            "Authorization",
            format!(
                "Basic {}", base64::encode(format!(
                    "{}:{}",
                    self.config.username,
                    self.config.password
                ))
            ),
        ).json(&request).send()?;

        if response.status().is_success() {
            let result: SchemaRegisterResponse = response.json()?;

            Ok(result.id)
        } else {
            Err(Box::new(IOError::new(ErrorKind::Other, format!(
                "bad status returned:{}, body:{}",
                response.status(),
                response.text()?
            ))))
        }
    }

    pub fn new(config: SchemaRegistryConfig) -> Self {
        let db = DB::open_default("db").unwrap();

        SchemaRegistry { config, db }
    }
}