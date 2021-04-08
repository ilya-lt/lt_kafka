use std::error::Error;

use reqwest::blocking::Response;
use serde::{Serialize, Deserialize};
use log::info;

#[derive(Serialize, Deserialize, Clone)]
pub struct SchemaRegistryConfig {
    hostname: String,
    username: String,
    password: String,
}

pub struct SchemaRegistry {
    config: SchemaRegistryConfig
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

impl SchemaRegistry {
    fn registry_get(self: &Self, path: String) -> Result<Response, Box<dyn Error>> {
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

    pub fn get_subject_name(self: &mut Self, schema_id: u32) -> Result<Option<String>, Box<dyn Error>> {
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
            }
        }

        Ok(subject_name)
    }

    pub fn get_schema(self: &mut Self, schema_id: u32) -> Result<String, Box<dyn Error>> {
        let body: SchemaRequestResponse = self.registry_get(format!(
            "schemas/ids/{}",
            schema_id
        ))?.json()?;

        Ok(body.schema)
    }

    pub fn new(config: SchemaRegistryConfig) -> Self {
        SchemaRegistry{ config }
    }
}