use std::collections::{HashMap, HashSet};
use std::error::Error;

use log::info;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct SchemaRegistryConfig {
    hostname: String,
    username: String,
    password: String,
}

pub struct SchemaRegistry {
    seen_schemas: HashSet<u32>,
    subject_names: HashMap<u32, String>,
    config: SchemaRegistryConfig,
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
    pub fn new(config: SchemaRegistryConfig) -> SchemaRegistry {
        SchemaRegistry {
            config,
            seen_schemas: HashSet::new(),
            subject_names: HashMap::new(),
        }
    }

    pub fn get_subject_name(self: &mut Self, schema_id: u32) -> Result<&String, Box<dyn Error>> {
        if !self.subject_names.contains_key(&schema_id) {
            let client = reqwest::blocking::Client::new();

            let subject_name = {
                let resp = client.get(format!(
                    "https://{}/schemas/ids/{}/versions",
                    self.config.hostname,
                    schema_id
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
                    .send()?;

                let body: Vec<VersionRequestResponse> = resp.json()?;

                body
                    .iter()
                    .max_by_key(|i| i.version)
                    .unwrap()
                    .subject
                    .to_owned()
            };

            info!("query subject name for:{} subject_name:{}", schema_id, subject_name);

            self.subject_names.insert(schema_id, subject_name);
        }

        Ok(self.subject_names.get(&schema_id).unwrap())
    }

    pub fn new_schema(self: &mut Self, schema_id: u32) -> Result<Option<String>, Box<dyn Error>> {
        if !self.seen_schemas.contains(&schema_id) {
            let client = reqwest::blocking::Client::new();

            let resp = client.get(format!(
                "https://{}/schemas/ids/{}",
                self.config.hostname,
                schema_id
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
                .send()?;

            let body: SchemaRequestResponse = resp.json()?;
            self.seen_schemas.insert(schema_id);

            Ok(Some(body.schema))
        } else {
            Ok(None)
        }
    }
}