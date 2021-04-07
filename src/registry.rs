use std::collections::{HashMap, HashSet};
use std::error::Error;

use log::info;
use reqwest::blocking::Response;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::fs::File;

#[derive(Serialize, Deserialize, Clone)]
pub struct SchemaRegistryConfig {
    hostname: String,
    username: String,
    password: String,
}

type SubjectMap = HashMap<u32, Option<String>>;
type SubjectVector = Vec<(u32, Option<String>)>;

pub struct SchemaRegistry {
    seen_schemas: HashSet<u32>,
    subject_names: SubjectMap,
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

const SUBJECTS_CACHE:&str = "subject_names.cache";

impl SchemaRegistry {
    pub fn new(config: SchemaRegistryConfig) -> SchemaRegistry {
        let mut subject_names:SubjectMap = HashMap::new();

        if Path::new(SUBJECTS_CACHE).exists() {
            let file = File::open(SUBJECTS_CACHE).unwrap();
            let subject_vector:SubjectVector = bincode::deserialize_from(file).unwrap();

            for (schema_id, subject_name) in subject_vector {
                subject_names.insert(schema_id, subject_name);
            }
        }

        SchemaRegistry {
            config,
            seen_schemas: HashSet::new(),
            subject_names
        }
    }

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

    pub fn get_subject_name(self: &mut Self, schema_id: u32) -> Result<&Option<String>, Box<dyn Error>> {

        if !self.subject_names.contains_key(&schema_id) {
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

            {
                let mut subject_vector:SubjectVector = if Path::new(SUBJECTS_CACHE).exists() {
                    let file = File::open(SUBJECTS_CACHE)?;
                    bincode::deserialize_from(file)?
                } else {
                    Vec::new()
                };

                subject_vector.push((schema_id, subject_name.to_owned()));

                let file = File::create(SUBJECTS_CACHE)?;
                bincode::serialize_into(file, &subject_vector)?;
            }

            self.subject_names.insert(schema_id, subject_name);




        }

        Ok(self.subject_names.get(&schema_id).unwrap())
    }

    pub fn new_schema(self: &mut Self, schema_id: u32) -> Result<Option<String>, Box<dyn Error>> {
        if !self.seen_schemas.contains(&schema_id) {
            let body: SchemaRequestResponse = self.registry_get(format!(
                "schemas/ids/{}",
                schema_id
            ))?.json()?;

            self.seen_schemas.insert(schema_id);

            Ok(Some(body.schema))
        } else {
            Ok(None)
        }
    }
}