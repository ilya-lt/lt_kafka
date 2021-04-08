use crate::schema_registry::SchemaRegistry;
use std::collections::{HashSet, HashMap};
use crate::adapters::{RegistryAdapter, Schema};
use std::error::Error;

type SubjectMap = HashMap<u32, Option<String>>;

pub struct MemoryAdapter {
    seen_schemas: HashSet<u32>,
    subject_names: SubjectMap,
    schema_registry: SchemaRegistry
}

impl MemoryAdapter {
    pub fn new(schema_registry: SchemaRegistry) -> Self {
        MemoryAdapter {
            seen_schemas: HashSet::new(),
            subject_names: HashMap::new(),
            schema_registry
        }
    }
}

impl RegistryAdapter for MemoryAdapter {
    fn get_schema(self: &mut Self, schema_id: u32) -> Result<Schema, Box<dyn Error>> {
        if !self.seen_schemas.contains(&schema_id) {
            let schema = self.schema_registry.get_schema(schema_id)?;

            self.seen_schemas.insert(schema_id);

            Ok(Schema::New(schema))
        } else {
            Ok(Schema::Seen)
        }
    }

    fn get_subject_name(self: &mut Self, schema_id: u32) -> Result<&Option<String>, Box<dyn Error>> {
        if !self.subject_names.contains_key(&schema_id) {
            let subject_name = self.schema_registry.get_subject_name(schema_id)?;

            self.subject_names.insert(schema_id, subject_name);
        }

        Ok(self.subject_names.get(&schema_id).unwrap())
    }
}

