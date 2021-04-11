use std::error::Error;

pub use memory::MemoryAdapter;

mod memory;

pub enum Schema {
    Seen,
    New(String),
}

pub trait RegistryAdapter {
    fn get_schema(self: &mut Self, schema_id: u32) -> Result<Schema, Box<dyn Error>>;
    fn get_subject_name(self: &mut Self, schema_id: u32) -> Result<&Option<String>, Box<dyn Error>>;
}
