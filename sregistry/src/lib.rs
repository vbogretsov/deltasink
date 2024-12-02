pub mod avro;
pub mod client;

pub use client::Client;
pub use client::Subject;
pub use client::Reference;

#[derive(Debug)]
pub enum RegistryError {
    ExpectedRecord,
    ClientError(String),
    ResolutionFailed(String),
    DeserializationFailed(String),
}
