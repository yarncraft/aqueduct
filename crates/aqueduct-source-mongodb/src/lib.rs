//! MongoDB change-stream source. The resume token is the checkpoint position.
pub mod config;
pub mod mapping;
pub mod source;

pub use config::MongoConfig;
pub use source::MongoSource;
