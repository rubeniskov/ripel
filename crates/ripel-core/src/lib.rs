//! Core types and event-driven architecture for RIPeL

pub mod error;
pub mod event;
pub mod processor;
pub mod stream;
pub mod generated {
    #![allow(clippy::all)]
    #![allow(dead_code)]
    pub mod ripel {
        pub mod events {
            pub mod v1 {
                include!("generated/ripel.events.v1.rs");
            }
        }
    }
}

pub use error::*;
pub use event::*;
pub use processor::*;
pub use stream::*;

// Re-export specific protobuf types to avoid conflicts
pub use generated::ripel::events::v1::{
    Event as ProtoEvent,
    DatabaseChangeEvent as ProtoDatabaseChangeEvent,
    OperationType as ProtoOperationType,
};