//! `ripel` — convenience façade that re-exports `ripel-core` and `ripel-derive`.

#![deny(missing_docs)]

/// Re-export **everything** from ripel-core at the crate root, so users can `use ripel::*;`.
#[doc(inline)]
pub use ripel_core::*;

pub use ripel_core::value::{DynamicValue, ObjectValue};

/// Also expose ripel-core as a nested module if you like `ripel::core::...` paths.
pub use ripel_core as core;

/// Re-export the derive macro so downstream crates can `#[derive(Entity)]`
/// after depending only on `ripel` (with feature `derive` enabled).
#[cfg(feature = "derive")]
#[doc(inline)]
pub use ripel_derive::Entity;