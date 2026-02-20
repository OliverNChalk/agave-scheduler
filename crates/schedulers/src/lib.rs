#[macro_use]
extern crate static_assertions;

#[cfg(feature = "batch")]
pub mod batch;
pub mod events;
pub mod shared;
