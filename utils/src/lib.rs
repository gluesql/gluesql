#![cfg_attr(not(feature = "std"), no_std)]
#![deny(clippy::str_to_string)]

extern crate alloc;

#[cfg(feature = "std")]
mod hashmap;
mod indexmap;
mod or_stream;
mod tribool;
mod vector;

pub use {self::indexmap::IndexMap, or_stream::OrStream, tribool::Tribool, vector::Vector};

#[cfg(feature = "std")]
pub use hashmap::HashMapExt;
