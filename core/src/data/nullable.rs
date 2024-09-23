//! Submodule providing the `Nullable` enum and associated traits.

#[derive(Debug, Clone, Copy, Hash)]
/// Enum representing a nullable value.
///
/// # Implementations details
/// This enum differs from the `Option` enum in that it does not represent
/// a `Option::None` value, but rather a `Nullable::Null` SQL `NULL` value.
pub enum Nullable<T> {
    Null,
    Entry(T),
}
