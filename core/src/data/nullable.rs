//! Submodule providing the `Nullable` enum and associated traits.

use core::ops::Not;

#[derive(Debug, Clone, Copy, Hash)]
/// Enum representing a nullable value.
///
/// # Implementations details
/// This enum differs from the `Option` enum in that it does not represent
/// a `Option::None` value, but rather a `Nullable::Null` SQL `NULL` value.
pub enum Nullable<T> {
    /// Represents an SQL `NULL` value.
    Null,
    /// Represents a non-`NULL` value.
    Entry(T),
}

impl<T> Nullable<T> {
    /// When the value is non-null, maps it to another nullable value.
    pub(crate) fn then<U, F: FnOnce(T) -> Nullable<U>>(self, f: F) -> Nullable<U> {
        match self {
            Nullable::Null => Nullable::Null,
            Nullable::Entry(value) => f(value),
        }
    }

    /// Maps a nullable value to another nullable value, providing a default value if the value is `NULL`.
    pub(crate) fn map_or<U, F: FnOnce(T) -> U>(self, default: U, f: F) -> U {
        match self {
            Nullable::Null => default,
            Nullable::Entry(value) => f(value),
        }
    }
}

#[cfg(test)]
impl<T> Nullable<T> {
    /// Returns `true` if the value is `Nullable::Null`.
    pub(crate) fn is_null(&self) -> bool {
        matches!(self, Nullable::Null)
    }

    /// Returns the value if it is an `Entry`, otherwise panics.
    pub(crate) fn unwrap(self) -> T {
        match self {
            Nullable::Null => panic!("called `Nullable::unwrap()` on a `Null` value"),
            Nullable::Entry(value) => value,
        }
    }
}

impl<T> From<T> for Nullable<T> {
    fn from(value: T) -> Self {
        Nullable::Entry(value)
    }
}

impl<T: Not<Output = T>> Not for Nullable<T> {
    type Output = Self;

    /// Negates a nullable value, returning NULL if the value is NULL.
    fn not(self) -> Self::Output {
        match self {
            Nullable::Null => Nullable::Null,
            Nullable::Entry(value) => Nullable::Entry(!value),
        }
    }
}
