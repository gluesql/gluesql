//! Submodule providing the `Nullable` enum and associated traits.

use core::ops::Not;
use std::ops::BitOr;

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

impl<T: BitOr<Output = T>> BitOr for Nullable<T> {
    type Output = Self;

    /// Bitwise ORs two nullable values, returning NULL if either value is NULL.
    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Nullable::Null, _) | (_, Nullable::Null) => Nullable::Null,
            (Nullable::Entry(lhs), Nullable::Entry(rhs)) => Nullable::Entry(lhs | rhs),
        }
    }
}
