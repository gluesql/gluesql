//! Submodule providing the `Nullable` enum and associated traits.

use core::ops::Not;
use std::ops::BitOr;

#[derive(Debug, Clone, Copy, Hash)]
/// Enum representing a nullable value.
///
/// # Implementations details
/// This enum differs from the `Option` enum in that it does not represent
/// a `Option::None` value, but rather a `Nullable::Null` SQL `NULL` value.
///
/// # Examples
///
/// ```
/// use gluesql_core::data::Nullable;
///
/// let null: Nullable<i32> = Nullable::Null;
///
/// assert!(null.is_null());
///
/// let entry: Nullable<i32> = 10.into();
///
/// assert!(!entry.is_null());
/// ```
///
pub enum Nullable<T> {
    /// Represents an SQL `NULL` value.
    Null,
    /// Represents a non-`NULL` value.
    Entry(T),
}

impl<T> Nullable<T> {
    /// Returns `true` if the value is `Nullable::Null`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let null: Nullable<i32> = Nullable::Null;
    ///
    /// assert!(null.is_null());
    ///
    /// let entry: Nullable<i32> = 10.into();
    ///
    /// assert!(!entry.is_null());
    /// ```
    pub fn is_null(&self) -> bool {
        matches!(self, Nullable::Null)
    }

    #[cfg(test)]
    /// Returns the value if it is an `Entry`, otherwise panics.
    pub fn unwrap(self) -> T {
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
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let lhs: Nullable<i32> = Nullable::Null;
    /// let rhs: Nullable<i32> = 10.into();
    ///
    /// assert!((lhs | rhs).is_null());
    /// assert!((rhs | lhs).is_null());
    ///
    /// let lhs: Nullable<i32> = 20.into();
    ///
    /// assert!(!(lhs | rhs).is_null());
    /// ```
    ///
    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Nullable::Null, _) | (_, Nullable::Null) => Nullable::Null,
            (Nullable::Entry(lhs), Nullable::Entry(rhs)) => Nullable::Entry(lhs | rhs),
        }
    }
}
