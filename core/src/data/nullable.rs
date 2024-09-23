//! Submodule providing the `Nullable` enum and associated traits.

use core::ops::{Add, Div, Mul, Not, Sub};
use std::ops::{
    AddAssign, BitAnd, BitAndAssign, BitOr, BitOrAssign, DivAssign, MulAssign, SubAssign,
};

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
/// assert!(!null.is_entry());
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
    /// assert!(!null.is_entry());
    ///
    /// let entry: Nullable<i32> = 10.into();
    ///
    /// assert!(!entry.is_null());
    /// assert!(entry.is_entry());
    /// ```
    pub fn is_null(&self) -> bool {
        matches!(self, Nullable::Null)
    }

    /// Returns `true` if the value is `Nullable::Entry`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let null: Nullable<i32> = Nullable::Null;
    ///
    /// assert!(null.is_null());
    /// assert!(!null.is_entry());
    ///
    /// let entry: Nullable<i32> = 10.into();
    ///
    /// assert!(!entry.is_null());
    /// assert!(entry.is_entry());
    /// ```
    pub fn is_entry(&self) -> bool {
        matches!(self, Nullable::Entry(_))
    }

    /// Returns the value if it is `Nullable::Entry`, otherwise returns `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let null: Nullable<i32> = Nullable::Null;
    ///
    /// assert_eq!(null.as_entry(), None);
    ///
    /// let entry: Nullable<i32> = 10.into();
    ///
    /// assert_eq!(entry.as_entry(), Some(&10));
    /// ```
    pub fn as_entry(&self) -> Option<&T> {
        match self {
            Nullable::Null => None,
            Nullable::Entry(value) => Some(value),
        }
    }

    /// Returns the value if it is `Nullable::Entry`, otherwise returns `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let mut null: Nullable<i32> = Nullable::Null;
    ///
    /// assert_eq!(null.as_entry_mut(), None);
    ///
    /// let mut entry: Nullable<i32> = 10.into();
    ///
    /// assert_eq!(entry.as_entry_mut(), Some(&mut 10));
    /// ```
    pub fn as_entry_mut(&mut self) -> Option<&mut T> {
        match self {
            Nullable::Null => None,
            Nullable::Entry(value) => Some(value),
        }
    }

    /// Returns the value if it is `Nullable::Entry`, otherwise returns `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let null: Nullable<i32> = Nullable::Null;
    ///
    /// assert_eq!(null.into_entry(), None);
    ///
    /// let entry: Nullable<i32> = 10.into();
    ///
    /// assert_eq!(entry.into_entry(), Some(10));
    /// ```
    pub fn into_entry(self) -> Option<T> {
        self.into()
    }

    /// Returns the value if it is `Nullable::Entry`, otherwise returns `default`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let null: Nullable<i32> = Nullable::Null;
    ///
    /// assert_eq!(null.unwrap_or(10), 10);
    ///
    /// let entry: Nullable<i32> = 20.into();
    ///
    /// assert_eq!(entry.unwrap_or(10), 20);
    /// ```
    pub fn unwrap_or(self, default: T) -> T {
        match self {
            Nullable::Null => default,
            Nullable::Entry(value) => value,
        }
    }

    /// Returns the value if it is `Nullable::Entry`, otherwise panics.
    ///
    /// # Panics
    /// * If the value is `Nullable::Null`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let entry: Nullable<i32> = 20.into();
    ///
    /// assert_eq!(entry.unwrap(), 20);
    /// ```
    ///
    /// ```should_panic
    /// use gluesql_core::data::Nullable;
    ///
    /// let null: Nullable<i32> = Nullable::Null;
    ///
    /// null.unwrap();
    /// ```
    pub fn unwrap(self) -> T {
        self.expect("called `Nullable::unwrap()` on a `Nullable::Null` value")
    }

    /// Returns the value if it is `Nullable::Entry`, otherwise panics with the provided message.
    ///
    /// # Panics
    /// * If the value is `Nullable::Null`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let entry: Nullable<i32> = 20.into();
    ///
    /// assert_eq!(entry.expect("value is not null"), 20);
    /// ```
    ///
    /// ```should_panic
    /// use gluesql_core::data::Nullable;
    ///
    /// let null: Nullable<i32> = Nullable::Null;
    ///
    /// null.expect("value is not null");
    /// ```
    pub fn expect(self, message: &str) -> T {
        match self {
            Nullable::Null => panic!("{}", message),
            Nullable::Entry(value) => value,
        }
    }
}

impl<T> From<T> for Nullable<T> {
    fn from(value: T) -> Self {
        Nullable::Entry(value)
    }
}

impl<T> From<Option<T>> for Nullable<T> {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => Nullable::Entry(value),
            None => Nullable::Null,
        }
    }
}

impl<T> From<Nullable<T>> for Option<T> {
    fn from(value: Nullable<T>) -> Self {
        match value {
            Nullable::Null => None,
            Nullable::Entry(value) => Some(value),
        }
    }
}

impl<T: Not<Output = T>> Not for Nullable<T> {
    type Output = Self;

    /// Negates a nullable value, returning NULL if the value is NULL.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let null: Nullable<bool> = Nullable::Null;
    ///
    /// assert!(null.is_null());
    /// assert!((!null).is_null());
    ///
    /// let entry: Nullable<bool> = true.into();
    ///
    /// assert_eq!((entry).unwrap(), true);
    /// assert_eq!((!entry).unwrap(), false);
    /// ```
    fn not(self) -> Self::Output {
        match self {
            Nullable::Null => Nullable::Null,
            Nullable::Entry(value) => Nullable::Entry(!value),
        }
    }
}

impl<T: Add<Output = T>> Add for Nullable<T> {
    type Output = Self;

    /// Adds two nullable values, returning NULL if either value is NULL.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let lhs: Nullable<i32> = Nullable::Null;
    /// let rhs: Nullable<i32> = 10.into();
    ///
    /// assert!((lhs + rhs).is_null());
    /// assert!((rhs + lhs).is_null());
    ///
    /// let lhs: Nullable<i32> = 20.into();
    ///
    /// assert_eq!((lhs + rhs).unwrap(), 30);
    /// ```
    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Nullable::Null, _) | (_, Nullable::Null) => Nullable::Null,
            (Nullable::Entry(lhs), Nullable::Entry(rhs)) => Nullable::Entry(lhs + rhs),
        }
    }
}

impl<T: Add<Output = T> + Copy> AddAssign for Nullable<T> {
    /// Adds two nullable values, returning NULL if either value is NULL.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let mut lhs: Nullable<i32> = Nullable::Null;
    /// let rhs: Nullable<i32> = 10.into();
    ///
    /// lhs += rhs;
    ///
    /// assert!(lhs.is_null());
    ///
    /// let mut lhs: Nullable<i32> = 20.into();
    ///
    /// lhs += rhs;
    ///
    /// assert_eq!(lhs.unwrap(), 30);
    /// ```
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}

impl<T: Sub<Output = T>> Sub for Nullable<T> {
    type Output = Self;

    /// Subtracts two nullable values, returning NULL if either value is NULL.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let lhs: Nullable<i32> = Nullable::Null;
    /// let rhs: Nullable<i32> = 10.into();
    ///
    /// assert!((lhs - rhs).is_null());
    /// assert!((rhs - lhs).is_null());
    ///
    /// let lhs: Nullable<i32> = 20.into();
    ///
    /// assert_eq!((lhs - rhs).unwrap(), 10);
    /// assert_eq!((rhs - lhs).unwrap(), -10);
    /// ```
    ///
    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Nullable::Null, _) | (_, Nullable::Null) => Nullable::Null,
            (Nullable::Entry(lhs), Nullable::Entry(rhs)) => Nullable::Entry(lhs - rhs),
        }
    }
}

impl<T: Sub<Output = T> + Copy> SubAssign for Nullable<T> {
    /// Subtracts two nullable values, returning NULL if either value is NULL.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let mut lhs: Nullable<i32> = Nullable::Null;
    /// let rhs: Nullable<i32> = 10.into();
    ///
    /// lhs -= rhs;
    ///
    /// assert!(lhs.is_null());
    ///
    /// let mut lhs: Nullable<i32> = 20.into();
    ///
    /// lhs -= rhs;
    ///
    /// assert_eq!(lhs.unwrap(), 10);
    /// ```
    fn sub_assign(&mut self, rhs: Self) {
        *self = *self - rhs;
    }
}

impl<T: Mul<Output = T>> Mul for Nullable<T> {
    type Output = Self;

    /// Multiplies two nullable values, returning NULL if either value is NULL.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let lhs: Nullable<i32> = Nullable::Null;
    /// let rhs: Nullable<i32> = 10.into();
    ///
    /// assert!((lhs * rhs).is_null());
    /// assert!((rhs * lhs).is_null());
    ///
    /// let lhs: Nullable<i32> = 20.into();
    ///
    /// assert_eq!((lhs * rhs).unwrap(), 200);
    /// ```
    ///
    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Nullable::Null, _) | (_, Nullable::Null) => Nullable::Null,
            (Nullable::Entry(lhs), Nullable::Entry(rhs)) => Nullable::Entry(lhs * rhs),
        }
    }
}

impl<T: Mul<Output = T> + Copy> MulAssign for Nullable<T> {
    /// Multiplies two nullable values, returning NULL if either value is NULL.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let mut lhs: Nullable<i32> = Nullable::Null;
    /// let rhs: Nullable<i32> = 10.into();
    ///
    /// lhs *= rhs;
    ///
    /// assert!(lhs.is_null());
    ///
    /// let mut lhs: Nullable<i32> = 20.into();
    ///
    /// lhs *= rhs;
    ///
    /// assert_eq!(lhs.unwrap(), 200);
    /// ```
    fn mul_assign(&mut self, rhs: Self) {
        *self = *self * rhs;
    }
}

impl<T: Div<Output = T>> Div for Nullable<T> {
    type Output = Self;

    /// Divides two nullable values, returning NULL if either value is NULL.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let lhs: Nullable<i32> = Nullable::Null;
    /// let rhs: Nullable<i32> = 10.into();
    ///
    /// assert!((lhs / rhs).is_null());
    /// assert!((rhs / lhs).is_null());
    ///
    /// let lhs: Nullable<i32> = 20.into();
    ///
    /// assert_eq!((lhs / rhs).unwrap(), 2);
    /// ```
    ///
    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Nullable::Null, _) | (_, Nullable::Null) => Nullable::Null,
            (Nullable::Entry(lhs), Nullable::Entry(rhs)) => Nullable::Entry(lhs / rhs),
        }
    }
}

impl<T: Div<Output = T> + Copy> DivAssign for Nullable<T> {
    /// Divides two nullable values, returning NULL if either value is NULL.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let mut lhs: Nullable<i32> = Nullable::Null;
    /// let rhs: Nullable<i32> = 10.into();
    ///
    /// lhs /= rhs;
    ///
    /// assert!(lhs.is_null());
    ///
    /// let mut lhs: Nullable<i32> = 20.into();
    ///
    /// lhs /= rhs;
    ///
    /// assert_eq!(lhs.unwrap(), 2);
    /// ```
    fn div_assign(&mut self, rhs: Self) {
        *self = *self / rhs;
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
    /// assert_eq!((lhs | rhs).unwrap(), 30);
    /// ```
    ///
    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Nullable::Null, _) | (_, Nullable::Null) => Nullable::Null,
            (Nullable::Entry(lhs), Nullable::Entry(rhs)) => Nullable::Entry(lhs | rhs),
        }
    }
}

impl<T: BitOr<Output = T> + Copy> BitOrAssign for Nullable<T> {
    /// Bitwise ORs two nullable values, returning NULL if either value is NULL.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let mut lhs: Nullable<bool> = Nullable::Null;
    ///
    /// let rhs: Nullable<bool> = true.into();
    ///
    /// lhs |= rhs;
    ///
    /// assert!(lhs.is_null());
    ///
    /// let mut lhs: Nullable<bool> = true.into();
    ///
    /// lhs |= rhs;
    ///
    /// assert_eq!(lhs.unwrap(), true);
    ///
    /// let mut lhs: Nullable<bool> = false.into();
    ///
    /// assert_eq!(lhs.unwrap(), false);
    ///
    /// lhs |= rhs;
    ///
    /// assert_eq!(lhs.unwrap(), true);
    /// ```
    ///
    fn bitor_assign(&mut self, rhs: Self) {
        *self = *self | rhs;
    }
}

impl<T: BitAnd<Output = T>> BitAnd for Nullable<T> {
    type Output = Self;

    /// Bitwise ANDs two nullable values, returning NULL if either value is NULL.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let lhs: Nullable<i32> = Nullable::Null;
    /// let rhs: Nullable<i32> = 10.into();
    ///
    /// assert!((lhs & rhs).is_null());
    /// assert!((rhs & lhs).is_null());
    ///
    /// let lhs: Nullable<i32> = 20.into();
    ///
    /// assert_eq!((lhs & rhs).unwrap(), 0);
    /// ```
    ///
    fn bitand(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Nullable::Null, _) | (_, Nullable::Null) => Nullable::Null,
            (Nullable::Entry(lhs), Nullable::Entry(rhs)) => Nullable::Entry(lhs & rhs),
        }
    }
}

impl<T: BitAnd<Output = T> + Copy> BitAndAssign for Nullable<T> {
    /// Bitwise ANDs two nullable values, returning NULL if either value is NULL.
    ///
    /// # Examples
    ///
    /// ```
    /// use gluesql_core::data::Nullable;
    ///
    /// let mut lhs: Nullable<bool> = Nullable::Null;
    ///
    /// let rhs: Nullable<bool> = true.into();
    ///
    /// lhs &= rhs;
    ///
    /// assert!(lhs.is_null());
    ///
    /// let mut lhs: Nullable<bool> = true.into();
    ///
    /// lhs &= rhs;
    ///
    /// assert_eq!(lhs.unwrap(), true);
    ///
    /// let mut lhs: Nullable<bool> = false.into();
    ///
    /// assert_eq!(lhs.unwrap(), false);
    ///
    /// lhs &= rhs;
    ///
    /// assert_eq!(lhs.unwrap(), false);
    ///
    /// let mut lhs: Nullable<bool> = false.into();
    ///
    /// lhs &= rhs;
    ///
    /// assert_eq!(lhs.unwrap(), false);
    /// ```
    fn bitand_assign(&mut self, rhs: Self) {
        *self = *self & rhs;
    }
}
