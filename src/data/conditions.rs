//! This file contains all the types of comparison that can be created in a WHERE clause.
//! TODO: Add more documentation about how each comparison works.

use crate::Value;
use sqlparser::ast::{Expr, Query};

/// The conditions are the base type of condition, and are used to describe all WHERE closes supported.
pub enum Condition {
    /// Represents an expression of the type `column_name = value`
    /// For example: `country = "France"`
    Equals {
        column_name: String,
        value: Value,
    },
    /// Represents an expression of the type `column_name != value` or `column_name <> value`
    /// For example: `stock != 0` or `stock <> 0`
    NotEquals {
        column_name: String,
        value: Value,
    },
    /// Represents an expression of the type `column_name > value`
    /// For example: `price > 50`
    GreaterThan {
        column_name: String,
        value: Value,
    },
    /// Represents an expression of the type `column_name >= value`
    /// For example: `price >= 40`
    GreaterThanOrEquals {
        column_name: String,
        value: Value,
    },
    /// Represents an expression of the type `column_name < value`
    /// For example: `price < 60`
    LessThan {
        column_name: String,
        value: Value,
    },
    /// Represents an expression of the type `column_name <= value`
    /// For example: `price <= 35`
    LessThanOrEquals {
        column_name: String,
        value: Value,
    },
    /// Represents an expression of the type `column_name BETWEEN min AND max`
    /// For example: `price BETWEEN 20 AND 30`
    Between {
        column_name: String,
        min: Value,
        max: Value,
    },
    /// Represents an expression of the type `column_name IS NULL`
    /// For example: `price IS NULL`
    IsNull {
        column_name: String,
    },
    /// Represents an expression of the type `column_name IS NOT NULL`
    /// For example: `price IS NOT NULL`
    /// It's better to keep it as a condition instead of a `Not(Null(column_name))` as it allows for specific optimizations in the backend.
    IsNotNull {
        column_name: String,
    },
    /// Represents an expression of the type `column_name IS IN (values...)`
    /// For example: `country IS IN ("France", "Switzerland")`
    InList {
        column_name: String,
        list_elem: Vec<Value>,
    },
    CompareColumns {
        column_left: String,
    },
    /// Represents an expression of a Subquery, following this pattern: `(SELECT ...)`
    ExistsSubquery {
        query: Box<Query>,
    },
    /// An condition that always return true.
    /// For example `1 = 1`
    True,
    /// An expression that always return false.
    /// For example `1 = 2`
    False,
}

/// The links are the links between the different conditions in place.
pub enum Link {
    /// Condition is a special kind of link, that allows to convert a condition to a link
    /// This is mandatory, because if this link were not there,
    /// it would not be possible to chain conditions without difficulty.
    Condition(Condition),
    /// Represents the combination of two conditions.
    /// It is true only if **both** internal conditions are true.
    /// For example: `country = "France" AND price < 30`
    And(Box<Link>, Box<Link>),
    /// Represents the logical disjunction of the two internal conditions.
    /// It is true if **at least one** internal condition is true.
    /// For example: `country = "France" OR price < 30`
    Or(Box<Link>, Box<Link>),
    /// Represents the negation of the internal condition.
    /// It is true if the internal condition is false.
    /// For example: `NOT country = "France"`
    Not(Box<Link>),
}

impl Condition {
    pub fn from_bool(value: bool) -> Condition {
        if value {
            Condition::True
        } else {
            Condition::False
        }
    }
}
