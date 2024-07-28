//! Submodule implementing the CHECK constraint AST node.

use serde::{Deserialize, Serialize};

use super::{Expr, ToSql};

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Struct representing a CHECK constraint.
///
/// # Examples
/// A check constraint may be defined as follows:
///
/// ## Case associated with a column
/// ```sql
/// CREATE TABLE t (
///    a INT CHECK (a > 0)
/// );
/// ```
///
/// ## Case associated with a table
/// ```sql
/// CREATE TABLE t (
///    a INT,
///    b INT,
///    CHECK (a > 0 AND b > 0)
/// );
/// ```
///
/// ## Case associated with a table with a name
/// ```sql
/// CREATE TABLE t (
///    a INT,
///    b INT,
///    CONSTRAINT check_a_b CHECK (a > 0 AND b > 0)
/// );
/// ```
pub struct CheckConstraint {
    /// Optional name of the CHECK constraint.
    pub name: Option<String>,

    /// The expression that defines the CHECK constraint.
    /// This expression should evaluate to a boolean value.
    pub expression: Expr,
}

impl CheckConstraint {
    /// Creates a new `CheckConstraint` instance.
    pub fn new(name: Option<String>, expression: Expr) -> Self {
        Self { name, expression }
    }

    /// Creates a new anonymous `CheckConstraint` instance.
    pub fn anonymous(expression: Expr) -> Self {
        Self::new(None, expression)
    }
}

impl ToSql for CheckConstraint {
    /// Converts the `CheckConstraint` to its SQL representation.
    /// 
    /// # Implementation notes
    /// Since we normalize all column-level check constraints to
    /// a standard table-level check constraint, we do not need to
    /// differentiate between the two cases, but we always output
    /// the constraint as a table-level constraint.
    fn to_sql(&self) -> String {
        let name = match &self.name {
            Some(name) => format!("CONSTRAINT {} ", name),
            None => "".to_string(),
        };

        format!("{}CHECK {}", name, self.expression.to_sql())
    }
}

#[cfg(test)]
mod tests {
    use bigdecimal::BigDecimal;

    use crate::ast::{AstLiteral, BinaryOperator};

    use super::*;

    #[test]
    fn test_check_constraint() {
        let check = CheckConstraint::anonymous(Expr::BinaryOp{
            left: Box::new(Expr::Identifier("a".to_string())),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Literal(AstLiteral::Number(BigDecimal::from(0)))),
        });

        assert_eq!(check.to_sql(), "CHECK \"a\" > 0");
    }

    #[test]
    fn test_check_constraint_with_name() {
        let check = CheckConstraint::new(
            Some("check_a".to_string()),
            Expr::BinaryOp{
                left: Box::new(Expr::Identifier("a".to_string())),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Literal(AstLiteral::Number(BigDecimal::from(0)))),
            }
        );

        assert_eq!(check.to_sql(), "CONSTRAINT check_a CHECK \"a\" > 0");
    }
}