//! The Expr layout stored in v1/v2 schemas, before Regex shifted its variant tags.

use {
    super::{
        SchemaDecodeError,
        function::{Aggregate, Function},
        query::Query,
    },
    gluesql_core::{
        ast::{self, DataType, DateTimeField, Literal, UnaryOperator},
        data::Value,
    },
    serde::Deserialize,
};

#[derive(Deserialize)]
pub(super) enum Expr {
    Identifier(String),
    CompoundIdentifier {
        alias: String,
        ident: String,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    Like {
        expr: Box<Expr>,
        negated: bool,
        pattern: Box<Expr>,
    },
    ILike {
        expr: Box<Expr>,
        negated: bool,
        pattern: Box<Expr>,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Nested(Box<Expr>),
    Literal(Literal),
    Value(Value),
    TypedString {
        data_type: DataType,
        value: String,
    },
    Function(Box<Function>),
    Aggregate(Box<Aggregate>),
    Exists {
        subquery: Box<Query>,
        negated: bool,
    },
    Subquery(Box<Query>),
    Case {
        operand: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    ArrayIndex {
        obj: Box<Expr>,
        indexes: Vec<Expr>,
    },
    Interval {
        expr: Box<Expr>,
        leading_field: Option<DateTimeField>,
        last_field: Option<DateTimeField>,
    },
    Array {
        elem: Vec<Expr>,
    },
}

impl Expr {
    pub(super) fn into_current(self) -> Result<ast::Expr, SchemaDecodeError> {
        Ok(match self {
            Self::Identifier(ident) => ast::Expr::Identifier(ident),
            Self::CompoundIdentifier { alias, ident } => {
                ast::Expr::CompoundIdentifier { alias, ident }
            }
            Self::IsNull(expr) => ast::Expr::IsNull(Box::new(expr.into_current()?)),
            Self::IsNotNull(expr) => ast::Expr::IsNotNull(Box::new(expr.into_current()?)),
            Self::InList {
                expr,
                list,
                negated,
            } => ast::Expr::InList {
                expr: Box::new(expr.into_current()?),
                list: list
                    .into_iter()
                    .map(Expr::into_current)
                    .collect::<Result<_, _>>()?,
                negated,
            },
            Self::InSubquery {
                expr,
                subquery,
                negated,
            } => ast::Expr::InSubquery {
                expr: Box::new(expr.into_current()?),
                subquery: Box::new(subquery.into_current()?),
                negated,
            },
            Self::Between {
                expr,
                negated,
                low,
                high,
            } => ast::Expr::Between {
                expr: Box::new(expr.into_current()?),
                negated,
                low: Box::new(low.into_current()?),
                high: Box::new(high.into_current()?),
            },
            Self::Like {
                expr,
                negated,
                pattern,
            } => ast::Expr::Like {
                expr: Box::new(expr.into_current()?),
                negated,
                pattern: Box::new(pattern.into_current()?),
            },
            Self::ILike {
                expr,
                negated,
                pattern,
            } => ast::Expr::ILike {
                expr: Box::new(expr.into_current()?),
                negated,
                pattern: Box::new(pattern.into_current()?),
            },
            Self::BinaryOp { left, op, right } => ast::Expr::BinaryOp {
                left: Box::new(left.into_current()?),
                op: op.into_current(),
                right: Box::new(right.into_current()?),
            },
            Self::UnaryOp { op, expr } => ast::Expr::UnaryOp {
                op,
                expr: Box::new(expr.into_current()?),
            },
            Self::Nested(expr) => ast::Expr::Nested(Box::new(expr.into_current()?)),
            Self::Literal(literal) => ast::Expr::Literal(literal),
            Self::Value(value) => ast::Expr::Value(value),
            Self::TypedString { data_type, value } => ast::Expr::TypedString { data_type, value },
            Self::Function(function) => ast::Expr::Function(Box::new(function.into_current()?)),
            Self::Aggregate(aggregate) => ast::Expr::Aggregate(Box::new(aggregate.into_current()?)),
            Self::Exists { subquery, negated } => ast::Expr::Exists {
                subquery: Box::new(subquery.into_current()?),
                negated,
            },
            Self::Subquery(query) => ast::Expr::Subquery(Box::new(query.into_current()?)),
            Self::Case {
                operand,
                when_then,
                else_result,
            } => ast::Expr::Case {
                operand: operand
                    .map(|expr| expr.into_current().map(Box::new))
                    .transpose()?,
                when_then: when_then
                    .into_iter()
                    .map(|(when, then)| Ok((when.into_current()?, then.into_current()?)))
                    .collect::<Result<_, SchemaDecodeError>>()?,
                else_result: else_result
                    .map(|expr| expr.into_current().map(Box::new))
                    .transpose()?,
            },
            Self::ArrayIndex { obj, indexes } => ast::Expr::ArrayIndex {
                obj: Box::new(obj.into_current()?),
                indexes: indexes
                    .into_iter()
                    .map(Expr::into_current)
                    .collect::<Result<_, _>>()?,
            },
            Self::Interval {
                expr,
                leading_field,
                last_field,
            } => ast::Expr::Interval {
                expr: Box::new(expr.into_current()?),
                leading_field,
                last_field,
            },
            Self::Array { elem } => ast::Expr::Array {
                elem: elem
                    .into_iter()
                    .map(Expr::into_current)
                    .collect::<Result<_, _>>()?,
            },
        })
    }
}

// Preserve the operator variants and their order from v0.19.0.
#[derive(Deserialize)]
pub(super) enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    StringConcat,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
    NotEq,
    And,
    Or,
    Xor,
    BitwiseAnd,
    BitwiseShiftLeft,
    BitwiseShiftRight,
    Arrow,
}

impl BinaryOperator {
    fn into_current(self) -> ast::BinaryOperator {
        match self {
            Self::Plus => ast::BinaryOperator::Plus,
            Self::Minus => ast::BinaryOperator::Minus,
            Self::Multiply => ast::BinaryOperator::Multiply,
            Self::Divide => ast::BinaryOperator::Divide,
            Self::Modulo => ast::BinaryOperator::Modulo,
            Self::StringConcat => ast::BinaryOperator::StringConcat,
            Self::Gt => ast::BinaryOperator::Gt,
            Self::Lt => ast::BinaryOperator::Lt,
            Self::GtEq => ast::BinaryOperator::GtEq,
            Self::LtEq => ast::BinaryOperator::LtEq,
            Self::Eq => ast::BinaryOperator::Eq,
            Self::NotEq => ast::BinaryOperator::NotEq,
            Self::And => ast::BinaryOperator::And,
            Self::Or => ast::BinaryOperator::Or,
            Self::Xor => ast::BinaryOperator::Xor,
            Self::BitwiseAnd => ast::BinaryOperator::BitwiseAnd,
            Self::BitwiseShiftLeft => ast::BinaryOperator::BitwiseShiftLeft,
            Self::BitwiseShiftRight => ast::BinaryOperator::BitwiseShiftRight,
            Self::Arrow => ast::BinaryOperator::Arrow,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{super::expr as old, Expr},
        crate::migration::decode,
        gluesql_core::{
            ast::{self as current, DataType, DateTimeField, Literal, UnaryOperator},
            data::Value,
        },
    };

    #[test]
    fn old_expr_tags_keep_their_meaning_after_regex_shifts_current_tags() {
        // Regex was inserted before these variants in v0.20.0:
        // Literal 12 -> 13, Value 13 -> 14, TypedString 14 -> 15.
        let literal = [12, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, b'x'];
        let value = [13, 0, 0, 0, 4, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0];
        let typed = [14, 0, 0, 0, 13, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, b'x'];
        for (bytes, current_tag, expected) in [
            (
                literal.as_slice(),
                13_u32,
                current::Expr::Literal(Literal::QuotedString("x".into())),
            ),
            (value.as_slice(), 14, current::Expr::Value(Value::I64(7))),
            (
                typed.as_slice(),
                15,
                current::Expr::TypedString {
                    data_type: DataType::Text,
                    value: "x".into(),
                },
            ),
        ] {
            let restored = decode::deserialize::<Expr>(bytes)
                .unwrap()
                .into_current()
                .unwrap();
            assert_eq!(restored, expected);
            assert_eq!(
                &bincode::serialize(&restored).unwrap()[..4],
                &current_tag.to_le_bytes()
            );
        }
    }

    macro_rules! test {
        ($case:expr) => {{
            let input = {
                #[allow(unused_imports)]
                use old::{BinaryOperator, Expr, Function};
                #[allow(dead_code)]
                fn ident(name: &str) -> old::Expr {
                    old::Expr::Identifier(name.into())
                }
                $case
            };
            let expected = {
                #[allow(unused_imports)]
                use current::{BinaryOperator, Expr, Function};
                #[allow(dead_code)]
                fn ident(name: &str) -> current::Expr {
                    current::Expr::Identifier(name.into())
                }
                $case
            };
            let actual = input.into_current().unwrap();
            assert_eq!(actual, expected, "{}", stringify!($case));
        }};
    }

    #[test]
    fn expression_variants() {
        test!(Expr::Identifier("column".into()));
        test!(Expr::CompoundIdentifier {
            alias: "source".into(),
            ident: "column".into(),
        });
        test!(Expr::IsNull(Box::new(ident("value"))));
        test!(Expr::IsNotNull(Box::new(ident("value"))));
        test!(Expr::InList {
            expr: Box::new(ident("value")),
            list: vec![],
            negated: false,
        });
        test!(Expr::InList {
            expr: Box::new(ident("value")),
            list: vec![ident("first"), ident("second")],
            negated: true,
        });
        test!(Expr::Between {
            expr: Box::new(ident("value")),
            negated: false,
            low: Box::new(ident("low")),
            high: Box::new(ident("high")),
        });
        test!(Expr::Between {
            expr: Box::new(ident("value")),
            negated: true,
            low: Box::new(ident("low")),
            high: Box::new(ident("high")),
        });
        test!(Expr::Like {
            expr: Box::new(ident("text")),
            negated: false,
            pattern: Box::new(ident("pattern")),
        });
        test!(Expr::Like {
            expr: Box::new(ident("text")),
            negated: true,
            pattern: Box::new(ident("pattern")),
        });
        test!(Expr::ILike {
            expr: Box::new(ident("text")),
            negated: false,
            pattern: Box::new(ident("pattern")),
        });
        test!(Expr::ILike {
            expr: Box::new(ident("text")),
            negated: true,
            pattern: Box::new(ident("pattern")),
        });
        test!(Expr::BinaryOp {
            left: Box::new(ident("left")),
            op: BinaryOperator::Minus,
            right: Box::new(ident("right")),
        });
        test!(Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(ident("value")),
        });
        test!(Expr::Nested(Box::new(ident("value"))));
        test!(Expr::Literal(Literal::QuotedString("text".into())));
        test!(Expr::Value(Value::I64(17)));
        test!(Expr::TypedString {
            data_type: DataType::Date,
            value: "2020-01-02".into(),
        });
        test!(Expr::Function(Box::new(Function::Abs(ident("value")))));
        test!(Expr::Case {
            operand: None,
            when_then: vec![],
            else_result: None,
        });
        test!(Expr::Case {
            operand: Some(Box::new(ident("operand"))),
            when_then: vec![
                (ident("when_first"), ident("then_first")),
                (ident("when_second"), ident("then_second")),
            ],
            else_result: Some(Box::new(ident("fallback"))),
        });
        test!(Expr::ArrayIndex {
            obj: Box::new(ident("array")),
            indexes: vec![],
        });
        test!(Expr::ArrayIndex {
            obj: Box::new(ident("array")),
            indexes: vec![ident("row"), ident("column")],
        });
        test!(Expr::Interval {
            expr: Box::new(ident("duration")),
            leading_field: None,
            last_field: None,
        });
        test!(Expr::Interval {
            expr: Box::new(ident("duration")),
            leading_field: Some(DateTimeField::Day),
            last_field: Some(DateTimeField::Second),
        });
        test!(Expr::Array { elem: vec![] });
        test!(Expr::Array {
            elem: vec![ident("first"), ident("second")],
        });
    }

    #[test]
    fn binary_operator_variants() {
        macro_rules! test {
            ($case:expr) => {{
                let input = {
                    use old::BinaryOperator;
                    $case
                };
                let expected = {
                    use current::BinaryOperator;
                    $case
                };
                assert_eq!(input.into_current(), expected, "{}", stringify!($case));
            }};
        }

        test!(BinaryOperator::Plus);
        test!(BinaryOperator::Minus);
        test!(BinaryOperator::Multiply);
        test!(BinaryOperator::Divide);
        test!(BinaryOperator::Modulo);
        test!(BinaryOperator::StringConcat);
        test!(BinaryOperator::Gt);
        test!(BinaryOperator::Lt);
        test!(BinaryOperator::GtEq);
        test!(BinaryOperator::LtEq);
        test!(BinaryOperator::Eq);
        test!(BinaryOperator::NotEq);
        test!(BinaryOperator::And);
        test!(BinaryOperator::Or);
        test!(BinaryOperator::Xor);
        test!(BinaryOperator::BitwiseAnd);
        test!(BinaryOperator::BitwiseShiftLeft);
        test!(BinaryOperator::BitwiseShiftRight);
        test!(BinaryOperator::Arrow);
    }
}
