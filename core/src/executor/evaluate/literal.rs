use {
    crate::{
        ast::{BinaryOperator, DataType, ToSql},
        data::{BigDecimalExt, StringExt},
        result::Result,
    },
    Literal::*,
    bigdecimal::BigDecimal,
    serde::Serialize,
    std::{
        borrow::Cow,
        cmp::Ordering,
        fmt::{self, Debug, Display},
    },
    thiserror::Error,
    utils::Tribool,
};

#[derive(Error, Serialize, Debug, PartialEq, Eq)]
pub enum LiteralError {
    #[error("unsupported literal binary operation {} {} {}", .left, .op.to_sql(), .right)]
    UnsupportedBinaryOperation {
        left: String,
        op: BinaryOperator,
        right: String,
    },

    #[error("a operand '{0}' is not integer type")]
    BitwiseNonIntegerOperand(String),

    #[error("given operands is not Number literal type")]
    BitwiseNonNumberLiteral,

    #[error("overflow occured while running bitwise operation")]
    BitwiseOperationOverflow,

    #[error("impossible conversion from {0} to {1} type")]
    ImpossibleConversion(String, String),

    #[error("the divisor should not be zero")]
    DivisorShouldNotBeZero,

    #[error("literal unary operation on non-numeric")]
    UnaryOperationOnNonNumeric,

    #[error("unreachable literal binary arithmetic")]
    UnreachableBinaryArithmetic,

    #[error("unreachable literal unary operation")]
    UnreachableUnaryOperation,

    #[error("operator doesn't exist: {base} {case} {pattern}", case = if *case_sensitive { "LIKE" } else { "ILIKE" })]
    LikeOnNonString {
        base: String,
        pattern: String,
        case_sensitive: bool,
    },

    #[error("literal {literal} is incompatible with data type {data_type:?}")]
    IncompatibleLiteralForDataType {
        data_type: DataType,
        literal: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Literal<'a> {
    Number(Cow<'a, BigDecimal>),
    Text(Cow<'a, str>),
}

impl Display for Literal<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Number(n) => write!(f, "{n}"),
            Literal::Text(t) => f.write_str(t),
        }
    }
}

fn unsupported_binary_op(left: &Literal, op: BinaryOperator, right: &Literal) -> LiteralError {
    LiteralError::UnsupportedBinaryOperation {
        left: left.to_string(),
        op,
        right: right.to_string(),
    }
}

impl<'a> Literal<'a> {
    pub fn evaluate_eq(&self, other: &Literal<'_>) -> Tribool {
        Tribool::from(self == other)
    }

    pub fn evaluate_cmp(&self, other: &Literal<'a>) -> Option<Ordering> {
        match (self, other) {
            (Number(l), Number(r)) => Some(l.cmp(r)),
            (Text(l), Text(r)) => Some(l.cmp(r)),
            _ => None,
        }
    }

    pub fn unary_plus(&self) -> Result<Self> {
        match self {
            Number(v) => Ok(Number(v.clone())),
            Text(_) => Err(LiteralError::UnaryOperationOnNonNumeric.into()),
        }
    }

    pub fn unary_minus(&self) -> Result<Self> {
        match self {
            Number(v) => Ok(Number(Cow::Owned(-v.as_ref()))),
            Text(_) => Err(LiteralError::UnaryOperationOnNonNumeric.into()),
        }
    }

    #[must_use]
    pub fn concat(self, other: Literal<'_>) -> Self {
        let convert = |literal| match literal {
            Number(v) => v.to_string(),
            Text(v) => v.into_owned(),
        };

        Literal::Text(Cow::Owned(convert(self) + &convert(other)))
    }

    pub fn add(&self, other: &Literal<'a>) -> Result<Literal<'static>> {
        match (self, other) {
            (Number(l), Number(r)) => Ok(Number(Cow::Owned(l.as_ref() + r.as_ref()))),
            _ => Err(unsupported_binary_op(self, BinaryOperator::Plus, other).into()),
        }
    }

    pub fn subtract(&self, other: &Literal<'a>) -> Result<Literal<'static>> {
        match (self, other) {
            (Number(l), Number(r)) => Ok(Number(Cow::Owned(l.as_ref() - r.as_ref()))),
            _ => Err(unsupported_binary_op(self, BinaryOperator::Minus, other).into()),
        }
    }

    pub fn multiply(&self, other: &Literal<'a>) -> Result<Literal<'static>> {
        match (self, other) {
            (Number(l), Number(r)) => Ok(Number(Cow::Owned(l.as_ref() * r.as_ref()))),
            _ => Err(unsupported_binary_op(self, BinaryOperator::Multiply, other).into()),
        }
    }

    pub fn divide(&self, other: &Literal<'a>) -> Result<Literal<'static>> {
        match (self, other) {
            (Number(l), Number(r)) => {
                if *r.as_ref() == 0.into() {
                    Err(LiteralError::DivisorShouldNotBeZero.into())
                } else {
                    Ok(Number(Cow::Owned(l.as_ref() / r.as_ref())))
                }
            }
            _ => Err(unsupported_binary_op(self, BinaryOperator::Divide, other).into()),
        }
    }

    pub fn bitwise_and(&self, other: &Literal<'a>) -> Result<Literal<'static>> {
        match (self, other) {
            (Number(l), Number(r)) => match (l.to_i64(), r.to_i64()) {
                (Some(l), Some(r)) => Ok(Number(Cow::Owned(BigDecimal::from(l & r)))),
                _ => Err(LiteralError::UnsupportedBinaryOperation {
                    left: self.to_string(),
                    op: BinaryOperator::BitwiseAnd,
                    right: other.to_string(),
                }
                .into()),
            },
            _ => Err(unsupported_binary_op(self, BinaryOperator::BitwiseAnd, other).into()),
        }
    }

    pub fn modulo(&self, other: &Literal<'a>) -> Result<Literal<'static>> {
        match (self, other) {
            (Number(l), Number(r)) => {
                if *r.as_ref() == 0.into() {
                    Err(LiteralError::DivisorShouldNotBeZero.into())
                } else {
                    Ok(Number(Cow::Owned(l.as_ref() % r.as_ref())))
                }
            }
            _ => Err(unsupported_binary_op(self, BinaryOperator::Modulo, other).into()),
        }
    }

    pub fn bitwise_shift_left(&self, other: &Literal<'a>) -> Result<Literal<'static>> {
        match (self, other) {
            (Number(l), Number(r)) => {
                let l = l
                    .to_i64()
                    .ok_or(LiteralError::BitwiseNonIntegerOperand(l.to_string()))?;
                if !r.is_integer_representation() {
                    return Err(LiteralError::BitwiseNonIntegerOperand(r.to_string()).into());
                }
                let r = r.to_u32().ok_or(LiteralError::ImpossibleConversion(
                    r.to_string(),
                    "u32".to_owned(),
                ))?;
                let res = l
                    .checked_shl(r)
                    .ok_or(LiteralError::BitwiseOperationOverflow)?;
                Ok(Number(Cow::Owned(BigDecimal::from(res))))
            }
            _ => Err(LiteralError::BitwiseNonNumberLiteral.into()),
        }
    }

    pub fn bitwise_shift_right(&self, other: &Literal<'a>) -> Result<Literal<'static>> {
        match (self, other) {
            (Number(l), Number(r)) => {
                let l = l
                    .to_i64()
                    .ok_or(LiteralError::BitwiseNonIntegerOperand(l.to_string()))?;
                if !r.is_integer_representation() {
                    return Err(LiteralError::BitwiseNonIntegerOperand(r.to_string()).into());
                }
                let r = r.to_u32().ok_or(LiteralError::ImpossibleConversion(
                    r.to_string(),
                    "u32".to_owned(),
                ))?;
                let res = l
                    .checked_shr(r)
                    .ok_or(LiteralError::BitwiseOperationOverflow)?;
                Ok(Number(Cow::Owned(BigDecimal::from(res))))
            }
            _ => Err(LiteralError::BitwiseNonNumberLiteral.into()),
        }
    }

    pub fn like(&self, other: &Literal<'a>, case_sensitive: bool) -> Result<bool> {
        match (self, other) {
            (Text(l), Text(r)) => l.like(r, case_sensitive),
            _ => Err(LiteralError::LikeOnNonString {
                base: self.to_string(),
                pattern: other.to_string(),
                case_sensitive,
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::Literal::{self, *},
        crate::{ast::BinaryOperator, executor::LiteralError},
        bigdecimal::BigDecimal,
        std::{borrow::Cow, str::FromStr},
        utils::Tribool,
    };

    fn num(n: i32) -> Literal<'static> {
        Number(Cow::Owned(BigDecimal::from(n)))
    }

    fn text(value: &str) -> Literal<'static> {
        Text(Cow::Owned(value.to_owned()))
    }

    #[test]
    fn arithmetic_and_bitwise() {
        assert_eq!(
            num(4).divide(&num(2)),
            Ok(Number(Cow::Owned(BigDecimal::from(2))))
        );
        assert_eq!(
            num(3).bitwise_and(&num(1)),
            Ok(Number(Cow::Owned(BigDecimal::from(1))))
        );
        assert_eq!(
            num(1).bitwise_shift_left(&num(2)),
            Ok(Number(Cow::Owned(BigDecimal::from(4))))
        );
    }

    #[test]
    fn concat_and_equality() {
        macro_rules! num_literal {
            ($num: expr) => {
                Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }

        assert_eq!(text("Foo").concat(text("Bar")), text("FooBar"));
        assert_eq!(num_literal!("1").concat(num_literal!("2")), text("12"));

        assert_eq!(
            Tribool::True,
            num_literal!("1").evaluate_eq(&num_literal!("1"))
        );
        assert_eq!(Tribool::False, num_literal!("1").evaluate_eq(&text("foo")));
    }

    #[test]
    fn comparison_for_numbers_and_text() {
        use std::cmp::Ordering;
        macro_rules! num_literal {
            ($num: expr) => {
                Number(Cow::Owned(BigDecimal::from_str($num).unwrap()))
            };
        }

        assert_eq!(
            num_literal!("1").evaluate_cmp(&num_literal!("2")),
            Some(Ordering::Less)
        );
        assert_eq!(text("a").evaluate_cmp(&text("b")), Some(Ordering::Less));
    }

    #[test]
    fn modulo_errors_on_text() {
        let err = text("foo").modulo(&num(2)).unwrap_err();
        assert!(matches!(
            err,
            crate::result::Error::Literal(LiteralError::UnsupportedBinaryOperation {
                op: BinaryOperator::Modulo,
                ..
            })
        ));
    }
}
