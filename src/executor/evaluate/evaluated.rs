use {
    super::EvaluateError,
    crate::{
        data,
        data::Value,
        result::{Error, Result},
    },
    sqlparser::ast::{DataType, Value as AstValue},
    std::{
        cmp::Ordering,
        convert::{TryFrom, TryInto},
    },
};

/// `LiteralRef`, `Literal` and `StringRef` are used when it is not possible to specify what kind of `Value`
/// can be applied.
///
/// * `1 + 1` is converted into `LiteralRef + LiteralRef`, `LiteralRef` of `1` can
/// become `Value::I64` but it can be also `Value::F64`.
///
/// * Specifing column `id`, it is converted into `ValueRef` because `id` can be specified from table
/// schema.
///
/// * `"hello"` is converted into `StringRef`, similar with `LiteralRef` but it is for storing
/// string value.
///
/// * Evaluated result of `1 + 1` becomes `Literal`, not `LiteralRef` because executor has
/// ownership of `1 + 1`.
///
/// * Similar with `Literal`, `Value` is also generated by any other operation with `ValueRef` or
/// `Value`.
/// e.g. `LiteralRef` + `ValueRef`, `LiteralRef` * `Value`, ...
#[derive(std::fmt::Debug)]
pub enum Evaluated<'a> {
    LiteralRef(&'a AstValue),
    Literal(AstValue),
    StringRef(&'a str),
    ValueRef(&'a Value),
    Value(Value),
}

impl<'a> PartialEq for Evaluated<'a> {
    fn eq(&self, other: &Evaluated<'a>) -> bool {
        let eq_ast = |l: &AstValue, r| match l {
            AstValue::SingleQuotedString(l) => l == r,
            _ => false,
        };

        let eq_val = |l: &Value, r| match l {
            Value::Str(l) => l == r,
            _ => false,
        };

        {
            use Evaluated::*;

            match self {
                LiteralRef(l) => match other {
                    LiteralRef(r) => l == r,
                    StringRef(r) => eq_ast(l, r),
                    ValueRef(r) => r == l,
                    Value(r) => &r == l,
                    Literal(r) => *l == r,
                },
                StringRef(l) => match other {
                    LiteralRef(r) => eq_ast(r, l),
                    StringRef(r) => l == r,
                    ValueRef(r) => eq_val(r, l),
                    Value(r) => eq_val(&r, l),
                    Literal(_) => false,
                },
                ValueRef(l) => match other {
                    LiteralRef(r) => l == r,
                    Literal(r) => l == &r,
                    StringRef(r) => eq_val(l, r),
                    ValueRef(r) => l == r,
                    Value(r) => l == &r,
                },
                Value(l) => match other {
                    LiteralRef(r) => &l == r,
                    StringRef(r) => eq_val(&l, r),
                    ValueRef(r) => &l == r,
                    Value(r) => l == r,
                    Literal(r) => l == r,
                },
                Literal(l) => match other {
                    Literal(r) => l == r,
                    LiteralRef(r) => &l == r,
                    ValueRef(r) => r == &l,
                    Value(r) => r == l,
                    StringRef(_) => false,
                },
            }
        }
    }
}

impl<'a> PartialOrd for Evaluated<'a> {
    fn partial_cmp(&self, other: &Evaluated<'a>) -> Option<Ordering> {
        use Evaluated::*;

        match self {
            LiteralRef(l) => match other {
                LiteralRef(r) => literal_partial_cmp(l, r),
                ValueRef(r) => r.partial_cmp(l).map(|o| o.reverse()),
                Value(r) => r.partial_cmp(*l).map(|o| o.reverse()),
                StringRef(_) => None,
                Literal(r) => literal_partial_cmp(&l, &r),
            },
            ValueRef(l) => match other {
                LiteralRef(r) => l.partial_cmp(r),
                ValueRef(r) => l.partial_cmp(r),
                Value(r) => l.partial_cmp(&r),
                StringRef(r) => match l {
                    data::Value::Str(l) => (&l.as_str()).partial_cmp(r),
                    _ => None,
                },
                Literal(r) => l.partial_cmp(&r),
            },
            Value(l) => match other {
                LiteralRef(r) => l.partial_cmp(*r),
                ValueRef(r) => l.partial_cmp(*r),
                Value(r) => l.partial_cmp(r),
                StringRef(r) => match l {
                    data::Value::Str(l) => (&l.as_str()).partial_cmp(r),
                    _ => None,
                },
                Literal(r) => l.partial_cmp(r),
            },
            StringRef(l) => match other {
                LiteralRef(_) => None,
                ValueRef(data::Value::Str(r)) => l.partial_cmp(&r.as_str()),
                Value(data::Value::Str(r)) => l.partial_cmp(&r.as_str()),
                StringRef(r) => l.partial_cmp(r),
                Literal(_) => None,
                _ => None,
            },
            Literal(l) => match other {
                LiteralRef(r) => literal_partial_cmp(&l, &r),
                ValueRef(r) => r.partial_cmp(&l).map(|o| o.reverse()),
                Value(r) => r.partial_cmp(l).map(|o| o.reverse()),
                StringRef(_) => None,
                Literal(r) => literal_partial_cmp(l, r),
            },
        }
    }
}

fn literal_partial_cmp(a: &AstValue, b: &AstValue) -> Option<Ordering> {
    match (a, b) {
        (AstValue::Number(a), AstValue::Number(b)) => match (a.parse::<i64>(), b.parse::<i64>()) {
            (Ok(a), Ok(b)) => Some(a.cmp(&b)),
            (_, Ok(b)) => match a.parse::<f64>() {
                Ok(a) => a.partial_cmp(&(b as f64)),
                _ => None,
            },
            (Ok(a), _) => match b.parse::<f64>() {
                Ok(b) => (a as f64).partial_cmp(&b),
                _ => None,
            },
            _ => match (a.parse::<f64>(), b.parse::<f64>()) {
                (Ok(a), Ok(b)) => a.partial_cmp(&b),
                _ => None,
            },
        },
        (AstValue::SingleQuotedString(l), AstValue::SingleQuotedString(r)) => Some(l.cmp(r)),
        _ => None,
    }
}

impl TryInto<Value> for Evaluated<'_> {
    type Error = Error;

    fn try_into(self) -> Result<Value> {
        match self {
            Evaluated::LiteralRef(v) => Value::try_from(v),
            Evaluated::Literal(v) => Value::try_from(&v),
            Evaluated::StringRef(v) => Ok(Value::Str(v.to_string())),
            Evaluated::ValueRef(v) => Ok(v.clone()),
            Evaluated::Value(v) => Ok(v),
        }
    }
}

impl<'a> Evaluated<'a> {
    pub fn add(&self, other: &Evaluated<'a>) -> Result<Evaluated<'a>> {
        use Evaluated::*;

        let unreachable = || Err(EvaluateError::UnreachableEvaluatedArithmetic.into());

        let add_literal = |l, other: &Evaluated<'a>| match other {
            LiteralRef(r) => literal_add(l, r).map(Evaluated::Literal),
            Literal(r) => literal_add(l, &r).map(Evaluated::Literal),
            ValueRef(r) => r.add(&r.clone_by(l)?).map(Evaluated::Value),
            Value(r) => r.add(&r.clone_by(l)?).map(Evaluated::Value),
            StringRef(_) => unreachable(),
        };

        let add_value = |l: &data::Value, other: &Evaluated<'a>| match other {
            LiteralRef(r) => l.add(&l.clone_by(r)?).map(Evaluated::Value),
            Literal(r) => l.add(&l.clone_by(&r)?).map(Evaluated::Value),
            ValueRef(r) => l.add(r).map(Evaluated::Value),
            Value(r) => l.add(&r).map(Evaluated::Value),
            StringRef(_) => unreachable(),
        };

        match self {
            LiteralRef(l) => add_literal(l, other),
            Literal(l) => add_literal(&l, other),
            ValueRef(l) => add_value(l, other),
            Value(l) => add_value(&l, other),
            StringRef(_) => unreachable(),
        }
    }

    pub fn subtract(&self, other: &Evaluated<'a>) -> Result<Evaluated<'a>> {
        use Evaluated::*;

        let unreachable = || Err(EvaluateError::UnreachableEvaluatedArithmetic.into());

        let subtract_literal = |l, other: &Evaluated<'a>| match other {
            LiteralRef(r) => literal_subtract(l, r).map(Evaluated::Literal),
            Literal(r) => literal_subtract(l, &r).map(Evaluated::Literal),
            ValueRef(r) => (r.clone_by(l)?).subtract(r).map(Evaluated::Value),
            Value(r) => (r.clone_by(l)?).subtract(r).map(Evaluated::Value),
            StringRef(_) => unreachable(),
        };

        let subtract_value = |l: &data::Value, other: &Evaluated<'a>| match other {
            LiteralRef(r) => l.subtract(&l.clone_by(r)?).map(Evaluated::Value),
            Literal(r) => l.subtract(&l.clone_by(&r)?).map(Evaluated::Value),
            ValueRef(r) => l.subtract(r).map(Evaluated::Value),
            Value(r) => l.subtract(&r).map(Evaluated::Value),
            StringRef(_) => unreachable(),
        };

        match self {
            LiteralRef(l) => subtract_literal(l, other),
            Literal(l) => subtract_literal(&l, other),
            ValueRef(l) => subtract_value(l, other),
            Value(l) => subtract_value(&l, other),
            StringRef(_) => unreachable(),
        }
    }

    pub fn multiply(&self, other: &Evaluated<'a>) -> Result<Evaluated<'a>> {
        use Evaluated::*;

        let unreachable = || Err(EvaluateError::UnreachableEvaluatedArithmetic.into());

        let multiply_literal = |l, other: &Evaluated<'a>| match other {
            LiteralRef(r) => literal_multiply(l, r).map(Evaluated::Literal),
            Literal(r) => literal_multiply(l, &r).map(Evaluated::Literal),
            ValueRef(r) => (r.clone_by(l)?).multiply(r).map(Evaluated::Value),
            Value(r) => (r.clone_by(l)?).multiply(r).map(Evaluated::Value),
            StringRef(_) => unreachable(),
        };

        let multiply_value = |l: &data::Value, other: &Evaluated<'a>| match other {
            LiteralRef(r) => l.multiply(&l.clone_by(r)?).map(Evaluated::Value),
            Literal(r) => l.multiply(&l.clone_by(&r)?).map(Evaluated::Value),
            ValueRef(r) => l.multiply(r).map(Evaluated::Value),
            Value(r) => l.multiply(&r).map(Evaluated::Value),
            StringRef(_) => unreachable(),
        };

        match self {
            LiteralRef(l) => multiply_literal(l, other),
            Literal(l) => multiply_literal(&l, other),
            ValueRef(l) => multiply_value(l, other),
            Value(l) => multiply_value(&l, other),
            StringRef(_) => unreachable(),
        }
    }

    pub fn divide(&self, other: &Evaluated<'a>) -> Result<Evaluated<'a>> {
        use Evaluated::*;

        let unreachable = || Err(EvaluateError::UnreachableEvaluatedArithmetic.into());

        let divide_literal = |l, other: &Evaluated<'a>| match other {
            LiteralRef(r) => literal_divide(l, r).map(Evaluated::Literal),
            Literal(r) => literal_divide(l, &r).map(Evaluated::Literal),
            ValueRef(r) => (r.clone_by(l)?).divide(r).map(Evaluated::Value),
            Value(r) => (r.clone_by(l)?).divide(r).map(Evaluated::Value),
            StringRef(_) => unreachable(),
        };

        let divide_value = |l: &data::Value, other: &Evaluated<'a>| match other {
            LiteralRef(r) => l.divide(&l.clone_by(r)?).map(Evaluated::Value),
            Literal(r) => l.divide(&l.clone_by(&r)?).map(Evaluated::Value),
            ValueRef(r) => l.divide(r).map(Evaluated::Value),
            Value(r) => l.divide(&r).map(Evaluated::Value),
            StringRef(_) => unreachable(),
        };

        match self {
            LiteralRef(l) => divide_literal(l, other),
            Literal(l) => divide_literal(&l, other),
            ValueRef(l) => divide_value(l, other),
            Value(l) => divide_value(&l, other),
            StringRef(_) => unreachable(),
        }
    }

    pub fn is_some(&self) -> bool {
        match self {
            Evaluated::ValueRef(v) => v.is_some(),
            Evaluated::Value(v) => v.is_some(),
            Evaluated::Literal(v) => v != &AstValue::Null,
            Evaluated::LiteralRef(v) => v != &&AstValue::Null,
            Evaluated::StringRef(_v) => true,
        }
    }

    pub fn unary_plus(&self) -> Result<Evaluated<'a>> {
        use Evaluated::*;

        let unreachable = || Err(EvaluateError::UnreachableEvaluatedArithmetic.into());

        let plus_literal = |v| literal_plus(v).map(Evaluated::Literal);
        let plus_value = |v: &data::Value| v.unary_plus().map(Evaluated::Value);

        match self {
            LiteralRef(v) => plus_literal(v),
            Literal(v) => plus_literal(&v),
            ValueRef(v) => plus_value(v),
            Value(v) => plus_value(&v),
            StringRef(_) => unreachable(),
        }
    }

    pub fn unary_minus(&self) -> Result<Evaluated<'a>> {
        use Evaluated::*;

        let unreachable = || Err(EvaluateError::UnreachableEvaluatedArithmetic.into());

        let minus_literal = |v| literal_minus(v).map(Evaluated::Literal);
        let minus_value = |v: &data::Value| v.unary_minus().map(Evaluated::Value);

        match self {
            LiteralRef(v) => minus_literal(v),
            Literal(v) => minus_literal(&v),
            ValueRef(v) => minus_value(v),
            Value(v) => minus_value(&v),
            StringRef(_) => unreachable(),
        }
    }

    pub fn cast(&self, data_type: &DataType) -> Result<Evaluated<'a>> {
        use Evaluated::*;

        let cast_literal =
            |value: &AstValue| cast_ast_value(value.to_owned(), data_type).map(Evaluated::Literal);
        let cast_value = |value: &data::Value| value.cast(data_type).map(Evaluated::Value);

        // Decided: Due to the explicit call, we can abandon -Ref
        match self {
            StringRef(value) => cast_literal(&AstValue::SingleQuotedString(value.to_string())),
            LiteralRef(value) => cast_literal(value.to_owned()),
            Literal(value) => cast_literal(value),
            ValueRef(value) => cast_value(value.to_owned()),
            Value(value) => cast_value(value),
        }
    }
}

macro_rules! literal_binary_op {
    ($e1:expr, $e2:expr, $op:tt) => {
        match ($e1, $e2) {
            (AstValue::Number(a), AstValue::Number(b)) => match (a.parse::<i64>(), b.parse::<i64>()) {
                (Ok(a), Ok(b)) => Ok(AstValue::Number((a $op b).to_string())),
                (Ok(a), _) => match b.parse::<f64>() {
                    Ok(b) => Ok(AstValue::Number(((a as f64) $op b).to_string())),
                    _ => Err(EvaluateError::UnreachableLiteralArithmetic.into()),
                },
                (_, Ok(b)) => match a.parse::<f64>() {
                    Ok(a) => Ok(AstValue::Number((a $op (b as f64)).to_string())),
                    _ => Err(EvaluateError::UnreachableLiteralArithmetic.into()),
                },
                (_, _) => match (a.parse::<f64>(), b.parse::<f64>()) {
                    (Ok(a), Ok(b)) => Ok(AstValue::Number((a $op b).to_string())),
                    _ => Err(EvaluateError::UnreachableLiteralArithmetic.into()),
                },
            },
            (AstValue::Null, AstValue::Number(_)) | (AstValue::Number(_), AstValue::Null) => {
                Ok(AstValue::Null)
            }
            _ => Err(EvaluateError::UnreachableLiteralArithmetic.into()),
        }
    };
}

fn literal_add(a: &AstValue, b: &AstValue) -> Result<AstValue> {
    literal_binary_op!(a, b, +)
}

fn literal_subtract(a: &AstValue, b: &AstValue) -> Result<AstValue> {
    literal_binary_op!(a, b, -)
}

fn literal_multiply(a: &AstValue, b: &AstValue) -> Result<AstValue> {
    literal_binary_op!(a, b, *)
}

fn literal_divide(a: &AstValue, b: &AstValue) -> Result<AstValue> {
    literal_binary_op!(a, b, /)
}

fn literal_plus(v: &AstValue) -> Result<AstValue> {
    match v {
        AstValue::Number(v) => v
            .parse::<i64>()
            .map_or_else(
                |_| v.parse::<f64>().map(|_| AstValue::Number(v.to_string())),
                |v| Ok(AstValue::Number(v.to_string())),
            )
            .map_err(|_| EvaluateError::LiteralUnaryPlusOnNonNumeric.into()),
        AstValue::Null => Ok(AstValue::Null),
        _ => Err(EvaluateError::LiteralUnaryPlusOnNonNumeric.into()),
    }
}

fn literal_minus(v: &AstValue) -> Result<AstValue> {
    match v {
        AstValue::Number(v) => v
            .parse::<i64>()
            .map_or_else(
                |_| v.parse::<f64>().map(|v| AstValue::Number((-v).to_string())),
                |v| Ok(AstValue::Number((-v).to_string())),
            )
            .map_err(|_| EvaluateError::LiteralUnaryMinusOnNonNumeric.into()),
        AstValue::Null => Ok(AstValue::Null),
        _ => Err(EvaluateError::LiteralUnaryMinusOnNonNumeric.into()),
    }
}

pub fn cast_ast_value(value: AstValue, data_type: &DataType) -> Result<AstValue> {
    match (data_type, value) {
        (DataType::Boolean, AstValue::SingleQuotedString(value))
        | (DataType::Boolean, AstValue::Number(value)) => Ok(match value.to_uppercase().as_str() {
            "TRUE" | "1" => Ok(AstValue::Boolean(true)),
            "FALSE" | "0" => Ok(AstValue::Boolean(false)),
            _ => Err(EvaluateError::ImpossibleCast),
        }?),
        (DataType::Int, AstValue::Number(value)) => Ok(AstValue::Number(
            value
                .parse::<f64>()
                .map_err(|_| EvaluateError::UnreachableImpossibleCast)?
                .trunc()
                .to_string(),
        )),
        (DataType::Int, AstValue::SingleQuotedString(value))
        | (DataType::Float(_), AstValue::SingleQuotedString(value)) => Ok(AstValue::Number(value)),
        (DataType::Int, AstValue::Boolean(value))
        | (DataType::Float(_), AstValue::Boolean(value)) => Ok(AstValue::Number(
            (if value { "1" } else { "0" }).to_string(),
        )),
        (DataType::Float(_), AstValue::Number(value)) => Ok(AstValue::Number(value)),
        (DataType::Text, AstValue::Boolean(value)) => Ok(AstValue::SingleQuotedString(
            (if value { "TRUE" } else { "FALSE" }).to_string(),
        )),
        (DataType::Text, AstValue::Number(value)) => Ok(AstValue::SingleQuotedString(value)),
        (_, AstValue::Null) => Ok(AstValue::Null),
        _ => Err(EvaluateError::UnimplementedCast.into()),
    }
}
