use {
    super::{error::EvaluateError, function, literal::Literal},
    crate::{
        ast::{BinaryOperator, DataType, TrimWhereField},
        data::{BigDecimalExt, Key, Value, ValueError, value::BTreeMapJsonExt},
        result::{Error, Result},
    },
    bigdecimal::BigDecimal,
    std::{borrow::Cow, collections::BTreeMap, convert::TryFrom, ops::Range},
    utils::Tribool,
    uuid::Uuid,
};

mod cmp;
mod eq;
mod literal;

pub(crate) use literal::literal_to_value;

#[derive(Clone, Debug, PartialEq)]
pub enum Evaluated<'a> {
    Literal(Literal<'a>),
    StrSlice {
        source: Cow<'a, str>,
        range: Range<usize>,
    },
    Value(Value),
}

impl TryFrom<Evaluated<'_>> for Value {
    type Error = Error;

    fn try_from(e: Evaluated<'_>) -> Result<Value> {
        match e {
            Evaluated::Literal(v) => Value::try_from(v),
            Evaluated::StrSlice {
                source: s,
                range: r,
            } => Ok(Value::Str(s[r].to_owned())),
            Evaluated::Value(v) => Ok(v),
        }
    }
}

impl TryFrom<Evaluated<'_>> for Key {
    type Error = Error;

    fn try_from(evaluated: Evaluated<'_>) -> Result<Self> {
        Self::try_from(&evaluated)
    }
}

impl TryFrom<&Evaluated<'_>> for Key {
    type Error = Error;

    fn try_from(evaluated: &Evaluated<'_>) -> Result<Self> {
        match evaluated {
            Evaluated::Literal(l) => Value::try_from(l)?.try_into(),
            Evaluated::StrSlice { source, range } => Ok(Key::Str(source[range.clone()].to_owned())),
            Evaluated::Value(v) => v.try_into(),
        }
    }
}

impl TryFrom<Evaluated<'_>> for bool {
    type Error = Error;

    fn try_from(e: Evaluated<'_>) -> Result<bool> {
        match e {
            Evaluated::Value(Value::Bool(v)) => Ok(v),
            Evaluated::Literal(v) => {
                Err(EvaluateError::BooleanTypeRequired(format!("{v:?}")).into())
            }
            Evaluated::StrSlice { source, range } => {
                Err(EvaluateError::BooleanTypeRequired(source[range].to_owned()).into())
            }
            Evaluated::Value(v) => Err(EvaluateError::BooleanTypeRequired(format!("{v:?}")).into()),
        }
    }
}

impl TryFrom<Evaluated<'_>> for BTreeMap<String, Value> {
    type Error = Error;

    fn try_from(evaluated: Evaluated<'_>) -> Result<BTreeMap<String, Value>> {
        match evaluated {
            Evaluated::Literal(Literal::Text(v)) => BTreeMap::parse_json_object(v.as_ref()),
            Evaluated::Literal(v) => {
                Err(EvaluateError::TextLiteralRequired(format!("{v:?}")).into())
            }
            Evaluated::Value(Value::Str(v)) => BTreeMap::parse_json_object(v.as_str()),
            Evaluated::Value(Value::Map(v)) => Ok(v),
            Evaluated::Value(v) => Err(EvaluateError::MapOrStringValueRequired(v.into()).into()),
            Evaluated::StrSlice { source, range } => BTreeMap::parse_json_object(&source[range]),
        }
    }
}

fn binary_op<'c, T>(
    l: &Evaluated<'_>,
    r: &Evaluated<'_>,
    op: BinaryOperator,
    value_op: T,
) -> Result<Evaluated<'c>>
where
    T: FnOnce(&Value, &Value) -> Result<Value>,
{
    match (l, r) {
        (Evaluated::Literal(l), Evaluated::Value(r)) => {
            value_op(&Value::try_from(l)?, r).map(Evaluated::Value)
        }
        (Evaluated::Value(l), Evaluated::Literal(r)) => {
            value_op(l, &Value::try_from(r)?).map(Evaluated::Value)
        }
        (Evaluated::Value(l), Evaluated::Value(r)) => value_op(l, r).map(Evaluated::Value),
        (l, r) => Err(EvaluateError::UnsupportedBinaryOperation {
            left: format!("{l:?}"),
            op,
            right: format!("{r:?}"),
        }
        .into()),
    }
}

fn unsupported_literal_binary_op(
    left: &Literal<'_>,
    op: BinaryOperator,
    right: &Literal<'_>,
) -> Error {
    EvaluateError::UnsupportedBinaryOperation {
        left: left.to_string(),
        op,
        right: right.to_string(),
    }
    .into()
}

fn incompatible_bit_operation(left: &Literal<'_>, right: &Literal<'_>) -> Error {
    EvaluateError::IncompatibleBitOperation(left.to_string(), right.to_string()).into()
}

pub fn exceptional_int_val_to_eval<'a>(name: String, v: &Value) -> Result<Evaluated<'a>> {
    match v {
        Value::Null => Ok(Evaluated::Value(Value::Null)),
        _ => Err(EvaluateError::FunctionRequiresIntegerValue(name).into()),
    }
}

impl From<Tribool> for Evaluated<'_> {
    fn from(x: Tribool) -> Self {
        Evaluated::Value(Value::from(x))
    }
}

impl<'a> Evaluated<'a> {
    pub fn add<'b, 'c>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'c>> {
        if let (Evaluated::Literal(Literal::Number(l)), Evaluated::Literal(Literal::Number(r))) =
            (self, other)
        {
            return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                l.as_ref() + r.as_ref(),
            ))));
        }

        binary_op(self, other, BinaryOperator::Plus, Value::add)
    }

    pub fn subtract<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Literal(Literal::Number(l)), Evaluated::Literal(Literal::Number(r))) =
            (self, other)
        {
            return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                l.as_ref() - r.as_ref(),
            ))));
        }

        binary_op(self, other, BinaryOperator::Minus, Value::subtract)
    }

    pub fn multiply<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Literal(Literal::Number(l)), Evaluated::Literal(Literal::Number(r))) =
            (self, other)
        {
            return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                l.as_ref() * r.as_ref(),
            ))));
        }

        binary_op(self, other, BinaryOperator::Multiply, Value::multiply)
    }

    pub fn divide<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Literal(Literal::Number(l)), Evaluated::Literal(Literal::Number(r))) =
            (self, other)
        {
            if *r.as_ref() == BigDecimal::from(0) {
                return Err(EvaluateError::DivisorShouldNotBeZero.into());
            }

            return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                l.as_ref() / r.as_ref(),
            ))));
        }

        binary_op(self, other, BinaryOperator::Divide, Value::divide)
    }

    pub fn bitwise_and<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Literal(left), Evaluated::Literal(right)) = (self, other) {
            match (left, right) {
                (Literal::Number(l), Literal::Number(r)) => {
                    let lhs = l.to_i64().ok_or_else(|| {
                        unsupported_literal_binary_op(left, BinaryOperator::BitwiseAnd, right)
                    })?;
                    let rhs = r.to_i64().ok_or_else(|| {
                        unsupported_literal_binary_op(left, BinaryOperator::BitwiseAnd, right)
                    })?;

                    return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                        BigDecimal::from(lhs & rhs),
                    ))));
                }
                _ => {
                    return Err(unsupported_literal_binary_op(
                        left,
                        BinaryOperator::BitwiseAnd,
                        right,
                    ));
                }
            }
        }

        binary_op(self, other, BinaryOperator::BitwiseAnd, Value::bitwise_and)
    }

    pub fn modulo<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (Evaluated::Literal(Literal::Number(l)), Evaluated::Literal(Literal::Number(r))) =
            (self, other)
        {
            if *r.as_ref() == BigDecimal::from(0) {
                return Err(EvaluateError::DivisorShouldNotBeZero.into());
            }

            return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                l.as_ref() % r.as_ref(),
            ))));
        }

        binary_op(self, other, BinaryOperator::Modulo, Value::modulo)
    }

    pub fn bitwise_shift_left<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (
            Evaluated::Literal(left @ Literal::Number(l)),
            Evaluated::Literal(right @ Literal::Number(r)),
        ) = (self, other)
        {
            let lhs = l
                .to_i64()
                .ok_or_else(|| incompatible_bit_operation(left, right))?;

            if !r.is_integer_representation() {
                return Err(incompatible_bit_operation(left, right));
            }

            let rhs = r
                .to_u32()
                .ok_or_else(|| incompatible_bit_operation(left, right))?;

            let result = lhs
                .checked_shl(rhs)
                .ok_or_else(|| incompatible_bit_operation(left, right))?;

            return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                BigDecimal::from(result),
            ))));
        }

        binary_op(
            self,
            other,
            BinaryOperator::BitwiseShiftLeft,
            Value::bitwise_shift_left,
        )
    }

    pub fn bitwise_shift_right<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        if let (
            Evaluated::Literal(left @ Literal::Number(l)),
            Evaluated::Literal(right @ Literal::Number(r)),
        ) = (self, other)
        {
            let lhs = l
                .to_i64()
                .ok_or_else(|| incompatible_bit_operation(left, right))?;

            if !r.is_integer_representation() {
                return Err(incompatible_bit_operation(left, right));
            }

            let rhs = r
                .to_u32()
                .ok_or_else(|| incompatible_bit_operation(left, right))?;

            let result = lhs
                .checked_shr(rhs)
                .ok_or_else(|| incompatible_bit_operation(left, right))?;

            return Ok(Evaluated::Literal(Literal::Number(Cow::Owned(
                BigDecimal::from(result),
            ))));
        }

        binary_op(
            self,
            other,
            BinaryOperator::BitwiseShiftRight,
            Value::bitwise_shift_right,
        )
    }

    pub fn arrow<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        let selector = Value::try_from(other.clone())?;

        if selector.is_null() {
            return Ok(Evaluated::Value(Value::Null));
        }

        let value_result = if let Evaluated::Value(base) = self {
            function::select_arrow_value(base, &selector)
        } else {
            let base = Value::try_from(self.clone())?;
            function::select_arrow_value(&base, &selector)
        };

        value_result.map(Evaluated::Value)
    }

    pub fn unary_plus(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(literal @ Literal::Number(_)) => {
                Ok(Evaluated::Literal(literal.clone()))
            }
            Evaluated::Literal(literal) => {
                Err(EvaluateError::UnsupportedUnaryPlus(literal.to_string()).into())
            }
            Evaluated::Value(v) => v.unary_plus().map(Evaluated::Value),
            Evaluated::StrSlice { source, range } => {
                Err(EvaluateError::UnsupportedUnaryPlus(source[range.clone()].to_owned()).into())
            }
        }
    }
    pub fn unary_minus(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(Literal::Number(value)) => Ok(Evaluated::Literal(Literal::Number(
                Cow::Owned(-value.as_ref()),
            ))),
            Evaluated::Literal(literal) => {
                Err(EvaluateError::UnsupportedUnaryMinus(literal.to_string()).into())
            }
            Evaluated::Value(v) => v.unary_minus().map(Evaluated::Value),
            Evaluated::StrSlice { source, range } => {
                Err(EvaluateError::UnsupportedUnaryMinus(source[range.clone()].to_owned()).into())
            }
        }
    }

    pub fn unary_not(self) -> Result<Evaluated<'a>> {
        if self.is_null() {
            Ok(self)
        } else {
            self.try_into()
                .map(|v: bool| Evaluated::Value(Value::Bool(!v)))
        }
    }

    pub fn unary_factorial(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(v) => Value::try_from(v).and_then(|v| v.unary_factorial()),
            Evaluated::Value(v) => v.unary_factorial(),
            Evaluated::StrSlice { source, range } => Err(EvaluateError::UnsupportedUnaryFactorial(
                source[range.clone()].to_owned(),
            )
            .into()),
        }
        .map(Evaluated::Value)
    }

    pub fn unary_bitwise_not(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(v) => Value::try_from(v).and_then(|v| v.unary_bitwise_not()),
            Evaluated::Value(v) => v.unary_bitwise_not(),
            Evaluated::StrSlice { source, range } => {
                Err(EvaluateError::IncompatibleUnaryBitwiseNotOperation(
                    source[range.clone()].to_owned(),
                )
                .into())
            }
        }
        .map(Evaluated::Value)
    }

    pub fn cast(self, data_type: &DataType) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(literal) => literal::try_cast_literal_to_value(data_type, &literal),
            Evaluated::Value(value) => value.cast(data_type),
            Evaluated::StrSlice { source, range } => {
                Value::Str(source[range].to_owned()).cast(data_type)
            }
        }
        .map(Evaluated::Value)
    }

    pub fn concat(self, other: Evaluated) -> Result<Evaluated<'a>> {
        let evaluated = match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => Evaluated::Literal(l.concat(r)),
            (Evaluated::Literal(l), Evaluated::Value(r)) => {
                Evaluated::Value((Value::try_from(l)?).concat(r))
            }
            (Evaluated::Value(l), Evaluated::Literal(r)) => {
                Evaluated::Value(l.concat(Value::try_from(r)?))
            }
            (Evaluated::Value(l), Evaluated::Value(r)) => Evaluated::Value(l.concat(r)),
            (Evaluated::Literal(l), Evaluated::StrSlice { source, range }) => {
                Evaluated::Value((Value::try_from(l)?).concat(Value::Str(source[range].to_owned())))
            }
            (Evaluated::Value(l), Evaluated::StrSlice { source, range }) => {
                Evaluated::Value(l.concat(Value::Str(source[range].to_owned())))
            }
            (Evaluated::StrSlice { source, range }, Evaluated::Literal(r)) => {
                Evaluated::Value(Value::Str(source[range].to_owned()).concat(Value::try_from(r)?))
            }
            (Evaluated::StrSlice { source, range }, Evaluated::Value(r)) => {
                Evaluated::Value(Value::Str(source[range].to_owned()).concat(r))
            }
            (
                Evaluated::StrSlice {
                    source: a,
                    range: ar,
                },
                Evaluated::StrSlice {
                    source: b,
                    range: br,
                },
            ) => {
                Evaluated::Value(Value::Str(a[ar].to_owned()).concat(Value::Str(b[br].to_owned())))
            }
        };

        Ok(evaluated)
    }

    pub fn like(&self, other: Evaluated<'a>, case_sensitive: bool) -> Result<Evaluated<'a>> {
        let evaluated = match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => {
                Evaluated::Value(Value::Bool(l.like(&r, case_sensitive)?))
            }
            (Evaluated::Literal(l), Evaluated::Value(r)) => {
                Evaluated::Value((Value::try_from(l)?).like(&r, case_sensitive)?)
            }
            (Evaluated::Value(l), Evaluated::Literal(r)) => {
                Evaluated::Value(l.like(&Value::try_from(r)?, case_sensitive)?)
            }
            (Evaluated::Value(l), Evaluated::Value(r)) => {
                Evaluated::Value(l.like(&r, case_sensitive)?)
            }
            (Evaluated::Literal(l), Evaluated::StrSlice { source, range }) => Evaluated::Value(
                Value::try_from(l)?.like(&Value::Str(source[range].to_owned()), case_sensitive)?,
            ),
            (Evaluated::StrSlice { source, range }, Evaluated::Literal(r)) => Evaluated::Value(
                Value::Str(source[range.clone()].to_owned())
                    .like(&Value::try_from(r)?, case_sensitive)?,
            ),
            (
                Evaluated::StrSlice {
                    source: a,
                    range: ar,
                },
                Evaluated::StrSlice {
                    source: b,
                    range: br,
                },
            ) => Evaluated::Value(
                Value::Str(a[ar.clone()].to_owned())
                    .like(&Value::Str(b[br].to_owned()), case_sensitive)?,
            ),
            (Evaluated::StrSlice { source, range }, Evaluated::Value(r)) => Evaluated::Value(
                Value::Str(source[range.clone()].to_owned()).like(&r, case_sensitive)?,
            ),
            (Evaluated::Value(l), Evaluated::StrSlice { source, range }) => {
                Evaluated::Value(l.like(&Value::Str(source[range].to_owned()), case_sensitive)?)
            }
        };

        Ok(evaluated)
    }

    pub fn ltrim(self, name: String, chars: Option<Evaluated<'_>>) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Literal(Literal::Text(l)) => {
                let end = l.len();
                (l, 0..end)
            }
            Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null));
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(Value::Str(v)) => {
                let end = v.len();
                (Cow::Owned(v), 0..end)
            }
            _ => return Err(EvaluateError::FunctionRequiresStringValue(name).into()),
        };

        let filter_chars = match chars {
            Some(expr) => match expr.try_into()? {
                Value::Str(value) => value,
                Value::Null => {
                    return Ok(Evaluated::Value(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresStringValue(name).into());
                }
            }
            .chars()
            .collect::<Vec<_>>(),
            None => vec![' '],
        };
        let sliced_expr = &source[range.clone()];
        let matched_vec: Vec<_> = sliced_expr.match_indices(&filter_chars[..]).collect();

        //"x".trim_start_matches(['x','y','z']) => ""
        if matched_vec.len() == sliced_expr.len() {
            return Ok(Evaluated::StrSlice {
                source,
                range: 0..0,
            });
        }
        //"tuv".trim_start_matches(['x','y','z']) => "tuv"
        if matched_vec.is_empty() {
            return Ok(Evaluated::StrSlice { source, range });
        }
        //"txu".trim_start_matches(['x','y','z']) => "txu"
        if matched_vec[0].0 != 0 && matched_vec[matched_vec.len() - 1].0 != sliced_expr.len() - 1 {
            return Ok(Evaluated::StrSlice { source, range });
        }
        let pivot = matched_vec
            .iter()
            .enumerate()
            .skip_while(|(vec_idx, (slice_idx, _))| vec_idx == slice_idx)
            .map(|(vec_idx, (_, _))| vec_idx)
            .next();

        let start = match pivot {
            Some(idx) => match idx {
                0 => 0,
                _ => matched_vec[idx - 1].0 + 1,
            },
            _ => matched_vec[matched_vec.len() - 1].0 + 1,
        };

        Ok(Evaluated::StrSlice {
            source,
            range: range.start + start..range.end,
        })
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Evaluated::Value(v) if v.is_null())
    }

    pub fn rtrim(self, name: String, chars: Option<Evaluated<'_>>) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Literal(Literal::Text(l)) => {
                let end = l.len();
                (l, 0..end)
            }
            Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null));
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(Value::Str(v)) => {
                let end = v.len();
                (Cow::Owned(v), 0..end)
            }
            _ => return Err(EvaluateError::FunctionRequiresStringValue(name).into()),
        };

        let filter_chars = match chars {
            Some(expr) => match expr.try_into()? {
                Value::Str(value) => value,
                Value::Null => {
                    return Ok(Evaluated::Value(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresStringValue(name).into());
                }
            }
            .chars()
            .collect::<Vec<_>>(),
            None => vec![' '],
        };
        let sliced_expr = &source[range.clone()];
        let matched_vec: Vec<_> = sliced_expr.match_indices(&filter_chars[..]).collect();

        //"x".trim_end_matches(['x','y','z']) => ""
        if matched_vec.len() == sliced_expr.len() {
            return Ok(Evaluated::StrSlice {
                source,
                range: 0..0,
            });
        }
        //"tuv".trim_end_matches(['x','y','z']) => "tuv"
        if matched_vec.is_empty() {
            return Ok(Evaluated::StrSlice { source, range });
        }
        //"txu".trim_end_matches(['x','y','z']) => "txu"
        if matched_vec[0].0 != 0 && matched_vec[matched_vec.len() - 1].0 != sliced_expr.len() - 1 {
            return Ok(Evaluated::StrSlice { source, range });
        }

        let pivot = matched_vec
            .iter()
            .rev()
            .enumerate()
            .skip_while(|(vec_idx, (slice_idx, _))| *vec_idx == sliced_expr.len() - slice_idx - 1)
            .map(|(vec_idx, (_, _))| vec_idx)
            .next();

        let end = match pivot {
            Some(idx) => match idx {
                0 => range.end,
                _ => matched_vec[matched_vec.len() - idx].0,
            },
            _ => matched_vec[0].0,
        };

        Ok(Evaluated::StrSlice {
            source,
            range: range.start..end,
        })
    }

    pub fn substr(
        self,
        name: String,
        start: Evaluated<'a>,
        count: Option<Evaluated<'a>>,
    ) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Literal(Literal::Text(l)) => {
                let end = l.len();
                (l, 0..end)
            }
            Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null));
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(Value::Str(v)) => {
                let end = v.len();
                (Cow::Owned(v), 0..end)
            }
            _ => return Err(EvaluateError::FunctionRequiresStringValue(name).into()),
        };

        let start = {
            let value = start.try_into()?;
            match value {
                Value::I64(num) => num,
                _ => return exceptional_int_val_to_eval(name, &value),
            }
        } - 1;

        let count = match count {
            Some(eval) => {
                let value = eval.try_into()?;
                match value {
                    Value::I64(num) => num,
                    _ => return exceptional_int_val_to_eval(name, &value),
                }
            }
            None => source.len() as i64,
        };

        let end = if count < 0 {
            return Err(EvaluateError::NegativeSubstrLenNotAllowed.into());
        } else {
            (range.start as i64 + start + count).clamp(0, source.len() as i64) as usize
        };

        let start = (start + range.start as i64).clamp(0, source.len() as i64) as usize;

        Ok(Evaluated::StrSlice {
            source,
            range: start..end,
        })
    }

    pub fn trim(
        self,
        name: String,
        filter_chars: Option<Evaluated<'_>>,
        trim_where_field: Option<&TrimWhereField>,
    ) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Literal(Literal::Text(l)) => {
                let end = l.len();
                (l, 0..end)
            }
            Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null));
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(Value::Str(v)) => {
                let end = v.len();
                (Cow::Owned(v), 0..end)
            }
            _ => return Err(EvaluateError::FunctionRequiresStringValue(name).into()),
        };

        let filter_chars = match filter_chars {
            Some(expr) => match expr.try_into()? {
                Value::Str(value) => value,
                Value::Null => {
                    return Ok(Evaluated::Value(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresStringValue(name).into());
                }
            }
            .chars()
            .collect::<Vec<_>>(),
            None => vec![' '],
        };
        let sliced_expr = &source[range.clone()];
        let matched_vec: Vec<_> = sliced_expr.match_indices(&filter_chars[..]).collect();
        //filter_chars => ['x','y','z']
        //"x".trim_matches(filter_chars[..]) => ""
        if matched_vec.len() == sliced_expr.len() {
            return Ok(Evaluated::StrSlice {
                source,
                range: 0..0,
            });
        }
        //filter_chars => ['x','y','z']
        //"tuv".trim_matches(filter_chars[..]) => "tuv"
        if matched_vec.is_empty() {
            return Ok(Evaluated::StrSlice { source, range });
        }
        //filter_chars => ['x','y','z']
        //"txu".trim_matches(filter_chars[..]) => "txu"
        if matched_vec[0].0 != 0 && matched_vec[matched_vec.len() - 1].0 != sliced_expr.len() - 1 {
            return Ok(Evaluated::StrSlice { source, range });
        }
        match trim_where_field {
            Some(TrimWhereField::Both) => {
                //filter_chars => ['x','y','z']
                //"xyzbyxlxyz  ".trim_matches(filter_chars[..]) => "byxlxyz  "
                if matched_vec[0].0 == 0
                    && matched_vec[matched_vec.len() - 1].0 != sliced_expr.len() - 1
                {
                    let pivot = matched_vec
                        .iter()
                        .enumerate()
                        .skip_while(|(vec_idx, (slice_idx, _))| vec_idx == slice_idx)
                        .map(|(vec_idx, (_, _))| vec_idx)
                        .next();

                    let start = match pivot {
                        Some(idx) => matched_vec[idx - 1].0 + 1,
                        _ => matched_vec[matched_vec.len() - 1].0 + 1,
                    };

                    return Ok(Evaluated::StrSlice {
                        source,
                        range: range.start + start..range.end,
                    });
                }
                //filter_chars => ['x','y','z']
                //"  xyzblankxyzxx".trim_matches(filter_chars[..]) => "  xyzblank"
                if matched_vec[0].0 != 0
                    && matched_vec[matched_vec.len() - 1].0 == sliced_expr.len() - 1
                {
                    let pivot = matched_vec
                        .iter()
                        .rev()
                        .enumerate()
                        .skip_while(|(vec_idx, (slice_idx, _))| {
                            *vec_idx == sliced_expr.len() - slice_idx - 1
                        })
                        .map(|(vec_idx, (_, _))| vec_idx)
                        .next();

                    let end = match pivot {
                        Some(idx) => matched_vec[matched_vec.len() - idx].0,
                        _ => matched_vec[0].0,
                    };

                    return Ok(Evaluated::StrSlice {
                        source,
                        range: range.start..end,
                    });
                }
                //filter_chars => ['x','y','z']
                //"xxbyz".trim_matches(filter_chars[..]) => "b"
                let pivot = matched_vec
                    .iter()
                    .enumerate()
                    .skip_while(|(vec_idx, (slice_idx, _))| vec_idx == slice_idx)
                    .map(|(vec_idx, (_, _))| vec_idx)
                    .next()
                    .unwrap_or(0);

                let trim_range = matched_vec[pivot - 1].0..(matched_vec[pivot].0 + range.start);

                Ok(Evaluated::StrSlice {
                    source,
                    range: range.start + trim_range.start + 1..trim_range.end,
                })
            }
            Some(TrimWhereField::Leading) => {
                let pivot = matched_vec
                    .iter()
                    .enumerate()
                    .skip_while(|(vec_idx, (slice_idx, _))| vec_idx == slice_idx)
                    .map(|(vec_idx, (_, _))| vec_idx)
                    .next();

                let start = match pivot {
                    Some(idx) => match idx {
                        0 => 0,
                        _ => matched_vec[idx - 1].0 + 1,
                    },
                    _ => matched_vec[matched_vec.len() - 1].0 + 1,
                };

                Ok(Evaluated::StrSlice {
                    source,
                    range: range.start + start..range.end,
                })
            }
            Some(TrimWhereField::Trailing) => {
                let pivot = matched_vec
                    .iter()
                    .rev()
                    .enumerate()
                    .skip_while(|(vec_idx, (slice_idx, _))| {
                        *vec_idx == sliced_expr.len() - slice_idx - 1
                    })
                    .map(|(vec_idx, (_, _))| vec_idx)
                    .next();

                let end = match pivot {
                    Some(idx) => match idx {
                        0 => range.end,
                        _ => matched_vec[matched_vec.len() - idx].0,
                    },
                    _ => matched_vec[0].0,
                };

                Ok(Evaluated::StrSlice {
                    source,
                    range: range.start..end,
                })
            }
            None => {
                let start = source
                    .chars()
                    .skip(range.start)
                    .enumerate()
                    .find(|(_, c)| !c.is_whitespace())
                    .map_or(0, |(idx, _)| idx + range.start);

                let end = source.len()
                    - source
                        .chars()
                        .rev()
                        .skip(source.len() - range.end)
                        .enumerate()
                        .find(|(_, c)| !c.is_whitespace())
                        .map_or(0, |(idx, _)| source.len() - (range.end - idx));

                Ok(Evaluated::StrSlice {
                    source,
                    range: start..end,
                })
            }
        }
    }

    pub fn try_into_value(self, data_type: &DataType, nullable: bool) -> Result<Value> {
        let value = match self {
            Evaluated::Literal(v) => literal_to_value(data_type, &v)?,
            Evaluated::Value(Value::Bytea(bytes)) if data_type == &DataType::Uuid => {
                Uuid::from_slice(&bytes)
                    .map_err(|_| ValueError::FailedToParseUUID(hex::encode(bytes)))
                    .map(|uuid| Value::Uuid(uuid.as_u128()))?
            }
            Evaluated::Value(value) => value,
            Evaluated::StrSlice {
                source: s,
                range: r,
            } => Value::Str(s[r].to_owned()),
        };

        value.validate_null(nullable)?;

        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::executor::evaluate::literal::Literal,
        std::{borrow::Cow, str::FromStr},
    };

    fn num(value: &str) -> Literal<'static> {
        Literal::Number(Cow::Owned(BigDecimal::from_str(value).unwrap()))
    }

    fn text(value: &str) -> Literal<'static> {
        Literal::Text(Cow::Owned(value.to_owned()))
    }

    fn eval(literal: Literal<'static>) -> Evaluated<'static> {
        Evaluated::Literal(literal)
    }

    #[test]
    fn literal_arithmetic_operations() {
        let one = eval(num("1"));
        let two = eval(num("2"));
        let zero = eval(num("0"));

        assert_eq!(one.add(&two).unwrap(), eval(num("3")));
        assert_eq!(two.subtract(&one).unwrap(), eval(num("1")));
        assert_eq!(one.multiply(&two).unwrap(), eval(num("2")));
        assert_eq!(two.divide(&one).unwrap(), eval(num("2")));
        assert_eq!(two.modulo(&one).unwrap(), eval(num("0")));

        assert!(matches!(
            one.divide(&zero),
            Err(crate::result::Error::Evaluate(
                EvaluateError::DivisorShouldNotBeZero
            ))
        ));
        assert!(matches!(
            one.modulo(&zero),
            Err(crate::result::Error::Evaluate(
                EvaluateError::DivisorShouldNotBeZero
            ))
        ));
    }

    #[test]
    fn literal_bitwise_operations() {
        let eight = eval(num("8"));
        let two = eval(num("2"));

        assert_eq!(eight.bitwise_and(&two).unwrap(), eval(num("0")));
        assert_eq!(eight.bitwise_shift_left(&two).unwrap(), eval(num("32")));
        assert_eq!(eight.bitwise_shift_right(&two).unwrap(), eval(num("2")));

        let invalid = eval(text("foo"));
        assert!(matches!(
            invalid.bitwise_and(&eight),
            Err(crate::result::Error::Evaluate(
                EvaluateError::UnsupportedBinaryOperation { .. }
            ))
        ));

        let fractional = eval(num("2.5"));
        assert!(matches!(
            eight.bitwise_shift_left(&fractional),
            Err(crate::result::Error::Evaluate(
                EvaluateError::IncompatibleBitOperation(_, _)
            ))
        ));
    }
}
