use {
    super::error::EvaluateError,
    crate::{
        ast::{BinaryOperator, DataType, TrimWhereField},
        data::{value::HashMapJsonExt, Key, Literal, Value},
        result::{Error, Result},
    },
    std::{borrow::Cow, cmp::Ordering, collections::HashMap, ops::Range},
};

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
            Evaluated::Literal(Literal::Boolean(v)) => Ok(v),
            Evaluated::Literal(v) => {
                Err(EvaluateError::BooleanTypeRequired(format!("{:?}", v)).into())
            }
            Evaluated::StrSlice { source, range } => {
                Err(EvaluateError::BooleanTypeRequired(source[range].to_owned()).into())
            }
            Evaluated::Value(Value::Bool(v)) => Ok(v),
            Evaluated::Value(v) => {
                Err(EvaluateError::BooleanTypeRequired(format!("{:?}", v)).into())
            }
        }
    }
}

impl TryFrom<Evaluated<'_>> for HashMap<String, Value> {
    type Error = Error;

    fn try_from(evaluated: Evaluated<'_>) -> Result<HashMap<String, Value>> {
        match evaluated {
            Evaluated::Literal(Literal::Text(v)) => HashMap::parse_json_object(v.as_ref()),
            Evaluated::Literal(v) => {
                Err(EvaluateError::TextLiteralRequired(format!("{v:?}")).into())
            }
            Evaluated::Value(Value::Str(v)) => HashMap::parse_json_object(v.as_str()),
            Evaluated::Value(Value::Map(v)) => Ok(v),
            Evaluated::Value(v) => Err(EvaluateError::MapOrStringValueRequired(v.into()).into()),
            Evaluated::StrSlice { source, range } => HashMap::parse_json_object(&source[range]),
        }
    }
}

fn binary_op<'a, 'b, T, U>(
    l: &Evaluated<'a>,
    r: &Evaluated<'b>,
    op: BinaryOperator,
    value_op: T,
    literal_op: U,
) -> Result<Evaluated<'b>>
where
    T: FnOnce(&Value, &Value) -> Result<Value>,
    U: FnOnce(&Literal<'a>, &Literal<'b>) -> Result<Literal<'b>>,
{
    match (l, r) {
        (Evaluated::Literal(l), Evaluated::Literal(r)) => literal_op(l, r).map(Evaluated::Literal),
        (Evaluated::Literal(l), Evaluated::Value(r)) => {
            value_op(&Value::try_from(l)?, r).map(Evaluated::Value)
        }
        (Evaluated::Value(l), Evaluated::Literal(r)) => {
            value_op(l, &Value::try_from(r)?).map(Evaluated::Value)
        }
        (Evaluated::Value(l), Evaluated::Value(r)) => value_op(l, r).map(Evaluated::Value),
        (l, r) => Err(EvaluateError::UnsupportedBinaryOperation {
            left: format!("{:?}", l),
            op,
            right: format!("{:?}", r),
        }
        .into()),
    }
}

pub fn exceptional_int_val_to_eval<'a>(name: String, v: Value) -> Result<Evaluated<'a>> {
    match v {
        Value::Null => Ok(Evaluated::Value(Value::Null)),
        _ => Err(EvaluateError::FunctionRequiresIntegerValue(name).into()),
    }
}

impl<'a> Evaluated<'a> {
    pub fn evaluate_eq(&self, other: &Evaluated<'a>) -> bool {
        match (self, other) {
            (Evaluated::Literal(a), Evaluated::Literal(b)) => a.evaluate_eq(b),
            (Evaluated::Literal(b), Evaluated::Value(a))
            | (Evaluated::Value(a), Evaluated::Literal(b)) => a.evaluate_eq_with_literal(b),
            (Evaluated::Value(a), Evaluated::Value(b)) => a.evaluate_eq(b),
            (Evaluated::Literal(a), Evaluated::StrSlice { source, range })
            | (Evaluated::StrSlice { source, range }, Evaluated::Literal(a)) => {
                let b = &source[range.clone()];

                a.evaluate_eq(&Literal::Text(Cow::Borrowed(b)))
            }
            (Evaluated::Value(a), Evaluated::StrSlice { source, range })
            | (Evaluated::StrSlice { source, range }, Evaluated::Value(a)) => {
                let b = &source[range.clone()];

                a.evaluate_eq_with_literal(&Literal::Text(Cow::Borrowed(b)))
            }
            (
                Evaluated::StrSlice { source, range },
                Evaluated::StrSlice {
                    source: source2,
                    range: range2,
                },
            ) => source[range.clone()] == source2[range2.clone()],
        }
    }

    pub fn evaluate_cmp(&self, other: &Evaluated<'a>) -> Option<Ordering> {
        match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => l.evaluate_cmp(r),
            (Evaluated::Literal(l), Evaluated::Value(r)) => {
                r.evaluate_cmp_with_literal(l).map(|o| o.reverse())
            }
            (Evaluated::Value(l), Evaluated::Literal(r)) => l.evaluate_cmp_with_literal(r),
            (Evaluated::Value(l), Evaluated::Value(r)) => l.evaluate_cmp(r),
            (Evaluated::Literal(l), Evaluated::StrSlice { source, range }) => {
                let r = Literal::Text(Cow::Borrowed(&source[range.clone()]));

                l.evaluate_cmp(&r)
            }
            (Evaluated::Value(l), Evaluated::StrSlice { source, range }) => {
                let r = Literal::Text(Cow::Borrowed(&source[range.clone()]));

                l.evaluate_cmp_with_literal(&r)
            }
            (Evaluated::StrSlice { source, range }, Evaluated::Literal(l)) => {
                let r = Literal::Text(Cow::Borrowed(&source[range.clone()]));

                l.evaluate_cmp(&r).map(|o| o.reverse())
            }
            (Evaluated::StrSlice { source, range }, Evaluated::Value(r)) => {
                let l = Literal::Text(Cow::Borrowed(&source[range.clone()]));

                r.evaluate_cmp_with_literal(&l).map(|o| o.reverse())
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
            ) => a[ar.clone()].partial_cmp(&b[br.clone()]),
        }
    }

    pub fn add<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(
            self,
            other,
            BinaryOperator::Plus,
            |l, r| l.add(r),
            |l, r| l.add(r),
        )
    }

    pub fn subtract<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(
            self,
            other,
            BinaryOperator::Minus,
            |l, r| l.subtract(r),
            |l, r| l.subtract(r),
        )
    }

    pub fn multiply<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(
            self,
            other,
            BinaryOperator::Multiply,
            |l, r| l.multiply(r),
            |l, r| l.multiply(r),
        )
    }

    pub fn divide<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(
            self,
            other,
            BinaryOperator::Divide,
            |l, r| l.divide(r),
            |l, r| l.divide(r),
        )
    }

    pub fn bitwise_and<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(
            self,
            other,
            BinaryOperator::BitwiseAnd,
            |l, r| l.bitwise_and(r),
            |l, r| l.bitwise_and(r),
        )
    }

    pub fn modulo<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(
            self,
            other,
            BinaryOperator::Modulo,
            |l, r| l.modulo(r),
            |l, r| l.modulo(r),
        )
    }

    pub fn bitwise_shift_left<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(
            self,
            other,
            BinaryOperator::BitwiseShiftLeft,
            |l, r| l.bitwise_shift_left(r),
            |l, r| l.bitwise_shift_left(r),
        )
    }

    pub fn bitwise_shift_right<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(
            self,
            other,
            BinaryOperator::BitwiseShiftRight,
            |l, r| l.bitwise_shift_right(r),
            |l, r| l.bitwise_shift_right(r),
        )
    }

    pub fn unary_plus(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(v) => v.unary_plus().map(Evaluated::Literal),
            Evaluated::Value(v) => v.unary_plus().map(Evaluated::Value),
            Evaluated::StrSlice { source, range } => {
                Err(EvaluateError::UnsupportedUnaryPlus(source[range.clone()].to_owned()).into())
            }
        }
    }

    pub fn unary_minus(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(v) => v.unary_minus().map(Evaluated::Literal),
            Evaluated::Value(v) => v.unary_minus().map(Evaluated::Value),
            Evaluated::StrSlice { source, range } => {
                Err(EvaluateError::UnsupportedUnaryMinus(source[range.clone()].to_owned()).into())
            }
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
            Evaluated::Literal(literal) => Value::try_cast_from_literal(data_type, &literal),
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
                Evaluated::Literal(l.like(&r, case_sensitive)?)
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
            Evaluated::Literal(Literal::Null) | Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null))
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
        match self {
            Evaluated::Value(v) => v.is_null(),
            Evaluated::StrSlice { .. } => false,
            Evaluated::Literal(v) => matches!(v, &Literal::Null),
        }
    }

    pub fn rtrim(self, name: String, chars: Option<Evaluated<'_>>) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Literal(Literal::Text(l)) => {
                let end = l.len();
                (l, 0..end)
            }
            Evaluated::Literal(Literal::Null) | Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null))
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
            Evaluated::Literal(Literal::Null) | Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null))
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
                _ => return exceptional_int_val_to_eval(name, value),
            }
        } - 1;

        let count = match count {
            Some(eval) => {
                let value = eval.try_into()?;
                match value {
                    Value::I64(num) => num,
                    _ => return exceptional_int_val_to_eval(name, value),
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
        trim_where_field: &Option<TrimWhereField>,
    ) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Literal(Literal::Text(l)) => {
                let end = l.len();
                (l, 0..end)
            }
            Evaluated::Literal(Literal::Null) | Evaluated::Value(Value::Null) => {
                return Ok(Evaluated::Value(Value::Null))
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
                    .map(|(idx, _)| idx + range.start)
                    .unwrap_or(0);

                let end = source.len()
                    - source
                        .chars()
                        .rev()
                        .skip(source.len() - range.end)
                        .enumerate()
                        .find(|(_, c)| !c.is_whitespace())
                        .map(|(idx, _)| source.len() - (range.end - idx))
                        .unwrap_or(0);

                Ok(Evaluated::StrSlice {
                    source,
                    range: start..end,
                })
            }
        }
    }

    pub fn try_into_value(self, data_type: &DataType, nullable: bool) -> Result<Value> {
        let value = match self {
            Evaluated::Literal(v) => Value::try_from_literal(data_type, &v)?,
            Evaluated::Value(v) => v,
            Evaluated::StrSlice {
                source: s,
                range: r,
            } => Value::Str(s[r].to_owned()),
        };

        value.validate_null(nullable)?;

        Ok(value)
    }
}
