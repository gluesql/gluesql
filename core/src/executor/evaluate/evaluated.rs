use {
    super::error::EvaluateError,
    crate::{
        ast::{DataType, TrimWhereField},
        data::{Key, Literal, Value},
        result::{Error, Result},
    },
    std::{
        cmp::{max, min, Ordering},
        ops::Range,
    },
};

#[derive(Clone, Debug)]
pub enum Evaluated<'a> {
    Literal(Literal<'a>),
    StrSlice { source: String, range: Range<usize> },
    Value(Value),
}

impl<'a> From<Value> for Evaluated<'a> {
    fn from(value: Value) -> Self {
        Evaluated::Value(value)
    }
}

impl TryFrom<Evaluated<'_>> for Value {
    type Error = Error;

    fn try_from(e: Evaluated<'_>) -> Result<Value> {
        match e {
            Evaluated::Literal(v) => Value::try_from(v),
            Evaluated::StrSlice {
                source: s,
                range: r,
            } => match s.as_str() {
                "NULL" => Ok(Value::Null),
                _ => Ok(Value::Str(s[r].to_owned())),
            },
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
            Evaluated::StrSlice { source, range: _ } => Ok(Key::from(source)),
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
            Evaluated::StrSlice { source, range: _ } => {
                Err(EvaluateError::BooleanTypeRequired(format!("{:?}", source)).into())
            }
            Evaluated::Value(Value::Bool(v)) => Ok(v),
            Evaluated::Value(v) => {
                Err(EvaluateError::BooleanTypeRequired(format!("{:?}", v)).into())
            }
        }
    }
}

impl<'a> PartialEq for Evaluated<'a> {
    fn eq(&self, other: &Evaluated<'a>) -> bool {
        match (self, other) {
            (Evaluated::Literal(a), Evaluated::Literal(b)) => a == b,
            (Evaluated::Literal(b), Evaluated::Value(a))
            | (Evaluated::Value(a), Evaluated::Literal(b)) => a == b,
            (Evaluated::Value(a), Evaluated::Value(b)) => a == b,
            (
                Evaluated::Literal(a),
                Evaluated::StrSlice {
                    source: b,
                    range: _,
                },
            ) => a == b,
            (
                Evaluated::Value(a),
                Evaluated::StrSlice {
                    source: b,
                    range: _,
                },
            ) => a == b,
            (
                Evaluated::StrSlice {
                    source: a,
                    range: _,
                },
                Evaluated::Literal(b),
            ) => b == a,
            (
                Evaluated::StrSlice {
                    source: a,
                    range: _,
                },
                Evaluated::Value(b),
            ) => b == a,
            (
                Evaluated::StrSlice {
                    source: a,
                    range: _,
                },
                Evaluated::StrSlice {
                    source: b,
                    range: _,
                },
            ) => a == b,
        }
    }
}

impl<'a> PartialOrd for Evaluated<'a> {
    fn partial_cmp(&self, other: &Evaluated<'a>) -> Option<Ordering> {
        match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => l.partial_cmp(r),
            (Evaluated::Literal(l), Evaluated::Value(r)) => r.partial_cmp(l).map(|o| o.reverse()),
            (Evaluated::Value(l), Evaluated::Literal(r)) => l.partial_cmp(r),
            (Evaluated::Value(l), Evaluated::Value(r)) => l.partial_cmp(r),
            (
                Evaluated::Literal(l),
                Evaluated::StrSlice {
                    source: r,
                    range: _,
                },
            ) => l.partial_cmp(r),
            (
                Evaluated::Value(l),
                Evaluated::StrSlice {
                    source: r,
                    range: _,
                },
            ) => l.partial_cmp(r),
            (
                Evaluated::StrSlice {
                    source: l,
                    range: _,
                },
                Evaluated::Literal(r),
            ) => r.partial_cmp(l).map(|o| o.reverse()),
            (
                Evaluated::StrSlice {
                    source: l,
                    range: _,
                },
                Evaluated::Value(r),
            ) => r.partial_cmp(l).map(|o| o.reverse()),
            (
                Evaluated::StrSlice {
                    source: l,
                    range: _,
                },
                Evaluated::StrSlice {
                    source: r,
                    range: _,
                },
            ) => l.partial_cmp(r),
        }
    }
}

fn binary_op<'a, 'b, T, U>(
    l: &Evaluated<'a>,
    r: &Evaluated<'b>,
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
            value_op(&Value::try_from(l)?, r).map(Evaluated::from)
        }
        (Evaluated::Value(l), Evaluated::Literal(r)) => {
            value_op(l, &Value::try_from(r)?).map(Evaluated::from)
        }
        (Evaluated::Value(l), Evaluated::Value(r)) => value_op(l, r).map(Evaluated::from),
        (
            Evaluated::Literal(l),
            Evaluated::StrSlice {
                source: r,
                range: _,
            },
        ) => value_op(&Value::try_from(l)?, &Value::Str(r.to_owned())).map(Evaluated::from),
        (
            Evaluated::StrSlice {
                source: l,
                range: _,
            },
            Evaluated::Literal(r),
        ) => value_op(&Value::Str(l.to_owned()), &Value::try_from(r)?).map(Evaluated::from),
        (
            Evaluated::StrSlice {
                source: l,
                range: _,
            },
            Evaluated::StrSlice {
                source: r,
                range: _,
            },
        ) => value_op(&Value::Str(l.to_owned()), &Value::Str(r.to_owned())).map(Evaluated::from),
        (
            Evaluated::StrSlice {
                source: l,
                range: _,
            },
            Evaluated::Value(r),
        ) => value_op(&Value::Str(l.to_owned()), r).map(Evaluated::from),
        (
            Evaluated::Value(l),
            Evaluated::StrSlice {
                source: r,
                range: _,
            },
        ) => value_op(l, &Value::Str(r.to_owned())).map(Evaluated::from),
    }
}

pub fn exceptional_int_val_to_eval<'a>(name: String, v: Value) -> Result<Evaluated<'a>> {
    match v {
        Value::Null => Ok(Evaluated::from(Value::Null)),
        Value::I64(num) => Ok(Evaluated::from(Value::I64(num))),
        _ => Err(EvaluateError::FunctionRequiresIntegerValue(name).into()),
    }
}

pub fn exceptional_str_val_to_eval<'a>(name: String, v: Value) -> Result<Evaluated<'a>> {
    match v {
        Value::Null => Ok(Evaluated::from(Value::Null)),
        Value::Str(s) => Ok(Evaluated::from(Value::Str(s))),
        _ => Err(EvaluateError::FunctionRequiresStringValue(name).into()),
    }
}

impl<'a> Evaluated<'a> {
    pub fn add<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(self, other, |l, r| l.add(r), |l, r| l.add(r))
    }

    pub fn subtract<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(self, other, |l, r| l.subtract(r), |l, r| l.subtract(r))
    }

    pub fn multiply<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(self, other, |l, r| l.multiply(r), |l, r| l.multiply(r))
    }

    pub fn divide<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(self, other, |l, r| l.divide(r), |l, r| l.divide(r))
    }

    pub fn modulo<'b>(&'a self, other: &Evaluated<'b>) -> Result<Evaluated<'b>> {
        binary_op(self, other, |l, r| l.modulo(r), |l, r| l.modulo(r))
    }

    pub fn unary_plus(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(v) => v.unary_plus().map(Evaluated::Literal),
            Evaluated::Value(v) => v.unary_plus().map(Evaluated::from),
            Evaluated::StrSlice {
                source: s,
                range: _,
            } => Value::Str(s.to_owned()).unary_plus().map(Evaluated::from),
        }
    }

    pub fn unary_minus(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(v) => v.unary_minus().map(Evaluated::Literal),
            Evaluated::Value(v) => v.unary_minus().map(Evaluated::from),
            Evaluated::StrSlice {
                source: s,
                range: _,
            } => Value::Str(s.to_owned()).unary_minus().map(Evaluated::from),
        }
    }

    pub fn unary_factorial(&self) -> Result<Evaluated<'a>> {
        match self {
            Evaluated::Literal(v) => Value::try_from(v).and_then(|v| v.unary_factorial()),
            Evaluated::Value(v) => v.unary_factorial(),
            Evaluated::StrSlice {
                source: s,
                range: _,
            } => Value::Str(s.to_owned()).unary_factorial(),
        }
        .map(Evaluated::from)
    }

    pub fn cast(self, data_type: &DataType) -> Result<Evaluated<'a>> {
        let cast_literal = |literal: &Literal| Value::try_cast_from_literal(data_type, literal);
        let cast_value = |value: &Value| value.cast(data_type);

        match self {
            Evaluated::Literal(value) => cast_literal(&value),
            Evaluated::Value(value) => cast_value(&value),
            Evaluated::StrSlice {
                source: s,
                range: _,
            } => cast_value(&Value::Str(s)),
        }
        .map(Evaluated::from)
    }

    pub fn concat(self, other: Evaluated) -> Result<Evaluated<'a>> {
        let evaluated = match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => Evaluated::Literal(l.concat(r)),
            (Evaluated::Literal(l), Evaluated::Value(r)) => {
                Evaluated::from((Value::try_from(l)?).concat(r))
            }
            (Evaluated::Value(l), Evaluated::Literal(r)) => {
                Evaluated::from(l.concat(Value::try_from(r)?))
            }
            (Evaluated::Value(l), Evaluated::Value(r)) => Evaluated::from(l.concat(r)),
            (
                Evaluated::Literal(l),
                Evaluated::StrSlice {
                    source: r,
                    range: _,
                },
            ) => Evaluated::from((Value::try_from(l)?).concat(Value::Str(r))),
            (
                Evaluated::Value(l),
                Evaluated::StrSlice {
                    source: r,
                    range: _,
                },
            ) => Evaluated::from(l.concat(Value::Str(r))),
            (
                Evaluated::StrSlice {
                    source: l,
                    range: _,
                },
                Evaluated::Literal(r),
            ) => Evaluated::from(Value::Str(l).concat(Value::try_from(r)?)),
            (
                Evaluated::StrSlice {
                    source: l,
                    range: _,
                },
                Evaluated::Value(r),
            ) => Evaluated::from(Value::Str(l).concat(r)),
            (
                Evaluated::StrSlice {
                    source: l,
                    range: _,
                },
                Evaluated::StrSlice {
                    source: r,
                    range: _,
                },
            ) => Evaluated::from(Value::Str(l).concat(Value::Str(r))),
        };

        Ok(evaluated)
    }

    pub fn like(&self, other: Evaluated<'a>, case_sensitive: bool) -> Result<Evaluated<'a>> {
        let evaluated = match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => {
                Evaluated::Literal(l.like(&r, case_sensitive)?)
            }
            (Evaluated::Literal(l), Evaluated::Value(r)) => {
                Evaluated::from((Value::try_from(l)?).like(&r, case_sensitive)?)
            }
            (Evaluated::Value(l), Evaluated::Literal(r)) => {
                Evaluated::from(l.like(&Value::try_from(r)?, case_sensitive)?)
            }
            (Evaluated::Value(l), Evaluated::Value(r)) => {
                Evaluated::from(l.like(&r, case_sensitive)?)
            }
            (
                Evaluated::Literal(l),
                Evaluated::StrSlice {
                    source: r,
                    range: _,
                },
            ) => Evaluated::from(Value::try_from(l)?.like(&Value::Str(r), case_sensitive)?),
            (
                Evaluated::StrSlice {
                    source: l,
                    range: _,
                },
                Evaluated::Literal(r),
            ) => Evaluated::from(
                Value::Str(l.to_owned()).like(&Value::try_from(r)?, case_sensitive)?,
            ),
            (
                Evaluated::StrSlice {
                    source: l,
                    range: _,
                },
                Evaluated::StrSlice {
                    source: r,
                    range: _,
                },
            ) => Evaluated::from(Value::Str(l.to_owned()).like(&Value::Str(r), case_sensitive)?),
            (
                Evaluated::StrSlice {
                    source: l,
                    range: _,
                },
                Evaluated::Value(r),
            ) => Evaluated::from(Value::Str(l.to_owned()).like(&r, case_sensitive)?),
            (
                Evaluated::Value(l),
                Evaluated::StrSlice {
                    source: r,
                    range: _,
                },
            ) => Evaluated::from(l.like(&Value::Str(r), case_sensitive)?),
        };

        Ok(evaluated)
    }

    pub fn ltrim(self, name: String, chars: Option<Evaluated<'_>>) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Literal(literal) => {
                let value = Value::try_from(literal)?;
                match value {
                    Value::Str(string) => {
                        let end = string.len();
                        (string, 0..end)
                    }
                    _ => return exceptional_str_val_to_eval(name, value),
                }
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(value) => match value {
                Value::Str(string) => {
                    let end = string.len();
                    (string, 0..end)
                }
                _ => return exceptional_str_val_to_eval(name, value),
            },
        };

        let expr_str = source.as_str();
        let filter_chars = match chars {
            Some(expr) => match expr.try_into()? {
                Value::Str(value) => value,
                Value::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresStringValue(name).into());
                }
            }
            .chars()
            .collect::<Vec<_>>(),
            None => vec![' '],
        };
        let sliced_expr = &expr_str[range.clone()];
        let matched_vec: Vec<_> = sliced_expr.match_indices(&filter_chars[..]).collect();

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
            source: expr_str.to_owned(),
            range: range.start + start..range.end,
        })
    }

    pub fn is_null(&self) -> bool {
        match self {
            Evaluated::Value(v) => v.is_null(),
            Evaluated::StrSlice {
                source: s,
                range: _,
            } => Value::Str(s.to_owned()).is_null(),
            Evaluated::Literal(v) => matches!(v, &Literal::Null),
        }
    }

    pub fn rtrim(self, name: String, chars: Option<Evaluated<'_>>) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Literal(literal) => {
                let value = Value::try_from(literal)?;
                match value {
                    Value::Str(string) => {
                        let end = string.len();
                        (string, 0..end)
                    }
                    _ => return exceptional_str_val_to_eval(name, value),
                }
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(value) => match value {
                Value::Str(string) => {
                    let end = string.len();
                    (string, 0..end)
                }
                _ => return exceptional_str_val_to_eval(name, value),
            },
        };

        let expr_str = source.as_str();
        let filter_chars = match chars {
            Some(expr) => match expr.try_into()? {
                Value::Str(value) => value,
                Value::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresStringValue(name).into());
                }
            }
            .chars()
            .collect::<Vec<_>>(),
            None => vec![' '],
        };
        let sliced_expr = &expr_str[range.clone()];
        let matched_vec: Vec<_> = sliced_expr.match_indices(&filter_chars[..]).collect();
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
            _ => match matched_vec[matched_vec.len() - 1].0 == sliced_expr.len() - 1 {
                true => matched_vec[0].0,
                false => range.start,
            },
        };

        Ok(Evaluated::StrSlice {
            source: expr_str.to_owned(),
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
            Evaluated::Literal(literal) => {
                let value = Value::try_from(literal)?;
                match value {
                    Value::Str(string) => {
                        let end = string.len();
                        (string, 0..end)
                    }
                    _ => return exceptional_str_val_to_eval(name, value),
                }
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(value) => match value {
                Value::Str(string) => {
                    let end = string.len();
                    (string, 0..end)
                }
                _ => return exceptional_str_val_to_eval(name, value),
            },
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
            min(
                max(range.start as i64 + start + count, 0),
                source.len() as i64,
            ) as usize
        };

        let start = min(max(start + range.start as i64, 0), source.len() as i64) as usize;

        Ok(Evaluated::StrSlice {
            source,
            range: start..end,
        })
    }

    pub fn trim(
        self,
        name: String,
        filter_chars: Option<Evaluated<'_>>,
        trim_where_field: &'a Option<TrimWhereField>,
    ) -> Result<Evaluated<'a>> {
        let (source, range) = match self {
            Evaluated::Literal(literal) => {
                let value = Value::try_from(literal)?;
                match value {
                    Value::Str(string) => {
                        let end = string.len();
                        (string, 0..end)
                    }
                    _ => return exceptional_str_val_to_eval(name, value),
                }
            }
            Evaluated::StrSlice { source, range } => (source, range),
            Evaluated::Value(value) => match value {
                Value::Str(string) => {
                    let end = string.len();
                    (string, 0..end)
                }
                _ => return exceptional_str_val_to_eval(name, value),
            },
        };

        let expr_str = source.as_str();
        let filter_chars = match filter_chars {
            Some(expr) => match expr.try_into()? {
                Value::Str(value) => value,
                Value::Null => {
                    return Ok(Evaluated::from(Value::Null));
                }
                _ => {
                    return Err(EvaluateError::FunctionRequiresStringValue(name).into());
                }
            }
            .chars()
            .collect::<Vec<_>>(),
            None => vec![' '],
        };
        let sliced_expr = &expr_str[range.clone()];
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
        //"txu".trim_matches(filter_chars[..]) => "txv"
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
                        Some(idx) => match idx {
                            0 => 0,
                            _ => matched_vec[idx - 1].0 + 1,
                        },
                        _ => matched_vec[matched_vec.len() - 1].0 + 1,
                    };

                    return Ok(Evaluated::StrSlice {
                        source: expr_str.to_owned(),
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
                        source: expr_str.to_owned(),
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
                    .next();

                let trim_range = match pivot {
                    Some(idx) => matched_vec[idx - 1].0..(matched_vec[idx].0 + range.start),
                    _ => matched_vec[matched_vec.len() - 1].0..range.end,
                };

                Ok(Evaluated::StrSlice {
                    source: expr_str.to_owned(),
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
                    source: expr_str.to_owned(),
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
                    _ => match matched_vec[matched_vec.len() - 1].0 == sliced_expr.len() - 1 {
                        true => matched_vec[0].0,
                        false => range.start,
                    },
                };

                Ok(Evaluated::StrSlice {
                    source: expr_str.to_owned(),
                    range: range.start..end,
                })
            }
            None => {
                let start = expr_str
                    .chars()
                    .skip(range.start)
                    .enumerate()
                    .find(|(_, c)| !c.is_whitespace())
                    .map(|(idx, _)| idx + range.start)
                    .unwrap_or(0);

                let end = expr_str.len()
                    - expr_str
                        .chars()
                        .rev()
                        .skip(expr_str.len() - range.end)
                        .enumerate()
                        .find(|(_, c)| !c.is_whitespace())
                        .map(|(idx, _)| expr_str.len() - (range.end - idx))
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
