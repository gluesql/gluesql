use {
    super::Evaluated,
    crate::{
        data::{StringExt, Value},
        executor::evaluate::error::EvaluateError,
        result::Result,
    },
};

impl<'a> Evaluated<'a> {
    pub fn like(&self, other: Evaluated<'a>, case_sensitive: bool) -> Result<Evaluated<'a>> {
        let evaluated = match (self, other) {
            (Evaluated::Text(lhs), Evaluated::Text(rhs)) => Evaluated::Value(Value::Bool(
                lhs.as_ref().like(rhs.as_ref(), case_sensitive)?,
            )),
            (
                left @ (Evaluated::Number(_) | Evaluated::Text(_)),
                right @ (Evaluated::Number(_) | Evaluated::Text(_)),
            ) => {
                return Err(EvaluateError::LikeOnNonStringLiteral {
                    base: literal_string(left),
                    pattern: literal_string(&right),
                    case_sensitive,
                }
                .into());
            }
            (literal @ (Evaluated::Number(_) | Evaluated::Text(_)), Evaluated::Value(r)) => {
                Evaluated::Value(Value::try_from(literal.clone())?.like(&r, case_sensitive)?)
            }
            (Evaluated::Value(l), literal @ (Evaluated::Number(_) | Evaluated::Text(_))) => {
                Evaluated::Value(l.like(&Value::try_from(literal.clone())?, case_sensitive)?)
            }
            (Evaluated::Value(l), Evaluated::Value(r)) => {
                Evaluated::Value(l.like(&r, case_sensitive)?)
            }
            (
                literal @ (Evaluated::Number(_) | Evaluated::Text(_)),
                Evaluated::StrSlice { source, range },
            ) => Evaluated::Value(
                Value::try_from(literal.clone())?
                    .like(&Value::Str(source[range].to_owned()), case_sensitive)?,
            ),
            (
                Evaluated::StrSlice { source, range },
                literal @ (Evaluated::Number(_) | Evaluated::Text(_)),
            ) => Evaluated::Value(
                Value::Str(source[range.clone()].to_owned())
                    .like(&Value::try_from(literal.clone())?, case_sensitive)?,
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
}

fn literal_string(evaluated: &Evaluated<'_>) -> String {
    match evaluated {
        Evaluated::Number(value) => value.to_string(),
        Evaluated::Text(value) => value.to_string(),
        _ => format!("{evaluated:?}"),
    }
}
