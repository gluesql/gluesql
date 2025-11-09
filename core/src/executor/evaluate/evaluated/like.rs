use {
    super::Evaluated,
    crate::{data::Value, result::Result},
};

impl<'a> Evaluated<'a> {
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
}
