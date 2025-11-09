use {
    super::Evaluated,
    crate::{data::Value, result::Result},
};

impl<'a> Evaluated<'a> {
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
}
