use {super::Evaluated, crate::data::Literal, std::borrow::Cow, utils::Tribool};

impl<'a> Evaluated<'a> {
    pub fn evaluate_eq(&self, other: &Evaluated<'a>) -> Tribool {
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
            ) => Tribool::from(source[range.clone()] == source2[range2.clone()]),
        }
    }
}
