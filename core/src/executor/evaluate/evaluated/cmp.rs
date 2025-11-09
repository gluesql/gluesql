use {
    super::Evaluated,
    crate::data::Literal,
    std::{borrow::Cow, cmp::Ordering},
};

impl<'a> Evaluated<'a> {
    pub fn evaluate_cmp(&self, other: &Evaluated<'a>) -> Option<Ordering> {
        match (self, other) {
            (Evaluated::Literal(l), Evaluated::Literal(r)) => l.evaluate_cmp(r),
            (Evaluated::Literal(l), Evaluated::Value(r)) => {
                r.evaluate_cmp_with_literal(l).map(Ordering::reverse)
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

                l.evaluate_cmp(&r).map(Ordering::reverse)
            }
            (Evaluated::StrSlice { source, range }, Evaluated::Value(r)) => {
                let l = Literal::Text(Cow::Borrowed(&source[range.clone()]));

                r.evaluate_cmp_with_literal(&l).map(Ordering::reverse)
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
}
