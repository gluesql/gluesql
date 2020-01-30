use nom_sql::LimitClause;
use std::convert::From;

pub struct Limit<'a> {
    limit_clause: &'a Option<LimitClause>,
}

impl Limit<'_> {
    pub fn check(&self, i: &usize) -> bool {
        let i = *i as u64;

        self.limit_clause
            .as_ref()
            .map_or(true, |LimitClause { limit, offset }| {
                i >= *offset && i < *offset + *limit
            })
    }
}

impl<'a> From<&'a Option<LimitClause>> for Limit<'a> {
    fn from(limit_clause: &'a Option<LimitClause>) -> Self {
        Limit { limit_clause }
    }
}
