use nom_sql::LimitClause;
use std::convert::From;

pub struct Limit {
    limit_clause: Option<LimitClause>,
}

impl Limit {
    pub fn check(&self, i: &usize) -> bool {
        let i = *i as u64;

        self.limit_clause
            .as_ref()
            .map_or(true, |LimitClause { limit, offset }| {
                i >= *offset && i < *offset + *limit
            })
    }
}

impl From<Option<LimitClause>> for Limit {
    fn from(limit_clause: Option<LimitClause>) -> Self {
        Limit { limit_clause }
    }
}
