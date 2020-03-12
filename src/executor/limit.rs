use nom_sql::LimitClause;

pub struct Limit<'a> {
    limit_clause: &'a Option<LimitClause>,
}

impl<'a> Limit<'a> {
    pub fn new(limit_clause: &'a Option<LimitClause>) -> Self {
        Limit { limit_clause }
    }

    pub fn check(&self, i: usize) -> bool {
        let i = i as u64;

        self.limit_clause
            .as_ref()
            .map_or(true, |LimitClause { limit, offset }| {
                i >= *offset && i < *offset + *limit
            })
    }
}
