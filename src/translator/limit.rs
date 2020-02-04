use nom_sql::LimitClause;

pub struct Limit<'a> {
    pub limit_clause: &'a Option<LimitClause>,
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
