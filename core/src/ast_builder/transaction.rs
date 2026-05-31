use crate::{ast::Statement, plan::StatementPlan};

pub fn begin() -> StatementPlan {
    Statement::StartTransaction.into()
}
pub fn commit() -> StatementPlan {
    Statement::Commit.into()
}
pub fn rollback() -> StatementPlan {
    Statement::Rollback.into()
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{begin, commit, rollback, test};

    #[test]
    fn transaction() {
        let actual = begin();
        let expected = "START TRANSACTION";
        test(&Ok(actual), expected);

        let actual = commit();
        let expected = "COMMIT";
        test(&Ok(actual), expected);

        let actual = rollback();
        let expected = "ROLLBACK";
        test(&Ok(actual), expected);
    }
}
