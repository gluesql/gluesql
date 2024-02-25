use crate::ast::Statement;

pub fn begin() -> Statement {
    Statement::StartTransaction
}
pub fn commit() -> Statement {
    Statement::Commit
}
pub fn rollback() -> Statement {
    Statement::Rollback
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{begin, commit, rollback, test};

    #[test]
    fn transaction() {
        let actual = begin();
        let expected = "START TRANSACTION";
        test(Ok(actual), expected);

        let actual = commit();
        let expected = "COMMIT";
        test(Ok(actual), expected);

        let actual = rollback();
        let expected = "ROLLBACK";
        test(Ok(actual), expected);
    }
}
