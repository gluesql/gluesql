use crate::{ast::Statement, result::Result};

pub fn begin() -> Result<Statement> {
    Ok(Statement::StartTransaction)
}
pub fn commit() -> Result<Statement> {
    Ok(Statement::Commit)
}
pub fn rollback() -> Result<Statement> {
    Ok(Statement::Rollback)
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{begin, commit, rollback, test};

    #[test]
    fn transaction() {
        let actual = begin();
        let expected = "START TRANSACTION";
        test(actual, expected);

        let actual = commit();
        let expected = "COMMIT";
        test(actual, expected);

        let actual = rollback();
        let expected = "ROLLBACK";
        test(actual, expected);
    }
}
