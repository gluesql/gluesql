#![cfg(feature = "transaction")]

use crate::{
    ast::Statement,
    result::Result,
};

pub fn begin() -> Result<Statement> {Ok(Statement::StartTransaction)}
pub fn commit() -> Result<Statement> {Ok(Statement::Commit)}
pub fn rollback() -> Result<Statement> {Ok(Statement::Rollback)}

#[cfg(all(test, feature = "transaction"))]
mod tests {
    use crate::ast_builder::{test};
    use crate::ast_builder::transaction::{begin, commit, rollback};

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