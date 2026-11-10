use {
    super::Build,
    crate::{ast::Statement, plan::StatementPlan, result::Result},
};

#[derive(Clone, Debug)]
pub enum TransactionNode {
    Begin,
    Commit,
    Rollback,
}

pub fn begin() -> TransactionNode {
    TransactionNode::Begin
}

pub fn commit() -> TransactionNode {
    TransactionNode::Commit
}

pub fn rollback() -> TransactionNode {
    TransactionNode::Rollback
}

impl Build for TransactionNode {
    fn build(self) -> Result<StatementPlan> {
        let statement = match self {
            Self::Begin => Statement::StartTransaction,
            Self::Commit => Statement::Commit,
            Self::Rollback => Statement::Rollback,
        };

        Ok(statement.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::query_builder::{Build, begin, commit, rollback, test};

    #[test]
    fn transaction() {
        let actual = begin().build();
        let expected = "START TRANSACTION";
        test(&actual, expected);

        let actual = commit().build();
        let expected = "COMMIT";
        test(&actual, expected);

        let actual = rollback().build();
        let expected = "ROLLBACK";
        test(&actual, expected);
    }
}
