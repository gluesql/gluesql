use super::OrderByNode;

use {
    super::ExprList,
    crate::{
        ast::{Expr, Query, SetExpr, Statement, Values},
        result::Result,
    },
};

#[derive(Clone, Debug)]
pub struct ValuesNode<'a> {
    pub values: Vec<ExprList<'a>>,
}

impl<'a> ValuesNode<'a> {
    // pub fn ordery_by(self) -> OrderByNode<'a> {}

    pub fn build(self) -> Result<Statement> {
        let values = self
            .values
            .into_iter()
            .map(|a| a.try_into())
            .collect::<Result<Vec<Vec<Expr>>>>()?;

        Ok(Statement::Query(Query {
            body: SetExpr::Values(Values(values)),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }))
    }
}

pub fn values<'a, T: Into<ExprList<'a>>>(values: Vec<T>) -> ValuesNode<'a> {
    let values: Vec<ExprList> = values.into_iter().map(Into::into).collect();

    ValuesNode { values }
}

#[cfg(test)]
mod tests {
    use crate::ast_builder::{num, test};

    use super::values;

    #[test]
    fn values_test() {
        let actual = values(vec![vec![num(7)]]).build();
        let expected = "VALUES(7)";
        test(actual, expected);

        let actual = values(vec!["1, 'a'", "2, 'b'"]).build();
        let expected = "VALUES(1, 'a'), (2, 'b')";
        test(actual, expected);

        // let actual = values(vec!["1, 'a'", "2, 'b'"])
        //     .order_by(vec!["column1 desc"])
        //     .build();
        // let expected = "VALUES(1, 'a'), (2, 'b')";
        // test(actual, expected);
    }
}
