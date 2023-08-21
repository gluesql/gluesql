use {
    super::ExprNode,
    crate::{
        ast::Expr,
        parse_sql::parse_comma_separated_exprs,
        result::{Error, Result},
        translate::translate_expr,
    },
    std::borrow::Cow,
};

#[derive(Clone, Debug)]
pub enum ExprList<'a> {
    Text(Cow<'a, str>),
    Exprs(Cow<'a, [ExprNode<'a>]>),
}

impl<'a> From<&'a str> for ExprList<'a> {
    fn from(exprs: &'a str) -> Self {
        ExprList::Text(Cow::Borrowed(exprs))
    }
}

impl<'a> From<String> for ExprList<'a> {
    fn from(exprs: String) -> Self {
        ExprList::Text(Cow::from(exprs))
    }
}

impl<'a, T: Into<ExprNode<'a>>> From<Vec<T>> for ExprList<'a> {
    fn from(exprs: Vec<T>) -> Self {
        ExprList::Exprs(Cow::Owned(exprs.into_iter().map(Into::into).collect()))
    }
}

impl<'a, T: Into<ExprNode<'a>> + Copy> From<&'a Vec<T>> for ExprList<'a> {
    fn from(exprs: &'a Vec<T>) -> Self {
        exprs.as_slice().into()
    }
}

impl<'a, T: Into<ExprNode<'a>> + Copy> From<&'a [T]> for ExprList<'a> {
    fn from(exprs: &'a [T]) -> Self {
        ExprList::Exprs(Cow::Owned(exprs.iter().map(|&v| v.into()).collect()))
    }
}

impl<'a> From<&'a [ExprNode<'a>]> for ExprList<'a> {
    fn from(exprs: &'a [ExprNode<'a>]) -> Self {
        ExprList::Exprs(Cow::Borrowed(exprs))
    }
}

impl<'a> TryFrom<ExprList<'a>> for Vec<Expr> {
    type Error = Error;

    fn try_from(expr_list: ExprList<'a>) -> Result<Self> {
        match expr_list {
            ExprList::Text(exprs) => parse_comma_separated_exprs(exprs)?
                .iter()
                .map(translate_expr)
                .collect::<Result<Vec<_>>>(),
            ExprList::Exprs(exprs) => {
                let exprs = exprs.into_owned();

                exprs
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>>>()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::ExprList,
        crate::{
            ast::Expr, ast_builder::col, parse_sql::parse_comma_separated_exprs, result::Result,
            translate::translate_expr,
        },
        pretty_assertions::assert_eq,
    };

    fn test(actual: ExprList, expected: &str) {
        let parsed = parse_comma_separated_exprs(expected).expect(expected);
        let expected = parsed
            .iter()
            .map(translate_expr)
            .collect::<Result<Vec<Expr>>>();

        assert_eq!(actual.try_into(), expected);
    }

    #[test]
    fn into_expr_list() {
        let actual: ExprList = "1, a * 2, b".into();
        let expected = "1, a * 2, b";
        test(actual, expected);

        let actual: ExprList = String::from("'hello' || 'world', col1").into();
        let expected = "'hello' || 'world', col1";
        test(actual, expected);

        // Vec<Into<ExprNode>>
        let actual: ExprList = vec!["id", "name"].into();
        let expected = "id, name";
        test(actual, expected);

        // Vec<ExprNode>
        let actual: ExprList = vec![col("id"), col("name")].into();
        let expected = "id, name";
        test(actual, expected);

        // &Vec<ExprNode>
        let actual = vec!["id", "name"];
        let actual: ExprList = (&actual).into();
        let expected = "id, name";
        test(actual, expected);

        // &[Into<ExprNode>]
        let actual = vec!["rate / 10", "col1"];
        let actual: ExprList = actual.as_slice().into();
        let expected = "rate / 10, col1";
        test(actual, expected);

        // &[ExprNode]
        let actual = vec![col("id"), col("name")];
        let actual: ExprList = actual.as_slice().into();
        let expected = "id, name";
        test(actual, expected);
    }
}
