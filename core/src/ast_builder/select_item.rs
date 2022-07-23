use {
    super::ExprNode,
    crate::{
        ast::{Expr, SelectItem, ToSql},
        parse_sql::parse_select_item,
        result::{Error, Result},
        translate::translate_select_item,
    },
};

#[derive(Clone)]
pub enum SelectItemNode {
    SelectItem(SelectItem),
    Expr(ExprNode),
    Text(String),
}

impl From<SelectItem> for SelectItemNode {
    fn from(select_item: SelectItem) -> Self {
        Self::SelectItem(select_item)
    }
}

impl From<ExprNode> for SelectItemNode {
    fn from(expr_node: ExprNode) -> Self {
        Self::Expr(expr_node)
    }
}

impl From<&str> for SelectItemNode {
    fn from(select_item: &str) -> Self {
        Self::Text(select_item.to_owned())
    }
}

impl TryFrom<SelectItemNode> for SelectItem {
    type Error = Error;

    fn try_from(select_item_node: SelectItemNode) -> Result<Self> {
        match select_item_node {
            SelectItemNode::SelectItem(select_item) => Ok(select_item),
            SelectItemNode::Text(select_item) => {
                parse_select_item(select_item).and_then(|item| translate_select_item(&item))
            }
            SelectItemNode::Expr(expr_node) => {
                let expr = Expr::try_from(expr_node)?;
                let label = expr.to_sql();

                Ok(SelectItem::Expr { expr, label })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::SelectItem,
        ast_builder::{col, SelectItemNode},
        parse_sql::parse_select_item,
        translate::translate_select_item,
    };

    fn test(actual: SelectItemNode, expected: &str) {
        let parsed = &parse_select_item(expected).unwrap();
        let expected = translate_select_item(parsed);
        assert_eq!(actual.try_into(), expected);
    }

    #[test]
    fn select_item() {
        let actual = SelectItem::Wildcard.into();
        let expected = "*";
        test(actual, expected);

        let actual = "Foo.*".into();
        let expected = "Foo.*";
        test(actual, expected);

        let actual = "id as hello".into();
        let expected = "id as hello";
        test(actual, expected);

        let actual = col("id").into();
        let expected = "id";
        test(actual, expected);
    }
}
