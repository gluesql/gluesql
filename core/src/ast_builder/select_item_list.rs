use {
    super::{ExprNode, SelectItemNode},
    crate::{
        ast::SelectItem,
        parse_sql::parse_select_items,
        result::{Error, Result},
        translate::translate_select_item,
    },
};

#[derive(Clone)]
pub enum SelectItemList {
    Text(String),
    SelectItems(Vec<SelectItemNode>),
}

impl From<&str> for SelectItemList {
    fn from(exprs: &str) -> Self {
        SelectItemList::Text(exprs.to_owned())
    }
}

impl From<Vec<&str>> for SelectItemList {
    fn from(select_items: Vec<&str>) -> Self {
        SelectItemList::SelectItems(select_items.into_iter().map(Into::into).collect())
    }
}

impl From<ExprNode> for SelectItemList {
    fn from(expr_node: ExprNode) -> Self {
        SelectItemList::SelectItems(vec![expr_node.into()])
    }
}

impl From<Vec<ExprNode>> for SelectItemList {
    fn from(expr_nodes: Vec<ExprNode>) -> Self {
        SelectItemList::SelectItems(expr_nodes.into_iter().map(Into::into).collect())
    }
}

impl TryFrom<SelectItemList> for Vec<SelectItem> {
    type Error = Error;

    fn try_from(select_items: SelectItemList) -> Result<Self> {
        match select_items {
            SelectItemList::Text(items) => parse_select_items(items)?
                .iter()
                .map(translate_select_item)
                .collect::<Result<Vec<_>>>(),
            SelectItemList::SelectItems(items) => items
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>>>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::SelectItem,
        ast_builder::{col, SelectItemList},
        parse_sql::parse_select_items,
        result::Result,
        translate::translate_select_item,
    };

    fn test(actual: SelectItemList, expected: &str) {
        let parsed = parse_select_items(expected).expect(expected);
        let expected = parsed
            .iter()
            .map(translate_select_item)
            .collect::<Result<Vec<SelectItem>>>();

        assert_eq!(actual.try_into(), expected);
    }

    #[test]
    fn select_item_list() {
        let actual = "id, name".into();
        let expected = "id, name";
        test(actual, expected);

        let actual = vec!["id", "name"].into();
        let expected = "id, name";
        test(actual, expected);

        let actual = col("id").into();
        let expected = "id";
        test(actual, expected);

        let actual = vec![col("id"), "name".into()].into();
        let expected = "id, name";
        test(actual, expected);
    }
}
