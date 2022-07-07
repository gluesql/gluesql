use crate::{
    ast::SelectItem,
    parse_sql::parse_select_items,
    result::{Error, Result},
    translate::translate_select_item,
};

#[derive(Clone)]
pub enum SelectItemList {
    Text(String),
}

impl From<&str> for SelectItemList {
    fn from(exprs: &str) -> Self {
        SelectItemList::Text(exprs.to_owned())
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
        }
    }
}
