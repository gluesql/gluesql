use {
    super::BlendContext,
    crate::data::{Row, Value},
    std::{fmt::Debug, rc::Rc},
};

#[derive(Debug)]
enum Content<'a> {
    Some { table_alias: &'a str, row: &'a Row },
    None,
}

#[derive(Debug)]
pub struct FilterContext<'a> {
    content: Content<'a>,
    next: Option<Rc<FilterContext<'a>>>,
    next2: Option<Rc<BlendContext<'a>>>,
}

impl<'a> FilterContext<'a> {
    pub fn new(table_alias: &'a str, row: &'a Row, next: Option<Rc<FilterContext<'a>>>) -> Self {
        Self {
            content: Content::Some { table_alias, row },
            next,
            next2: None,
        }
    }

    pub fn concat(
        filter_context: Option<Rc<FilterContext<'a>>>,
        blend_context: Option<Rc<BlendContext<'a>>>,
    ) -> Self {
        Self {
            content: Content::None,
            next: filter_context,
            next2: blend_context,
        }
    }

    pub fn get_value(&'a self, target: &str) -> Option<&'a Value> {
        if let Content::Some { row, .. } = &self.content {
            let value = row.get_value(target);

            if value.is_some() {
                return value;
            }
        }

        match (&self.next, &self.next2) {
            (None, None) => None,
            (Some(fc), None) => fc.get_value(target),
            (None, Some(bc)) => bc.get_value(target),
            (Some(fc), Some(bc)) => match bc.get_value(target) {
                v @ Some(_) => v,
                None => fc.get_value(target),
            },
        }
    }

    pub fn get_alias_value(&'a self, target_alias: &str, target: &str) -> Option<&'a Value> {
        match &self.content {
            Content::Some { table_alias, row } if table_alias == &target_alias => {
                let value = row.get_value(target);

                if value.is_some() {
                    return value;
                }
            }
            _ => {}
        };

        match (&self.next, &self.next2) {
            (None, None) => None,
            (Some(fc), None) => fc.get_alias_value(target_alias, target),
            (None, Some(bc)) => bc.get_alias_value(target_alias, target),
            (Some(fc), Some(bc)) => match bc.get_alias_value(target_alias, target) {
                v @ Some(_) => v,
                None => fc.get_alias_value(target_alias, target),
            },
        }
    }
}
