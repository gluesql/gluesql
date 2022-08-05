use std::rc::Rc;

pub struct Context<'a> {
    content: Option<Content<'a>>,
    next: Option<Rc<Context<'a>>>,
    next2: Option<Rc<Context<'a>>>,
}

struct Content<'a> {
    alias: String,
    columns: Vec<&'a str>,
    primary_key: Option<&'a str>,
}

impl<'a> Context<'a> {
    pub fn new(
        alias: String,
        columns: Vec<&'a str>,
        primary_key: Option<&'a str>,
        next: Option<Rc<Context<'a>>>,
        next2: Option<Rc<Context<'a>>>,
    ) -> Self {
        Context {
            content: Some(Content {
                alias,
                columns,
                primary_key,
            }),
            next,
            next2,
        }
    }

    pub fn concat(next: Option<Rc<Context<'a>>>, next2: Option<Rc<Context<'a>>>) -> Self {
        Context {
            content: None,
            next,
            next2,
        }
    }

    pub fn contains_alias(&self, target: &str) -> bool {
        if let Some(Content { alias, .. }) = &self.content {
            if alias == target {
                return true;
            }
        }

        match (self.next.as_ref(), self.next2.as_ref()) {
            (Some(next), Some(next2)) => {
                next.contains_alias(target) || next2.contains_alias(target)
            }
            (Some(context), None) | (None, Some(context)) => context.contains_alias(target),
            (None, None) => false,
        }
    }

    pub fn contains_column(&self, target: &str) -> bool {
        if let Some(Content { columns, .. }) = &self.content {
            if columns.iter().any(|column| column == &target) {
                return true;
            }
        }

        match (self.next.as_ref(), self.next2.as_ref()) {
            (Some(next), Some(next2)) => {
                next.contains_column(target) || next2.contains_column(target)
            }
            (Some(context), None) | (None, Some(context)) => context.contains_column(target),
            (None, None) => false,
        }
    }

    pub fn contains_aliased_column(&self, target_alias: &str, target_column: &str) -> bool {
        if let Some(content) = &self.content {
            let Content { alias, columns, .. } = content;

            if alias == target_alias {
                return columns.iter().any(|column| column == &target_column);
            }
        }

        match (self.next.as_ref(), self.next2.as_ref()) {
            (Some(next), Some(next2)) => {
                next.contains_aliased_column(target_alias, target_column)
                    || next2.contains_aliased_column(target_alias, target_column)
            }
            (Some(context), None) | (None, Some(context)) => {
                context.contains_aliased_column(target_alias, target_column)
            }
            (None, None) => false,
        }
    }

    pub fn contains_primary_key(&self, target_key: &str) -> bool {
        match self.content {
            Some(Content {
                primary_key: Some(primary_key),
                ..
            }) => primary_key == target_key,
            _ => false,
        }
    }
}
