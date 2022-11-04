use {
    crate::ast::{ColumnDef, ColumnOption, Expr},
    chrono::NaiveDateTime,
    serde::{Deserialize, Serialize},
    std::fmt::Debug,
    strum_macros::Display,
};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum SchemaIndexOrd {
    Asc,
    Desc,
    Both,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SchemaIndex {
    pub name: String,
    pub expr: Expr,
    pub order: SchemaIndexOrd,
    pub created: NaiveDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub table_name: String,
    pub column_defs: Vec<ColumnDef>,
    pub indexes: Vec<SchemaIndex>,
    pub created: NaiveDateTime,
}

impl ColumnDef {
    pub fn is_nullable(&self) -> bool {
        self.options
            .iter()
            .any(|option| option == &ColumnOption::Null)
    }

    pub fn get_default(&self) -> Option<&Expr> {
        self.options.iter().find_map(|option| match option {
            ColumnOption::Default(expr) => Some(expr),
            _ => None,
        })
    }
}
