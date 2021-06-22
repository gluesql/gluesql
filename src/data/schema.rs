use {
    crate::ast::{ColumnDef, ColumnOption, ColumnOptionDef, Expr},
    serde::{Deserialize, Serialize},
};

#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum SchemaIndexOrd {
    Asc,
    Desc,
    Both,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SchemaIndex {
    pub name: String,
    pub expr: Expr,
    pub order: SchemaIndexOrd,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Schema {
    pub table_name: String,
    pub column_defs: Vec<ColumnDef>,
    pub indexes: Vec<SchemaIndex>,
}

pub trait ColumnDefExt {
    fn is_nullable(&self) -> bool;

    fn get_default(&self) -> Option<&Expr>;
}

impl ColumnDefExt for ColumnDef {
    fn is_nullable(&self) -> bool {
        self.options
            .iter()
            .any(|ColumnOptionDef { option, .. }| option == &ColumnOption::Null)
    }

    fn get_default(&self) -> Option<&Expr> {
        self.options
            .iter()
            .filter_map(|ColumnOptionDef { option, .. }| match option {
                ColumnOption::Default(expr) => Some(expr),
                _ => None,
            })
            .next()
    }
}
