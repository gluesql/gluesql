use crate::ast::{Statement, ToSql};

use {
    crate::ast::{ColumnDef, ColumnOption, ColumnOptionDef, Expr},
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub table_name: String,
    pub column_defs: Vec<ColumnDef>,
    pub indexes: Vec<SchemaIndex>,
}

impl Schema {
    fn to_ddl(self) -> String {
        let Schema {
            table_name: name,
            column_defs: columns,
            ..
        } = self;

        Statement::CreateTable {
            if_not_exists: false,
            name,
            columns,
            source: None,
        }
        .to_sql()

        // let Schema { table_name, .. } = self;
        // let column_defs = self
        //     .column_defs
        //     .iter()
        //     .map(
        //         |ColumnDef {
        //              name,
        //              data_type,
        //              options,
        //          }| {
        //             let options = options
        //                 .iter()
        //                 .map(|ColumnOptionDef { option, .. }| match option {
        //                     ColumnOption::Null => "NULL".to_owned(),
        //                     ColumnOption::NotNull => "NOT NULL".to_owned(),
        //                     ColumnOption::Default(expr) => format!("DEFAULT {}", expr.to_sql()),
        //                     ColumnOption::Unique { is_primary } => match is_primary {
        //                         true => "PRIMARY KEY".to_owned(),
        //                         false => "UNIQUE".to_owned(),
        //                     },
        //                 })
        //                 .collect::<Vec<_>>()
        //                 .join(" ");

        //             format!("{name} {data_type} {options}")
        //         },
        //     )
        //     .collect::<Vec<_>>()
        //     .join(",");

        // format!("CREATE TABLE {table_name} ({column_defs})")
    }
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
            .find_map(|ColumnOptionDef { option, .. }| match option {
                ColumnOption::Default(expr) => Some(expr),
                _ => None,
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{ColumnDef, ColumnOption::Unique, ColumnOptionDef},
        chrono::Utc,
        data::Schema,
        prelude::DataType,
    };

    #[test]
    fn table_basic() {
        let schema = Schema {
            table_name: "Foo".to_owned(),
            column_defs: vec![ColumnDef {
                name: "no".to_owned(),
                data_type: DataType::Int,
                options: Vec::new(),
            }],
            indexes: Vec::new(),
            created: Utc::now().naive_utc(),
        };
        assert_eq!(schema.to_ddl(), "CREATE TABLE Foo (no INT NOT NULL)");
    }

    #[test]
    fn table_primary() {
        let schema = Schema {
            table_name: "Foo".to_owned(),
            column_defs: vec![ColumnDef {
                name: "no".to_owned(),
                data_type: DataType::Int,
                options: vec![ColumnOptionDef {
                    name: None,
                    option: Unique { is_primary: true },
                }],
            }],
            indexes: Vec::new(),
            created: Utc::now().naive_utc(),
        };
        assert_eq!(
            schema.to_ddl(),
            "CREATE TABLE Foo (no INT NOT NULL PRIMARY KEY)"
        );
    }
}
