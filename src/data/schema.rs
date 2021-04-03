use {
    serde::{Deserialize, Serialize},
    sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef, Expr},
};

#[cfg(feature = "auto-increment")]
use sqlparser::{
    dialect::keywords::Keyword,
    tokenizer::{Token, Word},
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Schema {
    pub table_name: String,
    pub column_defs: Vec<ColumnDef>,
}

pub trait ColumnDefExt {
    fn is_nullable(&self) -> bool;

    #[cfg(feature = "auto-increment")]
    fn is_auto_incremented(&self) -> bool;

    fn get_default(&self) -> Option<&Expr>;
}

impl ColumnDefExt for ColumnDef {
    fn is_nullable(&self) -> bool {
        self.options
            .iter()
            .any(|ColumnOptionDef { option, .. }| option == &ColumnOption::Null)
    }

    #[cfg(feature = "auto-increment")]
    fn is_auto_incremented(&self) -> bool {
        self.options
            .iter()
            .any(|ColumnOptionDef { option, .. }| option.is_auto_increment())
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

pub trait ColumnOptionExt {
    fn is_auto_increment(&self) -> bool;
}

#[cfg(not(feature = "auto-increment"))]
impl ColumnOptionExt for ColumnOption {
    fn is_auto_increment(&self) -> bool {
        false
    }
}

#[cfg(feature = "auto-increment")]
impl ColumnOptionExt for ColumnOption {
    fn is_auto_increment(&self) -> bool {
        matches!(self,
        ColumnOption::DialectSpecific(tokens)
            if matches!(
                tokens[..],
                [
                    Token::Word(Word {
                        keyword: Keyword::AUTO_INCREMENT,
                        ..
                    }),
                    ..
                ]
            )
        )
    }
}
