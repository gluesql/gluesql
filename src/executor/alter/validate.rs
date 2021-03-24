use {
    super::AlterError,
    crate::result::Result,
    sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef, DataType},
};

#[cfg(feature = "auto-increment")]
use sqlparser::{
    dialect::keywords::Keyword,
    tokenizer::{Token, Word},
};

pub fn validate(column_def: &ColumnDef) -> Result<()> {
    let ColumnDef {
        data_type,
        options,
        name,
        ..
    } = column_def;

    // data type
    if !matches!(
        data_type,
        DataType::Boolean | DataType::Int | DataType::Float(_) | DataType::Text
    ) {
        return Err(AlterError::UnsupportedDataType(data_type.to_string()).into());
    }

    // column option
    if let Some(option) = options.iter().find(|ColumnOptionDef { option, .. }| {
        #[allow(clippy::let_and_return)]
        let result = !matches!(
            option,
            ColumnOption::Null
                | ColumnOption::NotNull
                | ColumnOption::Default(_)
                | ColumnOption::Unique { .. }
        );
        #[allow(clippy::let_and_return)]
        #[cfg(feature = "auto-increment")]
        let result = result
            && !matches!(option,
            ColumnOption::DialectSpecific(tokens)
                if matches!(
                    tokens[..],
                    [
                        Token::Word(Word {
                            keyword: Keyword::AUTO_INCREMENT,
                            ..
                        }),
                        ..
                    ] | [
                        Token::Word(Word {
                            keyword: Keyword::AUTOINCREMENT,
                            ..
                        }),
                        ..
                    ]
                ));
        #[allow(clippy::let_and_return)]
        result
    }) {
        return Err(AlterError::UnsupportedColumnOption(option.to_string()).into());
    }

    // unique + data type
    if matches!(data_type, DataType::Float(_))
        && options
            .iter()
            .any(|ColumnOptionDef { option, .. }| matches!(option, ColumnOption::Unique { .. }))
    {
        return Err(AlterError::UnsupportedDataTypeForUniqueColumn(
            name.to_string(),
            data_type.to_string(),
        )
        .into());
    }

    Ok(())
}
