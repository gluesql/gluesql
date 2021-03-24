#![cfg(feature = "auto-increment")]
use {
    crate::{
        data::{Row, Value},
        result::MutResult,
        store::{AlterTable, AutoIncrement, Store, StoreMut},
    },
    futures::executor,
    sqlparser::{
        ast::{ColumnDef, ColumnOption, ColumnOptionDef},
        dialect::keywords::Keyword,
        tokenizer::{Token, Word},
    },
    std::fmt::Debug,
};

pub fn run<T: 'static + Debug, U: Store<T> + StoreMut<T> + AlterTable + AutoIncrement>(
    storage: U,
    rows: Vec<Row>,
    column_defs: &[ColumnDef],
    table_name: &str,
) -> MutResult<U, Vec<Row>> {
    let auto_increment_columns: Vec<(usize, &ColumnDef)> = column_defs
        .iter()
        .enumerate()
        .filter(|(_, ColumnDef { options, .. })| {
            options
                .iter()
                .find_map(|ColumnOptionDef { option, .. }| {
                    if is_auto_increment(option) {
                        Some(true)
                    } else {
                        None
                    }
                })
                .unwrap_or(false)
        })
        .collect();

    if !auto_increment_columns.is_empty() {
        let mut new_rows: Vec<Row> = vec![];
        let storage = rows.into_iter().fold(storage, |storage, row| {
            let mut row = row.0;
            let storage =
                auto_increment_columns
                    .iter()
                    .fold(storage, |storage, (index, column)| {
                        if matches!(row[*index], Value::Null) {
                            let name = &column.name.value;

                            match executor::block_on(storage.generate_value(table_name, name)) {
                                Ok((storage, value)) => {
                                    row[*index] = value;
                                    storage
                                }
                                Err((storage, e)) => {
                                    println!("ERROR! {:?}", e);
                                    storage
                                }
                            }
                        } else {
                            storage
                        }
                    });
            new_rows.push(Row(row));
            storage
        });
        Ok((storage, new_rows))
    } else {
        Ok((storage, rows))
    }
}

fn is_auto_increment(option: &ColumnOption) -> bool {
    matches!(
        option,
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
            | [
                Token::Word(Word {
                    keyword: Keyword::AUTOINCREMENT,
                    ..
                }),
                ..
            ]
        )
    )
}
