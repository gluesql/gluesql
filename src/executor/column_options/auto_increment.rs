use {
    crate::{
        data::{Row, Value},
        result::MutResult,
        store::{AlterTable, Store, StoreMut},
    },
    futures::executor,
    sqlparser::{
        ast::{ColumnDef, ColumnOption, ColumnOptionDef},
        dialect::keywords::Keyword,
        tokenizer::{Token, Word},
    },
    std::fmt::Debug,
};

pub fn run<T: 'static + Debug, U: Store<T> + StoreMut<T> + AlterTable>(
    storage: U,
    rows: Vec<Row>,
    column_defs: &Vec<ColumnDef>,
    table_name: &str,
) -> MutResult<U, Vec<Row>> {
    let auto_increment_columns: Vec<(usize, &ColumnDef)> = column_defs
        .iter()
        .enumerate()
        .filter(|(_, ColumnDef { options, .. })| {
            match options.iter().find_map(|ColumnOptionDef { option, .. }| {
                if matches!(option, ColumnOption::DialectSpecific(tokens)
                if match tokens[..] {
                    [Token::Word(Word {
                        keyword: Keyword::AUTO_INCREMENT,
                        ..
                    }), ..]
                    | [Token::Word(Word {
                        keyword: Keyword::AUTOINCREMENT,
                        ..
                    }), ..] => true, // Doubled due to OR in paterns being experimental; TODO: keyword: Keyword::AUTO_INCREMENT | Keyword::AUTOINCREMENT
                    _ => false,
                }) {
                    Some(true)
                } else {
                    None
                }
            }) {
                Some(value) => value,
                None => false,
            }
        })
        .collect();

    if auto_increment_columns.len() > 0 {
        let mut new_rows: Vec<Row> = vec![];
        let storage = rows.into_iter().fold(storage, |storage, row| {
            let mut row = row.0;
            let storage =
                auto_increment_columns
                    .iter()
                    .fold(storage, |storage, (index, column)| {
                        if matches!(row[*index], Value::Null) {
                            let name = &column.name.value;
                            let generated =
                                executor::block_on(storage.get_generator(table_name, name)).ok();
                            let value = match generated {
                                Some(Value::I64(value)) => value,
                                _ => 1,
                            };

                            row[*index] = Value::I64(value);
                            println!("Row: {:?}", row[*index]);

                            match executor::block_on(storage.set_generator(
                                table_name,
                                name,
                                Value::I64(value + 1),
                            )) {
                                Ok((storage, _)) => storage,
                                Err((storage, _)) => storage,
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
