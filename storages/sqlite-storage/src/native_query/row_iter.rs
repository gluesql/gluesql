use {
    crate::{Result, codec::decode},
    gluesql_core::{ast::DataType, data::Value},
    ouroboros::self_referencing,
    rusqlite::{Rows, Statement, params_from_iter, types::Value as SqlValue},
};

#[self_referencing]
struct NativeCursor<'conn> {
    statement: Statement<'conn>,
    #[borrows(mut statement)]
    #[not_covariant]
    rows: Rows<'this>,
}

pub(super) struct SqliteNativeRowIter<'conn> {
    cursor: Option<NativeCursor<'conn>>,
    output_types: Vec<DataType>,
}

impl<'conn> SqliteNativeRowIter<'conn> {
    pub(super) fn new(
        statement: Statement<'conn>,
        params: &[SqlValue],
        output_types: Vec<DataType>,
    ) -> Result<Self> {
        let cursor = NativeCursorTryBuilder {
            statement,
            rows_builder: |statement| statement.query(params_from_iter(params)),
        }
        .try_build()?;

        Ok(Self {
            cursor: Some(cursor),
            output_types,
        })
    }
}

impl Iterator for SqliteNativeRowIter<'_> {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        let cursor = self.cursor.as_mut()?;
        let result = cursor.with_rows_mut(|rows| -> Result<Option<Vec<Value>>> {
            let Some(row) = rows.next()? else {
                return Ok(None);
            };
            self.output_types
                .iter()
                .enumerate()
                .map(|(index, data_type)| decode(row.get_ref(index)?, data_type))
                .collect::<Result<Vec<_>>>()
                .map(Some)
        });

        if !matches!(result, Ok(Some(_))) {
            self.cursor = None;
        }

        result.transpose()
    }
}
