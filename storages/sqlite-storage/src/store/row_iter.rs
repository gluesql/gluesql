use {
    super::StoredRow,
    crate::{Result, codec::decode},
    gluesql_core::{ast::DataType, data::Key},
    ouroboros::self_referencing,
    rusqlite::{Rows, Statement},
};

// Rows borrows its statement; both must stay alive until iteration ends or is dropped.
#[self_referencing]
struct RowCursor<'conn> {
    statement: Statement<'conn>,
    #[borrows(mut statement)]
    #[not_covariant]
    rows: Rows<'this>,
}

pub(super) struct SqliteRowIter<'conn> {
    cursor: Option<RowCursor<'conn>>,
    primary_type: Option<DataType>,
    selected_types: Vec<DataType>,
}

impl<'conn> SqliteRowIter<'conn> {
    pub(super) fn new(
        statement: Statement<'conn>,
        primary_type: Option<DataType>,
        selected_types: Vec<DataType>,
    ) -> Result<Self> {
        let cursor = RowCursorTryBuilder {
            statement,
            rows_builder: |statement| statement.query([]),
        }
        .try_build()?;

        Ok(Self {
            cursor: Some(cursor),
            primary_type,
            selected_types,
        })
    }
}

impl Iterator for SqliteRowIter<'_> {
    type Item = Result<StoredRow>;

    fn next(&mut self) -> Option<Self::Item> {
        let cursor = self.cursor.as_mut()?;
        let result = cursor.with_rows_mut(|rows| -> Result<Option<StoredRow>> {
            let Some(row) = rows.next()? else {
                return Ok(None);
            };

            let rowid = row.get(0)?;
            let (key, offset) = if let Some(data_type) = &self.primary_type {
                let value = decode(row.get_ref(1)?, data_type)?;
                (Key::try_from(&value)?, 2)
            } else {
                (Key::I64(rowid), 1)
            };

            let values = self
                .selected_types
                .iter()
                .enumerate()
                .map(|(position, data_type)| decode(row.get_ref(position + offset)?, data_type))
                .collect::<Result<Vec<_>>>()?;

            Ok(Some(StoredRow { rowid, key, values }))
        });

        if !matches!(result, Ok(Some(_))) {
            // Release the SQLite read cursor on both exhaustion and error.
            self.cursor = None;
        }

        result.transpose()
    }
}
