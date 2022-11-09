use {
    crate::{csv_table::CsvTable, error::StorageError, CsvStorage},
    async_trait::async_trait,
    csv::ReaderBuilder,
    gluesql_core::{
        ast::Expr, data::Schema, prelude::*, result::Result, store::RowIter, store::Store,
    },
};

#[async_trait(?Send)]
impl Store for CsvStorage {
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        let schema = self
            .tables
            .get(table_name)
            .map(|table| table.schema.clone());
        Ok(schema)
    }

    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let schemas = self
            .tables
            .values()
            .map(|table| table.schema.clone())
            .collect();
        Ok(schemas)
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Row>> {
        todo!()
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let CsvTable { schema, file_path } = self
            .tables
            .get(table_name)
            .ok_or(StorageError::TableNotFound(table_name.to_string()))?;

        let column_defs = schema.column_defs.to_owned();
        let columns: Vec<String> = column_defs.iter().map(|cd| cd.name.to_owned()).collect();
        let data_types: Vec<DataType> = column_defs
            .iter()
            .map(|cd| cd.data_type.to_owned())
            .collect();

        let rows = ReaderBuilder::new()
            .from_path(file_path)
            .map_err(StorageError::from_csv_error)?
            .into_records();

        let rows = rows.map(move |row| -> Result<Row> {
            let values = row
                .map_err(StorageError::from_csv_error)?
                .iter()
                .zip(data_types.clone())
                .map(|(value, data_type)| Expr::TypedString {
                    data_type,
                    value: value.to_string(),
                })
                .collect::<Vec<_>>();
            Row::new(&column_defs, &columns, &values)
        });

        let row_counts = (0..).map(|i| Key::I128(i));
        Ok(Box::new(row_counts.zip(rows).map(
            |(key, row_result)| match row_result {
                Ok(row) => Ok((key, row)),
                Err(e) => Err(e),
            },
        )))
    }
}
