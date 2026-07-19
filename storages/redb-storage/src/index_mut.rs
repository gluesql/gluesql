use {
    crate::{
        RedbStorage,
        index_sync::{delete_index_table, prepare_backfill},
    },
    gluesql_core::{
        ast::OrderByExpr,
        chrono::Utc,
        data::{SchemaIndex, SchemaIndexOrd},
        error::{Error, IndexError, Result},
        store::IndexMut,
    },
};

impl IndexMut for RedbStorage {
    fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        column: &OrderByExpr,
    ) -> Result<()> {
        let mut schema = self
            .0
            .fetch_schema(table_name)
            .map_err(Error::from)?
            .ok_or_else(|| IndexError::TableNotFound(table_name.to_owned()))?;

        if schema.indexes.iter().any(|index| index.name == index_name) {
            return Err(IndexError::IndexNameAlreadyExists(index_name.to_owned()).into());
        }

        let rows = self
            .0
            .scan_data(table_name)
            .map_err(Error::from)?
            .map(|row| row.map_err(Error::from))
            .map(|row| {
                let (key, row) = row?;
                let table_key = key.to_cmp_be_bytes()?;

                Ok((table_key, row))
            })
            .collect::<Result<Vec<_>>>()?;
        let index = SchemaIndex {
            name: index_name.to_owned(),
            expr: column.expr.clone(),
            order: SchemaIndexOrd::Both,
            created: Utc::now().naive_utc(),
        };
        let index_changes = prepare_backfill(&schema, &index, &rows)?;
        schema.indexes.push(index);

        {
            let txn = self.0.txn_mut().map_err(Error::from)?;
            index_changes.apply(txn)?;
        }
        self.0.insert_schema(&schema).map_err(Error::from)
    }

    fn drop_index(&mut self, table_name: &str, index_name: &str) -> Result<()> {
        let mut schema = self
            .0
            .fetch_schema(table_name)
            .map_err(Error::from)?
            .ok_or_else(|| IndexError::TableNotFound(table_name.to_owned()))?;
        let Some(index_position) = schema
            .indexes
            .iter()
            .position(|index| index.name == index_name)
        else {
            return Err(IndexError::IndexNameDoesNotExist(index_name.to_owned()).into());
        };
        schema.indexes.remove(index_position);

        {
            let txn = self.0.txn_mut().map_err(Error::from)?;
            delete_index_table(txn, table_name, index_name)?;
        }
        self.0.insert_schema(&schema).map_err(Error::from)
    }
}
