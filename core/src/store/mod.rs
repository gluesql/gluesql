//! The storage module.
mod alter_table;
mod data_row;
mod function;
mod index;
mod metadata;
mod transaction;

pub use {
    crate::{
        data::{Key, Schema},
        result::Result,
    },
    alter_table::{AlterTable, AlterTableError},
    async_trait::async_trait,
    data_row::DataRow,
    function::{CustomFunction, CustomFunctionMut},
    index::{Index, IndexError, IndexMut},
    metadata::{MetaIter, Metadata},
    transaction::Transaction,
};

/// Type enabling streaming of table data.
pub type RowIter = Box<dyn Iterator<Item = Result<(Key, DataRow)>>>;

/// Implementing this trait will unlock SELECT queries for you database.
///
/// Example
/// ```
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct Item {
///     pub schema: Schema,
///     pub rows: BTreeMap<Key, DataRow>,
/// }
///
/// #[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// pub struct MemoryStorage {
///     pub id_counter: i64,
///     pub items: HashMap<String, Item>,
///     pub metadata: HashMap<String, HashMap<String, Value>>,
///     pub functions: HashMap<String, StructCustomFunction>,
/// }
///
///[async_trait(?Send)]
///mpl Store for MemoryStorage {
///   async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
///       let mut schemas = self
///           .items
///           .values()
///           .map(|item| item.schema.clone())
///           .collect::<Vec<_>>();
///       schemas.sort_by(|a, b| a.table_name.cmp(&b.table_name));
///       Ok(schemas)
///   }
///   async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
///       self.items
///           .get(table_name)
///           .map(|item| Ok(item.schema.clone()))
///           .transpose()
///   }
///   async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>> {
///       let row = self
///           .items
///           .get(table_name)
///           .and_then(|item| item.rows.get(key).map(Clone::clone));
///       Ok(row)
///   }
///   async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
///       let rows: RowIter = match self.items.get(table_name) {
///           Some(item) => Box::new(item.rows.clone().into_iter().map(Ok)),
///           None => Box::new(empty()),
///       };
///       Ok(rows)
///   }
///
/// ```
#[async_trait(?Send)]
pub trait Store {
    /// Fetch the [`data::Schema`] of a given table.
    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    /// Fetch [`data::Schema`]s of every talbe in your database.
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>>;

    /// Retrive [`data::DataRow`] from a specific table.
    async fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<DataRow>>;

    /// Retrie a [`data::RowIter`] over the rows of a specific table.
    async fn scan_data(&self, table_name: &str) -> Result<RowIter>;
}

/// Implementing this trait will unlock `INSERT`, `CREATE TABLE`, `DELETE`, `UPDATE`, and `DROP TABLES` operations for you database.
///
/// ## Example
/// ```
///#[derive(Debug, Clone, Serialize, Deserialize)]
///pub struct Item {
///    pub schema: Schema,
///    pub rows: BTreeMap<Key, DataRow>,
///}
///
///#[derive(Debug, Clone, Serialize, Deserialize, Default)]
///pub struct MemoryStorage {
///    pub id_counter: i64,
///    pub items: HashMap<String, Item>,
///    pub metadata: HashMap<String, HashMap<String, Value>>,
///    pub functions: HashMap<String, StructCustomFunction>,
///}
///
///#[async_trait(?Send)]
///impl StoreMut for MemoryStorage {
///    async fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
///        let created = HashMap::from([(
///            "CREATED".to_owned(),
///            Value::Timestamp(Utc::now().naive_utc()),
///        )]);
///        let meta = HashMap::from([(schema.table_name.clone(), created)]);
///        self.metadata.extend(meta);
///
///     let table_name = schema.table_name.clone();
///        let item = Item {
///            schema: schema.clone(),
///            rows: BTreeMap::new(),
///        };
///        self.items.insert(table_name, item);
///
///        Ok(())
///    }
///
///    async fn delete_schema(&mut self, table_name: &str) -> Result<()> {
///        self.items.remove(table_name);
///        self.metadata.remove(table_name);
///
///        Ok(())
///    }
///
///    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()> {
///        if let Some(item) = self.items.get_mut(table_name) {
///            for row in rows {
///                self.id_counter += 1;
///
///                item.rows.insert(Key::I64(self.id_counter), row);
///            }
///        }
///
///        Ok(())
///    }
///
///    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()> {
///        if let Some(item) = self.items.get_mut(table_name) {
///            for (key, row) in rows {
///                item.rows.insert(key, row);
///            }
///        }
///
///        Ok(())
///    }
///
///    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()> {
///        if let Some(item) = self.items.get_mut(table_name) {
///            for key in keys {
///                item.rows.remove(&key);
///            }
///        }
///
///        Ok(())
///    }
///}
/// ```
#[async_trait(?Send)]
pub trait StoreMut {
    /// Creates a table according to the given [data::Schema]
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()>;

    /// Deletes table of the given name.
    async fn delete_schema(&mut self, table_name: &str) -> Result<()>;

    /// Insert new data to exisiting table without primary key index.
    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()>;

    /// Insert new data to exisitng table having primary key index.
    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()>;

    /// Delete data of exisitng table corresponding to given primary key index.
    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()>;
}

pub trait GStore: Store + Index + Metadata + CustomFunction {}
impl<S: Store + Index + Metadata + CustomFunction> GStore for S {}

pub trait GStoreMut:
    StoreMut + IndexMut + AlterTable + Transaction + CustomFunction + CustomFunctionMut
{
}
impl<S: StoreMut + IndexMut + AlterTable + Transaction + CustomFunction + CustomFunctionMut>
    GStoreMut for S
{
}
