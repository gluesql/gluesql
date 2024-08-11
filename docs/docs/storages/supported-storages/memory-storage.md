# Memory Storage

MemoryStorage is a foundational storage option designed for in-memory, non-persistent data. Despite its simplicity, it is robust enough for use in production environments.

A key aspect of MemoryStorage is not only its functionality but also its role as an exemplary case showcasing how simple it is to develop custom storage in GlueSQL. It provides a practical demonstration of what a minimalistic, yet fully functional storage interface can look like in GlueSQL.

MemoryStorage is accessible across multiple environments, including Rust, Rust (WASM), JavaScript (Web), and Node.js.

The storage interface is implemented with the following traits: `Store`, `StoreMut`, `AlterTable`, `CustomFunction`, `CustomFunctionMut`, and `Metadata`.

Consider the Rust code structure for MemoryStorage:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub schema: Schema,
    pub rows: BTreeMap<Key, DataRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MemoryStorage {
    pub id_counter: i64,
    pub items: HashMap<String, Item>,
    pub metadata: HashMap<String, HashMap<String, Value>>,
    pub functions: HashMap<String, StructCustomFunction>,
}
```

This structure defines the `Item` and `MemoryStorage` structs. `Item` struct holds the schema and rows, while `MemoryStorage` struct consists of `id_counter` (to keep track of the row IDs), `items` (to store the actual data), `metadata` (to keep metadata), and `functions` (to store custom functions).

Below are the implementations of the `Store` and `StoreMut` traits for `MemoryStorage`:

```rust
#[async_trait(?Send)]
impl Store for MemoryStorage {
    // Code for fetching schemas and data
}

#[async_trait(?Send)]
impl StoreMut for MemoryStorage {
    // Code for manipulating schemas and data
}
```

The Store trait implementation provides methods for fetching all schemas, fetching a specific schema, fetching data from a specific table with a given key, and scanning all data from a particular table.

On the other hand, the StoreMut trait implementation provides methods for inserting a new schema, deleting an existing schema, appending data to a table, inserting data into a table with a specific key, and deleting data from a table with given keys.

In summary, the MemoryStorage structure in GlueSQL is a straightforward yet powerful tool that elegantly showcases how simple it is to create a custom storage system. It's a testament to the power and flexibility of GlueSQL's design and the ease of implementing robust storage solutions with it.
