# File Storage

File Storage is a simple persistent storage backend that writes data to the local filesystem.
For each table a schema file is written as `TABLE_NAME.sql` using the original `CREATE TABLE`
statement.  Every inserted row is stored as an individual RON file in a directory with the
same name as the table.

This storage is useful when you need lightweight persistence without running a database
server.  Because it relies on the filesystem, it is available only for Rust targets that have
`std::fs` access.

## Setup

Add `gluesql-file-storage` to your `Cargo.toml` and create the storage by specifying a path
where files should be stored:

```toml
[dependencies]
gluesql-file-storage = "*"
```

```rust
use gluesql::prelude::Glue;
use gluesql_file_storage::FileStorage;

let storage = FileStorage::new("./data").unwrap();
let mut glue = Glue::new(storage);
```

## Basic Usage

Once the storage is created you can use normal SQL statements.  Below is a short example
showing table creation, inserting data and querying it back:

```rust
use gluesql::prelude::Value::I64;

glue.execute(
    "CREATE TABLE Todo (id INTEGER, task TEXT);"
).await.unwrap();

glue.execute(
    "INSERT INTO Todo VALUES (1, 'write docs'), (2, 'run tests');"
).await.unwrap();

let result = glue.execute("SELECT * FROM Todo;").await.unwrap();
```

After running these commands the directory structure under `./data` will look similar to:

```
./data/
├── Todo.sql
└── Todo/
    ├── <uuid1>.ron
    └── <uuid2>.ron
```

Each `.ron` file contains the serialized row together with its key.

## Limitations

- Transaction and index related features are not implemented.
- Every row is saved as a separate file, so it may not scale well for very large
datasets or heavy concurrent workloads.
- This storage backend only works in environments that provide filesystem access.
