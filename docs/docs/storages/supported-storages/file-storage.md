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
).unwrap();

glue.execute(
    "INSERT INTO Todo VALUES (1, 'write docs'), (2, 'run tests');"
).unwrap();

let result = glue.execute("SELECT * FROM Todo;").unwrap();
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

## Storage format migration

A storage written before the format marker was introduced is storage format v1, and opening one fails with `[FileStorage] migration required for table schema '...' (found v1, expected v2); migrate file-storage data to the latest format before opening`. Upgrade it once:

```shell
gluesql --storage file --path ./data --upgrade
```

It reports what it converted:

```text
[file-storage] upgraded ./data
[file-storage] migration report: migrated_tables=2, unchanged_tables=0, rewritten_rows=4
```

Library users can call `gluesql_file_storage::migrate_to_latest(path)` instead, which returns the same report. Running it again on a storage that is already up to date does nothing.

The migration does not edit the storage in place: it builds a complete copy beside the storage and swaps it in. Three paths appear next to the storage directory while it runs — `./data.migrating/`, `./data.backup/` and `./data.migration-lock` — and all three are gone when it finishes. So:

- It needs free disk space of roughly the size of the storage.
- Nothing else may be using the storage while it runs. `FileStorage::new` refuses to open the storage while the lock exists.
- Those three paths must be free. If you keep your own copy of the storage next to it, do not name it `./data.backup/`; the migration refuses to start rather than touch a directory it did not create.

If a run is interrupted, run the same command again. Your data is in either `./data/` or `./data.backup/` the whole time, and an interrupted swap is finished or undone automatically.

The one state that is not resolved automatically is a leftover `./data.migrating/` next to an intact `./data/`, because a copy being built right now looks the same as one left behind by a crash. The migration reports that state and changes nothing; once you are sure no other migration is running, remove `./data.migrating/` and `./data.migration-lock` and run the upgrade again.
