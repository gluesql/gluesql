# Redb Storage

RedbStorage stores GlueSQL data in a single file using the
[redb](https://github.com/cberner/redb) embedded database. It supports ACID
transactions.

RedbStorage implements GlueSQL's `Store`, `StoreMut`, `Transaction`, `Index`,
`IndexMut`, and `AlterTable` traits.

## Example

```rust
use gluesql::{gluesql_redb_storage::RedbStorage, prelude::Glue};

fn main() {
    let storage = RedbStorage::new("data/my_db.redb").unwrap();
    let mut glue = Glue::new(storage);

    let sql = "
        CREATE TABLE Foo (id INT, name TEXT);
        INSERT INTO Foo VALUES (1, 'Alice'), (2, 'Bob');
        SELECT * FROM Foo;
    ";

    let payloads = glue.execute(sql).unwrap();
    println!("{:#?}", payloads);
}
```

## Indexes

Create indexes on columns or expressions with
[`CREATE INDEX`](../../sql-syntax/statements/data-definition/create-index.md):

```sql
CREATE INDEX by_name ON Foo (name);
SELECT * FROM Foo WHERE name = 'Alice';
DROP INDEX Foo.by_name;
```

Indexes can be used for range queries and for `ORDER BY` in either direction.

## Things to keep in mind

- Nested transactions are not supported.
- Only one RedbStorage instance should open the same database file at a time.
- Table names cannot contain `/`. The names `__SCHEMA__`, `__GLUESQL_META__`,
  and all names starting with `__GLUESQL_INDEX__` are reserved.

## File format migration

New databases use GlueSQL storage format v5. Existing databases in formats
v1 through v4 require migration before opening them.

Back up the database first. Older GlueSQL releases cannot open the upgraded
database.

```shell
gluesql --storage redb --path data.redb --upgrade
```

Library users can call `gluesql_redb_storage::migrate_to_latest(path)` instead.
