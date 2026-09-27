# Sled Storage

> **Deprecated:** `SledStorage` is deprecated as of v0.20.0 and will be removed in v0.21.0. Existing deployments can continue using it during the deprecation period, but new persistent-storage deployments should use [Redb Storage](redb-storage.md).

SledStorage is based on the Sled key-value embedded database built in Rust ([Sled on GitHub](https://github.com/spacejam/sled)) and can only be used in a Rust environment. This page remains available for existing deployments during the deprecation period.

## How to use
You can simply create a SledStorage instance using a path, as shown below:

```rust
use {
    gluesql::{prelude::Glue, sled_storage::SledStorage},
    sled_storage::sled,
    std::convert::TryFrom,
};

fn main() {
    let storage = SledStorage::new("data/temp").unwrap();
    let mut glue = Glue::new(storage);

    let sqls = "
        CREATE TABLE Glue (id INTEGER);
        INSERT INTO Glue VALUES (100), (200);
    ";

    glue.execute(sqls).unwrap();
}
```

If you want to use the Sled that SledStorage uses directly with a specific configuration, you can do so as follows:

```rust
let config = sled::Config::default()
    .path("data/using_config")
    .temporary(true)
    .mode(sled::Mode::HighThroughput);

let storage = SledStorage::try_from(config).unwrap();
let mut glue = Glue::new(storage);
```

## Things to Know About Transactions

The implementation of transactions in SledStorage manages not only data but also indexes and schema information based on snapshots. For example, if you use the following commands:

```sql
BEGIN;

CREATE TABLE Foo;
INSERT INTO Foo VALUES (1);

ROLLBACK;
```

The above usage will result in a rollback of even the contents regarding the Foo table. The transaction isolation level is repeatable read (snapshot isolation).

By default, there is a timeout for Transactions. The default is set to one hour, but you can modify the value or remove the timeout if desired.

```rust
storage.set_transaction_timeout(Some(1000)); // 1 sec
storage.set_transaction_timeout(None); // no timeout
```

## Summary
Existing deployments can continue using SledStorage during the v0.20 deprecation period. New persistent-storage deployments should use [Redb Storage](redb-storage.md), and existing users should migrate before SledStorage is removed in v0.21.0.
