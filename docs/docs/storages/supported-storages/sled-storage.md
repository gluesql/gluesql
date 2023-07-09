---
sidebar_position: 3
---

# Sled Storage

SledStorage is currently the representative persistent data storage for GlueSQL. As the name suggests, it's a storage option based on the Sled key-value embedded database built in Rust ([Sled on Github](https://github.com/spacejam/sled)).

SledStorage can only be used in a Rust environment. It is the only storage among those currently supported by GlueSQL that implements all Store traits, from non-clustered indexes to transactions. If you're looking for a basic storage to handle and store data in a Rust environment, SledStorage is an excellent choice.

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
If you're looking for a storage to handle data for general purposes in a Rust environment, SledStorage would be your go-to choice. It offers all the necessary features of a database system, such as managing non-clustered indexes, handling transactions, and maintaining persistent storage. Additionally, its snapshot-based transaction model ensures consistency and reliability, making it an excellent choice for applications requiring persistent data storage.