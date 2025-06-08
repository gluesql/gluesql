# Mongo Storage

Mongo Storage allows GlueSQL to use MongoDB as a backend for SQL queries. It enables features such as joins and aggregations on top of MongoDB collections while optionally enforcing GlueSQL's schema system.

## Prerequisites

Install and run MongoDB.

### 1. Using Docker

```bash
docker run --name mongo-glue -d -p 27017:27017 mongo
```

### 2. Local Installation

Follow the [official MongoDB installation guide](https://www.mongodb.com/docs/manual/installation/).

## Example

Below is a minimal example showing how to execute SQL statements on MongoDB using GlueSQL in Rust:

```rust
use gluesql::{prelude::Glue, mongo_storage::MongoStorage};

#[tokio::main]
async fn main() {
    let conn_str = "mongodb://localhost:27017";
    let storage = MongoStorage::new(conn_str, "my_db").await.unwrap();
    let mut glue = Glue::new(storage);

    let sql = "
        CREATE TABLE Foo (id INT, name TEXT);
        INSERT INTO Foo VALUES (1, 'Alice'), (2, 'Bob');
        SELECT * FROM Foo;
    ";

    let payloads = glue.execute(sql).await.unwrap();
    println!("{:#?}", payloads);
}
```

After running the above code you will have a `Foo` collection in MongoDB populated with two documents. You can continue to query it using SQL syntax:

```sql
SELECT name FROM Foo WHERE id > 1;
```

```text
| name |
|------|
| Bob  |
```

## Summary

Mongo Storage integrates MongoDB with GlueSQL so you can work with your MongoDB data using standard SQL. Ensure MongoDB is running before connecting.
