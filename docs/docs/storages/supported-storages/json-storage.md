---
sidebar_position: 4
---

# JSON Storage

## Introduction

The JSON Storage system is comprised of two types of files: [Schema file](#schema-file)(optional) and [Data file](#data-file). The Schema file is written in Standard SQL and is responsible for storing the structure of the table. The Data file contains the actual data and supports two file formats: `*.json` and `*.jsonl`. This document provides detailed [examples](#examples) of how to create schema and read/write data using the Json Storage system. While it supports all DML features, it is particularly specialized for `SELECT` and `APPEND INSERT`. For further information, please refer to the [Limitations](#limitation) section.

## Structure

JSON Storage is based on two types of files: `Schema` and `Data`. The `Schema` file contains the definition of the structure of the data, while the `Data` file contains the actual data.

```sql
gluesql> CREATE TABLE User (
    id INT,
    name TEXT
)
```

```sh
$ ls -l

User.sql # Schema file
User.jsonl # Data file
```

## Schema File

The schema definition is saved in a file named `{TABLE_NAME}.sql` using standard SQL. For example, if the table name is `User`, then the schema file will be named `User.sql`.

```sql
--! User.sql
CREATE TABLE User (
  id INT,
  name TEXT,
);
```

### Schemaless Table

A schemaless table is optional, and if there is no corresponding `{TABLE_NAME}.sql` file, the table is schemaless. A schemaless table can save any data regardless of column name and data type.

```sql
gluesql> CREATE Table User;
```

```sh
$ ls -l

User.jsonl
```

## Data File

JSON Storage saves data in two types of data files: `*.jsonl` (default) and `*.json`.

### `*.jsonl` File Format

The `*.jsonl` file format is a file containing one JSON object per line. For example:

```json
{"id": 1, "name": "Glue"}
{"id": 2, "name": "SQL"}
```

### `*.json` File Format

The `*.json` file format supports two different formats:

1. Array of JSON

```json
[
  {
    "id": 1,
    "name": "Glue"
  },
  {
    "id": 2,
    "name": "SQL"
  }
]
```

2. Single JSON

```json
{
  "name": "GlueSQL"
  "keywords": ["Database, Rust"]
  "stars": 999999
}
```

## Examples

### Read Existing JSON/JSONL Schemaless Files

1. Locate your JSON/JSONL schemaless files in the data path. Here, we use `./data`.

```
$ ls -rlt ./data

User.json
LoginHistory.jsonl
```

Keep in mind that if there are no `*.sql` files, the data is considered schemaless, meaning that the number of columns in each row may vary.

```json
//! User.json
[
  {
    "id": 1,
    "name": "Alice",
    "location": "New York"
  },
  {
    "id": 2,
    "name": "Bob",
    "language": "Rust"
  },
  {
    "id": 3,
    "name": "Eve"
  }
]
```

```json
//! LoginHistory.jsonl
{"timestamp": "2023-05-01T14:36:22.000Z", "userId": 1, "action": "login"}
{"timestamp": "2023-05-01T14:38:17.000Z", "userId": 2, "action": "logout"}
{"timestamp": "2023-05-02T08:12:05.000Z", "userId": 2, "action": "logout"}
{"timestamp": "2023-05-02T09:45:13.000Z", "userId": 3, "action": "login"}
{"timestamp": "2023-05-03T16:21:44.000Z", "userId": 1, "action": "logout"}
```

2. Read with GlueSQL JSON Storage

```rust
let path = "./data/";
let json_storage = JsonStorage::new(path).unwrap();
let mut glue = Glue::new(json_storage);

glue.execute("
SELECT *
FROM User U
JOIN LoginHistory L ON U.id = L.userId;
");
```

| action | id  | language | location | name  | timestamp                | userId |
| ------ | --- | -------- | -------- | ----- | ------------------------ | ------ |
| login  | 1   |          | New York | Alice | 2023-05-01T14:36:22.000Z | 1      |
| logout | 1   |          | New York | Alice | 2023-05-03T16:21:44.000Z | 1      |
| logout | 2   | Rust     |          | Bob   | 2023-05-01T14:38:17.000Z | 2      |
| logout | 2   | Rust     |          | Bob   | 2023-05-02T08:12:05.000Z | 2      |
| login  | 3   |          |          | Eve   | 2023-05-02T09:45:13.000Z | 3      |

### Create Schema Table

1. Create Table

```rust
let path = "./data/";
let json_storage = JsonStorage::new(path).unwrap();
let mut glue = Glue::new(json_storage);

glue.execute("
CREATE TABLE Account (
  accountId INT NOT NULL,
  accountOwner TEXT NOT NULL,
  accountType TEXT NOT NULL,
  balance INT NOT NULL,
  isActive BOOLEAN NOT NULL
);
");
```

```sh
$ ls -l

Account.sql
Account.jsonl
```

2. Verity Schema file

```sql
--! Account.sql
CREATE TABLE Account (
  accountId INT NOT NULL,
  accountOwner TEXT NOT NULL,
  accountType TEXT NOT NULL,
  balance INT NOT NULL,
  isActive BOOLEAN NOT NULL
);
```

3. Insert data

```rust
glue.execute("
INSERT INTO Account VALUES
  (10001, 'John Smith', 'Checking', 5000, true),
  (10002, 'Jane Doe', 'Savings', 10000, true),
  (10003, 'Robert Johnson', 'Checking', 2500, false),
  (10004, 'Alice Kim', 'Savings', 7500, true),
  (10005, 'Michael Chen', 'Checking', 10000, true);
");
```

4. Select data

```rust
glue.execute("SELECT * FROM Account;");
```

| accountId | accountOwner   | accountType | balance | isActive |
| --------- | -------------- | ----------- | ------- | -------- |
| 10001     | John Smith     | Checking    | 5000    | TRUE     |
| 10002     | Jane Doe       | Savings     | 10000   | TRUE     |
| 10003     | Robert Johnson | Checking    | 2500    | FALSE    |
| 10004     | Alice Kim      | Savings     | 7500    | TRUE     |
| 10005     | Michael Chen   | Checking    | 10000   | TRUE     |

5. Verify Data file

```json
//! Account.jsonl
{"accountId":10001,"accountOwner":"John Smith","accountType":"Checking","balance":5000,"isActive":true}
{"accountId":10002,"accountOwner":"Jane Doe","accountType":"Savings","balance":10000,"isActive":true}
{"accountId":10003,"accountOwner":"Robert Johnson","accountType":"Checking","balance":2500,"isActive":false}
{"accountId":10004,"accountOwner":"Alice Kim","accountType":"Savings","balance":7500,"isActive":true}
{"accountId":10005,"accountOwner":"Michael Chen","accountType":"Checking","balance":10000,"isActive":true}
```

## Limitation

JSON Storage is capable of supporting a variety of operations, including `SELECT`, `INSERT`, `DELETE`, and `UPDATE`.  
However, its design primarily emphasizes `SELECT` and `APPEND INSERT` functionality.  
It's important to note that if you perform `DELETE`, `UPDATE`, or `INSERT in the middle of the rows`, it can cause the internal rewriting of all the rows, which can lead to a decrease in performance.
