---
sidebar_position: 4
---

# JSON Storage

## Introduction

The JSON Storage system deals with two types of files: `Schema` and `Data` and supports two file formats: `*.json` and `*.jsonl`. The schema file defines the structure of the data saved in the data file, and the data file contains the actual data. A schemaless table is also supported, which can save any type of data. This document provides examples of how to read and create schema tables using GlueSQL JSON Storage.

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

## Example

### Read Existing JSON/JSONL Schemaless Files

1. Locate your JSON/JSONL schemaless files in the data path. Here, we use `./data`.

```
$ ls -rlt ./data

User.jsonl
Dept.json
```

```json
//! User.jsonl
{"id": 1, "name": "Alice", "deptId": 1}
{"id": 2, "name": "Bob", "deptId": 2}
{"id": 3, "name": "Carol", "deptId": 1}
{"id": 4, "name": "Dave", "deptId": 2}
{"id": 5, "name": "Eve", "deptId": 3}
```

```json
//! Dept.json
[
  {
    "id": 1,
    "name": "Sales",
    "location": "New York"
  },
  {
    "id": 2,
    "name": "Marketing",
    "location": "Chicago"
  },
  {
    "id": 3,
    "name": "Finance",
    "location": "San Francisco"
  }
]
```

2. Read with GlueSQL JSON Storage

```rust
let path = "./data/";
let json_storage = JsonStorage::new(path).unwrap();
let mut glue = Glue::new(json_storage);

glue.execute("
SELECT U.id, U.name, D.name as deptName, D.location
FROM User U
JOIN Dept D ON U.deptId = D.id;
");
```

| id  | name  | deptName  | location      |
| --- | ----- | --------- | ------------- |
| 1   | Alice | Sales     | New York      |
| 2   | Bob   | Marketing | Chicago       |
| 3   | Carol | Sales     | New York      |
| 4   | Dave  | Marketing | Chicago       |
| 5   | Eve   | Finance   | San Francisco |

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

## Summary

The JSON Storage system deals with two types of files: Schema and Data, and supports two file formats: `*.json` and `*.jsonl`. The schema file defines the structure of the data saved in the data file, and the data file contains the actual data. A schemaless table is also supported, which can save any type of data. The schema definition is saved in TableName.sql with standard SQL. If there is no TableName.sql, the table is schemaless. JSON Storage saves data in two types of datafile: `*.jsonl` (default) and `*.json`. This document provides examples of how to read and create schema tables using GlueSQL JSON Storage, including how to read existing JSON/JSONL schemaless files, create schema tables, insert data into a table, and select data from a table.
