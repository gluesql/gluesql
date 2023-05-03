---
sidebar_position: 4
---

# JSON Storage

## Introduction

The JSON Storage system deals with two types of files `Schema` and `Data` and supports two file formats `*.json` and `*.jsonl`. The schema file defines the structure of the data saved in the data file, and the data file contains the actual data. A schemaless table is also supported, which can save any type of data. This document provides examples of how to read and create schema tables using GlueSQL JSON Storage.

## Structure

- Basically, JSON Storage deals with two types of file: `Schema` and `Data`

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

## Schema file

### Schema

- Schema definition is saved in `TableName.sql` with standard SQL

```sql
--! User.sql
CREATE TABLE User (
  id INT,
  name TEXT,
);
```

### Schemaless

- Schema file is optional. if there is no `User.sql`, the table is schemaless
- Schemaless Table can save any data regardless of column name and data type

```sql
gluesql> CREATE Table User;
```

```sh
$ ls -l

User.json
```

## Data file

- JSON Storage saves data in two types of datafile: `*.jsonl` (default) and `*.json`

### `*.jsonl`

```json
{"id": 1, "name": "Glue"}
{"id": 2, "name": "SQL"}
```

### `*.json`

- `*.json` supports two kinds of format

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
  "data": [
    {
      "id": 1,
      "name": "Glue"
    },
    {
      "id": 2,
      "name": "SQL"
    }
  ]
}
```

## Example

### Read existing json/jsonl schemaless files.

1. Locate your json/jsonl schemaless files in data path (here we use `./data`).

```
$ ls -rlt ./data

User.jsonl
Dept.json
```

2. Read with GlueSQL JSON Storage

```rust
let path = "./data/";
let json_storage = JsonStorage::new(path).unwrap();
let mut glue = Glue::new(json_storage);

glue.execute("SELECT * FROM User JOIN Dept ON User.dept_id = Dept.id");
```

### Create Schema table

1. Create Table

```rust
let path = "./data/";
let json_storage = JsonStorage::new(path).unwrap();
let mut glue = Glue::new(json_storage);

glue.execute("CREATE TABLE Account (no INT, name TEXT)");
```

```sh
$ ls -l

Account.sql
Account.jsonl
```

2. Insert data

```rust
glue.execute("INSERT INTO Account VALUES(1, 'A')");
```

3. Select data

```rust
glue.execute("SELECT * FROM Account");
```

## Summary

JSON Storage system which deals with two types of files: Schema and Data, and supports two file formats: `*.json` and `*.jsonl`. The Schema file defines the structure of the data saved in the data file, and the Data file contains the actual data. The Schemaless table is also supported, which can save any type of data. The Schema definition is saved in TableName.sql with standard SQL. If there is no Schema file, the table is Schemaless. JSON Storage saves data in two types of Data files: `*.jsonl` (default) and `*.json`. The document provides examples of how to read and create Schema tables using GlueSQL JSON Storage. The document also gives examples of how to read and create Schema tables, how to insert data into a table, and how to select data from a table.
