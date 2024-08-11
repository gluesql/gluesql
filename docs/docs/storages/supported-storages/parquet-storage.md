# Parquet Storage

## Introduction

The Parquet Storage Extension empowers users to interact with Parquet files through SQL statements efficiently, making the reading and writing of Parquet files straightforward and user-friendly.

## Structure

The extension is designed to handle files with a `.parquet` extension and performs read and write operations in the path specified by the user. It provides flexibility for creating files with or without a predefined schema using the DDL statement, adjusting to user needs effectively.

- **Schema or Schema-less Files Creation:**
   Users can create either schema or schema-less files using the table name defined in the DDL statement. It adjusts the Parquet's schema and field information to align with GlueSQL's constructs, enabling efficient data querying processes.

- **Data Querying:**
   When querying data, the extension converts Parquet's schema and field information to GlueSQL's corresponding constructs. This conversion allows users to perform data queries seamlessly, leveraging the uniformity in schema and field information representation.

- **Data Modification:**
   Any changes in the data are reverted from GlueSQL's schema and field information back to Parquet's original constructs before being written back to a `.parquet` file. This bidirectional conversion ensures data integrity and consistency between the two formats during read and write operations.

## Schema File

With this extension, you can create new schemas using DDL statements and modify data using DML statements, ensuring seamless interaction with Parquet files.

### Examples

> **Note:** `{}` denotes a placeholder that you must replace with actual values.

To start interacting with the Parquet extension, use the following command in your CLI:

```sh
./gluesql -p {workspace path} -s parquet
```

#### Creating a Table

```sql
gluesql> CREATE TABLE food (name TEXT);
```

```bash
Table created
```

At this point, you can verify the creation of the food.parquet file in the specified path (./).

#### Inserting Data and Querying

```sql
INSERT INTO food VALUES('sushi'), ('steak');

SELECT * FROM food;
```

```bash
2 row inserted

| name  |
|-------|
| sushi |
| steak |
```

#### Updating Data and Querying

```sql
UPDATE food SET name = 'Nigiri Sushi' WHERE name='sushi';
SELECT * FROM food;
```

```bash
1 row updated

| name         |
|--------------|
| Nigiri Sushi |
| steak        |
```

#### Deleting Data and Querying

```sql
DELETE name FROM food WHERE name = 'steak';
SELECT * FROM food;
```

```bash
1 row deleted

| name         |
|--------------|
| Nigiri Sushi |
```

Remember to replace placeholders with the appropriate values and paths when using the commands, and follow the structured steps for effective interaction with Parquet files using GlueSQL.

In rust.

```rust
let path = "./";
let parquet_storage = ParquetStorage::new(path).unwrap();
let mut glue = Glue::new(parquet_storage);
glue.execute("CREATE TABLE food (name TEXT);")
    .await
    .unwrap();

glue.execute("INSERT INTO food VALUES('sushi'), ('steak');")
    .await
    .unwrap();

glue.execute("UPDATE food SET name = 'Nigiri Sushi' WHERE name='sushi';")
    .await
    .unwrap();

glue.execute("DELETE name FROM food WHERE name = 'steak';")
    .await
    .unwrap();

glue.execute("SELECT * FROM food;").await.unwrap();
```

## Schemaless File Interaction

Parquet files inherently require a predefined schema. When creating tables without an explicit schema (schemaless tables), this extension establishes a temporary schema utilizing the Map datatype for the parquet file. This functionality ensures that even schemaless instances can process queries and modifications effectively.

### Implications

- **Ease of Interaction:** The temporary schema creation allows users to interact with schemaless parquet files with ease, facilitating various operations such as data retrieval and modifications effectively.

- **Structured Interaction:** The use of the Map datatype as a temporary schema enables structured interaction with schemaless parquet files, ensuring a smooth user experience.

### Examples

#### Creating Schemaless Table, Inserting, and Querying Data

```sql
CREATE TABLE Logs;
INSERT INTO Logs VALUES
    ('{ "id": 1, "value": 30 }'),
    ('{ "id": 2, "rate": 3.0, "list": [1, 2, 3] }'),
    ('{ "id": 3, "rate": 5.0, "value": 100 }');
SELECT id, rate, list FROM Logs WHERE id = 2;
```

```bash
Table created
3 rows inserted

| id | rate | list    |
|----|------|---------|
| 2  | 3    | [1,2,3] |
```

#### Updating Data and Querying

```sql
UPDATE Logs SET list='[5,6]' where id = 2;
SELECT id, rate, list FROM Logs WHERE id = 2;
```

```bash
1 row updated
| id | rate | list  |
|----|------|-------|
| 2  | 3    | [5,6] |
```

#### Deleting Data and Querying
> **Caution: Deleting data in a schemaless table removes all the data within it**

```sql
DELETE from Logs where id = 2;
SELECT id, rate, list FROM Logs WHERE id = 2;
```

```bash
1 row deleted
| id | rate | list |
```

In rust.

```rust
let path = "./";
let parquet_storage = ParquetStorage::new(path).unwrap();
let mut glue = Glue::new(parquet_storage);
glue.execute("CREATE TABLE Logs;")
    .await
    .unwrap();

glue.execute("INSERT INTO Logs VALUES
    ('{ "id": 1, "value": 30 }'),
    ('{ "id": 2, "rate": 3.0, "list": [1, 2, 3] }'),
    ('{ "id": 3, "rate": 5.0, "value": 100 }');")
    .await
    .unwrap();

glue.execute("UPDATE Logs SET list='[5,6]' where id = 2;")
    .await
    .unwrap();

//Caution: Deleting data in a schemaless table removes all the data within it
glue.execute("DELETE from Logs where id = 2;")
    .await
    .unwrap();

glue.execute("SELECT * FROM food;").await.unwrap();
```

## Limitations

1. For Parquet files storing data with `parquet::record::api::Field::MapInternal`, errors are encountered if the key information utilizes a data type other than string as the key.
This is attributed to the fact that GlueSQL's HashMap is of type <String, Value>, hence, limiting the use of other data types as keys.

2. The interface for reading data in columnar units is currently not supported by GlueSQL, which might result in suboptimal read and write performance.

3. Incompatibility with Parquet Physical Types:

GlueSQL currently lacks support for certain Parquet physical types, specifically INT96 and FIXED_LENGTH_BYTE_ARRAY. As a result, when executing data modification queries like INSERT or UPDATE on Parquet files, the data type for these columns will be transformed. Columns originally in the `INT96` type will be changed to GlueSQL's `Int128`, and those in `FIXED_LENGTH_BYTE_ARRAY` will be converted to GlueSQL's `Bytea` type. This conversion can have implications on data consistency and might necessitate additional transformations when interacting with other systems or tools that expect the original Parquet physical types.

## Conclusion

Despite certain limitations, this extension significantly simplifies interactions with Parquet files, making GlueSQL a more versatile tool by supporting a popular columnar storage file format.
