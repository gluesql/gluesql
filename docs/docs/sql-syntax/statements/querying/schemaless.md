---
title: "Schemaless Data"
sidebar_position: 5
---

# Querying Schemaless Data

GlueSQL is an SQL database that provides a unique feature: it allows you to work with schemaless data, similar to NoSQL databases. Please note this point in the documentation.

## Creating a Schemaless Table

To create a schemaless table, you don't need to specify columns when creating the table. For example:

```sql
CREATE TABLE Sample;
```

This creates a schemaless table. You can now insert data freely into each row, like a NoSQL database. Nested data is also supported.

## Example SQL Queries

Here are some example SQL queries that demonstrate how to use GlueSQL with schemaless data:

### Creating Tables

```sql
CREATE TABLE Player;
CREATE TABLE Item;
```

### Inserting Data

```sql
INSERT INTO Player VALUES ('{"id": 1001, "name": "Beam", "flag": 1}'), ('{"id": 1002, "name": "Seo"}');
INSERT INTO Item VALUES ('{"id": 100, "name": "Test 001", "dex": 324, "rare": false, "obj": {"cost": 3000}}'), ('{"id": 200}');
```

### Selecting Data

```sql
SELECT name, dex, rare FROM Item WHERE id = 100;
SELECT name, dex, rare FROM Item;
SELECT * FROM Item;
```

### Updating Data

```sql
DELETE FROM Item WHERE id > 100;
UPDATE Item SET id = id + 1, rare = NOT rare;
UPDATE Item SET new_field = 'Hello';
```

### Selecting with Aliases and Joins

```sql
SELECT
    Player.id AS player_id,
    Player.name AS player_name,
    Item.obj['cost'] AS item_cost
FROM Item
JOIN Player
WHERE flag IS NOT NULL;
```

## Notable Exception Cases

Here are some example SQL queries that will raise errors, along with explanations of the issues:

### Inserting Invalid Data

- Inserting multiple values for a schemaless row:

  ```sql
  INSERT INTO Item VALUES ('{"a": 10}', '{"b": true}');
  ```
  
  Schemaless rows accept only single values.

- Inserting data from a SELECT statement:

  ```sql
  INSERT INTO Item SELECT id, name FROM Item LIMIT 1;
  ```
  
  Schemaless rows cannot be inserted using a SELECT statement.

- Inserting a JSON array:

  ```sql
  INSERT INTO Item VALUES ('[1, 2, 3]');
  ```
  
  Only JSON objects are allowed for schemaless rows.

- Inserting a boolean value:

  ```sql
  INSERT INTO Item VALUES (true);
  ```
  
  Text literals are required for schemaless rows.

- Inserting an expression result:

  ```sql
  INSERT INTO Item VALUES (CAST(1 AS INTEGER) + 4);
  ```
  
  Map or string values are required for schemaless rows.

- Inserting data from a SELECT statement with LIMIT:

  ```sql
  INSERT INTO Item SELECT id FROM Item LIMIT 1;
  ```
  
  Map type values are required for schemaless rows.

### Selecting Invalid Data

- Using IN with a schemaless subquery:

  ```sql
  SELECT id FROM Item WHERE id IN (SELECT * FROM Item);
  ```
  
  Schemaless projections are not allowed for IN subqueries.

- Using a comparison with a schemaless subquery:

  ```sql
  SELECT id FROM Item WHERE id = (SELECT * FROM Item LIMIT 1);
  ```
  
  Schemaless projections are not allowed for subqueries.