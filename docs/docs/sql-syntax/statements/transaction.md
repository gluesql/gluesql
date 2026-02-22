---
sidebar_position: 4
---

# Transaction

Transactions in SQL are a series of queries that are executed as a single unit of work. In GlueSQL, transactions help to ensure the consistency and integrity of the database. They follow the ACID properties: Atomicity, Consistency, Isolation, and Durability.

**Note: In GlueSQL, transactions are an optional feature. Support for transactions depends on the storage engine being used. Currently, both `SledStorage` and `RedbStorage` support transactions, and additional storages may add support in the future. Transaction isolation levels may also vary depending on the storage engine. For example, the current transaction isolation level for `SledStorage` is SNAPSHOT ISOLATION.**

## BEGIN TRANSACTION

To start a new transaction, use the `BEGIN` keyword:

```
BEGIN;
```

## COMMIT TRANSACTION

To permanently save the changes made during the transaction, use the `COMMIT` keyword:

```
COMMIT;
```

## ROLLBACK TRANSACTION

To undo the changes made during the transaction and revert the database to its state before the transaction started, use the `ROLLBACK` keyword:

```
ROLLBACK;
```

## Example

Consider the following table `TxTest` with columns `id` (INTEGER) and `name` (TEXT):

```sql
CREATE TABLE TxTest (
    id INTEGER,
    name TEXT
);
```

Insert sample data into the table:

```sql
INSERT INTO TxTest VALUES
    (1, 'Friday'),
    (2, 'Phone');
```

### Inserting Data

Start a new transaction and insert a new row:

```sql
BEGIN;
INSERT INTO TxTest VALUES (3, 'New one');
```

Rollback the transaction to undo the insertion:

```sql
ROLLBACK;
```

Now, start a new transaction and insert a new row with different data:

```sql
BEGIN;
INSERT INTO TxTest VALUES (3, 'Vienna');
COMMIT;
```

### Deleting Data

Start a new transaction and delete a row:

```sql
BEGIN;
DELETE FROM TxTest WHERE id = 3;
ROLLBACK;
```

The deletion will be undone due to the rollback. To permanently delete the row, commit the transaction:

```sql
BEGIN;
DELETE FROM TxTest WHERE id = 3;
COMMIT;
```

### Updating Data

Start a new transaction and update a row:

```sql
BEGIN;
UPDATE TxTest SET name = 'Sunday' WHERE id = 1;
ROLLBACK;
```

The update will be undone due to the rollback. To permanently update the row, commit the transaction:

```sql
BEGIN;
UPDATE TxTest SET name = 'Sunday' WHERE id = 1;
COMMIT;
```