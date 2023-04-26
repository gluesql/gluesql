---
sidebar_position: 3
---

# Node.js

This guide will help you get started with GlueSQL in a Node.js project. First, install the `gluesql` package using npm by running the following command in your terminal:

```sh
npm install gluesql
```

Alternatively, you can add it as a dependency in your `package.json` file:

```json
{
  "dependencies": {
    "gluesql": "latest"
  }
}
```

Please note that the Node.js version of GlueSQL currently supports only non-persistent memory storage.

Next, you can use GlueSQL in your Node.js project by following this simple example:

```javascript
const { gluesql } = require('gluesql');
const db = gluesql();

async function run() {
  await db.query(`
    CREATE TABLE User (id INTEGER, name TEXT);
    CREATE TABLE Device (name TEXT, userId INTEGER);
    INSERT INTO User VALUES (1, 'glue'), (2, 'sticky'), (3, 'watt');
    INSERT INTO Device VALUES ('Phone', 1), ('Mic', 1), ('Monitor', 3), ('Mouse', 2), ('Touchpad', 2);
  `);

  const sql = `
    SELECT
      u.name as user,
      d.name as device
    FROM User u
    JOIN Device d ON u.id = d.userId
  `;
  const [{ rows }] = await db.query(sql);
  console.table(rows);
}

run();
```

This example demonstrates how to create tables, insert data, and perform a join query using GlueSQL in Node.js.