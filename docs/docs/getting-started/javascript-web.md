---
sidebar_position: 2
---

# JavaScript (Web Browser)

GlueSQL is a SQL database engine written in Rust, compiled to WebAssembly, and can be used in JavaScript. This guide will walk you through the process of installing and using the GlueSQL package.

## Installation

Installing GlueSQL is as simple as running the following command:

```bash
npm install gluesql
```

In your `package.json`, it will be added to the dependencies list as follows:

```json
{
  "dependencies": {
    "gluesql": "latest"
  }
}
```

## Usage

GlueSQL can be used in different environments. Here we will look at how to use it with JavaScript modules, Webpack, and Rollup.

### JavaScript Modules

In an HTML file, you can use GlueSQL by importing it with a script tag:

```html
<script type="module">
  import { gluesql } from 'gluesql';

  async function main() {
    const db = await gluesql();
    await db.loadIndexedDB();

    const result = await db.query(`
      CREATE TABLE Foo (id INTEGER) ENGINE = memory;
      INSERT INTO Foo (1, 'glue'), (2, 'sql');
      SELECT * FROM Foo;
    `);

    console.log(result);
   }
</script>
```

### Webpack

For Webpack, the usage is almost the same as JavaScript modules:

```javascript
import { gluesql } from 'gluesql';

async function run() {
  const db = await gluesql();
  await db.loadIndexedDB();

  const result = await db.query(`
    CREATE TABLE Foo (id INTEGER) ENGINE = memory;
    INSERT INTO Foo VALUES (1, 'glue'), (2, 'sql');
    SELECT * FROM Foo;
  `);

  console.log(result);
}
```

### Rollup

For Rollup, you need to adjust your import statement as follows:

```javascript
import { gluesql } from 'gluesql/gluesql.rollup';

// ...
```

## Supported Storage Engines

GlueSQL supports four storage types: In-Memory Storage, Local Storage, Session Storage, and IndexedDB. 

You can specify the storage type when creating a table using the `ENGINE` clause:

- For In-Memory Storage: `ENGINE = memory`
- For Local Storage: `ENGINE = localStorage`
- For Session Storage: `ENGINE = sessionStorage`
- For IndexedDB: `ENGINE = indexedDB`

For example:

```sql
CREATE TABLE Foo (id INTEGER) ENGINE = memory;
```