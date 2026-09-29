---
sidebar_position: 2
---

# JavaScript (Web Browser)

GlueSQL is a SQL database engine written in Rust, compiled to WebAssembly, and can be used in JavaScript. This guide will walk you through the process of installing and using the GlueSQL package.

The JavaScript package source and release workflows are maintained in the [gluesql-js repository](https://github.com/gluesql/gluesql-js).

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

## Choosing an Entry Point

The `gluesql` package provides three browser entry points. Pick the one that matches where your data should live:

| Entry point | Storage | Persistence |
| --- | --- | --- |
| `gluesql` | `memory`, `localStorage`, `sessionStorage` | Memory is lost on reload; Web Storage follows the browser's storage rules |
| `gluesql/opfs` | OPFS file | Survives reloads and browser restarts, one tab at a time |
| `gluesql/opfs/shared` | OPFS file, shared across tabs | Survives reloads and browser restarts, safe to open in many tabs |

## Usage

GlueSQL can be used in different environments. Here we will look at how to use it with JavaScript modules, Webpack, and Rollup.

### JavaScript Modules

In an HTML file, you can use GlueSQL by importing it with a script tag:

```html
<script type="module">
  import { gluesql } from 'https://cdn.jsdelivr.net/npm/gluesql/gluesql.js';

  async function main() {
    const db = await gluesql();

    const result = await db.query(`
      CREATE TABLE Foo (id INTEGER, name TEXT) ENGINE = memory;
      INSERT INTO Foo VALUES (1, 'glue'), (2, 'sql');
      SELECT * FROM Foo;
    `);

    console.log(result);
  }

  main();
</script>
```

### Webpack

For Webpack, the usage is almost the same as JavaScript modules:

```javascript
import { gluesql } from 'gluesql';

async function run() {
  const db = await gluesql();

  const result = await db.query(`
    CREATE TABLE Foo (id INTEGER, name TEXT) ENGINE = memory;
    INSERT INTO Foo VALUES (1, 'glue'), (2, 'sql');
    SELECT * FROM Foo;
  `);

  console.log(result);
}
```

### Rollup

For Rollup, you need to adjust your import statement and add some configurations to your `rollup.config.js` file.

First, modify your import statement as follows:

```javascript
import { gluesql } from 'gluesql/gluesql.rollup';
// ...
```

Second, add the following configurations to your `rollup.config.js` file:

```javascript
import resolve from '@rollup/plugin-node-resolve';
import { wasm } from '@rollup/plugin-wasm';

export default {
  input: 'main.js',
  output: {
    file: 'dist/bundle.js',
    format: 'iife',
  },
  plugins: [
    resolve({ browser: true }),
    wasm({ targetEnv: 'auto-inline' }),
  ],
};
```

These configurations allow Rollup to correctly handle WebAssembly modules and resolve dependencies for browsers.

Don't forget to run the `rollup` command to bundle your JavaScript files:

```bash
rollup -c
```

Now, you can use GlueSQL in your Rollup project as you would in any other JavaScript module.

## Supported Storage Engines

The main `gluesql` entry point supports three storage types: In-Memory Storage, Local Storage, and Session Storage.

You can specify the storage type when creating a table using the `ENGINE` clause:

- For In-Memory Storage: `ENGINE = memory`
- For Local Storage: `ENGINE = localStorage`
- For Session Storage: `ENGINE = sessionStorage`

For example:

```sql
CREATE TABLE Foo (id INTEGER) ENGINE = memory;
```

Tables stored in different engines can be joined in a single query.

When the `ENGINE` clause is omitted, the default engine is used. The default engine is `memory` initially, and you can change it with `setDefaultEngine`:

```javascript
db.setDefaultEngine('localStorage');
```

Web Storage is limited to a few megabytes per origin. For larger or long-lived data, use the OPFS entry point.

## Persistent Storage with OPFS

The `gluesql/opfs` entry point stores the database as a file in the [Origin Private File System (OPFS)](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system). Data survives page reloads and browser restarts.

GlueSQL and its storage run inside a Dedicated Worker, which keeps queries off the main thread. The `gluesql()` function returns a proxy whose `query` method sends SQL to the worker and returns a `Promise`.

```javascript
import { gluesql } from 'gluesql/opfs';

const db = gluesql();

await db.query(`
  CREATE TABLE IF NOT EXISTS User (id INTEGER, name TEXT);
  INSERT INTO User VALUES (1, 'glue');
`);

// The data is still there after a reload.
const [{ rows }] = await db.query('SELECT * FROM User;');
```

Unlike the main entry point, `gluesql()` here does not need to be awaited. OPFS is the only storage in this entry point, so the `ENGINE` clause and `setDefaultEngine` do not apply.

Use the `namespace` option to keep separate databases, each stored in its own file. When it is omitted, the `gluesql` namespace is used.

```javascript
const app1 = gluesql({ namespace: 'app1' });
const app2 = gluesql({ namespace: 'app2' });
```

Call `terminate()` to stop the worker. Queries that are still pending are rejected.

```javascript
db.terminate();
```

### Worker Location

The entry point loads `gluesql.opfs.worker.js` relative to its own module URL, and the worker loads its WebAssembly from the `dist_opfs/` directory next to it. When the package files are served from your own origin, no extra configuration is needed.

Browsers only allow workers from the same origin, so the OPFS entry points cannot be loaded directly from a third-party CDN. If you serve the worker from a different path, copy `dist_opfs/` next to it and pass its URL:

```javascript
const db = gluesql({ workerUrl: '/assets/gluesql.opfs.worker.js' });
```

### Sharing a Database Across Tabs

The OPFS file handle is exclusive, so a namespace opened with `gluesql/opfs` can only be used by one tab at a time. To open the same database in multiple tabs, use `gluesql/opfs/shared` (experimental). The API is the same:

```javascript
import { gluesql } from 'gluesql/opfs/shared';

const db = gluesql({ namespace: 'app' });

await db.query('CREATE TABLE IF NOT EXISTS Log (at TEXT);');
```

Tabs using the same namespace elect a leader with the [Web Locks API](https://developer.mozilla.org/en-US/docs/Web/API/Web_Locks_API). Only the leader starts the database worker and holds the OPFS file, and the other tabs send their queries to it through a [`BroadcastChannel`](https://developer.mozilla.org/en-US/docs/Web/API/BroadcastChannel). A write from one tab is immediately visible to the others.

If the leader tab closes or crashes, another tab becomes the leader, and queries sent in the meantime are delivered to it. A query that the lost leader had already received is rejected with a `leader lost` error, because it may or may not have been applied. Retry reads freely, but guard non-idempotent writes in your application.

If the Web Locks API or `BroadcastChannel` is unavailable, this entry point falls back to the single-tab behavior of `gluesql/opfs`.

For the full list of caveats, see the [gluesql-js README](https://github.com/gluesql/gluesql-js#every-tab-one-database-gluesqlopfsshared-experimental).

### Browser Requirements

The OPFS entry points require:

- A [secure context](https://developer.mozilla.org/en-US/docs/Web/Security/Secure_Contexts): serve the page over HTTPS or `localhost`. Opening it via `file://` does not work.
- A browser that supports [module workers](https://developer.mozilla.org/en-US/docs/Web/API/Worker/Worker) and [`FileSystemSyncAccessHandle`](https://developer.mozilla.org/en-US/docs/Web/API/FileSystemSyncAccessHandle) in Dedicated Workers.

If `FileSystemSyncAccessHandle` is unavailable, queries fail with an error. GlueSQL does not silently fall back to another storage.

A runnable example is available in [`examples/web/opfs`](https://github.com/gluesql/gluesql-js/tree/main/examples/web/opfs).
