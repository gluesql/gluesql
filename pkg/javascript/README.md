# GlueSQL.js

[![npm](https://img.shields.io/npm/v/gluesql)](https://www.npmjs.com/package/gluesql)
[![GitHub](https://img.shields.io/github/stars/gluesql/gluesql)](https://github.com/gluesql/gluesql)
[![LICENSE](https://img.shields.io/crates/l/gluesql.svg)](https://github.com/gluesql/gluesql/blob/main/LICENSE)
[![Chat](https://img.shields.io/discord/780298017940176946)](https://discord.gg/C6TDEgzDzY)
[![Coverage Status](https://coveralls.io/repos/github/gluesql/gluesql/badge.svg?branch=main)](https://coveralls.io/github/gluesql/gluesql?branch=main)

GlueSQL.js is a SQL database for web browsers and Node.js. It works as an embedded database and entirely runs in the browser.
GlueSQL.js supports in-memory storage backend, but it will soon to have localStorage, sessionStorage and indexedDB backend supports.
## Installation

#### Yarn
```
yarn add gluesql
```

#### npm
```
npm install gluesql
```

#### JavaScript modules
```javascript
import { gluesql } from 'https://cdn.jsdelivr.net/npm/gluesql@0.13.0/gluesql.js';
```

## Usage

```javascript
import { gluesql } from 'gluesql';

const db = await gluesql();

await db.query(`
  CREATE TABLE User (id INTEGER, name TEXT);
  INSERT INTO User VALUES (1, "Hello"), (2, "World");
`);

const [{ rows }] = await db.query('SELECT * FROM User;');

console.log(rows);
```

## Examples
* [JavaScript modules](https://github.com/gluesql/gluesql/tree/main/pkg/javascript/examples/web/module)
* [Rollup](https://github.com/gluesql/gluesql/tree/main/pkg/javascript/examples/web/rollup)
* [Webpack](https://github.com/gluesql/gluesql/tree/main/pkg/javascript/examples/web/webpack)

## 🚧 Documentation- WIP
* [General examples](https://github.com/gluesql/gluesql/tree/main/test-suite/src)
* [Supported data types](https://github.com/gluesql/gluesql/tree/main/test-suite/src/data_type)
* [Supported SQL functions](https://github.com/gluesql/gluesql/tree/main/test-suite/src/function)
