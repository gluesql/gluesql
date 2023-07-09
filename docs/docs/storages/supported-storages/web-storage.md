---
sidebar_position: 5
---

# WebStorage (local & session)

WebStorage - yes, the localStorage and sessionStorage you're familiar with. With GlueSQL, you can use SQL to interact with these storages!

WebStorage serves as a data storage that supports READ & WRITE operations. As GlueSQL can be ported to any place where READ & WRITE are possible, it can utilize WebStorage as one of its storage systems.

WebStorage provides a very simple and easy-to-use interface. All you need to do is read and write data using a string key. If you need to manage more structured data, you can use GlueSQL.

WebStorage can be used in JavaScript (Web) environments and Rust WebAssembly environments.

## Usage
The way to use it is no different from using other storages.

```javascript
import { gluesql } from 'gluesql';

async function run() {
    const db = await gluesql();

    const result = await db.query(`
        CREATE TABLE Foo (id INTEGER, name TEXT) ENGINE = localStorage;
        INSERT INTO Foo VALUES (1, 'hello'), (2, 'world');
        SELECT * FROM Foo;
        CREATE TABLE Bar ENGINE = sessionStorage;
        INSERT INTO Bar VALUES ('{ "a": "schemaless", "b": 1024 }');
        SELECT * FROM Bar;
    `);

    console.log(result);
}
```

Simple, isn't it?

## Things to keep in mind
In the case of WebStorage, depending on the web browser, there is usually a size constraint of about 10MB for data storage. Even when using GlueSQL, you should keep in mind that it is used within these restrictions.

## Summary
To sum up, WebStorage is a handy feature that allows you to manipulate localStorage and sessionStorage with SQL in a browser environment. It's simple, easy to use, and can handle structured data which makes it an ideal choice for lightweight web applications.

However, due to storage limitations, it's not suitable for large-scale data handling. Remember to consider these limitations when choosing your storage options in GlueSQL. Even with these constraints, it serves as a great tool for managing and interacting with your browser's storage in a structured way using SQL.