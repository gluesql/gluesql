# GlueSQL NAPI-RS Bindings

This package provides NAPI-RS based bindings for GlueSQL, offering both Node.js native modules and WebAssembly support.

## Migration from wasm-bindgen

This package replaces the previous `wasm-bindgen` based JavaScript bindings with modern NAPI-RS v3 bindings that provide:

- Better performance through native Node.js addons
- WebAssembly support via NAPI-RS v3's WASM target
- Improved async/await support  
- Better TypeScript definitions
- Unified API across Node.js and WASM environments

## Installation

```bash
npm install gluesql-napi
```

## Usage

### Node.js (Native)

```javascript
const { Glue } = require('gluesql-napi');

async function example() {
  const db = new Glue();
  
  const result = await db.query(`
    CREATE TABLE users (id INTEGER, name TEXT);
    INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
    SELECT * FROM users;
  `);
  
  console.log(result);
}

example();
```

### Browser (WebAssembly)

```javascript
import { Glue } from 'gluesql-napi/browser';

async function example() {
  const db = new Glue();
  
  const result = await db.query(`
    CREATE TABLE users (id INTEGER, name TEXT);
    INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
    SELECT * FROM users;
  `);
  
  console.log(result);
}

example();
```

## API

### Class: Glue

#### `new Glue()`
Creates a new GlueSQL instance with memory storage.

#### `glue.query(sql: string): Promise<any>`
Executes SQL queries and returns results as a JSON array.

#### `glue.setDefaultEngine(engine: string): void` *(Node.js only)*
Sets the default storage engine. Currently supports:
- `"memory"` - In-memory storage

## Building from Source

### Prerequisites
- Node.js 16+
- Rust toolchain
- NAPI-RS CLI: `npm install -g @napi-rs/cli`

### Build Commands

```bash
# Build native addon
npm run build

# Build for WebAssembly
npm run build:wasm

# Development build
npm run build:debug
```

## Features

- ✅ SQL query execution
- ✅ Memory storage backend
- ✅ Async/await support
- ✅ TypeScript definitions
- ✅ Node.js native addon
- ✅ WebAssembly support
- 🚧 IndexedDB storage (planned)
- 🚧 Web Storage API (planned)

## Performance

NAPI-RS bindings provide significant performance improvements over wasm-bindgen:
- Direct native function calls (no JS/WASM boundary overhead)
- Better memory management
- Native async/await without polyfills
- Optimized serialization/deserialization

## Compatibility

- **Node.js**: 16.0.0+ (native addon)
- **Browsers**: Modern browsers with WebAssembly support
- **Targets**: x86_64, ARM64 on Windows, macOS, Linux + wasm32-wasi

## Migration Guide

### From wasm-bindgen JavaScript package

```javascript
// Before (wasm-bindgen)
import { gluesql } from 'gluesql';
const db = await gluesql();

// After (NAPI-RS)
import { Glue } from 'gluesql-napi';
const db = new Glue();
```

The query API remains the same:
```javascript
const result = await db.query('SELECT * FROM table');
```

## License

Apache-2.0
