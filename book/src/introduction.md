# Introduction

[![crates.io](https://img.shields.io/crates/v/gluesql.svg)](https://crates.io/crates/gluesql)
[![docs.rs](https://docs.rs/gluesql/badge.svg)](https://docs.rs/gluesql)
[![LICENSE](https://img.shields.io/crates/l/gluesql.svg)](https://github.com/gluesql/gluesql/blob/main/LICENSE)
[![Rust](https://github.com/gluesql/gluesql/workflows/Rust/badge.svg)](https://www.rust-lang.org/)
[![Chat](https://img.shields.io/discord/780298017940176946)](https://discord.gg/C6TDEgzDzY)

## SQL Database Engine as a Library

GlueSQL is a SQL database library written in Rust which provides parser ([sqlparser-rs](https://github.com/ballista-compute/sqlparser-rs)), execution layer, and an optional storage ([sled](https://github.com/spacejam/sled)).  
Developers can use GlueSQL to build their own SQL databases or they can simply use GlueSQL as an embedded SQL database using default storage.

## Usecases

### Standalone Mode

You can simply use GlueSQL as an embedded SQL database, GlueSQL provides [sled](https://github.com/spacejam/sled "sled") as a default storage engine.

#### Storages

GlueSQL provides options for different storages.

### Use SQL in web browsers!

[GlueSQL-js](https://github.com/gluesql/gluesql-js)  
Use SQL in web browsers!  
GlueSQL-js provides 3 storage options,

- in-memory
- localStorage
- sessionStorage
