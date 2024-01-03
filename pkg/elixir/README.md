# GlueSQL (Elixir Binding)

<!-- MDOC -->

GlueSQL is a SQL database library for Elixir.  
This repository is an Elixir binding of the original Rust library [`gluesql-rs`](https://github.com/gluesql/gluesql).

## Just another SQL database library?

### Various Storage Engines

GlueSQL supports various storages(pre-existing databases, files, etc), already implemented in `gluesql-rs`.

The list of supported storages are:

- [x] In-Memory
- [ ] Redis
- [ ] MongoDB Storage
- [ ] JSON file
- [ ] CSV file
- [ ] IDB
- [ ] [`sled`](https://docs.rs/sled/latest/sled/), a high-performant database written with Rust.

...and much more will be supported in future.  
(Unchecked items are yet bound to GlueSQL Elixir library).

### With-or-without schema

Unlike most of the SQL databases, GlueSQL supports not only schema-enabled queries but also schema-less queries.

It's better understood by reading the example below.

## Example

```elixir
alias GlueSQL.Storages.MemoryStorage
alias GlueSQL

# Create SQL database with memory storage
db =
  MemoryStorage.new()
  |> GlueSQL.glue_new()

# Create table with schema
GlueSQL.query(db, "CREATE TABLE users (id INTEGER, name STRING);")

# Insert values
GlueSQL.query(db, """
        INSERT INTO users VALUES
        (1, "Hoon Wee"),
        (2, "Eunbee Cho");
        """)

# We can also create table WITHOUT schema
GlueSQL.query(db, "CREATE TABLE friends;")

# Insert values
GlueSQL.query(db, """
        INSERT INTO friends VALUES
        ('{ "name": "Hoon Wee" }'),
        ('{ "username": "Eunbee Cho", "age": 12 }');
        """)
```

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `gluesql` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:gluesql, "~> 0.15.0"}
  ]
end

```
