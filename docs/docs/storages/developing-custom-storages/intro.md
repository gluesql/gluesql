---
sidebar_position: 1
---

# Introduction

With GlueSQL, you can adapt SQL and the Query Builder to a wide variety of environments. This includes file systems, key-value databases, complex NoSQL databases, and even remote APIs. As long as a system supports reading, it can support SELECT queries. If it supports both reading and writing, it can support most SQL operations, including UPDATE and DELETE.

Custom storage development has two main parts:

1. Understanding Store traits
2. Using the Test Suite

These topics are covered in more detail in their respective pages, but here we will provide a brief overview.

## Understanding Store Traits

A Rust storage implements these traits:

* `Store` - Reads schemas and rows.
* `StoreMut` - Writes schemas and rows.
* `AlterTable` - Changes table schemas.
* `Transaction` - Begins, commits, and rolls back transactions.
* `CustomFunction` - Reads user-defined functions.
* `CustomFunctionMut` - Creates and deletes user-defined functions.
* `Index` - Reads rows through secondary indexes.
* `IndexMut` - Creates and drops secondary indexes.
* `Metadata` - Returns table metadata.
* `Planner` - Plans queries.

`Store` requires four read methods. For the other traits, start with empty implementations and override the methods your storage needs. For example, `impl Planner for MyStorage {}` uses GlueSQL's default planner. Each trait's page describes its defaults.

## Using the Test Suite

The [Test Suite](using-test-suite.md) provides shared tests for the SQL features your storage supports. You can add tests for behavior specific to your implementation, such as persistence across restarts or transaction isolation.
