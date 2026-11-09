---
sidebar_position: 1
---

# Introduction

GlueSQL is not only suitable for use as a conventional database, but one of its key features is the ability for anyone to easily adapt SQL and the Query Builder to their desired file or storage system. This adaptability is achieved through the following topics covered in this section:

## Supported Storages

GlueSQL provides reference storages for in-memory data, embedded databases, local files, object stores, and external databases. [SlateDB Storage](supported-storages/slatedb-storage.md) uses object stores such as Amazon S3 and Cloudflare R2. See the [Rust getting-started guide](../getting-started/rust.md) for the full list of storage features.

## Developing Custom Storages

The [custom storage guide](developing-custom-storages/intro.md) explains the traits that connect your storage to SQL and the Query Builder, including the default implementations you can reuse.

Use the [shared test suite](developing-custom-storages/using-test-suite.md) to verify the SQL features your storage supports. Add storage-specific integration tests for behavior such as persistence, transaction isolation, and backend failures.

## Exploring the Storages Section

In the Storages section, you will find detailed information about the reference storages currently supported by GlueSQL, as well as guidelines for developing custom storages and what needs to be done to implement them.

Together, these resources make it easy to utilize and adapt GlueSQL for a variety of storage systems.
