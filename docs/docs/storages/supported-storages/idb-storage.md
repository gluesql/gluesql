---
sidebar_position: 6
---

# IndexedDB Storage

## Introduction

IndexedDB, now easily handled just like SQL with GlueSQL - this is truly magical! For those who have directly used IndexedDB before, it's well known that it's not the most intuitive type of database to interact with. Even the [MDN IndexedDB introduction page](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API) acknowledges this complexity, stating:

> Note: IndexedDB API is powerful, but may seem too complicated for simple cases. If you'd prefer a simple API, try libraries in See also section that make IndexedDB more programmer-friendly.

In particular, version management in IndexedDB might be a somewhat unfamiliar concept for regular database users. But worry not, GlueSQL has innovatively handled these intricacies, freeing you from the need to grapple with such complexities. You can just use SQL, and everything will work as expected.

## Behind the Scenes

When there are schema changes, like a CREATE TABLE query, GlueSQL increases the IndexedDB version and handles it internally. The data to be stored is also converted into a JSON format for storage. Thanks to this, you can easily check how GlueSQL is handling data by using the IndexedDB viewer in the web browser's developer console.

## Compatibility and Use

Currently, only the `Store` and `StoreMut` traits are implemented and supported. You can use it in both JavaScript (Web) and Rust WebAssembly environments.

When using it in a web environment, just set the ENGINE to indexedDB in the CREATE TABLE query.

```sql
CREATE TABLE Item (id INTEGER, name TEXT) ENGINE = indexedDB;
```

Just by using it like this, GlueSQL will operate based on the IndexedDB storage.

## Summary

In conclusion, if you are seeking for a way to interact with IndexedDB without the usual complexity, GlueSQL is a fantastic choice. It provides a clear, SQL-based approach to managing data, making IndexedDB much more accessible and user-friendly.
