# Composite Storage

## Introduction

CompositeStorage is a special type of Storage. It does not possess any real storage implementation itself. Instead, it offers a powerful capability - the ability to bundle together multiple existing storages. For instance, with CompositeStorage, you can bundle JsonStorage and SledStorage together for simultaneous use. This means you can perform JOIN operations across two distinct storages. You can even read and analyze JSON log files, and directly insert the data into SledStorage.

## Unified Interface across Different Storages

CompositeStorage lets you create tables backed by different storage implementations and query them through the same SQL interface. You can perform operations like JOIN across those tables without changing the query language for each backing store.

## Working across Storage Types

CompositeStorage can be immensely useful in applications that need to work across multiple storage backends. It does not hard-code a fixed list of allowed backends. Instead, it combines storages registered through `push` when they implement the required GlueSQL storage traits. This includes bundled storages such as JsonStorage for JSON and JSONL files, CSV and Parquet file storages, and service-backed storages like Redis and MongoDB.

In addition, just as you would use an ORM to handle multiple different SQL databases with the same interface, plans are in place to use existing SQL databases in a similar way as storages in GlueSQL. Once all these plans come to fruition, you will be able to implement your data pipelines very simply.

Moving data from Redis to MongoDB, or from MySQL to Redis will be a breeze - just by specifying the ENGINE using the same GlueSQL SQL or Query Builder.

## Limitations and Considerations

CompositeStorage might sound like a cure-all solution, but it does have its limitations. As it combines different data storages, certain boundaries exist. Transactions, for instance, are a major one. Each storage may have different transaction support and methods. Therefore, it is not advisable to use CompositeStorage for operations that require transactions. It's more suitable for moving or analyzing data across different storages.

## Summary

In conclusion, CompositeStorage is an exciting and powerful feature of GlueSQL, enabling users to combine and use different storage types seamlessly. However, users should also be aware of its limitations, such as transaction handling. Despite these constraints, the potential and flexibility offered by CompositeStorage make it a compelling choice for a variety of data manipulation tasks, especially when working with diverse storage types.
