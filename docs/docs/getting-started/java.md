---
sidebar_position: 4
---

# Java

This guide will help you get started with GlueSQL in a Java project. First, add the `gluesql-java` dependency to your project.

## Installation

### Gradle

Add the following dependency to your `build.gradle.kts` file:

```kotlin
dependencies {
    implementation("org.gluesql:gluesql:0.1.0")
}
```

### Maven

Add the following dependency to your `pom.xml` file:

```xml
<dependency>
    <groupId>org.gluesql</groupId>
    <artifactId>gluesql</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Usage

Here's a simple example of using GlueSQL in a Java application:

```java
import org.gluesql.client.java.GlueSQLClient;
import org.gluesql.storage.StorageFactory;
import org.gluesql.uniffi.QueryResult;

import java.util.List;

public class Example {
    public static void main(String[] args) throws Exception {
        // Create GlueSQL client with memory storage
        GlueSQLClient client = new GlueSQLClient(StorageFactory.memory());
        
        // Create table and insert data
        client.query("CREATE TABLE User (id INTEGER, name TEXT)");
        client.query("INSERT INTO User VALUES (1, 'glue'), (2, 'sql')");
        
        // Query data
        List<QueryResult> results = client.query("SELECT * FROM User");
        QueryResult.Select selectResult = (QueryResult.Select) results.get(0);
        
        System.out.println("Columns: " + selectResult.getResult().getLabels());
        System.out.println("Rows: " + selectResult.getResult().getRows());
    }
}
```

## Storage Types

GlueSQL supports multiple storage backends:

### Memory Storage
```java
GlueSQLClient client = new GlueSQLClient(StorageFactory.memory());
```

### JSON File Storage
```java
GlueSQLClient client = new GlueSQLClient(StorageFactory.json("/path/to/database.json"));
```

### Sled Storage
```java
import org.gluesql.storage.config.SledConfigBuilder;

SledConfig config = SledConfigBuilder.create("/path/to/database").build();
GlueSQLClient client = new GlueSQLClient(StorageFactory.sled(config));
```

### Shared Memory Storage
```java
GlueSQLClient client = new GlueSQLClient(StorageFactory.sharedMemory());
```

## Async Operations

For better performance, use asynchronous query execution:

```java
import java.util.concurrent.CompletableFuture;

CompletableFuture<List<QueryResult>> future = client.queryAsync("SELECT * FROM User");
future.thenAccept(results -> {
    // Process results
    System.out.println("Query completed: " + results);
});
```

## Error Handling

Handle database errors with try-catch blocks:

```java
import org.gluesql.uniffi.GlueSqlException;

try {
    client.query("SELECT * FROM non_existent_table");
} catch (GlueSqlException e) {
    System.err.println("Database error: " + e.getMessage());
}
```

This example demonstrates the basic usage of GlueSQL in Java applications with table creation, data insertion, and querying.