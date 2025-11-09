package org.gluesql.storage

import org.gluesql.uniffi.SledConfig
import org.gluesql.uniffi.Storage

/**
 * Factory class for creating Storage instances with Java-friendly syntax.
 *
 * This class provides static methods that Java developers can use to create
 * storage instances without dealing with Kotlin-specific syntax.
 *
 * ## Usage Examples
 *
 * ```java
 * // Memory storage
 * Storage storage = StorageFactory.memory();
 *
 * // JSON file storage
 * Storage storage = StorageFactory.json("/path/to/database.json");
 *
 * // Sled storage (simple)
 * Storage storage = StorageFactory.sled("/path/to/database");
 *
 * // Sled storage (advanced with builder)
 * SledConfig config = SledConfigBuilder.create("/path/to/database")
 *     .cacheCapacity(4096L)
 *     .mode(Mode.HIGH_THROUGHPUT)
 *     .useCompression(true)
 *     .build();
 * Storage storage = StorageFactory.sled(config);
 *
 * // Shared memory storage
 * Storage storage = StorageFactory.sharedMemory();
 * ```
 */
class StorageFactory {
    companion object {
        @JvmStatic
        fun memory(): Storage = Storage.Memory

        @JvmStatic
        fun json(path: String): Storage = Storage.Json(path)

        @JvmStatic
        fun sled(config: SledConfig): Storage = Storage.Sled(config)

        @JvmStatic
        fun sharedMemory(): Storage = Storage.SharedMemory
    }
}
