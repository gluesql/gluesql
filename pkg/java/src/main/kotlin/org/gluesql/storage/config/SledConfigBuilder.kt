package org.gluesql.storage.config

import org.gluesql.uniffi.Mode
import org.gluesql.uniffi.SledConfig

/**
 * Builder class for creating SledConfig instances with Java-friendly syntax.
 *
 * This builder provides a fluent API that Java developers can use to create
 * SledConfig instances step by step with method chaining.
 *
 * ## Usage Examples
 *
 * ```java
 * // Simple usage with defaults
 * SledConfig config = SledConfigBuilder.create("/path/to/database").build();
 *
 * // Advanced usage with custom settings
 * SledConfig config = SledConfigBuilder.create("/path/to/database")
 *     .cacheCapacity(4096L)
 *     .mode(Mode.HIGH_THROUGHPUT)
 *     .createNew(true)
 *     .useCompression(true)
 *     .compressionFactor(9)
 *     .build();
 * ```
 */
class SledConfigBuilder private constructor(private val path: String) {

    private var cacheCapacity: Long = 1024L * 1024L * 1024L // 1gb
    private var mode: Mode = Mode.LOW_SPACE
    private var createNew: Boolean = false
    private var temporary: Boolean = false
    private var useCompression: Boolean = false
    private var compressionFactor: Int = 5
    private var printProfileOnDrop: Boolean = false

    companion object {
        @JvmStatic
        fun create(path: String): SledConfigBuilder = SledConfigBuilder(path)
    }

    fun cacheCapacity(capacity: Long): SledConfigBuilder {
        this.cacheCapacity = capacity
        return this
    }

    fun mode(mode: Mode): SledConfigBuilder {
        this.mode = mode
        return this
    }

    fun createNew(createNew: Boolean): SledConfigBuilder {
        this.createNew = createNew
        return this
    }

    fun temporary(temporary: Boolean): SledConfigBuilder {
        this.temporary = temporary
        return this
    }

    fun useCompression(useCompression: Boolean): SledConfigBuilder {
        this.useCompression = useCompression
        return this
    }

    fun compressionFactor(factor: Int): SledConfigBuilder {
        this.compressionFactor = factor
        return this
    }

    fun printProfileOnDrop(print: Boolean): SledConfigBuilder {
        this.printProfileOnDrop = print
        return this
    }

    fun build() = SledConfig(
        path,
        cacheCapacity,
        mode,
        createNew,
        temporary,
        useCompression,
        compressionFactor,
        printProfileOnDrop
    )
}
