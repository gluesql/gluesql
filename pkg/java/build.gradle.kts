import org.gradle.internal.os.OperatingSystem

plugins {
    kotlin("jvm") version "1.9.20"
    `java-library`
    id("com.diffplug.spotless") version "6.25.0"
    id("com.vanniktech.maven.publish") version "0.28.0"
}

group = "org.gluesql"
version = "0.1.0"

base {
    archivesName.set("gluesql")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("net.java.dev.jna:jna:5.14.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    implementation("com.google.code.gson:gson:2.10.1")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
}

// Platform configuration for distribution builds
data class PlatformConfig(
    val rustTarget: String,
    val jnaPrefix: String,
    val libExtension: String
) {
    val libNameWithoutPrefix: String
        get() = "gluesql_java.$libExtension"

    val libNameWithPrefix: String
        get() = "libgluesql_java.$libExtension"

    // Windows doesn't use 'lib' prefix
    val libFileName: String
        get() = if (jnaPrefix.startsWith("win32")) libNameWithoutPrefix else libNameWithPrefix
}

val platforms = listOf(
    PlatformConfig("x86_64-unknown-linux-gnu", "linux-x86-64", "so"),
    PlatformConfig("x86_64-apple-darwin", "darwin-x86-64", "dylib"),
    PlatformConfig("aarch64-apple-darwin", "darwin-aarch64", "dylib"),
    PlatformConfig("x86_64-pc-windows-msvc", "win32-x86-64", "dll")
)

// Helper to get current platform's library extension
fun currentPlatformLibName(): String = when {
    OperatingSystem.current().isWindows -> "gluesql_java"
    else -> "libgluesql_java"
}
fun currentPlatformLibExtension(): String = when {
    OperatingSystem.current().isLinux -> "so"
    OperatingSystem.current().isMacOsX -> "dylib"
    OperatingSystem.current().isWindows -> "dll"
    else -> throw GradleException("Unsupported operating system: ${OperatingSystem.current()}")
}

// Build Rust library for local platform (for development and testing)
val buildRustLib = tasks.register("buildRustLib") {
    group = "build"
    description = "Build the Rust library for local platform (debug mode)"

    val debugLibPath = file("../../target/debug/${currentPlatformLibName()}.${currentPlatformLibExtension()}")
    outputs.file(debugLibPath)

    // Only rebuild if source files changed
    inputs.files(fileTree("src").include("**/*.rs"))
    inputs.file("Cargo.toml")

    doLast {
        exec {
            workingDir = projectDir
            commandLine("cargo", "build")
        }
    }
}

// Generate UniFFI bindings from Rust library
val generateBindings = tasks.register("generateBindings") {
    group = "build"
    description = "Generate UniFFI Kotlin bindings from Rust library"
    dependsOn(buildRustLib)

    val generatedDir = layout.buildDirectory.dir("generated/source/uniffi/kotlin").get().asFile
    val debugLibPath = "../../target/debug/libgluesql_java.${currentPlatformLibExtension()}"

    inputs.file(debugLibPath)
    inputs.files(fileTree("src").include("**/*.rs", "**/*.udl"))
    outputs.dir(generatedDir)

    doLast {
        generatedDir.mkdirs()

        exec {
            workingDir = projectDir
            commandLine(
                "cargo", "run", "--bin", "uniffi-bindgen", "generate",
                "--language", "kotlin",
                "--out-dir", generatedDir.absolutePath,
                "--library", debugLibPath,
                "--no-format"
            )
        }
    }
}

// Copy native libraries from CI artifacts or local builds to resources
// This task is primarily used in CI, but can be run locally if you have built all platforms
val copyNativeLibs = tasks.register("copyNativeLibs") {
    group = "build"
    description = "Copy pre-built native libraries to resources (for distribution)"

    val resourcesDir = file("src/main/resources/natives")

    doLast {
        // Clean and recreate natives directory
        delete(resourcesDir)
        resourcesDir.mkdirs()

        var copiedCount = 0
        platforms.forEach { platform ->
            val sourceFile = file("../../target/${platform.rustTarget}/release/${platform.libFileName}")
            val destDir = resourcesDir.resolve(platform.jnaPrefix)

            if (sourceFile.exists()) {
                copy {
                    from(sourceFile)
                    into(destDir)
                }
                copiedCount++
                logger.lifecycle("✓ Copied ${platform.jnaPrefix}/${platform.libFileName}")
            } else {
                logger.warn("⚠ Native library not found: ${sourceFile.absolutePath}")
                logger.warn("  This is expected if you haven't built for ${platform.rustTarget}")
            }
        }

        if (copiedCount == 0) {
            logger.warn("⚠ No native libraries were copied!")
            logger.warn("  Run 'cargo build --release --target <TARGET>' for each platform first,")
            logger.warn("  or let CI build them for you.")
        } else {
            logger.lifecycle("✓ Copied $copiedCount/${platforms.size} platform libraries")
        }
    }
}

// Clean native libraries from resources
val cleanNativeLibs = tasks.register("cleanNativeLibs") {
    group = "build"
    description = "Remove native libraries from resources"

    doLast {
        delete("src/main/resources/natives")
        logger.lifecycle("✓ Cleaned native libraries from resources")
    }
}

// Configure source sets
kotlin {
    jvmToolchain(11)

    sourceSets {
        main {
            kotlin.srcDir(layout.buildDirectory.dir("generated/source/uniffi/kotlin"))
        }
    }
}

// Task dependencies
tasks.compileKotlin {
    dependsOn(generateBindings)
}

// Ensure sources JAR includes generated bindings
tasks.named("kotlinSourcesJar") {
    dependsOn(generateBindings)
}

tasks.test {
    useJUnitPlatform()
    dependsOn(buildRustLib)
    // For testing, load from debug build directory
    systemProperty("jna.library.path", file("../../target/debug").absolutePath)
}

tasks.clean {
    dependsOn(cleanNativeLibs)
}

// Code formatting
spotless {
    java {
        target("src/**/*.java")
        googleJavaFormat()
        trimTrailingWhitespace()
        endWithNewline()
    }

    kotlin {
        target("src/**/*.kt")
        ktlint()
        trimTrailingWhitespace()
        endWithNewline()
    }
}

// Maven Central publishing configuration using vanniktech plugin
mavenPublishing {
    publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)

    coordinates(group.toString(), "gluesql", version.toString())

    pom {
        name.set("GlueSQL Java Bindings")
        description.set("Java/Kotlin bindings for GlueSQL - SQL database engine as a library")
        inceptionYear.set("2024")
        url.set("https://github.com/gluesql/gluesql")

        licenses {
            license {
                name.set("Apache License 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0")
                distribution.set("https://www.apache.org/licenses/LICENSE-2.0")
            }
        }

        developers {
            developer {
                id.set("junghoon-ban")
                name.set("Junghoon Ban")
                email.set("junghoon.ban@gmail.com")
                url.set("https://github.com/gluesql/gluesql")
            }
        }

        scm {
            url.set("https://github.com/gluesql/gluesql")
            connection.set("scm:git:git://github.com/gluesql/gluesql.git")
            developerConnection.set("scm:git:ssh://git@github.com/gluesql/gluesql.git")
        }
    }
}
