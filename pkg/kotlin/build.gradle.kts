plugins {
    kotlin("jvm") version "1.9.20"
    id("org.jlleitschuh.gradle.ktlint") version "12.1.0"
    id("com.vanniktech.maven.publish") version "0.28.0"
}

group = "org.gluesql"
version = "0.1.0"

base {
    archivesName.set("gluesql-kotlin")
}

repositories {
    mavenCentral()
}

dependencies {
    api("net.java.dev.jna:jna:5.14.0")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    implementation("com.google.code.gson:gson:2.10.1")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.7.3")
}

// Register native build tasks
registerNativeBuildTasks()

kotlin {
    jvmToolchain(11)

    sourceSets {
        main {
            kotlin.srcDir(layout.buildDirectory.dir("generated/source/uniffi/kotlin"))
        }
    }
}

tasks.compileKotlin {
    dependsOn("generateBindings")
}

tasks.named("kotlinSourcesJar") {
    dependsOn("generateBindings")
}

tasks.test {
    useJUnitPlatform()
    dependsOn("buildRustLib")
    // For testing, load from debug build directory
    val workspaceRoot = projectDir.parentFile.parentFile
    systemProperty("jna.library.path", workspaceRoot.resolve("target/debug").absolutePath)
}

ktlint {
    version.set("1.0.1")
    filter {
        exclude { it.file.path.contains("/generated") }
        exclude { it.file.path.contains("/build") }
    }
}

mavenPublishing {
    publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)

    coordinates(group.toString(), "gluesql-kotlin", version.toString())

    pom {
        name.set("GlueSQL Kotlin")
        description.set("Kotlin bindings for GlueSQL - SQL database engine as a library")
        inceptionYear.set("2025")
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
