plugins {
    kotlin("jvm") version "1.9.20"
    `java-library`
    `maven-publish`
}

group = "org.gluesql"
version = "0.1.0"

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

tasks.test {
    useJUnitPlatform()
    dependsOn(buildRustLib)
    systemProperty("jna.library.path", "${projectDir}/../../target/debug")
}


val buildRustLib = tasks.register("buildRustLib") {
    group = "build"
    description = "Build the Rust library"
    
    doLast {
        exec {
            workingDir = projectDir
            commandLine("cargo", "build")
        }
    }
}

val generateBindings = tasks.register("generateBindings") {
    group = "build"
    description = "Generate UniFFI bindings"
    dependsOn(buildRustLib)
    
    val generatedDir = layout.buildDirectory.dir("generated/source/uniffi/kotlin").get().asFile
    
    outputs.dir(generatedDir)
    
    doFirst {
        generatedDir.mkdirs()
    }
    
    doLast {
        exec {
            workingDir = projectDir
            commandLine(
                "cargo", "run", "--bin", "uniffi-bindgen", "generate",
                "--language", "kotlin",
                "--out-dir", generatedDir.absolutePath,
                "--config", "uniffi.toml",
                "src/gluesql.udl"
            )
        }
    }
}

kotlin {
    jvmToolchain(11)
    
    sourceSets {
        main {
            kotlin.srcDir(layout.buildDirectory.dir("generated/source/uniffi/kotlin"))
        }
    }
}

tasks.compileKotlin {
    dependsOn(generateBindings)
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            
            pom {
                name.set("GlueSQL Java Bindings")
                description.set("Java/Kotlin bindings for GlueSQL")
                url.set("https://github.com/gluesql/gluesql")
                
                licenses {
                    license {
                        name.set("Apache License 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }

                developers {
                    developer {
                        id.set("junghoon-ban")
                        name.set("Junghoon Ban")
                        email.set("junghoon.ban@gmail.com")
                    }
                }

                scm {
                    connection.set("scm:git:git://github.com/gluesql/gluesql.git")
                    developerConnection.set("scm:git:ssh://github.com:gluesql/gluesql.git")
                    url.set("https://github.com/gluesql/gluesql")
                }
            }
        }
    }
}
