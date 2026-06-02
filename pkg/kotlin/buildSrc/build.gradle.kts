plugins {
    `kotlin-dsl`
    id("org.jlleitschuh.gradle.ktlint") version "12.1.0"
}

repositories {
    mavenCentral()
}

ktlint {
    version.set("1.0.1")
}
