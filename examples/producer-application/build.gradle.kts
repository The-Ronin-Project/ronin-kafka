
@Suppress("DSL_SCOPE_VIOLATION")
plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.pretty.jupiter)
    alias(libs.plugins.ktlint)
    application
}

dependencies {
    implementation(libs.kotlin.stdlib)
    implementation(rootProject)
    implementation(project(":examples:data"))

    implementation(libs.kotlin.stdlib)
    implementation(libs.microutils.kotlin.logging)

    runtimeOnly(libs.logstash.logback)
    runtimeOnly(libs.logback.classic)
}

application {
    mainClass.set("MainKt")
}
