plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.pretty.jupiter)
}

dependencies {
    implementation(project(":lib"))

    implementation(libs.kotlin.stdlib)
    implementation(libs.microutils.kotlin.logging)
    implementation(libs.jackson)

    runtimeOnly(libs.logstash.logback)
    runtimeOnly(libs.logback.classic)
}
