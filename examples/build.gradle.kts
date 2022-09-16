plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.pretty.jupiter)
    alias(libs.plugins.ktlint)
}

dependencies {
    implementation(project(":lib"))

    implementation(libs.kotlin.stdlib)
    implementation(libs.microutils.kotlin.logging)
    implementation(libs.jackson)
    implementation(libs.kafka)

    runtimeOnly(libs.logstash.logback)
    runtimeOnly(libs.logback.classic)
}
