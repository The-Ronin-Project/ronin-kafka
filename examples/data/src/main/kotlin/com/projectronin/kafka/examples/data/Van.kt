package com.projectronin.kafka.examples.data

import java.time.Instant

data class Van(
    val id: String,
    val make: String,
    val model: String,
    val style: Style,
    val created: Instant = Instant.now(),
) {
    enum class Style {
        CAMPER, CREEPER, DELIVERY
    }
}
