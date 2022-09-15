package com.projectronin.kafka.data

import java.time.Instant
import java.util.*

data class RoninEvent<T: RoninEvent.Data<*>>(
    val id: String = UUID.randomUUID().toString(),
    val time: Instant = Instant.now(),
    val specVersion: String = "1.0",
    val dataSchema: String,
    val dataContentType: String = "application/json",
    val source: String,
    val type: String,
    val subject: String,
    val data: T,
) {
    interface Data<ID> {
        val id: ID
    }
}
