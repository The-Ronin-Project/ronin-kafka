package com.projectronin.kafka.data

import java.time.Instant
import java.util.*

data class RoninEvent<ID, DATA : RoninEvent.Data<ID>>(
    val id: String = UUID.randomUUID().toString(),
    val specVersion: String = "1.0",
    val dataContentType: String = "application/json",
    val source: String,
    val type: String,
    val dataSchema: String,
    val time: Instant = Instant.now(),
    val data: DATA,
) {
    val subject: String = "$type/${data.id}"

    interface Data<ID> {
        val id: ID
    }
}
