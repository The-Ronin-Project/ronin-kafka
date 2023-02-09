package com.projectronin.kafka.data

import java.time.Instant
import java.util.UUID

/**
 * Implementation of the [Ronin Event Standard](https://projectronin.atlassian.net/wiki/spaces/ENG/pages/1748041738/Ronin+Event+Standard)
 */
data class RoninEvent<T>(
    val id: String = UUID.randomUUID().toString(),
    val time: Instant = Instant.now(),
    val specVersion: String = "1.0",
    val dataSchema: String,
    val dataContentType: String = "application/json",
    val source: String,
    val type: String,
    val data: T,
    val subject: String? = null,
)
