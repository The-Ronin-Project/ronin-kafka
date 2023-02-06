package com.projectronin.kafka.data

import java.time.Instant
import java.util.UUID

/**
 * Implementation of the [Ronin Event Standard](https://projectronin.atlassian.net/wiki/spaces/ENG/pages/1748041738/Ronin+Event+Standard)
 */
data class RoninEvent<T>(
    val id: String = defaultId(),
    val time: Instant = defaultTime(),
    val specVersion: String = DEFAULT_VERSION,
    val dataSchema: String,
    val dataContentType: String = DEFAULT_CONTENT_TYPE,
    val source: String,
    val type: String,
    val subject: String?,
    val data: T,
) {
    constructor(headers: Map<String, String>, data: T) : this(
        id = headers[KafkaHeaders.id] ?: defaultId(),
        time = Instant.parse(headers[KafkaHeaders.time]) ?: defaultTime(),
        specVersion = headers[KafkaHeaders.specVersion] ?: DEFAULT_VERSION,
        dataSchema = headers[KafkaHeaders.dataSchema]!!,
        type = headers[KafkaHeaders.contentType]!!,
        source = headers[KafkaHeaders.source]!!,
        dataContentType = headers[KafkaHeaders.contentType] ?: DEFAULT_CONTENT_TYPE,
        subject = headers[KafkaHeaders.subject],
        data = data
    )

    companion object {
        private fun defaultId() = UUID.randomUUID().toString()
        private fun defaultTime(): Instant = Instant.now()
        private const val DEFAULT_VERSION = "1.0"
        private const val DEFAULT_CONTENT_TYPE = "application/json"
    }
}
