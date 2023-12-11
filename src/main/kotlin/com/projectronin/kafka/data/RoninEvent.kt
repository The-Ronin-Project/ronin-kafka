package com.projectronin.kafka.data

import java.time.Instant
import java.util.UUID

typealias Subject = String

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
    val data: T,
    @Deprecated("Subject is being replaced by 'resourceType' and 'resourceId'")
    private val subject: Subject? = null,
    val tenantId: String? = null,
    val patientId: String? = null,
    val resourceType: String? = subject?.resourceType(),
    val resourceId: String? = subject?.resourceId(),
    val resourceVersion: Int? = null
) {
    @Deprecated("Will be moving to private as headers concept should not be exposed")
    constructor(headers: Map<String, String>, data: T) : this(
        id = headers[KafkaHeaders.id] ?: defaultId(),
        time = Instant.parse(headers[KafkaHeaders.time]) ?: defaultTime(),
        specVersion = headers[KafkaHeaders.specVersion] ?: DEFAULT_VERSION,
        dataSchema = headers[KafkaHeaders.dataSchema]!!,
        type = headers[KafkaHeaders.type]!!,
        source = headers[KafkaHeaders.source]!!,
        dataContentType = headers[KafkaHeaders.contentType] ?: DEFAULT_CONTENT_TYPE,
        subject = headers[KafkaHeaders.subject],
        tenantId = headers[KafkaHeaders.tenantId],
        patientId = headers[KafkaHeaders.patientId],
        resourceType = (headers[KafkaHeaders.subject] as Subject).resourceType(),
        resourceId = (headers[KafkaHeaders.subject] as Subject).resourceId(),
        resourceVersion = headers[KafkaHeaders.resourceVersion]?.toIntOrNull(),
        data = data
    )

    fun getSubject(): Subject? {
        return subject ?: buildSubject(resourceType, resourceId)
    }

    companion object {
        private fun defaultId() = UUID.randomUUID().toString()
        private fun defaultTime() = Instant.now()
        private const val DEFAULT_VERSION = "1.0"
        private const val DEFAULT_CONTENT_TYPE = "application/json"
    }
}

private fun buildSubject(resourceType: String?, resourceId: String?): Subject? {
    return when {
        resourceType == null || resourceId == null -> null
        else -> "$resourceType/$resourceId"
    }
}

fun Subject.resourceType(): String? {
    val strings = this.split("/")
    return when (strings.size) {
        2 -> strings[0]
        else -> null
    }
}

fun Subject.resourceId(): String? {
    val strings = this.split("/")
    return when (strings.size) {
        2 -> strings[1]
        else -> null
    }
}
