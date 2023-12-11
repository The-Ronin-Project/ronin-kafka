package com.projectronin.kafka.serde

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.StringHeader
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Serializer
import java.time.format.DateTimeFormatter

class RoninEventSerializer<T> : Serializer<RoninEvent<T>> {
    private val mapper: ObjectMapper = MapperFactory.mapper

    override fun serialize(topic: String?, data: RoninEvent<T>?): ByteArray {
        throw SerializationException(
            "Serialize method without headers is not a valid means to deserialize a RoninEvent"
        )
    }

    override fun serialize(topic: String?, headers: Headers?, event: RoninEvent<T>?): ByteArray? {
        if (headers == null)
            throw SerializationException(
                "Headers are required to deserialize a RoninEvent into, but the headers were not supplied."
            )

        if (event == null)
            return null

        headers.addRoninEventHeaders(event)
        return mapper.writeValueAsBytes(event.data)
    }
}

fun Headers.addRoninEventHeaders(event: RoninEvent<*>) {
    add(KafkaHeaders.id, event.id)
    add(KafkaHeaders.source, event.source)
    add(KafkaHeaders.specVersion, event.specVersion)
    add(KafkaHeaders.type, event.type)
    add(KafkaHeaders.contentType, event.dataContentType)
    add(KafkaHeaders.dataSchema, event.dataSchema)
    add(KafkaHeaders.time, DateTimeFormatter.ISO_INSTANT.format(event.time))
    addWhenNotNull(KafkaHeaders.tenantId, event.tenantId)
    addWhenNotNull(KafkaHeaders.patientId, event.patientId)
    addWhenNotNull(KafkaHeaders.subject, event.getSubject())
    addWhenNotNull(KafkaHeaders.resourceVersion, event.resourceVersion.toString())
}

fun Headers.add(header: String, value: String): Headers = add(StringHeader(header, value))
fun Headers.addWhenNotNull(header: String, value: String?) = value?.let { add(header, value) }
