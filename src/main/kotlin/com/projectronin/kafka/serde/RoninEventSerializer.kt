package com.projectronin.kafka.serde

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.StringHeader
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Serializer
import java.lang.Exception
import java.time.format.DateTimeFormatter

class RoninEventSerializer<T> : Serializer<RoninEvent<T>> {
    private val mapper: ObjectMapper = MapperFactory.mapper
    private val instantFormatter = DateTimeFormatter.ISO_INSTANT

    override fun serialize(topic: String?, data: RoninEvent<T>?): ByteArray {
        throw Exception()
    }

    override fun serialize(topic: String?, headers: Headers?, event: RoninEvent<T>?): ByteArray? {
        if (headers == null)
            throw Exception("Headers be null")
        if (event == null)
            throw Exception("Event be null")

        if (event.data == null)
            return null

        headers.add(StringHeader(KafkaHeaders.id, event.id))
        headers.add(StringHeader(KafkaHeaders.source, event.source))
        headers.add(StringHeader(KafkaHeaders.specVersion, event.specVersion))
        headers.add(StringHeader(KafkaHeaders.type, event.type))
        headers.add(StringHeader(KafkaHeaders.contentType, event.dataContentType))
        headers.add(StringHeader(KafkaHeaders.dataSchema, event.dataSchema))
        headers.add(StringHeader(KafkaHeaders.time, instantFormatter.format(event.time)))

        if (event.subject != null)
            headers.add(StringHeader(KafkaHeaders.subject, event.subject))

        return mapper.writeValueAsBytes(event.data)
    }
}
