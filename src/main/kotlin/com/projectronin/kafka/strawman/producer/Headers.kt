package com.projectronin.kafka.strawman.producer

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.StringHeader
import org.apache.kafka.common.serialization.Serializer
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.reflect.KClass

fun  interface HeadersExtractor<T> {
    fun determineHeaders(value: T): RoninEventHeaders
}

// TODO: this should probably be a different interface for each header instead of 1, e.g. IdHeaderExtract.determineId
fun  interface HeaderExtractor<H, T> {
    fun determineHeader(event: T): H
}

data class RoninEventHeaders(
    val id: String,
    val time: Instant,
    val specVersion: String,
    val dataSchema: String,
    val dataContentType: String,
    val source: String,
    val type: String,
    val subject: String,
) {
    companion object {
        fun fromEvent(event: RoninEvent<*>) = RoninEventHeaders(
            event.id, event.time, event.specVersion, event.dataSchema, event.dataContentType, event.source, event.type, event.subject
        )
    }

    fun toHeaders() = listOf(
        StringHeader(KafkaHeaders.id, id),
        StringHeader(KafkaHeaders.source, source),
        StringHeader(KafkaHeaders.specVersion, specVersion),
        StringHeader(KafkaHeaders.type, type),
        StringHeader(KafkaHeaders.contentType, dataContentType),
        StringHeader(KafkaHeaders.dataSchema, dataSchema),
        StringHeader(KafkaHeaders.time, DateTimeFormatter.ISO_INSTANT.format(time)),
    )
}

class UUIDExtractor<E> : HeaderExtractor<String, E> {
    override fun determineHeader(event: E): String = UUID.randomUUID().toString()
}

class StaticValueExtractor<T, E>(private val value: T) : HeaderExtractor<T, E> {
    override fun determineHeader(event: E): T = value
}

class CurrentTimeExtractor<T> : HeaderExtractor<Instant, T> {
    override fun determineHeader(event: T): Instant = Instant.now()
}

class RoninKafkaSerializerFactory {
    fun <T : Any> forType(type: KClass<T>): Serializer<T> = TODO()
}

class RoninKafkaSerializer<T>(
    private var objectMapper: ObjectMapper = MapperFactory.mapper
) : Serializer<T> {
    override fun serialize(topic: String?, data: T): ByteArray = objectMapper.writeValueAsBytes(data)
}

fun interface KeyExtractor<T> {
    fun determineKey(event: T): String?
}

class RoninEventHeaderExtractor : HeadersExtractor<RoninEvent<*>> {
    override fun determineHeaders(event: RoninEvent<*>) = RoninEventHeaders.fromEvent(event)
}

// TODO: this feels like it's getting excessive... maybe fewer levels of indirection would be better even if less flexible
class DelegatingEventHeadersExtractor<T>(
    private val idExtractor: HeaderExtractor<String, T>,
    private val typeExtractor: HeaderExtractor<String, T>,
    private val schemaExtractor: HeaderExtractor<String, T>,
    private val sourceExtractor: HeaderExtractor<String, T>,
    private val timeExtractor: HeaderExtractor<Instant, T>,
    private val subjectExtractor: HeaderExtractor<String, T>,
) : HeadersExtractor<T> {
    override fun determineHeaders(event: T) = RoninEventHeaders(
        id = idExtractor.determineHeader(event),
        time = timeExtractor.determineHeader(event),
        specVersion = "1.0",
        // TODO: hardcoding this is wrong
        dataContentType = "application/json",
        source = sourceExtractor.determineHeader(event),
        type = typeExtractor.determineHeader(event),
        dataSchema = schemaExtractor.determineHeader(event),
        subject = subjectExtractor.determineHeader(event)
    )
}
