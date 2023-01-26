package com.projectronin.kafka.strawman

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.config.RoninProducerKafkaProperties
import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.StringHeader
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.Future
import kotlin.reflect.KClass

class RoninKafkaProducerFactory(
    private val config: RoninProducerKafkaProperties,
    // TODO: can't decide if this should take a serializer factory, or if the consumer should have to provider a serializer
    // or if it should just default to a WhateverSerializer instance or what. A lot more complicated on the consumer side
    // where consumer deserializers require config and can't just handle every type.
    private val serializerFactory: RoninKafkaSerializerFactory,
) {
    /**
     * Returns a producer that publishes [RoninEvent] instances of a single type to a single topic. Note that [T],
     * *must not* include RoninType.
     *
     * Example:
     *
     * val producer = createEventProducer("emr.patient.v1", PatientEventV1.class)
     * val event = RoninEvent(..., data = PatientEventV1(id = "123", name = "..."))
     * producer.send(event)
     */
    // TODO convenience method with reified generic
    fun <T : Any> createRoninEventProducer(topic: String, type: KClass<T>): RoninEventProducer<T> =
        RoninEventProducer<T>(topic, KafkaProducer<String, T>(config.properties, StringSerializer(), serializerFactory.forType(type)))

    fun <T : Any> createRoninProducer(
        topic: String,
        type: KClass<T>,
        keyExtractor: KeyExtractor<T>,
        headersExtractor: HeadersExtractor<T>
    ) = RoninProducer(
        topic = topic,
        producer = KafkaProducer<String, T>(config.properties, StringSerializer(), serializerFactory.forType(type)),
        keyExtractor = keyExtractor,
        headersExtractor = headersExtractor,
    )

    /**
     * Creates a producer that knows how to wrap events in a RoninEvent envelope automatically, which lets you send a
     * plain payload instead of a RoninEvent.
     */
    fun <T : Any> createRoninProducer(
        topic: String,
        type: KClass<T>,
        keyExtractor: KeyExtractor<T>,
        typeExtractor: HeaderExtractor<String, T>,
        sourceExtractor: HeaderExtractor<String, T>,
        schemaExtractor: HeaderExtractor<String, T>,
        subjectExtractor: HeaderExtractor<String, T> = StaticValueExtractor(""),
        idExtractor: HeaderExtractor<String, T> = UUIDExtractor(),
        timeExtractor: HeaderExtractor<Instant, T> = CurrentTimeExtractor(),
    ): RoninProducer<T> = createRoninProducer(
        topic,
        type,
        keyExtractor,
        DelegatingEventHeadersExtractor(
            idExtractor = idExtractor,
            typeExtractor = typeExtractor,
            schemaExtractor = schemaExtractor,
            sourceExtractor = sourceExtractor,
            timeExtractor = timeExtractor,
            subjectExtractor = subjectExtractor,
        )
    )

    fun <T : Any> createRoninProducer(
        topic: String,
        type: KClass<T>,
        eventType: String,
        eventSource: String,
        schema: String,
        keyExtractor: KeyExtractor<T>
    ) = createRoninProducer(
        topic = topic,
        type = type,
        keyExtractor = keyExtractor,
        typeExtractor = StaticValueExtractor(eventType),
        sourceExtractor = StaticValueExtractor(eventSource),
        schemaExtractor = StaticValueExtractor(schema),
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

class RoninProducer<T : Any>(
    private val topic: String,
    private val producer: KafkaProducer<String, T>,
    private val keyExtractor: KeyExtractor<T>,
    private val headersExtractor: HeadersExtractor<T>
) : AutoCloseable {
    fun send(event: T): Future<RecordMetadata> {
        val record = ProducerRecord<String, T>(
            topic,
            null,
            // TODO: is it reasonable to assume the key is always in the event (or that the user wants it to be null)??
            keyExtractor.determineKey(event), event, headersExtractor.determineHeaders(event).toHeaders()
        )
        return producer.send(record)
    }

    // TODO: how crazy would it be to let the user also just send a raw (but typed!) ProducerRecord?
    // That's an easy way to bypass key and header and whatnot quirks if they need to do something custom-ish without
    // dropping all the way to RoninProducer. But... on the other side of that coin, the factory should probably be capable
    // of just giving you a raw KafkaProducer<String, T> so this isn't needed.
    // fun send(ProducerRecord<String, T>) { ... }

    // TOOD: how bad of idea is this? Like how high volume of service can you get away with this before this is a
    // really, really stupid idea?
    fun sendSync(event: T): RecordMetadata {
        // KafkaProducer can choose to buffer messages, so we could wait forever if we just did future.get() without flushing
        // the downside is that flush() waits on _all_ pending messages to write, not just this one
        var future = send(event)
        flush()
        return future.get()
    }

    fun flush() = producer.flush()

    override fun close() = producer.close()
}

// This is an example of a medium amount of an opinionated option. Everything is still fairly manual, like specifying
// the key, building the RoninEvent, etc, but it's limited to a single type and topic.
class RoninEventProducer<T : Any>(
    private val topic: String,
    private val producer: KafkaProducer<String, T>,
) {
    fun send(key: String, event: RoninEvent<T>): Future<RecordMetadata> {
        val record = ProducerRecord<String, T>(
            topic, null, null, key, event.data, determineHeaders(event)
        )
        return producer.send(record)
    }

    fun sendSync(key: String, event: RoninEvent<T>): RecordMetadata {
        // KafkaProducer can choose to buffer messages, so we could wait forever if we just did future.get() without flushing
        // the downside is that flush() waits on _all_ pending messages to write, not just this one
        var future = send(key, event)
        flush()
        return future.get()
    }

    fun flush() = producer.flush()

    private fun determineHeaders(event: RoninEvent<T>) = listOf(
        StringHeader(KafkaHeaders.id, event.id),
        StringHeader(KafkaHeaders.source, event.source),
        StringHeader(KafkaHeaders.specVersion, event.specVersion),
        StringHeader(KafkaHeaders.type, event.type),
        StringHeader(KafkaHeaders.contentType, event.dataContentType),
        StringHeader(KafkaHeaders.dataSchema, event.dataSchema),
        StringHeader(KafkaHeaders.time, DateTimeFormatter.ISO_INSTANT.format(event.time)),
    )
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

interface KeyExtractor<T> {
    fun determineKey(event: T): String?
}

interface HeadersExtractor<T> {
    fun determineHeaders(value: T): RoninEventHeaders
}

// TODO: this should probably be a different interface for each header instead of 1, e.g. IdHeaderExtract.determineId
interface HeaderExtractor<H, T> {
    fun determineHeader(event: T): H
}

class HardcodedHeader<H, T>(
    private val value: H
) : HeaderExtractor<H, T> {
    override fun determineHeader(event: T) = value
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
