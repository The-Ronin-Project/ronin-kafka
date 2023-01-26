package com.projectronin.kafka.strawman.producer

import com.projectronin.kafka.config.RoninProducerKafkaProperties
import com.projectronin.kafka.data.RoninEvent
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Instant
import kotlin.reflect.KClass

class RoninKafkaProducerFactory(
    private val config: RoninProducerKafkaProperties,
    // TODO: can't decide if this should take a serializer factory, or if the consumer should have to provider a serializer
    // or if it should just default to a WhateverSerializer instance or what. A lot more complicated on the consumer side
    // where consumer deserializers require config and can't just handle every type.
    // TODO: also torn on whether this should be passed along programmatically in the KafkaProducer constructor or sent
    // along with the config properties. I much prefer to keep things as much as code as possible where I can't typo a
    // class name and don't have to deal with half a kilobyte of configuration, but... I'm not sure what the typical approach
    // would be in a Kafka world. People familiar with Kafka may expect to be setting value.serializer and
    // value.serializer.types config or something.
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

