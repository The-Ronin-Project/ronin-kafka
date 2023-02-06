package com.projectronin.kafka

import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.extensions.ProducerSubjectKeyInterceptor
import com.projectronin.kafka.serde.RoninEventDeserializer.Companion.RONIN_EVENT_DESERIALIZATION_TYPES_CONFIG
import com.projectronin.kafka.serde.RoninEventSerde
import mu.KLogger
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig.INTERCEPTOR_CLASSES_CONFIG
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG
import org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG
import org.apache.kafka.streams.StreamsConfig.PRODUCER_PREFIX
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.errors.DeserializationExceptionHandler
import org.apache.kafka.streams.kstream.ForeachAction
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.processor.ProcessorContext
import java.util.Properties
import kotlin.reflect.KClass

val logger: KLogger = KotlinLogging.logger { }

class RoninEventHandler private constructor(
    val topics: List<String>,
    val properties: Properties,
    private val handler: ForeachAction<String?, RoninEvent<*>>,
) {
    val streams: KafkaStreams
    val topology: Topology

    init {
        val builder = StreamsBuilder()
        val eventStream: KStream<String, RoninEvent<*>> = builder.stream(topics)

        eventStream.peek(handler)

        topology = builder.build()
        streams = KafkaStreams(topology, properties)
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
    }

    data class Builder(
        private var bootstrapServers: String? = null,
        private var applicationId: String? = null,
        private val topics: ArrayList<String> = ArrayList(),
        private val typeMap: HashMap<String, KClass<out Any>> = HashMap(),
        private val properties: Properties = defaultProperties(),
        private var handler: ForeachAction<String?, RoninEvent<*>> = defaultHandler
    ) {
        fun fromKafka(servers: String) = apply { this.bootstrapServers = servers }
        fun asApplication(applicationId: String) = apply { this.applicationId = applicationId }
        fun fromTopics(topics: List<String>) = apply { this.topics.addAll(topics) }
        fun withType(eventType: String, dataClass: KClass<out Any>) = apply { typeMap[eventType] = dataClass }
        fun withProperty(key: String, value: String) = apply { this.properties[key] = value }
        fun withProperties(properties: Properties) = apply { this.properties.putAll(properties) }
        fun withHandler(handler: ForeachAction<String?, RoninEvent<*>>) = apply { this.handler = handler }
        fun withDeserializationExceptionHandler(handlerClass: KClass<out Any>) =
            apply { properties[DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] = handlerClass.java.name }

        fun build(): RoninEventHandler {
            if (this.bootstrapServers == null) throw Exception("Bootstrap server[s] is required.")
            if (this.applicationId == null) throw Exception("Application ID is required.")
            if (this.topics.isEmpty()) throw Exception("At least one topic must be specified")

            properties[BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
            properties[APPLICATION_ID_CONFIG] = applicationId
            properties[RONIN_EVENT_DESERIALIZATION_TYPES_CONFIG] = typeMap.toKafkaPropertyString()

            return RoninEventHandler(
                topics = this.topics, properties = this.properties, handler = this.handler
            )
        }
    }

    fun start() {
        logger.info { "before stream start" }
        streams.start()
        logger.info { "after stream start" }
    }

    companion object {
        fun defaultProperties(): Properties {
            val properties = Properties()
            properties.putAll(
                mapOf(
                    DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.StringSerde::class.java.name,
                    DEFAULT_VALUE_SERDE_CLASS_CONFIG to RoninEventSerde::class.java.name,
                    "$PRODUCER_PREFIX$INTERCEPTOR_CLASSES_CONFIG" to ProducerSubjectKeyInterceptor::class.qualifiedName,
                    // METRIC_REPORTER_CLASSES_CONFIG to "com.projectronin...."
                    // DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG to "com.projectronin...."
                )
            )
            return properties
        }

        val defaultHandler: ForeachAction<String?, RoninEvent<*>> =
            ForeachAction { k, v -> logger.error("Message Handler not configured. Skipping message $k:$v.id") }
    }

    class SkipItDeserializationExceptionHandler : DeserializationExceptionHandler {
        override fun configure(configs: MutableMap<String, *>?) { /* Implement as needed */ }

        override fun handle(
            context: ProcessorContext?,
            record: ConsumerRecord<ByteArray, ByteArray>?,
            exception: Exception?
        ): DeserializationExceptionHandler.DeserializationHandlerResponse {
            logger.error { "Exception (${exception?.message}) deserializing topic ${context?.topic()} offset ${context?.offset()}" }
            return DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE
        }
    }
}

fun Map<String, KClass<out Any>>.toKafkaPropertyString(): String {
    return this.entries.joinToString(",", transform = { "${it.key}:${it.value.java.name}" })
}
