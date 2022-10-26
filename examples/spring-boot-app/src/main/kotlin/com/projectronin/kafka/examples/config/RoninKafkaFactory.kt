package com.projectronin.kafka.examples.config

import com.projectronin.kafka.RoninConsumer
import com.projectronin.kafka.RoninProducer
import com.projectronin.kafka.config.RoninConsumerKafkaProperties
import com.projectronin.kafka.config.RoninProducerKafkaProperties
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.examples.data.Van
import com.projectronin.kafka.examples.data.Wing
import com.projectronin.kafka.exceptions.ConsumerExceptionHandler
import io.micrometer.core.instrument.MeterRegistry
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class RoninKafkaFactory(
    @Value("\${kafka.bootstrap.servers}") val servers: String,
    @Value("\${kafka.properties.security.protocol}") val securityProtocol: String,
    @Value("\${kafka.properties.sasl.mechanism}") val saslMechanism: String,
    @Value("\${kafka.properties.sasl.jaas.config}") val saslJaasConfig: String,
) {
    @Bean
    fun roninKafkaConsumer(
        @Value("\${listen.topic}") topic: String,
        @Value("\${listen.group}") group: String,
        meterRegistry: MeterRegistry,
    ) =
        RoninConsumer(
            topics = listOf(topic),
            typeMap = mapOf(
                "van.cruising" to Van::class,
                "wing.flown" to Wing::class
            ),
            kafkaProperties = RoninConsumerKafkaProperties(
                "bootstrap.servers" to servers,
                "group.id" to group,
                "security.protocol" to securityProtocol,
                "sasl.mechanism" to saslMechanism,
                "sasl.jaas.config" to saslJaasConfig,
            ),
            exceptionHandler = object : ConsumerExceptionHandler {
                private val logger = KotlinLogging.logger {}

                override fun recordHandlingException(record: ConsumerRecord<String, ByteArray>, t: Throwable) {
                    logger.error(t) { "Failed to parse kafka record into a RoninEvent! - $record" }
                    // do something useful with the record
                }

                override fun eventProcessingException(events: List<RoninEvent<*>>, t: Throwable) {
                    logger.error(t) { "Unhandled exception while processing events!" }
                    // do something useful with the event(s). Dead letter queue?
                }
            },
            meterRegistry = meterRegistry,
        )

    @Bean
    fun roninKafkaProducer(
        @Value("\${send.topic}") topic: String,
        meterRegistry: MeterRegistry,
    ) =
        RoninProducer(
            topic = topic,
            source = "spring-boot-app",
            dataSchema = "https://schema",
            kafkaProperties = RoninProducerKafkaProperties(
                "bootstrap.servers" to servers,
                "security.protocol" to securityProtocol,
                "sasl.mechanism" to saslMechanism,
                "sasl.jaas.config" to saslJaasConfig,
            ),
            meterRegistry = meterRegistry
        )
}
