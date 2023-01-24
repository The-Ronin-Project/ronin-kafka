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
import org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG
import org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM
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
                BOOTSTRAP_SERVERS_CONFIG to servers,
                GROUP_ID_CONFIG to group,
                SECURITY_PROTOCOL_CONFIG to securityProtocol,
                SASL_MECHANISM to saslMechanism,
                SASL_JAAS_CONFIG to saslJaasConfig,
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
                BOOTSTRAP_SERVERS_CONFIG to servers,
                SECURITY_PROTOCOL_CONFIG to securityProtocol,
                SASL_MECHANISM to saslMechanism,
                SASL_JAAS_CONFIG to saslJaasConfig,
            ),
            meterRegistry = meterRegistry
        )
}
