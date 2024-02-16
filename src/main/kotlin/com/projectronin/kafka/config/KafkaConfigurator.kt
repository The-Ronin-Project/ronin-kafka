package com.projectronin.kafka.config

import com.projectronin.kafka.serde.RoninEventDeserializer
import com.projectronin.kafka.serde.RoninEventSerde
import com.projectronin.kafka.serde.RoninEventSerializer
import org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG
import org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.DeserializationExceptionHandler
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler
import org.apache.kafka.streams.errors.ProductionExceptionHandler
import java.util.concurrent.TimeUnit
import kotlin.reflect.KClass

@Deprecated("Library has been replaced by ronin-common kafka")
class KafkaConfigurator private constructor(
    private val configMap: MutableMap<String, Any?>
) {
    val topicMap: MutableMap<String, String> = HashMap()
    val typeMap: MutableMap<String, String> = HashMap()

    fun configs(): Map<String, Any?> {
        if (typeMap.isNotEmpty()) {
            configMap[RoninConfig.RONIN_DESERIALIZATION_TYPES_CONFIG] =
                typeMap.map { "${it.key}:${it.value}" }.joinToString(",")
        }
        if (topicMap.isNotEmpty()) {
            configMap[RoninConfig.RONIN_DESERIALIZATION_TOPICS_CONFIG] =
                topicMap.map { "${it.key}:${it.value}" }.joinToString(",")
        }
        return configMap
    }

    fun withConfig(key: String, value: Any?): KafkaConfigurator {
        configMap[key] = value
        return this
    }

    fun withKeySerializer(serializer: KClass<out Serializer<*>>): KafkaConfigurator {
        configMap[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = serializer.qualifiedName
        return this
    }

    fun withValueSerializer(serializer: KClass<out Serializer<*>>): KafkaConfigurator {
        configMap[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = serializer.qualifiedName
        return this
    }

    fun withKeySerializer(serializer: Class<out Serializer<*>>): KafkaConfigurator {
        configMap[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = serializer.name
        return this
    }

    fun withValueSerializer(serializer: Class<out Serializer<*>>): KafkaConfigurator {
        configMap[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = serializer.name
        return this
    }

    fun withKeyDeserializer(deserializer: KClass<out Deserializer<*>>): KafkaConfigurator {
        configMap[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = deserializer.qualifiedName
        return this
    }

    fun withValueDeserializer(deserializer: KClass<out Deserializer<*>>): KafkaConfigurator {
        configMap[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = deserializer.qualifiedName
        return this
    }

    fun withKeyDeserializer(deserializer: Class<out Deserializer<*>>): KafkaConfigurator {
        configMap[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = deserializer.name
        return this
    }

    fun withValueDeserializer(deserializer: Class<out Deserializer<*>>): KafkaConfigurator {
        configMap[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = deserializer.name
        return this
    }

    fun withKeySerde(serde: KClass<out Serde<*>>): KafkaConfigurator {
        configMap[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = serde.qualifiedName
        return this
    }

    fun withValueSerde(serde: KClass<out Serde<*>>): KafkaConfigurator {
        configMap[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = serde.qualifiedName
        return this
    }

    fun withKeySerde(serde: Class<out Serde<*>>): KafkaConfigurator {
        configMap[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = serde.name
        return this
    }

    fun withValueSerde(serde: Class<out Serde<*>>): KafkaConfigurator {
        configMap[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = serde.name
        return this
    }

    fun withDeserializationType(type: String, typeClass: KClass<*>): KafkaConfigurator {
        typeMap[type] = typeClass.qualifiedName!!
        return this
    }

    fun withDeserializationType(type: String, typeClass: Class<*>): KafkaConfigurator {
        typeMap[type] = typeClass.name
        return this
    }

    /**
     * Configures the deserializer to map all messages on the specified topic to an instance of the specified typeClass.
     */
    fun withDeserializationTopic(topic: String, typeClass: KClass<*>): KafkaConfigurator {
        topicMap[topic] = typeClass.qualifiedName!!
        return this
    }

    /**
     * Configures the deserializer to map all messages on the specified topic to an instance of the specified typeClass.
     */
    fun withDeserializationTopic(topic: String, typeClass: Class<*>): KafkaConfigurator {
        topicMap[topic] = typeClass.name
        return this
    }

    fun withApplicationId(applicationId: String): KafkaConfigurator {
        configMap[StreamsConfig.APPLICATION_ID_CONFIG] = applicationId
        return this
    }

    fun withDeserializationExceptionHandler(handler: KClass<out DeserializationExceptionHandler>): KafkaConfigurator {
        configMap[StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] = handler.qualifiedName
        return this
    }

    fun withProductionExceptionHandler(handler: KClass<out ProductionExceptionHandler>): KafkaConfigurator {
        configMap[StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG] = handler.qualifiedName
        return this
    }

    companion object {
        fun builder(type: ClientType, configs: Map<String, *>?): KafkaConfigurator {
            val defaults = mutableMapOf(
                BOOTSTRAP_SERVERS_CONFIG to configs?.get(BOOTSTRAP_SERVERS_CONFIG),
                SECURITY_PROTOCOL_CONFIG to configs?.get(SECURITY_PROTOCOL_CONFIG),
                SASL_MECHANISM to configs?.get(SASL_MECHANISM),
                SASL_JAAS_CONFIG to configs?.get(SASL_JAAS_CONFIG)
            )

            when (type) {
                ClientType.PRODUCER -> defaults.putAll(producerDefaults())
                ClientType.CONSUMER -> defaults.putAll(consumerDefaults())
                ClientType.STREAMS -> defaults.putAll(streamDefaults())
            }

            return KafkaConfigurator(defaults)
        }

        fun builder(type: ClientType, clusterConfig: ClusterConfiguration): KafkaConfigurator {
            val defaults: MutableMap<String, Any?> = mutableMapOf(
                BOOTSTRAP_SERVERS_CONFIG to clusterConfig.bootstrapServers,
                SECURITY_PROTOCOL_CONFIG to (clusterConfig.securityProtocol ?: "SASL_SSL")
            )

            if (defaults[SECURITY_PROTOCOL_CONFIG] == "SASL_SSL") {
                defaults.putAll(
                    mapOf(
                        SASL_MECHANISM to (clusterConfig.saslMechanism ?: "SCRAM-SHA-512"),
                        SASL_JAAS_CONFIG to (
                            clusterConfig.saslJaasConfig
                                ?: (
                                    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                                        "username=\"${clusterConfig.saslUsername}\" " +
                                        "password=\"${clusterConfig.saslPassword}\";"
                                    )
                            )
                    )
                )
            }

            when (type) {
                ClientType.PRODUCER -> defaults.putAll(producerDefaults())
                ClientType.CONSUMER -> defaults.putAll(consumerDefaults())
                ClientType.STREAMS -> defaults.putAll(streamDefaults())
            }

            return KafkaConfigurator(defaults)
        }

        private fun producerDefaults(): Map<String, Any?> {
            return mapOf(
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to false,
                ProducerConfig.RETRIES_CONFIG to 3,
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 1,
                ProducerConfig.MAX_BLOCK_MS_CONFIG to TimeUnit.MINUTES.toMillis(1),
                ProducerConfig.LINGER_MS_CONFIG to 5,
                ProducerConfig.COMPRESSION_TYPE_CONFIG to CompressionType.SNAPPY.name,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to RoninEventSerializer::class.qualifiedName
            )
        }

        private fun consumerDefaults(): Map<String, Any?> {
            return mapOf(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to RoninEventDeserializer::class.qualifiedName,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to TimeUnit.SECONDS.toMillis(1).toInt(),
            )
        }

        private fun streamDefaults(): Map<String, Any?> {
            return mapOf(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.StringSerde::class.java.name,
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to RoninEventSerde::class.qualifiedName,
                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG to
                    LogAndFailExceptionHandler::class.java.name
            )
        }
    }

    enum class ClientType {
        PRODUCER, CONSUMER, STREAMS
    }
}
