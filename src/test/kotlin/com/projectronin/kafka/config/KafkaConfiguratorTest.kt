package com.projectronin.kafka.config

import com.projectronin.kafka.config.RoninConfig.Companion.RONIN_DESERIALIZATION_TYPES_CONFIG
import com.projectronin.kafka.serde.RoninEventSerde
import com.projectronin.kafka.serde.RoninEventSerializer
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.DeserializationExceptionHandler
import org.apache.kafka.streams.errors.ProductionExceptionHandler
import org.apache.kafka.streams.processor.ProcessorContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class KafkaConfiguratorTest {
    private data class Stuff(val id: String)
    private data class Foo(val id: String)

    @Test
    fun `Cluster Configs SASL`() {
        val clusterConfig = stubClusterConfig(
            bootstrapServers = "bootstrap",
            saslUsername = "user",
            saslPassword = "pass"
        )

        val configs = KafkaConfigurator
            .builder(KafkaConfigurator.ClientType.CONSUMER, clusterConfig)
            .configs()

        assertEquals("bootstrap", configs[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG])
        assertEquals("SASL_SSL", configs[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG])
        assertEquals("SCRAM-SHA-512", configs[SaslConfigs.SASL_MECHANISM])
        assertEquals(
            "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"pass\";",
            configs[SaslConfigs.SASL_JAAS_CONFIG]
        )
    }

    @Test
    fun `Cluster Configs PLAINTEXT`() {
        val clusterConfig = stubClusterConfig(
            bootstrapServers = "bootstrap",
            securityProtocol = "PLAINTEXT"
        )

        val configs = KafkaConfigurator
            .builder(KafkaConfigurator.ClientType.CONSUMER, clusterConfig)
            .configs()

        assertEquals("bootstrap", configs[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG])
        assertEquals("PLAINTEXT", configs[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG])
        assertEquals(false, configs.containsKey(SaslConfigs.SASL_MECHANISM))
        assertEquals(false, configs.containsKey(SaslConfigs.SASL_JAAS_CONFIG))
    }

    @Test
    fun `Consumer default config`() {
        val clusterConfig = stubClusterConfig(bootstrapServers = "bootstrap", securityProtocol = "PLAINTEXT")
        val configs = KafkaConfigurator
            .builder(KafkaConfigurator.ClientType.CONSUMER, clusterConfig)
            .withDeserializationType("stuff", Stuff::class)
            .withDeserializationType("foo", Foo::class)
            .configs()

        assertEquals(
            "foo:com.projectronin.kafka.config.KafkaConfiguratorTest.Foo,stuff:" +
                "com.projectronin.kafka.config.KafkaConfiguratorTest.Stuff",
            configs[RONIN_DESERIALIZATION_TYPES_CONFIG]
        )
        assertEquals(false, configs[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        assertEquals("earliest", configs[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG])
        assertEquals(TimeUnit.SECONDS.toMillis(1).toInt(), configs[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG])
    }

    @Test
    fun `Producer default config`() {
        val clusterConfig = stubClusterConfig(bootstrapServers = "bootstrap", securityProtocol = "PLAINTEXT")
        val configs = KafkaConfigurator
            .builder(KafkaConfigurator.ClientType.PRODUCER, clusterConfig)
            .withKeySerializer(StringSerializer::class)
            .withValueSerializer(RoninEventSerializer::class)
            .configs()

        assertEquals("all", configs[ProducerConfig.ACKS_CONFIG])
        assertEquals(false, configs[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG])
        assertEquals(3, configs[ProducerConfig.RETRIES_CONFIG])
        assertEquals(1, configs[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION])
        assertEquals(TimeUnit.MINUTES.toMillis(1), configs[ProducerConfig.MAX_BLOCK_MS_CONFIG])
        assertEquals(5, configs[ProducerConfig.LINGER_MS_CONFIG])
        assertEquals(CompressionType.SNAPPY.name, configs[ProducerConfig.COMPRESSION_TYPE_CONFIG])
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG])
        assertEquals("com.projectronin.kafka.serde.RoninEventSerializer", configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG])
    }

    @Test
    fun `Stream default config`() {
        val clusterConfig = stubClusterConfig(bootstrapServers = "bootstrap", securityProtocol = "PLAINTEXT")
        val configs = KafkaConfigurator
            .builder(KafkaConfigurator.ClientType.STREAMS, clusterConfig)
            .withDeserializationExceptionHandler(TestDeserializationExceptionHandler::class)
            .configs()

        assertEquals("org.apache.kafka.common.serialization.Serdes\$StringSerde", configs[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG])
        assertEquals(RoninEventSerde::class.qualifiedName, configs[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG])
        assertEquals("com.projectronin.kafka.config.TestDeserializationExceptionHandler", configs[StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG])
    }

    @Test
    fun `Configure with config map`() {
        val configs = KafkaConfigurator.builder(
            KafkaConfigurator.ClientType.PRODUCER,
            mapOf(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "bootstrapServers",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "securityProtocol",
                SaslConfigs.SASL_MECHANISM to "saslMechanism",
                SaslConfigs.SASL_JAAS_CONFIG to "saslJaasConfig"
            )
        ).configs()
        assertEquals("bootstrapServers", configs[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG])
        assertEquals("securityProtocol", configs[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG])
        assertEquals("saslMechanism", configs[SaslConfigs.SASL_MECHANISM])
        assertEquals("saslJaasConfig", configs[SaslConfigs.SASL_JAAS_CONFIG])

        val consumer = KafkaConfigurator.builder(KafkaConfigurator.ClientType.CONSUMER, mapOf("" to "")).configs()
        assertEquals(false, consumer[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        val stream = KafkaConfigurator.builder(KafkaConfigurator.ClientType.STREAMS, mapOf("" to "")).configs()
        assertEquals("org.apache.kafka.common.serialization.Serdes\$StringSerde", stream[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG])
    }

    @Test
    fun `Configurator helper methods set configs`() {
        val clusterConfig = stubClusterConfig(
            bootstrapServers = "bootstrap"
        )
        val configurator = KafkaConfigurator
            .builder(KafkaConfigurator.ClientType.CONSUMER, clusterConfig)

        assertEquals("other", configurator.withConfig("bootstrapServers", "other").configs()["bootstrapServers"])
        assertEquals(
            "org.apache.kafka.common.serialization.Serializer",
            configurator.withKeySerializer(Serializer::class).configs()[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG]
        )
        assertEquals(
            "org.apache.kafka.common.serialization.Serializer",
            configurator.withValueSerializer(Serializer::class).configs()[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG]
        )
        assertEquals(
            "org.apache.kafka.common.serialization.Deserializer",
            configurator.withKeyDeserializer(Deserializer::class)
                .configs()[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG]
        )
        assertEquals(
            "org.apache.kafka.common.serialization.Deserializer",
            configurator.withValueDeserializer(Deserializer::class)
                .configs()[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG]
        )
        assertEquals(
            "org.apache.kafka.common.serialization.Serde",
            configurator.withKeySerde(Serde::class).configs()[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG]
        )
        assertEquals(
            "org.apache.kafka.common.serialization.Serde",
            configurator.withValueSerde(Serde::class).configs()[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG]
        )
        assertEquals(
            "com.projectronin.kafka.config.KafkaConfiguratorTest",
            configurator.withDeserializationType("type", this::class).typeMap["type"]
        )
        assertEquals("appId", configurator.withApplicationId("appId").configs()[StreamsConfig.APPLICATION_ID_CONFIG])
        assertEquals("com.projectronin.kafka.config.TestDeserializationExceptionHandler", configurator.withDeserializationExceptionHandler(TestDeserializationExceptionHandler::class).configs()[StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG])
        assertEquals("com.projectronin.kafka.config.TestProductionExceptionHandler", configurator.withProductionExceptionHandler(TestProductionExceptionHandler::class).configs()[StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG])
    }

    class stubClusterConfig(
        override val bootstrapServers: String,
        override val securityProtocol: String? = null,
        override val saslMechanism: String? = null,
        override val saslJaasConfig: String? = null,
        override val saslUsername: String? = null,
        override val saslPassword: String? = null
    ) : ClusterConfiguration
}

class TestDeserializationExceptionHandler : DeserializationExceptionHandler {
    override fun configure(configs: MutableMap<String, *>?) {}

    override fun handle(
        context: ProcessorContext?,
        record: ConsumerRecord<ByteArray, ByteArray>?,
        exception: Exception?
    ): DeserializationExceptionHandler.DeserializationHandlerResponse {
        return DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE
    }
}

class TestProductionExceptionHandler : ProductionExceptionHandler {
    override fun configure(configs: MutableMap<String, *>?) {}

    override fun handle(
        record: ProducerRecord<ByteArray, ByteArray>?,
        exception: Exception?
    ): ProductionExceptionHandler.ProductionExceptionHandlerResponse {
        return ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE
    }
}
