package com.projectronin.kafka.config

import org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
import org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.common.record.CompressionType.SNAPPY
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.TimeUnit

/**
 * Convenience class to store Ronin preferred KafkaProducer configurations as well as providing simple configuration
 * and testability of [com.projectronin.kafka.RoninProducer]
 *
 * Note: The following should be considered required to be set for RoninProducer usage:
 *
 *  * bootstrap.servers
 *
 */
@Deprecated("Library has been replaced by ronin-common kafka")
class RoninProducerKafkaProperties(vararg configs: Pair<String, *>) {
    companion object {
        private const val MB = 1024 * 1024
    }

    val properties: Properties by lazy {
        Properties()
            .apply {
                put("$KEY_SERIALIZER_CLASS_CONFIG.encoding", StandardCharsets.UTF_8.name())
                put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
                put("$VALUE_SERIALIZER_CLASS_CONFIG.encoding", StandardCharsets.UTF_8.name())
                put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.qualifiedName)
                put(ACKS_CONFIG, "all") // kafka default of all
                put(ENABLE_IDEMPOTENCE_CONFIG, false)
                put(RETRIES_CONFIG, 3)
                put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1) // kafka default is 5
                put(BUFFER_MEMORY_CONFIG, 32L * MB) // kafka default of 32MB
                put(MAX_BLOCK_MS_CONFIG, TimeUnit.MINUTES.toMillis(1))
                put(LINGER_MS_CONFIG, 5L)
                put(BATCH_SIZE_CONFIG, 16 * MB) // kafka default of 16MB
                put(COMPRESSION_TYPE_CONFIG, SNAPPY.name) // kafka default is "none"
                putAll(configs)
            }
    }
}
