package com.projectronin.kafka.config

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
class RoninProducerKafkaProperties(vararg configs: Pair<String, *>) {
    val properties: Properties by lazy {
        Properties()
            .apply {
                put("key.serializer.encoding", StandardCharsets.UTF_8.name())
                put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                put("value.serializer.encoding", StandardCharsets.UTF_8.name())
                put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                put("acks", "1") // 1 --> written to the leader, but not yet written to all followers
                put("enable.idempotent", false)
                put("retries", 3)
                put("max.in.flight.requests.per.connection", 1)
                put("buffer.memory", 32L * 1024L) // kafka default of 32MB
                put("max.block.ms", TimeUnit.MINUTES.toMillis(1))
                put("linger.ms", 5L)
                put("batch.size", 16 * 1024) // kafka default of 16MB
                put("compression.type", "snappy")
                putAll(configs)
            }
    }
}
