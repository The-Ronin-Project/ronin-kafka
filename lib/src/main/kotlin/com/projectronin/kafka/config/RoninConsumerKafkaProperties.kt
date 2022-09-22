package com.projectronin.kafka.config

import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.TimeUnit

/**
 * Convenience class to store Ronin preferred KafkaConsumer configurations as well as providing simple configuration
 * and testability of [com.projectronin.kafka.RoninConsumer]
 *
 * Note: The following should be considered required to be set for RoninConsumer usage:
 *
 *  * bootstrap.servers
 *  * group.id
 */
class RoninConsumerKafkaProperties(vararg configs: Pair<String, *>) {
    val properties: Properties by lazy {
        Properties()
            .apply {
                put("key.deserializer.encoding", StandardCharsets.UTF_8.name())
                put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                put("value.deserializer.encoding", StandardCharsets.UTF_8.name())
                put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

                put("enable.auto.commit", false)
                put("auto.offset.reset", "earliest")

                // kafka default of 500... set to 1 to only fetch a single record from kafka at a time
                put("max.poll.records", 500)
                put("fetch.max.wait.ms", TimeUnit.SECONDS.toMillis(1).toInt()) // kafka default is 500ms
                put("fetch.min.bytes", 1) // kafka default of 1

                put("session.timeout.ms", TimeUnit.SECONDS.toMillis(45).toInt()) // kafka default of 45 sec
                put("heartbeat.interval.ms", TimeUnit.SECONDS.toMillis(3).toInt()) // kafka default of 3 sec

                /**
                 * Number of times [com.projectronin.kafka.RoninConsumer] will re-send an event resulting in
                 * a [com.projectronin.kafka.data.RoninEventResult.TRANSIENT_FAILURE] to the handler lambda
                 */
                put("ronin.handler.transient.retries", 3)

                putAll(configs)
            }
    }
}
