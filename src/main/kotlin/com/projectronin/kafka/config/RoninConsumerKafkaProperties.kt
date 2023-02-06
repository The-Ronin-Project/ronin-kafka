package com.projectronin.kafka.config

import com.projectronin.kafka.serde.RoninEventDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.common.serialization.StringDeserializer
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
                put("$KEY_DESERIALIZER_CLASS_CONFIG.encoding", StandardCharsets.UTF_8.name())
                put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
                put("$VALUE_DESERIALIZER_CLASS_CONFIG.encoding", StandardCharsets.UTF_8.name())
                put(VALUE_DESERIALIZER_CLASS_CONFIG, RoninEventDeserializer::class.qualifiedName)

                put(ENABLE_AUTO_COMMIT_CONFIG, false)
                put(AUTO_OFFSET_RESET_CONFIG, "earliest") // kafka's default is "latest"

                // kafka default of 500... set to 1 to only fetch a single record from kafka at a time
                put(MAX_POLL_RECORDS_CONFIG, 500)
                put(FETCH_MAX_WAIT_MS_CONFIG, TimeUnit.SECONDS.toMillis(1).toInt()) // kafka default is 500ms
                put(FETCH_MIN_BYTES_CONFIG, 1) // kafka default of 1

                put(SESSION_TIMEOUT_MS_CONFIG, TimeUnit.SECONDS.toMillis(45).toInt()) // kafka default of 45 sec
                put(HEARTBEAT_INTERVAL_MS_CONFIG, TimeUnit.SECONDS.toMillis(3).toInt()) // kafka default of 3 sec

                /**
                 * Number of times [com.projectronin.kafka.RoninConsumer] will re-send an event resulting in
                 * a [com.projectronin.kafka.data.RoninEventResult.TRANSIENT_FAILURE] to the handler lambda
                 */
                put("ronin.handler.transient.retries", 3)

                putAll(configs)
            }
    }
}
