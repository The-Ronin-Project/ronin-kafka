package com.projectronin.kafka.handlers

import com.projectronin.kafka.config.KafkaConfigurator
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.ByteArraySerializer

@Deprecated("Library has been replaced by ronin-common kafka")
object DeadLetterProducer {
    private var producer: KafkaProducer<ByteArray, ByteArray>? = null

    fun producer(configs: MutableMap<String, *>?): KafkaProducer<ByteArray, ByteArray> {
        if (producer == null) {
            val dlqConfigs = KafkaConfigurator.builder(KafkaConfigurator.ClientType.PRODUCER, configs)
                .withKeySerializer(ByteArraySerializer::class)
                .withValueSerializer(ByteArraySerializer::class)
                .configs()
            producer = KafkaProducer(dlqConfigs)
        }
        return producer!!
    }
}
