package com.projectronin.kafka.work

import com.projectronin.kafka.data.KafkaHeaders
import org.apache.kafka.clients.consumer.ConsumerRecord

fun ConsumerRecord<String, ByteArray>.getHeaderMap(): Map<String, String> {
    return headers()
        .filter { it.value() != null && it.value().isNotEmpty() }
        .associate { it.key() to it.value().decodeToString() }
}

/**
 * Return the value for "KafkaHeaders.type"
 *   or return a literal string of "null" if not found
 */
fun ConsumerRecord<String, ByteArray>.getHeaderTypeValue(): String {
    return this.getHeaderMap()[KafkaHeaders.type] ?: "null"
}
